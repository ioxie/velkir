/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sentinel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ioxie/velkir/internal/resp"
)

// AuthSetTimeout caps a single per-pod SENTINEL SET + verify
// round-trip (dial + AUTH + SET + MASTER + parse). Sentinel
// processes both SET and MASTER locally with no network hop,
// so the budget bounds the dial + a couple of small request/
// reply pairs. A pod that exceeds it is presumed wedged; the
// orchestration moves on and emits SentinelAuthNotApplied.
const AuthSetTimeout = 5 * time.Second

// DefaultAuthRetryAttempts is the default per-pod max attempt count
// for the SET+verify round-trip. The first failure usually clears on
// the second attempt (transient TCP reset, sentinel mid-leader-
// election); beyond three the failure is structural (auth mismatch,
// sentinel wedged, network partition) and additional retries delay
// the SentinelAuthNotApplied event without changing the outcome.
// Override at runtime via Options.AuthRetryAttempts.
const DefaultAuthRetryAttempts = 3

// AuthRetryAttempts is retained for backward compatibility with
// callers and tests that referenced the prior constant name. New
// code should plumb Options.AuthRetryAttempts through the Manager.
const AuthRetryAttempts = DefaultAuthRetryAttempts

// authRetryBackoffs is the exponential backoff sequence applied
// between retries. AuthRetryAttempts attempts means
// len(authRetryBackoffs) sleeps; the i-th sleep precedes the
// (i+1)-th attempt. Total worst-case extra latency before the
// first event is 250ms + 500ms = 750ms — well under the per-
// reconcile budget.
var authRetryBackoffs = []time.Duration{
	250 * time.Millisecond,
	500 * time.Millisecond,
}

// AuthResult is one per-pod outcome from SetAuthPassAll. Name
// matches Endpoint.Name (used by the caller for per-pod event
// emission). Err is nil on success; non-nil errors are dial-time,
// AUTH, SET-reply, MASTER-reply, parse, or verification-mismatch
// failures that survived AuthRetryAttempts.
type AuthResult struct {
	Name string
	Err  error
}

// errAuthPassMismatch is the verification-failure sentinel
// returned from setAndVerifyOne when SENTINEL MASTER's auth-pass
// field does not echo what we just SET. Distinct from generic
// dial / wire errors so the caller can shape the event message
// with the structural cause; surfaced via errors.Is on the
// AuthResult.Err.
var errAuthPassMismatch = errors.New("auth-pass verify mismatch")

// SetAuthPassAll issues `SENTINEL SET <masterName> auth-pass
// <password>` against every endpoint in parallel, then verifies
// each by reading the auth-pass field back via `SENTINEL MASTER
// <masterName>`. Returns one AuthResult per endpoint in the same
// order. Never short-circuits — even if every dial fails, the
// slice has one entry per endpoint so the caller can emit one
// event per pod.
//
// password is the AUTH password for the sentinel client itself
// AND the value being propagated as the master's auth-pass —
// they are the same string by construction (the operator stores
// one Secret per CR; both the sentinel-client AUTH and the
// master auth-pass come from `auth.existingSecret`). When
// password is empty, the function returns an empty []AuthResult
// without any wire activity — no-AUTH CRs have nothing to
// propagate and verifying an empty auth-pass would always
// succeed trivially.
func SetAuthPassAll(ctx context.Context, endpoints []Endpoint, masterName, password string) []AuthResult {
	return setAuthPassAllWithAttempts(ctx, endpoints, masterName, password, DefaultAuthRetryAttempts)
}

// setAuthPassAllWithAttempts is the configurable variant — used by
// the Manager so Options.AuthRetryAttempts overrides the default
// without breaking the public SetAuthPassAll signature relied on by
// existing tests.
func setAuthPassAllWithAttempts(ctx context.Context, endpoints []Endpoint, masterName, password string, attempts int) []AuthResult {
	if password == "" || len(endpoints) == 0 {
		return nil
	}
	if attempts < 1 {
		attempts = DefaultAuthRetryAttempts
	}
	results := make([]AuthResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			results[i] = AuthResult{
				Name: ep.Name,
				Err:  setAndVerifyWithRetry(ctx, ep, masterName, password, attempts),
			}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// setAndVerifyWithRetry runs setAndVerifyOne up to `attempts` times,
// sleeping authRetryBackoffs[i] between attempts (clamped to the
// last entry once i exceeds the slice's length so configured
// attempts > default 3 still produce a bounded backoff sequence).
// Returns the final attempt's error (nil on any success). Aborts
// the loop early when ctx is cancelled.
func setAndVerifyWithRetry(ctx context.Context, ep Endpoint, masterName, password string, attempts int) error {
	var lastErr error
	for attempt := range attempts {
		if attempt > 0 {
			idx := attempt - 1
			if idx >= len(authRetryBackoffs) {
				idx = len(authRetryBackoffs) - 1
			}
			backoff := authRetryBackoffs[idx]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		if err := setAndVerifyOne(ctx, ep, masterName, password); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// setAndVerifyOne dials, authenticates, issues `SENTINEL SET
// <masterName> auth-pass <password>`, then reads back the
// auth-pass field via `SENTINEL MASTER <masterName>` and asserts
// it matches what we just sent. Returns nil on success; on
// verification mismatch returns a wrapped errAuthPassMismatch.
func setAndVerifyOne(ctx context.Context, ep Endpoint, masterName, password string) error {
	callCtx, cancel := context.WithTimeout(ctx, AuthSetTimeout)
	defer cancel()

	conn, err := dialSentinel(callCtx, ep.Addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			log.FromContext(ctx).V(1).Info("close conn failed", "err", cerr)
		}
	}()

	if dl, ok := callCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	rd := bufio.NewReaderSize(conn, readBufSize)

	if err := authIfNeeded(conn, rd, password); err != nil {
		return err
	}

	// SENTINEL SET <masterName> auth-pass <password>
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "SET", masterName, "auth-pass", password)); err != nil {
		return fmt.Errorf("write SENTINEL SET: %w", err)
	}
	setReply, err := readReply(rd)
	if err != nil {
		return fmt.Errorf("read SENTINEL SET: %w", err)
	}
	// SET reply on success is +OK (simple string).
	if s, ok := setReply.(string); !ok || s != "OK" {
		return fmt.Errorf("SENTINEL SET: unexpected reply %v", setReply)
	}

	// Verify by reading SENTINEL MASTER <masterName> back. The
	// reply is a flat key/value multi-bulk array; ParseSentinelMasterAuthPass
	// extracts the auth-pass value.
	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "MASTER", masterName)); err != nil {
		return fmt.Errorf("write SENTINEL MASTER: %w", err)
	}
	masterReply, err := readReply(rd)
	if err != nil {
		return fmt.Errorf("read SENTINEL MASTER: %w", err)
	}
	got, ok := ParseSentinelMasterAuthPass(masterReply)
	if !ok {
		// Distinguish malformed reply from well-formed reply
		// with auth-pass field absent. The latter happens on
		// Valkey/Redis versions that mask auth-pass in SENTINEL
		// MASTER output for security — the SET +OK above
		// confirms the value was accepted by this sentinel, so
		// trust it and skip the echo cross-check rather than
		// aborting the entire recovery path.
		if !isWellFormedMultiBulk(masterReply) {
			return fmt.Errorf("SENTINEL MASTER: malformed reply")
		}
		log.FromContext(ctx).V(1).Info(
			"SENTINEL MASTER auth-pass field omitted; trusting SET +OK",
			"endpoint", ep.Addr)
		return nil
	}
	if got != password {
		// Don't leak the password into the error string. The
		// mismatch sentinel is the actionable signal; attempt-
		// level diagnostics live in the per-pod event.
		return fmt.Errorf("verify: %w", errAuthPassMismatch)
	}
	return nil
}

// isWellFormedMultiBulk reports whether reply is a sentinel
// multi-bulk array (flat key/value pair sequence). Used to tell
// "Valkey version omits auth-pass" apart from "structurally
// malformed reply" without depending on which specific field
// the caller was looking for.
func isWellFormedMultiBulk(reply any) bool {
	arr, ok := reply.([]any)
	if !ok || len(arr) < 2 || len(arr)%2 != 0 {
		return false
	}
	return true
}
