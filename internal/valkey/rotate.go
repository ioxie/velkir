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

package valkey

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ioxie/velkir/internal/resp"
)

// ErrRewriteFailed marks a rotation result where CONFIG SET masterauth
// and CONFIG SET requirepass succeeded but CONFIG REWRITE did not. The
// pod's running auth has been updated; its on-disk valkey.conf has not.
// On restart the pod would revert to the previous on-disk credential.
// Callers (Phase 2 classifier in the reconciler) check via errors.Is
// to distinguish this in-memory-vs-on-disk inconsistency from a SET-
// time failure where neither layer was touched.
var ErrRewriteFailed = errors.New("CONFIG REWRITE failed")

// RotateTimeout caps a single per-pod rotation round-trip (dial +
// optional AUTH + 3 commands + 3 replies). Three short RESP round-
// trips on a local pod fit comfortably in 5s; a pod that exceeds
// the budget is presumed wedged and the per-pod retry / per-pod
// PodResult.Err lets the caller move on without stalling the
// whole rotation.
const RotateTimeout = 5 * time.Second

// RotateRetryAttempts is the per-pod max attempt count for the
// rotation round-trip. Mirrors internal/sentinel.AuthRetryAttempts:
// the first transient blip usually clears on the second attempt;
// beyond three the failure is structural (auth mismatch, pod
// wedged, network partition) and additional retries delay the
// outcome event without changing it.
//
// Coupled to len(rotateRetryBackoffs) — RotateRetryAttempts attempts
// produce RotateRetryAttempts-1 backoff sleeps, indexed
// [0..RotateRetryAttempts-2]. Bumping this constant requires
// extending rotateRetryBackoffs to match;
// TestRotateRetryAttemptsBackoffsCoupling pins the invariant.
const RotateRetryAttempts = 3

// rotateRetryBackoffs is the exponential backoff sequence applied
// between retries. RotateRetryAttempts attempts means
// len(rotateRetryBackoffs) sleeps; the i-th sleep precedes the
// (i+1)-th attempt. Total worst-case extra latency before the
// final result is 250ms + 500ms = 750ms.
//
// Length is pinned at RotateRetryAttempts-1 by
// TestRotateRetryAttemptsBackoffsCoupling — index access in
// rotateOneWithRetry would panic if these drift.
var rotateRetryBackoffs = []time.Duration{
	250 * time.Millisecond,
	500 * time.Millisecond,
}

// Endpoint identifies one valkey pod the rotator can talk to.
// Name is the pod's metadata.name (used as a Prometheus label and
// in events so an admin reading the timeline can map back to a
// specific pod); Addr is the dial target as host:port (built by
// the caller from pod.Status.PodIP + DefaultPort). Mirrors
// internal/sentinel.Endpoint so callers can build both lists from
// the same Pod-walk loop.
//
// Field-set is all-or-nothing: a fully zero-valued Endpoint signals
// "no master yet" (bootstrap-only replicas) and is silently skipped;
// any other shape must populate both Name and Addr. RotateAuth treats
// a partial-zero Endpoint (one field populated, the other empty) the
// same as fully zero — silently skipped — to avoid dialing against an
// empty Addr.
type Endpoint struct {
	Name string
	Addr string
}

// RotationPhase identifies which leg of the ordered rotation a
// PodResult belongs to. Surfaced so the caller can emit per-pod
// events that name which plane failed (replicas first, master last).
type RotationPhase string

const (
	// RotationPhaseReplica is set on PodResult entries produced by
	// the replica-plane leg. Replicas roll first so they keep
	// authenticating to master with the *old* masterauth until
	// master is rolled.
	RotationPhaseReplica RotationPhase = "replica"
	// RotationPhaseMaster is set on the single PodResult produced
	// by the master leg, which runs after every replica leg has
	// completed.
	RotationPhaseMaster RotationPhase = "master"
)

// PodResult is one per-pod outcome from RotateAuth. Sentinels are
// rotated separately by the caller via the existing
// internal/sentinel.Manager.IssueAuthPass surface; this type only
// covers the data-plane (valkey) leg.
type PodResult struct {
	// Endpoint is the pod the result corresponds to.
	Endpoint Endpoint
	// Phase is the leg of the ordered rotation this entry belongs
	// to. Replicas first; master last.
	Phase RotationPhase
	// Err is nil on success; non-nil errors are dial-time, AUTH,
	// CONFIG SET, or CONFIG REWRITE failures that survived
	// RotateRetryAttempts retries.
	Err error
}

// RotateAuth performs the ordered hot-rotation of the auth password
// across the valkey data plane. Replicas first (so they keep
// authenticating to master with the *old* masterauth until master
// rolls), then master.
//
// On each pod, the wire commands are:
//
//	AUTH <oldPassword>           (if oldPassword is non-empty)
//	CONFIG SET masterauth <new>
//	CONFIG SET requirepass <new>
//	CONFIG REWRITE
//
// Returns one PodResult per pod (every replica in input order, then
// the master) — even on dial failures, so the caller can emit one
// event per pod regardless of which step tripped. Within the
// replica plane, per-pod work runs in parallel; ordering is only
// enforced at the plane boundary (replicas finish, then master).
//
// Result-slice length: len(replicas) + 1 if the master Endpoint is
// fully populated, else len(replicas). A partial-zero or fully-zero
// master Endpoint produces no master entry — callers iterating per-
// result should NOT assume the slice has a master tail. Use Phase
// to identify replica vs master entries when emitting events.
//
// Per-pod retry: RotateRetryAttempts attempts with rotateRetryBackoffs
// between them. Per-call timeout: RotateTimeout. Sentinel-plane
// rotation is the caller's responsibility — it's already covered by
// internal/sentinel.Manager.IssueAuthPass. The reconciler stitches
// both halves.
//
// When oldPassword and newPassword are both empty, returns nil — the
// CR has no auth and there is nothing to rotate. An empty oldPassword
// with a non-empty newPassword is the "add auth" path (dial without
// AUTH; SET; REWRITE). A non-empty old with an empty new is the
// "remove auth" path; both shapes are exercised by the unit tests.
//
// Phase 1 (this commit) returns only the per-pod outcomes — partial-
// failure revert is Phase 2 territory. The caller is responsible for
// classifying the result slice into "all good", "some failed", or
// "all failed" and emitting the matching event(s).
func RotateAuth(ctx context.Context, replicas []Endpoint, master Endpoint, oldPassword, newPassword string) []PodResult {
	if oldPassword == "" && newPassword == "" {
		return nil
	}

	results := make([]PodResult, 0, len(replicas)+1)

	if len(replicas) > 0 {
		results = append(results, rotateMany(ctx, replicas, oldPassword, newPassword, RotationPhaseReplica)...)
	}

	if master.Name != "" && master.Addr != "" {
		results = append(results, PodResult{
			Endpoint: master,
			Phase:    RotationPhaseMaster,
			Err:      rotateOneWithRetry(ctx, master, oldPassword, newPassword),
		})
	}

	return results
}

// rotateMany fans out per-pod rotations in parallel within a single
// rotation plane. Mirrors the internal/sentinel.SetAuthPassAll shape:
// per-pod work is independent, so wall-clock latency is dominated by
// the slowest pod rather than summed across the plane.
func rotateMany(ctx context.Context, endpoints []Endpoint, oldPwd, newPwd string, phase RotationPhase) []PodResult {
	results := make([]PodResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			results[i] = PodResult{
				Endpoint: ep,
				Phase:    phase,
				Err:      rotateOneWithRetry(ctx, ep, oldPwd, newPwd),
			}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// rotateOneWithRetry runs rotateOne up to RotateRetryAttempts times,
// sleeping rotateRetryBackoffs[i] between attempts. Returns the
// final attempt's error (nil on any success). Aborts the loop early
// when ctx is cancelled.
func rotateOneWithRetry(ctx context.Context, ep Endpoint, oldPwd, newPwd string) error {
	var lastErr error
	for attempt := range RotateRetryAttempts {
		if attempt > 0 {
			backoff := rotateRetryBackoffs[attempt-1]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		if err := rotateOne(ctx, ep, oldPwd, newPwd, nil); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// rotateOne dials one valkey pod, optionally AUTHs with the old
// password, then issues CONFIG SET masterauth + CONFIG SET requirepass
// + CONFIG REWRITE. All four steps share one connection and one
// RotateTimeout deadline. dialer is the shared contextDialer seam
// (dial.go): nil in production (rotateOneWithRetry passes nil → a real
// net.Dialer), an in-memory fake under test.
func rotateOne(ctx context.Context, ep Endpoint, oldPwd, newPwd string, dialer contextDialer) error {
	callCtx, cancel := context.WithTimeout(ctx, RotateTimeout)
	defer cancel()

	// Shared dial + deadline preamble (dial.go). rotateOne carves its AUTH
	// tail into the caller: the two-password fallback below can't run
	// through the helper's single-password AUTH (which closes the
	// connection on the first AUTH failure), so dial with no password here
	// and AUTH below. callCtx carries the RotateTimeout deadline, so the
	// helper's clamp resolves to it — the ctx-deadline model is preserved.
	conn, rd, err := dialAndAuth(callCtx, ep.Addr, "", RotateTimeout, dialer)
	if err != nil {
		return err
	}
	defer closeConn(ctx, conn)

	if oldPwd != "" {
		if _, err := io.WriteString(conn, resp.EncodeCommand("AUTH", oldPwd)); err != nil {
			return fmt.Errorf("write AUTH: %w", err)
		}
		if _, err := readSimpleOrError(rd); err != nil {
			// AUTH with the cached old password failed — the pod is
			// already on the new credential (a prior rotation pass
			// updated the in-memory requirepass before CONFIG REWRITE
			// failed and the controller flipped to Failed, then the
			// next reconcile sees the hash mismatch and retries). Try
			// AUTH with newPwd: if it works, the in-memory state is
			// already the post-rotation state and the remaining
			// CONFIG SETs are idempotent no-ops, so the per-pod
			// rotation can complete cleanly. If newPwd also fails the
			// pod is in an unknown state and we surface the original
			// AUTH error so the caller classifies this as a real
			// failure. The fallback is gated on oldPwd != newPwd so a
			// caller passing the same password for both (initial-set
			// shape) preserves the original AUTH error.
			if newPwd == "" || newPwd == oldPwd {
				return fmt.Errorf("AUTH: %w", err)
			}
			if _, werr := io.WriteString(conn, resp.EncodeCommand("AUTH", newPwd)); werr != nil {
				return fmt.Errorf("write AUTH (newPwd fallback): %w", werr)
			}
			if _, rerr := readSimpleOrError(rd); rerr != nil {
				return fmt.Errorf("AUTH: %w", err)
			}
		}
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand("CONFIG", "SET", "masterauth", newPwd)); err != nil {
		return fmt.Errorf("write CONFIG SET masterauth: %w", err)
	}
	if _, err := readSimpleOrError(rd); err != nil {
		return fmt.Errorf("CONFIG SET masterauth: %w", err)
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand("CONFIG", "SET", "requirepass", newPwd)); err != nil {
		return fmt.Errorf("write CONFIG SET requirepass: %w", err)
	}
	if _, err := readSimpleOrError(rd); err != nil {
		return fmt.Errorf("CONFIG SET requirepass: %w", err)
	}

	if _, err := io.WriteString(conn, resp.EncodeCommand("CONFIG", "REWRITE")); err != nil {
		return fmt.Errorf("%w: write: %w", ErrRewriteFailed, err)
	}
	if _, err := readSimpleOrError(rd); err != nil {
		return fmt.Errorf("%w: %w", ErrRewriteFailed, err)
	}

	return nil
}
