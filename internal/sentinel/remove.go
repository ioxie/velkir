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
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ioxie/velkir/internal/resp"
)

// RemoveTimeout caps a single SENTINEL REMOVE round-trip per pod.
// Mirrors ResetTimeout — REMOVE is local synchronous state work,
// no fan-out happens in the command path.
const RemoveTimeout = 5 * time.Second

// RemoveResult is one per-pod outcome from RemoveAll. Name matches
// Endpoint.Name; Err is nil on success.
type RemoveResult struct {
	Name string
	Err  error
}

// RemoveAll issues `SENTINEL REMOVE <masterName>` against every
// endpoint in parallel, with a per-pod timeout. Returns one
// RemoveResult per endpoint in input order. REMOVE clears the
// master entry entirely (pointer, state, epoch, peer-list) — used
// by the stranded-sentinel recovery path before re-issuing
// MONITOR with a fresh master IP. Plain RESET alone keeps the
// master pointer at its prior (possibly stale) IP, so a follow-up
// MONITOR returns "-ERR Duplicated master name" and the rebuilt
// sentinel stays pointed at the stale master forever.
//
// password may be empty when the CR has no AUTH; authIfNeeded
// no-ops on empty password.
func RemoveAll(ctx context.Context, endpoints []Endpoint, masterName, password string) []RemoveResult {
	results := make([]RemoveResult, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()
			results[i] = RemoveResult{
				Name: ep.Name,
				Err:  removeOne(ctx, ep, masterName, password),
			}
		}(i, ep)
	}
	wg.Wait()
	return results
}

// removeOne dials, authenticates, and issues `SENTINEL REMOVE
// <masterName>` against one sentinel. Returns nil on success.
// Treats "ERR No such master with that name" as success — the
// caller's intent is "make sure the master entry is gone"; if
// it was already gone, that's the target state.
func removeOne(ctx context.Context, ep Endpoint, masterName, password string) error {
	callCtx, cancel := context.WithTimeout(ctx, RemoveTimeout)
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

	if _, err := io.WriteString(conn, resp.EncodeCommand("SENTINEL", "REMOVE", masterName)); err != nil {
		return fmt.Errorf("write SENTINEL REMOVE: %w", err)
	}
	reply, err := readReply(rd)
	if err != nil {
		// "No such master with that name" is the target state — the
		// rebuilt sentinel may already be free of the stale entry
		// (e.g., a prior recovery pass succeeded between probe and
		// REMOVE). Surface as success so the follow-up MONITOR runs.
		// Match Valkey/Redis canonical "ERR No such master with that
		// name" — tight enough to avoid swallowing unrelated errors
		// that happen to share the "no such master" substring.
		if strings.Contains(strings.ToLower(err.Error()), "no such master with that name") {
			return nil
		}
		return fmt.Errorf("read SENTINEL REMOVE: %w", err)
	}
	if s, ok := reply.(string); !ok || s != "OK" {
		return fmt.Errorf("SENTINEL REMOVE: unexpected reply %v", reply)
	}
	return nil
}
