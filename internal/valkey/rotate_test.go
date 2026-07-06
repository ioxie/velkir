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
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// startFakeRotatePod accepts one rotation client connection on a
// random localhost port. Validates the AUTH (when expectPassword is
// non-empty) and the four CONFIG SET / REWRITE commands carrying
// newPassword. Replies +OK to each. Returns the listener address.
//
// If failNth >= 1, the goroutine drops the connection mid-handshake
// after handling (failNth - 1) successful commands — tests use this
// to simulate transient failures that the per-pod retry should
// recover from on a second connection.
//
// The fixture self-cleans on test exit via t.Cleanup.
func startFakeRotatePod(t *testing.T, expectPassword, newPassword string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleRotatePod(conn, expectPassword, newPassword)
		}
	}()

	return ln.Addr().String()
}

// handleRotatePod runs the +OK protocol on one accepted connection.
// Closes the connection when done. Errors are silent — test fails
// via the higher-level assertions if the wire shape is wrong.
func handleRotatePod(conn net.Conn, expectPassword, newPassword string) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	rd := bufio.NewReader(conn)

	if expectPassword != "" {
		if !readArrayCommand(rd, "AUTH", expectPassword) {
			_, _ = io.WriteString(conn, "-ERR auth\r\n")
			return
		}
		_, _ = io.WriteString(conn, "+OK\r\n")
	}

	if !readArrayCommand(rd, "CONFIG", "SET", "masterauth", newPassword) {
		_, _ = io.WriteString(conn, "-ERR cmd\r\n")
		return
	}
	_, _ = io.WriteString(conn, "+OK\r\n")

	if !readArrayCommand(rd, "CONFIG", "SET", "requirepass", newPassword) {
		_, _ = io.WriteString(conn, "-ERR cmd\r\n")
		return
	}
	_, _ = io.WriteString(conn, "+OK\r\n")

	if !readArrayCommand(rd, "CONFIG", "REWRITE") {
		_, _ = io.WriteString(conn, "-ERR cmd\r\n")
		return
	}
	_, _ = io.WriteString(conn, "+OK\r\n")
}

// startFakeRotatePodOrdering is startFakeRotatePod with a recorded
// dial-time tracker — the test asserts replicas dial before the
// master. dialedAt is set on connection accept.
func startFakeRotatePodOrdering(t *testing.T, expectPassword, newPassword string, dialedAt *atomic.Int64, delayBeforeReply time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			dialedAt.Store(time.Now().UnixNano())
			go func(c net.Conn) {
				if delayBeforeReply > 0 {
					time.Sleep(delayBeforeReply)
				}
				handleRotatePod(c, expectPassword, newPassword)
			}(conn)
		}
	}()

	return ln.Addr().String()
}

// startFlakyRotatePod accepts up to (failures + 1) connections. The
// first `failures` are dropped immediately after accept (no reply);
// the (failures + 1)-th gets the full +OK protocol. Used to exercise
// rotateOneWithRetry — the per-pod retry budget should recover after
// the right number of transient blips.
func startFlakyRotatePod(t *testing.T, expectPassword, newPassword string, failures int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var connCount atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := connCount.Add(1)
			if int(n) <= failures {
				_ = conn.Close()
				continue
			}
			go handleRotatePod(conn, expectPassword, newPassword)
		}
	}()

	return ln.Addr().String()
}

// startAlwaysFailRotatePod accepts every connection and immediately
// closes it without replying. Used to exercise retry-exhausted —
// after RotateRetryAttempts attempts, the result surfaces an error.
func startAlwaysFailRotatePod(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	return ln.Addr().String()
}

func TestRotateAuthHappyPath(t *testing.T) {
	const old, new = "old-pwd", "new-pwd"
	rep1 := Endpoint{Name: "rep1", Addr: startFakeRotatePod(t, old, new)}
	rep2 := Endpoint{Name: "rep2", Addr: startFakeRotatePod(t, old, new)}
	master := Endpoint{Name: "mst", Addr: startFakeRotatePod(t, old, new)}

	got := RotateAuth(context.Background(), []Endpoint{rep1, rep2}, master, old, new)
	if len(got) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(got))
	}
	for i, r := range got {
		if r.Err != nil {
			t.Errorf("result[%d] (%s) err = %v, want nil", i, r.Endpoint.Name, r.Err)
		}
	}
	if got[0].Phase != RotationPhaseReplica || got[1].Phase != RotationPhaseReplica {
		t.Errorf("result[0..1] phase = %v / %v, want replica / replica", got[0].Phase, got[1].Phase)
	}
	if got[2].Phase != RotationPhaseMaster {
		t.Errorf("result[2] phase = %v, want master", got[2].Phase)
	}
	if got[2].Endpoint.Name != "mst" {
		t.Errorf("result[2] endpoint name = %q, want mst", got[2].Endpoint.Name)
	}
}

func TestRotateAuthOrderingReplicasFirstThenMaster(t *testing.T) {
	// Pins the load-bearing ordering invariant — the master must
	// not be dialed until every replica has finished.
	// A future refactor that fans out replicas + master in a single
	// parallel block would silently break the rotation contract:
	// the master would briefly accept the new password before the
	// replicas, so a replica reconnecting at that moment would
	// fail to authenticate against the master with the old
	// masterauth.
	const old, new = "old-pwd", "new-pwd"
	var rep1Dial, rep2Dial, masterDial atomic.Int64

	// Replicas reply slowly so master can't sneak in early — if the
	// implementation regressed to "fan out everyone in parallel",
	// the master's dial would land within the first few µs while
	// replicas are still in their delay. 500ms covers the tail of
	// scheduler jitter on saturated CI runners (the prior 100ms
	// produced occasional flakes when GitHub Actions runners were
	// under contention); the issue body suggested 250–500ms, we
	// pick the upper bound so the test stays robust as runner load
	// grows.
	rep1Addr := startFakeRotatePodOrdering(t, old, new, &rep1Dial, 500*time.Millisecond)
	rep2Addr := startFakeRotatePodOrdering(t, old, new, &rep2Dial, 500*time.Millisecond)
	masterAddr := startFakeRotatePodOrdering(t, old, new, &masterDial, 0)

	got := RotateAuth(context.Background(),
		[]Endpoint{{Name: "rep1", Addr: rep1Addr}, {Name: "rep2", Addr: rep2Addr}},
		Endpoint{Name: "mst", Addr: masterAddr}, old, new)

	if len(got) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(got))
	}
	for _, r := range got {
		if r.Err != nil {
			t.Fatalf("unexpected err on %s: %v", r.Endpoint.Name, r.Err)
		}
	}

	// Both replicas were dialed at some point.
	if rep1Dial.Load() == 0 || rep2Dial.Load() == 0 {
		t.Fatalf("replicas not dialed: rep1=%d rep2=%d", rep1Dial.Load(), rep2Dial.Load())
	}
	if masterDial.Load() == 0 {
		t.Fatal("master not dialed")
	}

	// Master must dial AFTER both replicas. Replicas may dial in
	// either order (parallel within plane); we only assert the
	// plane boundary.
	latestReplica := max(rep1Dial.Load(), rep2Dial.Load())
	if masterDial.Load() <= latestReplica {
		t.Errorf("master dialed at %d, want > latestReplica %d (replicas-first ordering broken)",
			masterDial.Load(), latestReplica)
	}
}

func TestRotateAuthRecoversFromTransientReplicaFailure(t *testing.T) {
	// One replica drops the first 2 connections, succeeds on the
	// 3rd. RotateRetryAttempts is 3, so the third attempt is the
	// last one — the result must still be success.
	const old, new = "old-pwd", "new-pwd"
	flakyAddr := startFlakyRotatePod(t, old, new, 2)
	healthy := Endpoint{Name: "rep-healthy", Addr: startFakeRotatePod(t, old, new)}
	flaky := Endpoint{Name: "rep-flaky", Addr: flakyAddr}
	master := Endpoint{Name: "mst", Addr: startFakeRotatePod(t, old, new)}

	got := RotateAuth(context.Background(),
		[]Endpoint{healthy, flaky}, master, old, new)
	if len(got) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(got))
	}
	for _, r := range got {
		if r.Err != nil {
			t.Errorf("result for %s err = %v, want nil (transient should recover)", r.Endpoint.Name, r.Err)
		}
	}
}

func TestRotateAuthSurfacesRetryExhaustedFailure(t *testing.T) {
	const old, new = "old-pwd", "new-pwd"
	deadAddr := startAlwaysFailRotatePod(t)
	healthy := Endpoint{Name: "rep-healthy", Addr: startFakeRotatePod(t, old, new)}
	dead := Endpoint{Name: "rep-dead", Addr: deadAddr}
	master := Endpoint{Name: "mst", Addr: startFakeRotatePod(t, old, new)}

	// Tighten ctx so retries don't blow the test wall-clock budget
	// (the RotateRetryAttempts loop with backoffs takes ~750ms +
	// per-call timeout).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got := RotateAuth(ctx, []Endpoint{healthy, dead}, master, old, new)
	if len(got) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(got))
	}

	var deadResult *PodResult
	for i := range got {
		if got[i].Endpoint.Name == "rep-dead" {
			deadResult = &got[i]
			break
		}
	}
	if deadResult == nil {
		t.Fatal("missing result for rep-dead")
	}
	if deadResult.Err == nil {
		t.Error("rep-dead err = nil, want non-nil after retry exhausted")
	}

	// Healthy replica + master must still succeed — a single-pod
	// failure does not cascade.
	for _, r := range got {
		if r.Endpoint.Name == "rep-dead" {
			continue
		}
		if r.Err != nil {
			t.Errorf("result for %s err = %v, want nil (one-pod failure should not cascade)", r.Endpoint.Name, r.Err)
		}
	}
}

func TestRotateAuthEmptyPasswordsNoOp(t *testing.T) {
	// Both old and new empty: the CR has no auth, nothing to do.
	// The fake server is set up but should never be dialed.
	dialed := atomic.Bool{}
	addr := startTrackingRotatePod(t, &dialed)

	got := RotateAuth(context.Background(),
		[]Endpoint{{Name: "rep", Addr: addr}},
		Endpoint{Name: "mst", Addr: addr}, "", "")
	if got != nil {
		t.Errorf("results = %v, want nil for empty-password no-op", got)
	}
	if dialed.Load() {
		t.Error("server was dialed for empty-password rotation; expected no-op")
	}
}

func TestRotateAuthAddingAuthFromEmpty(t *testing.T) {
	// Empty old password: the rotator must NOT send AUTH (the pod
	// has no requirepass set). Then it must SET masterauth + SET
	// requirepass + REWRITE with the new password.
	const new = "new-pwd"
	addr := startFakeRotatePod(t, "", new) // expectPassword="" — no AUTH expected
	got := RotateAuth(context.Background(),
		[]Endpoint{{Name: "rep", Addr: addr}},
		Endpoint{Name: "mst", Addr: addr}, "", new)
	if len(got) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(got))
	}
	for _, r := range got {
		if r.Err != nil {
			t.Errorf("%s err = %v, want nil (add-auth path)", r.Endpoint.Name, r.Err)
		}
	}
}

func TestRotateAuthRemovingAuth(t *testing.T) {
	// Non-empty old password, empty new: dial WITH AUTH using old,
	// then SET both fields to empty + REWRITE.
	const old = "old-pwd"
	addr := startFakeRotatePod(t, old, "") // newPassword="" — fake expects empty
	got := RotateAuth(context.Background(),
		[]Endpoint{{Name: "rep", Addr: addr}},
		Endpoint{Name: "mst", Addr: addr}, old, "")
	if len(got) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(got))
	}
	for _, r := range got {
		if r.Err != nil {
			t.Errorf("%s err = %v, want nil (remove-auth path)", r.Endpoint.Name, r.Err)
		}
	}
}

func TestRotateAuthDialFailureSurfacedAfterRetries(t *testing.T) {
	// Closed listener → every dial fails. After RotateRetryAttempts
	// attempts the result must carry a non-nil err mentioning dial.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got := RotateAuth(ctx, []Endpoint{{Name: "rep", Addr: addr}},
		Endpoint{Name: "mst", Addr: addr}, "old", "new")
	if len(got) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(got))
	}
	for _, r := range got {
		if r.Err == nil {
			t.Errorf("%s err = nil, want dial-failure error", r.Endpoint.Name)
			continue
		}
		if !strings.Contains(r.Err.Error(), "dial") {
			t.Errorf("%s err = %v, want mention of dial", r.Endpoint.Name, r.Err)
		}
	}
}

func TestRotateAuthMasterEndpointEmptySkipsMaster(t *testing.T) {
	// Some call sites (a replication-mode CR with only the
	// originally-bootstrapped replicas, no role-promoted master
	// today) may pass a zero-value master Endpoint. The orchestrator
	// must rotate replicas only, without dialing or producing a
	// master PodResult.
	const old, new = "old-pwd", "new-pwd"
	rep := Endpoint{Name: "rep", Addr: startFakeRotatePod(t, old, new)}

	got := RotateAuth(context.Background(),
		[]Endpoint{rep}, Endpoint{}, old, new)
	if len(got) != 1 {
		t.Fatalf("len(results) = %d, want 1 (master skipped)", len(got))
	}
	if got[0].Phase != RotationPhaseReplica {
		t.Errorf("got phase = %v, want replica", got[0].Phase)
	}
	if got[0].Err != nil {
		t.Errorf("err = %v, want nil", got[0].Err)
	}
}

// startTrackingRotatePod wraps a fake server with an "I was dialed"
// flag — used by TestRotateAuthEmptyPasswordsNoOp to assert the
// no-op path doesn't reach the wire.
func startTrackingRotatePod(t *testing.T, dialed *atomic.Bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			dialed.Store(true)
			_ = conn.Close()
		}
	}()

	return ln.Addr().String()
}

// startRewriteFailingRotatePod replies +OK to AUTH, CONFIG SET
// masterauth and CONFIG SET requirepass, then returns -ERR on CONFIG
// REWRITE. Models the in-memory-vs-on-disk inconsistency case (disk
// full, perms wrong, file-system mounted read-only) where the running
// auth has been updated but the next restart would revert.
func startRewriteFailingRotatePod(t *testing.T, expectPassword, newPassword string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_ = c.SetDeadline(time.Now().Add(3 * time.Second))
				rd := bufio.NewReader(c)

				if expectPassword != "" {
					if !readArrayCommand(rd, "AUTH", expectPassword) {
						_, _ = io.WriteString(c, "-ERR auth\r\n")
						return
					}
					_, _ = io.WriteString(c, "+OK\r\n")
				}
				if !readArrayCommand(rd, "CONFIG", "SET", "masterauth", newPassword) {
					_, _ = io.WriteString(c, "-ERR cmd\r\n")
					return
				}
				_, _ = io.WriteString(c, "+OK\r\n")
				if !readArrayCommand(rd, "CONFIG", "SET", "requirepass", newPassword) {
					_, _ = io.WriteString(c, "-ERR cmd\r\n")
					return
				}
				_, _ = io.WriteString(c, "+OK\r\n")
				if !readArrayCommand(rd, "CONFIG", "REWRITE") {
					_, _ = io.WriteString(c, "-ERR cmd\r\n")
					return
				}
				// Both SET commands have already succeeded; only REWRITE
				// fails — exactly the in-memory/on-disk divergence the
				// ErrRewriteFailed sentinel marks.
				_, _ = io.WriteString(c, "-ERR rewrite failed: read-only file system\r\n")
			}(conn)
		}
	}()

	return ln.Addr().String()
}

func TestRotateRetryAttemptsBackoffsCoupling(t *testing.T) {
	// rotateOneWithRetry indexes rotateRetryBackoffs[attempt-1] for
	// attempt in [1, RotateRetryAttempts). Bumping RotateRetryAttempts
	// to N requires extending rotateRetryBackoffs to len N-1; otherwise
	// the (N-1)th attempt panics with index-out-of-range at runtime.
	// This test pins the invariant so the failure mode surfaces in the
	// unit-test signal rather than in a live rotation.
	if got, want := len(rotateRetryBackoffs), RotateRetryAttempts-1; got != want {
		t.Fatalf("len(rotateRetryBackoffs) = %d, want RotateRetryAttempts-1 = %d. "+
			"Extend rotateRetryBackoffs whenever RotateRetryAttempts changes.",
			got, want)
	}
}

func TestRotateAuthMasterEndpointPartialZeroSkipsMaster(t *testing.T) {
	// A partial-zero master Endpoint (one field populated, the other
	// empty) is treated as "no master" — silently skipped, no dial
	// attempted. Pins the && semantics in RotateAuth's master-leg
	// guard; the previous || would have entered the leg with Addr=""
	// and failed dial against an empty target.
	const old, new = "old-pwd", "new-pwd"
	rep := Endpoint{Name: "rep", Addr: startFakeRotatePod(t, old, new)}

	cases := []struct {
		name   string
		master Endpoint
	}{
		{"name-only", Endpoint{Name: "mst"}},
		{"addr-only", Endpoint{Addr: "127.0.0.1:1"}}, // unreachable port; never dialed
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RotateAuth(context.Background(),
				[]Endpoint{rep}, tc.master, old, new)
			if len(got) != 1 {
				t.Fatalf("len(results) = %d, want 1 (master skipped on partial-zero)", len(got))
			}
			if got[0].Phase != RotationPhaseReplica {
				t.Errorf("got phase = %v, want replica", got[0].Phase)
			}
			if got[0].Err != nil {
				t.Errorf("err = %v, want nil", got[0].Err)
			}
		})
	}
}

func TestRotateRewriteFailureSurfacesSentinel(t *testing.T) {
	// CONFIG SET masterauth + CONFIG SET requirepass succeed; only
	// REWRITE fails. The PodResult.Err must wrap ErrRewriteFailed so
	// the Phase 2 classifier can distinguish the in-memory-vs-on-disk
	// inconsistency case from a SET-time failure where neither layer
	// was updated.
	const old, new = "old-pwd", "new-pwd"
	rewriteFailAddr := startRewriteFailingRotatePod(t, old, new)
	healthyMaster := Endpoint{Name: "mst", Addr: startFakeRotatePod(t, old, new)}

	got := RotateAuth(context.Background(),
		[]Endpoint{{Name: "rep-rw-fail", Addr: rewriteFailAddr}},
		healthyMaster, old, new)
	if len(got) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(got))
	}

	var failed *PodResult
	for i := range got {
		if got[i].Endpoint.Name == "rep-rw-fail" {
			failed = &got[i]
			break
		}
	}
	if failed == nil {
		t.Fatal("missing result for rep-rw-fail")
	}
	if failed.Err == nil {
		t.Fatal("rep-rw-fail err = nil, want REWRITE failure")
	}
	if !errors.Is(failed.Err, ErrRewriteFailed) {
		t.Errorf("rep-rw-fail err = %v, want errors.Is(_, ErrRewriteFailed)", failed.Err)
	}
}

func TestRotateAuthRespectsContextCancellation(t *testing.T) {
	// Pin the contract: parent-context cancellation propagates into
	// the rotation retry loop — both the inter-attempt backoff sleep
	// and the dial. Without this, a stuck rotation would burn the
	// full RotateRetryAttempts budget (~750ms total backoff) after
	// the reconcile-scoped ctx is already cancelled, holding the
	// reconciler past its requeue boundary.
	deadAddr := startAlwaysFailRotatePod(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel mid-flight: long enough for the first attempt to fail
	// and enter the first backoff sleep, short enough that the
	// remaining ~700ms of the budget is what the cancel must abort.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	started := time.Now()
	got := RotateAuth(ctx,
		[]Endpoint{{Name: "rep", Addr: deadAddr}},
		Endpoint{Name: "mst", Addr: deadAddr}, "old", "new")
	elapsed := time.Since(started)

	if len(got) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(got))
	}
	for _, r := range got {
		if r.Err == nil {
			t.Errorf("%s err = nil, want non-nil under ctx cancel", r.Endpoint.Name)
		}
	}

	// Wall-clock ceiling: the full retry budget without cancel is
	// 250ms + 500ms = 750ms across BOTH legs. With cancel at 50ms,
	// the master leg's first attempt fails fast on a cancelled ctx
	// and its first backoff aborts immediately. 800ms is generous
	// against scheduler jitter on saturated CI without giving the
	// budget room to fully drain across both legs (~1.5s without
	// cancel).
	const budgetCeiling = 800 * time.Millisecond
	if elapsed > budgetCeiling {
		t.Errorf("elapsed = %v, want < %v (ctx cancel should abort retry budget)",
			elapsed, budgetCeiling)
	}
}

// startReauthRotatePod models a pod that rejects the first AUTH (the old
// password) but keeps the connection open for rotateOne's two-password
// fallback: the second AUTH (the new password) is accepted when acceptNew
// is true (the recovery path completes the full CONFIG SET/REWRITE
// protocol) and rejected otherwise. Exercises the fallback branch through
// the converged dial preamble, which dials with no password so the AUTH
// tail — including this retry — runs in the caller.
func startReauthRotatePod(t *testing.T, oldPwd, newPwd string, acceptNew bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleReauthRotatePod(conn, oldPwd, newPwd, acceptNew)
		}
	}()

	return ln.Addr().String()
}

func handleReauthRotatePod(conn net.Conn, oldPwd, newPwd string, acceptNew bool) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	rd := bufio.NewReader(conn)

	// First AUTH carries the old password → reject, but stay open so the
	// caller's fallback can re-AUTH with the new password.
	if !readArrayCommand(rd, "AUTH", oldPwd) {
		_, _ = io.WriteString(conn, "-ERR unexpected first AUTH\r\n")
		return
	}
	_, _ = io.WriteString(conn, "-ERR WRONGPASS-OLD\r\n")

	if !readArrayCommand(rd, "AUTH", newPwd) {
		_, _ = io.WriteString(conn, "-ERR unexpected second AUTH\r\n")
		return
	}
	if !acceptNew {
		_, _ = io.WriteString(conn, "-ERR WRONGPASS-NEW\r\n")
		return
	}
	_, _ = io.WriteString(conn, "+OK\r\n")

	if !readArrayCommand(rd, "CONFIG", "SET", "masterauth", newPwd) {
		_, _ = io.WriteString(conn, "-ERR cmd\r\n")
		return
	}
	_, _ = io.WriteString(conn, "+OK\r\n")
	if !readArrayCommand(rd, "CONFIG", "SET", "requirepass", newPwd) {
		_, _ = io.WriteString(conn, "-ERR cmd\r\n")
		return
	}
	_, _ = io.WriteString(conn, "+OK\r\n")
	if !readArrayCommand(rd, "CONFIG", "REWRITE") {
		_, _ = io.WriteString(conn, "-ERR cmd\r\n")
		return
	}
	_, _ = io.WriteString(conn, "+OK\r\n")
}

func TestRotateTwoPasswordAuthFallbackRecovers(t *testing.T) {
	// old AUTH rejected, new AUTH accepted → the rotation completes
	// cleanly through the converged dial path (rotateOne's two-password
	// fallback re-AUTHs with newPwd and the idempotent CONFIG SETs land).
	addr := startReauthRotatePod(t, "old", "new", true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got := RotateAuth(ctx, []Endpoint{{Name: "rep", Addr: addr}},
		Endpoint{Name: "mst", Addr: addr}, "old", "new")
	if len(got) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(got))
	}
	for _, r := range got {
		if r.Err != nil {
			t.Errorf("%s err = %v, want nil (old-fails/new-succeeds recovery)", r.Endpoint.Name, r.Err)
		}
	}
}

func TestRotateTwoPasswordAuthFallbackBothFail(t *testing.T) {
	// old AUTH rejected AND new AUTH rejected → the ORIGINAL AUTH error
	// surfaces (wrapped "AUTH: ...", not the fallback write/read path) and
	// is non-nil, so the caller classifies a real failure.
	addr := startReauthRotatePod(t, "old", "new", false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got := RotateAuth(ctx, []Endpoint{{Name: "rep", Addr: addr}},
		Endpoint{Name: "mst", Addr: addr}, "old", "new")
	if len(got) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(got))
	}
	for _, r := range got {
		if r.Err == nil {
			t.Errorf("%s err = nil, want non-nil when both AUTHs fail", r.Endpoint.Name)
			continue
		}
		if !strings.Contains(r.Err.Error(), "AUTH") || strings.Contains(r.Err.Error(), "fallback") {
			t.Errorf("%s err = %v, want the original AUTH error surfaced (not the fallback path)", r.Endpoint.Name, r.Err)
		}
	}
}
