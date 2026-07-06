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

package controller

import (
	"testing"
	"time"
)

// TestObserveFailoverIfAddrChanged_BootstrapNotFailover: the first
// QuorumOK observation after operator startup stamps lastOKAddr
// without claiming a failover. Otherwise every operator restart
// against an already-running cluster would emit a phantom
// failover-completion event the moment QuorumOK arrives.
func TestObserveFailoverIfAddrChanged_BootstrapNotFailover(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	dur, trigger, ok := state.observeFailoverIfAddrChanged(true /* present */, true /* quorumOK */, "10.0.0.5:6379", t0)
	if ok {
		t.Errorf("first QuorumOK observation must not flag a failover, got %s trigger=%q dur=%v", trigger, trigger, dur)
	}
	if state.lastOKAddr != "10.0.0.5:6379" {
		t.Errorf("bootstrap observation must stamp lastOKAddr, got %q", state.lastOKAddr)
	}
}

// TestObserveFailoverIfAddrChanged_SameAddrNotFailover: a transient
// QuorumOK=false blip that resolves back to the same primary Addr
// is NOT a failover. The split-brain sustained-seconds tracker may
// have recorded a brief disagreement window, but if the primary
// pointer didn't actually change, no failover happened — could be
// a bootstrap-race blip or a sentinel hiccup. The histogram and
// counter must not increment for this case.
func TestObserveFailoverIfAddrChanged_SameAddrNotFailover(t *testing.T) {
	state := &crQuorumState{
		lastOKAddr: "10.0.0.5:6379",
	}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	t1 := t0
	since := t1
	state.splitBrainSince = &since
	t2 := t1.Add(8 * time.Second)
	dur, trigger, ok := state.observeFailoverIfAddrChanged(true, true, "10.0.0.5:6379", t2)
	if ok {
		t.Errorf("same-Addr QuorumOK transition must not flag a failover, got trigger=%q dur=%v", trigger, dur)
	}
}

// TestObserveFailoverIfAddrChanged_FailoverObserved: the load-
// bearing case. !QuorumOK with splitBrainSince stamped → QuorumOK
// with snap.Addr different from lastOKAddr → failover observed.
// Returns the elapsed time from disagreement start to now and the
// `sentinel_elected` trigger label.
func TestObserveFailoverIfAddrChanged_FailoverObserved(t *testing.T) {
	state := &crQuorumState{
		lastOKAddr: "10.0.0.5:6379",
	}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	since := t0
	state.splitBrainSince = &since
	t1 := t0.Add(12 * time.Second)
	dur, trigger, ok := state.observeFailoverIfAddrChanged(true, true, "10.0.0.7:6379", t1)
	if !ok {
		t.Fatalf("Addr change after disagreement must flag a failover")
	}
	if trigger != "sentinel_elected" {
		t.Errorf("expected trigger=sentinel_elected, got %q", trigger)
	}
	if dur != 12*time.Second {
		t.Errorf("expected elapsed=12s, got %v", dur)
	}
	if state.lastOKAddr != "10.0.0.7:6379" {
		t.Errorf("post-observation lastOKAddr must reflect the new primary, got %q", state.lastOKAddr)
	}
}

// TestObserveFailoverIfAddrChanged_DuringDisagreement: while
// !QuorumOK is still in flight, no failover is observed. This is
// what gates the histogram observation to the recovery edge rather
// than firing every reconcile during a disagreement window.
func TestObserveFailoverIfAddrChanged_DuringDisagreement(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	since := t0
	state.splitBrainSince = &since
	_, _, ok := state.observeFailoverIfAddrChanged(true, false /* quorumOK */, "10.0.0.5:6379", t0.Add(5*time.Second))
	if ok {
		t.Errorf("disagreement window must not flag a failover")
	}
}

// TestObserveFailoverIfAddrChanged_NilSplitBrainNoFailover: an Addr
// change between two OK observations with NO intervening
// disagreement window (splitBrainSince was never set) is NOT a
// failover. This pins the require-disagreement-window precondition
// — without it, any spurious observer-snapshot Addr flip would
// double-count as a failover and inflate the histogram.
func TestObserveFailoverIfAddrChanged_NilSplitBrainNoFailover(t *testing.T) {
	state := &crQuorumState{
		lastOKAddr: "10.0.0.5:6379",
		// splitBrainSince intentionally nil
	}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	dur, trigger, ok := state.observeFailoverIfAddrChanged(true, true, "10.0.0.7:6379", t0)
	if ok {
		t.Errorf("Addr change without an intervening disagreement window must not flag a failover, got trigger=%q dur=%v", trigger, dur)
	}
	// lastOKAddr must still advance so the next observation isn't
	// stuck looking at a stale baseline.
	if state.lastOKAddr != "10.0.0.7:6379" {
		t.Errorf("lastOKAddr must advance even on no-failover transition, got %q", state.lastOKAddr)
	}
}

// TestObserveFailoverIfAddrChanged_NoDoubleCount: consecutive
// reconciles observing the SAME failover (already advanced
// lastOKAddr) must not re-claim it. This is the critical anti-flap
// invariant — without it, every reconcile after a failover would
// re-fire FailoversTotal/FailoverDurationSeconds, polluting the
// histogram with thousands of phantom observations.
func TestObserveFailoverIfAddrChanged_NoDoubleCount(t *testing.T) {
	state := &crQuorumState{
		lastOKAddr: "10.0.0.5:6379",
	}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	since := t0
	state.splitBrainSince = &since
	t1 := t0.Add(12 * time.Second)

	// First observation: real failover.
	_, _, ok := state.observeFailoverIfAddrChanged(true, true, "10.0.0.7:6379", t1)
	if !ok {
		t.Fatalf("first OK after disagreement with changed Addr must flag a failover")
	}

	// Second observation in the same OK state at the same Addr —
	// must not re-claim the failover.
	_, _, ok = state.observeFailoverIfAddrChanged(true, true, "10.0.0.7:6379", t1.Add(5*time.Second))
	if ok {
		t.Errorf("second reconcile observing the same post-failover Addr must NOT re-claim the failover")
	}
}

// TestObserveMasterInfoTimeout_ResetOnSuccess: a successful INFO
// probe clears the timer; the gauge returns 0. Mirrors the
// updateSplitBrainSustained shape for symmetry.
func TestObserveMasterInfoTimeout_ResetOnSuccess(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	got := state.observeMasterInfoTimeout(false /* ok=false → probe failed */, t0)
	if got != 0 {
		t.Errorf("first failed probe must return 0, got %f", got)
	}
	if state.masterInfoTimeoutSince == nil {
		t.Fatalf("first failed probe must stamp masterInfoTimeoutSince")
	}

	got = state.observeMasterInfoTimeout(false, t0.Add(35*time.Second))
	if got != 35 {
		t.Errorf("continuing failure should report 35s elapsed, got %f", got)
	}

	got = state.observeMasterInfoTimeout(true /* ok */, t0.Add(40*time.Second))
	if got != 0 {
		t.Errorf("successful probe must reset gauge to 0, got %f", got)
	}
	if state.masterInfoTimeoutSince != nil {
		t.Errorf("successful probe must clear masterInfoTimeoutSince, got %v", state.masterInfoTimeoutSince)
	}
}
