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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

// TestFSMTransitionEdge pins the (prev → returned) contract of the
// per-CR last-state tracker that drives the abort and recovery
// transitions of the rollout phase 4. The detector is intentionally simple
// (return prior-call value, store current) because the (prev,
// current) decision logic lives in the reconciler call site, not
// the tracker — keeping this test small is a feature.
func TestFSMTransitionEdge(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns", Name: "vk"}

	t.Run("first observation returns empty (no prior)", func(t *testing.T) {
		r := &ValkeyReconciler{}
		if got := r.fsmTransitionEdge(key, orchestration.StateSteady); got != "" {
			t.Fatalf("first call prev=%q, want empty", got)
		}
	})

	t.Run("subsequent observation returns the prior call's value", func(t *testing.T) {
		r := &ValkeyReconciler{}
		_ = r.fsmTransitionEdge(key, orchestration.StateRolloutPending)
		if got := r.fsmTransitionEdge(key, orchestration.StateDegraded); got != orchestration.StateRolloutPending {
			t.Fatalf("prev=%q, want %q", got, orchestration.StateRolloutPending)
		}
	})

	t.Run("steady-state observation returns the same state as prev", func(t *testing.T) {
		r := &ValkeyReconciler{}
		_ = r.fsmTransitionEdge(key, orchestration.StateSteady)
		// No transition yet — prev should report Steady (set by the
		// previous call), and the detector keeps returning Steady on
		// every subsequent call until something else lands.
		if got := r.fsmTransitionEdge(key, orchestration.StateSteady); got != orchestration.StateSteady {
			t.Fatalf("prev=%q, want %q (steady-state observation)", got, orchestration.StateSteady)
		}
		if got := r.fsmTransitionEdge(key, orchestration.StateSteady); got != orchestration.StateSteady {
			t.Fatalf("third prev=%q, want %q", got, orchestration.StateSteady)
		}
	})

	t.Run("per-CR isolation — different keys hold independent prev", func(t *testing.T) {
		r := &ValkeyReconciler{}
		k1 := types.NamespacedName{Namespace: "ns", Name: "vk1"}
		k2 := types.NamespacedName{Namespace: "ns", Name: "vk2"}
		_ = r.fsmTransitionEdge(k1, orchestration.StateRolloutPending)
		_ = r.fsmTransitionEdge(k2, orchestration.StateRolloutReplicas)
		// Each CR's next call must report its own prior, not the other's.
		if got := r.fsmTransitionEdge(k1, orchestration.StateDegraded); got != orchestration.StateRolloutPending {
			t.Errorf("k1 prev=%q, want %q (cross-CR contamination)", got, orchestration.StateRolloutPending)
		}
		if got := r.fsmTransitionEdge(k2, orchestration.StateDegraded); got != orchestration.StateRolloutReplicas {
			t.Errorf("k2 prev=%q, want %q (cross-CR contamination)", got, orchestration.StateRolloutReplicas)
		}
	})

	t.Run("abort-then-recovery sequence", func(t *testing.T) {
		// Walk the canonical RolloutPending → Degraded → Steady path
		// and confirm the tracker reports the expected prev at each
		// step. The reconciler call site switches on these prev
		// values to decide which applyFSM(state, event, ...) to fire.
		r := &ValkeyReconciler{}
		// 1. First observation = RolloutPending (no prior; no edge).
		if got := r.fsmTransitionEdge(key, orchestration.StateRolloutPending); got != "" {
			t.Fatalf("step 1 prev=%q, want empty", got)
		}
		// 2. Quorum lost → Degraded. prev=RolloutPending — caller
		//    fires the abort transition here.
		if got := r.fsmTransitionEdge(key, orchestration.StateDegraded); got != orchestration.StateRolloutPending {
			t.Fatalf("step 2 prev=%q, want %q", got, orchestration.StateRolloutPending)
		}
		// 3. Quorum recovers, state returns to Steady. prev=Degraded —
		//    caller fires the recovery transition here.
		if got := r.fsmTransitionEdge(key, orchestration.StateSteady); got != orchestration.StateDegraded {
			t.Fatalf("step 3 prev=%q, want %q", got, orchestration.StateDegraded)
		}
	})
}

// TestReadSuspendedFromPending pins the helper that reads
// `Status.Rollout.SuspendedFrom` into the FSM's
// `RolloutSuspendedFromPending` guard. Only the literal
// "RolloutPending" returns true; nil pointers, empty status, and the
// "RolloutReplicas" case all return false — per the contract,
// SuspendedFrom=RolloutReplicas routes through the recovery edge +
// Phase 2 re-trigger, not the rollout-pending re-arm edge.
func TestReadSuspendedFromPending(t *testing.T) {
	cases := []struct {
		name string
		v    *valkeyv1beta1.Valkey
		want bool
	}{
		{
			name: "no Rollout substate",
			v:    &valkeyv1beta1.Valkey{},
			want: false,
		},
		{
			name: "Rollout substate present, SuspendedFrom nil",
			v: &valkeyv1beta1.Valkey{Status: valkeyv1beta1.ValkeyStatus{
				Rollout: &valkeyv1beta1.RolloutStatus{},
			}},
			want: false,
		},
		{
			name: `SuspendedFrom="RolloutPending" returns true`,
			v: &valkeyv1beta1.Valkey{Status: valkeyv1beta1.ValkeyStatus{
				Rollout: &valkeyv1beta1.RolloutStatus{
					SuspendedFrom: new("RolloutPending"),
				},
			}},
			want: true,
		},
		{
			name: `SuspendedFrom="RolloutReplicas" returns false`,
			v: &valkeyv1beta1.Valkey{Status: valkeyv1beta1.ValkeyStatus{
				Rollout: &valkeyv1beta1.RolloutStatus{
					SuspendedFrom: new("RolloutReplicas"),
				},
			}},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := readSuspendedFromPending(tc.v); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSuspendedFromHelpers_StampClearIdempotent pins the inline-patch
// helpers used at the FSM transition sites: the stamp is idempotent
// against re-stamps of the same value, can be overwritten with a
// different value, and the clear flips back to nil and is itself
// idempotent.
func TestSuspendedFromHelpers_StampClearIdempotent(t *testing.T) {
	s := pvcResizeTestScheme(t)
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = "vk"

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&valkeyv1beta1.Valkey{}).
		Build()
	r := &ValkeyReconciler{Client: c}
	ctx := context.Background()

	// Stamp from empty status — populates RolloutStatus + the field.
	r.stampSuspendedFrom(ctx, v, suspendedFromRolloutPending)
	if v.Status.Rollout == nil || v.Status.Rollout.SuspendedFrom == nil ||
		*v.Status.Rollout.SuspendedFrom != suspendedFromRolloutPending {
		t.Fatalf("after stamp: SuspendedFrom=%v, want %q", v.Status.Rollout, suspendedFromRolloutPending)
	}

	// Re-stamp same value — idempotent (no-op).
	r.stampSuspendedFrom(ctx, v, suspendedFromRolloutPending)
	if *v.Status.Rollout.SuspendedFrom != suspendedFromRolloutPending {
		t.Fatalf("re-stamp: SuspendedFrom=%q, want %q", *v.Status.Rollout.SuspendedFrom, suspendedFromRolloutPending)
	}

	// Stamp a different value — overwrites.
	r.stampSuspendedFrom(ctx, v, suspendedFromRolloutReplicas)
	if *v.Status.Rollout.SuspendedFrom != suspendedFromRolloutReplicas {
		t.Fatalf("after re-stamp: SuspendedFrom=%q, want %q", *v.Status.Rollout.SuspendedFrom, suspendedFromRolloutReplicas)
	}

	// Clear flips to nil.
	r.clearSuspendedFrom(ctx, v)
	if v.Status.Rollout != nil && v.Status.Rollout.SuspendedFrom != nil {
		t.Fatalf("after clear: SuspendedFrom=%q, want nil", *v.Status.Rollout.SuspendedFrom)
	}

	// Clear is idempotent.
	r.clearSuspendedFrom(ctx, v)
}

// TestStampSuspendedFrom_LogsNonConflictError pins the finding that a
// non-conflict status-patch failure is logged at V(1) (so a persistent
// RBAC/validation/apiserver regression is visible) while a conflict
// (benign race) and a successful patch stay silent.
func TestStampSuspendedFrom_LogsNonConflictError(t *testing.T) {
	s := pvcResizeTestScheme(t)

	newReconciler := func(patchErr error) (*ValkeyReconciler, *valkeyv1beta1.Valkey) {
		v := &valkeyv1beta1.Valkey{}
		v.Namespace = "ns"
		v.Name = "vk"
		b := fake.NewClientBuilder().WithScheme(s).WithObjects(v).
			WithStatusSubresource(&valkeyv1beta1.Valkey{})
		if patchErr != nil {
			b = b.WithInterceptorFuncs(interceptor.Funcs{
				SubResourcePatch: func(_ context.Context, _ client.Client, _ string, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
					return patchErr
				},
			})
		}
		return &ValkeyReconciler{Client: b.Build()}, v
	}

	capture := func(patchErr error) []string {
		var lines []string
		logger := funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{Verbosity: 1})
		r, v := newReconciler(patchErr)
		ctx := logf.IntoContext(context.Background(), logger)
		r.stampSuspendedFrom(ctx, v, suspendedFromRolloutPending)
		return lines
	}

	hasNonConflictLog := func(lines []string) bool {
		for _, l := range lines {
			if strings.Contains(l, "stampSuspendedFrom") && strings.Contains(l, "non-conflict") {
				return true
			}
		}
		return false
	}

	t.Run("non-conflict error is logged at V(1)", func(t *testing.T) {
		lines := capture(apierrors.NewInternalError(fmt.Errorf("boom")))
		if !hasNonConflictLog(lines) {
			t.Fatalf("expected a V(1) non-conflict log line; got %v", lines)
		}
	})

	t.Run("conflict error stays silent", func(t *testing.T) {
		conflict := apierrors.NewConflict(schema.GroupResource{Group: "velkir.ioxie.dev", Resource: "valkeys"}, "vk", fmt.Errorf("raced"))
		if hasNonConflictLog(capture(conflict)) {
			t.Fatalf("a benign conflict must not log a non-conflict line")
		}
	})

	t.Run("successful patch stays silent", func(t *testing.T) {
		if hasNonConflictLog(capture(nil)) {
			t.Fatalf("a successful patch must not log")
		}
	})
}

// runRecoveryTick threads a synthetic state observation through the
// production fsmAbortAndRecoveryDispatch so the recovery walks below
// exercise the same (prev, current) → applyFSM dispatch the
// reconciler runs.
func runRecoveryTick(t *testing.T, r *ValkeyReconciler, v *valkeyv1beta1.Valkey, key types.NamespacedName, state orchestration.State, quorumOK bool) {
	t.Helper()
	r.fsmAbortAndRecoveryDispatch(context.Background(), v, key, state, quorumOK)
}

// TestSuspendedFromRecoveryWalk_AbortReplicasThenRecover walks the production-shaped
// path: a quorum-loss aborts a mid-replicas rollout (the abort
// transition stamps SuspendedFrom="RolloutReplicas"), then quorum
// recovers cleanly to Steady. The FSM's recovery guard wins because
// `RolloutSuspendedFromPending = (SuspendedFrom == "RolloutPending")`
// is false; the `DegradedResolved` event fires once and
// SuspendedFrom is cleared.
func TestSuspendedFromRecoveryWalk_AbortReplicasThenRecover(t *testing.T) {
	s := pvcResizeTestScheme(t)
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = "vk"

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&valkeyv1beta1.Valkey{}).
		Build()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:   c,
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	key := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	// Tick 1: state=RolloutReplicas (mid-rollout), no prior. No abort
	// edge, no recovery edge. Tracker stores RolloutReplicas.
	runRecoveryTick(t, r, v, key, orchestration.StateRolloutReplicas, true)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("tick1: no events expected, got %v", got)
	}

	// Tick 2: quorum lost — state derives Degraded, prev=RolloutReplicas.
	// The abort transition fires: stamps SuspendedFrom="RolloutReplicas",
	// emits RolloutAbortedQuorumLost.
	runRecoveryTick(t, r, v, key, orchestration.StateDegraded, false)
	abort := drainAllEvents(rec.Events)
	if len(abort) != 1 || !strings.Contains(abort[0], "RolloutAbortedQuorumLost") {
		t.Fatalf("tick2 (T12 abort): expected RolloutAbortedQuorumLost, got %v", abort)
	}
	if v.Status.Rollout == nil || v.Status.Rollout.SuspendedFrom == nil ||
		*v.Status.Rollout.SuspendedFrom != suspendedFromRolloutReplicas {
		t.Fatalf("post-T12: SuspendedFrom=%v, want %q", v.Status.Rollout, suspendedFromRolloutReplicas)
	}

	// Tick 3: still Degraded — prev=Degraded, current=Degraded. No
	// abort edge (prev != rollout-state), no recovery edge (current ==
	// Degraded). No emit. SuspendedFrom stays stamped.
	runRecoveryTick(t, r, v, key, orchestration.StateDegraded, false)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("tick3 (Degraded continues): no events expected, got %v", got)
	}

	// Tick 4: quorum recovers, state=Steady, prev=Degraded. Recovery
	// edge fires with SuspendedFromPending=false ("RolloutReplicas" !=
	// "RolloutPending") → DegradedResolved. SuspendedFrom cleared.
	runRecoveryTick(t, r, v, key, orchestration.StateSteady, true)
	recovery := drainAllEvents(rec.Events)
	if len(recovery) != 1 || !strings.Contains(recovery[0], "DegradedResolved") {
		t.Fatalf("tick4 (T22 recovery): expected one DegradedResolved, got %v", recovery)
	}
	for _, e := range recovery {
		if strings.Contains(e, "RolloutResumed") {
			t.Errorf("tick4: RolloutResumed must NOT fire on T22 path, got %q", e)
		}
	}
	if v.Status.Rollout != nil && v.Status.Rollout.SuspendedFrom != nil {
		t.Fatalf("post-T22: SuspendedFrom=%q, want cleared", *v.Status.Rollout.SuspendedFrom)
	}

	// Tick 5: state=Steady continuing, prev=Steady. No edge fires —
	// the recovery branch is gated on prev==Degraded. No re-emit.
	runRecoveryTick(t, r, v, key, orchestration.StateSteady, true)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("tick5 (post-recovery steady-state): no events expected, got %v", got)
	}
}

// TestSuspendedFromRecoveryWalk_AbortPendingThenReArm walks the synthetic edge from
// the acceptance: a quorum loss aborts from RolloutPending (the
// abort transition stamps SuspendedFrom="RolloutPending"), then
// quorum recovers with the rollout still pending
// (state=RolloutPending, prev=Degraded).
// `RolloutSuspendedFromPending=true` makes the FSM pick the
// rollout-pending re-arm edge: `RolloutResumed`. Asserts the event
// fires exactly once,
// SuspendedFrom is cleared, and consecutive ticks at the same state
// do not re-emit (the tracker's prev-write gate prevents it).
//
// Note: with the current `deriveStateFromFacts`, this synthetic
// edge is not reachable in production — derive_state never returns
// RolloutPending today. The wiring is in place so a future
// derive-state enhancement (or an explicit FSM-state tracker)
// activates the path; the unit test exercises the dispatch contract
// independently.
func TestSuspendedFromRecoveryWalk_AbortPendingThenReArm(t *testing.T) {
	s := pvcResizeTestScheme(t)
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = "vk"

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&valkeyv1beta1.Valkey{}).
		Build()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:   c,
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	key := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	// Tick 1: state=RolloutPending (synthetic), no prior.
	runRecoveryTick(t, r, v, key, orchestration.StateRolloutPending, true)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("tick1: no events expected, got %v", got)
	}

	// Tick 2: quorum lost — Degraded, prev=RolloutPending. The abort
	// transition fires: stamps SuspendedFrom="RolloutPending", emits
	// RolloutAbortedQuorumLost.
	runRecoveryTick(t, r, v, key, orchestration.StateDegraded, false)
	abort := drainAllEvents(rec.Events)
	if len(abort) != 1 || !strings.Contains(abort[0], "RolloutAbortedQuorumLost") {
		t.Fatalf("tick2 (T8 abort): expected RolloutAbortedQuorumLost, got %v", abort)
	}
	if v.Status.Rollout == nil || v.Status.Rollout.SuspendedFrom == nil ||
		*v.Status.Rollout.SuspendedFrom != suspendedFromRolloutPending {
		t.Fatalf("post-T8: SuspendedFrom=%v, want %q", v.Status.Rollout, suspendedFromRolloutPending)
	}

	// Tick 3: still Degraded — no edge.
	runRecoveryTick(t, r, v, key, orchestration.StateDegraded, false)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("tick3 (Degraded continues): no events expected, got %v", got)
	}

	// Tick 4: quorum recovers, state=RolloutPending (rollout still
	// pending), prev=Degraded. Recovery edge fires with
	// SuspendedFromPending=true → RolloutResumed. Cleared.
	runRecoveryTick(t, r, v, key, orchestration.StateRolloutPending, true)
	recovery := drainAllEvents(rec.Events)
	if len(recovery) != 1 || !strings.Contains(recovery[0], "RolloutResumed") {
		t.Fatalf("tick4 (T23 recovery): expected one RolloutResumed, got %v", recovery)
	}
	for _, e := range recovery {
		if strings.Contains(e, "DegradedResolved") {
			t.Errorf("tick4: DegradedResolved must NOT fire on T23 path, got %q", e)
		}
	}
	if v.Status.Rollout != nil && v.Status.Rollout.SuspendedFrom != nil {
		t.Fatalf("post-T23: SuspendedFrom=%q, want cleared", *v.Status.Rollout.SuspendedFrom)
	}

	// Tick 5: still RolloutPending, prev=RolloutPending. Recovery
	// branch gated on prev==Degraded — does NOT re-fire. No re-emit.
	runRecoveryTick(t, r, v, key, orchestration.StateRolloutPending, true)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("tick5 (post-recovery): no events expected, got %v", got)
	}
}

// TestRecoveryEdge_AbortPrimary pins the StateRolloutPrimary abort
// branch: a quorum loss aborts a mid-primary-rollout into Degraded
// without stamping SuspendedFrom (primary aborts re-emerge through
// rollout-trigger re-detection, not the recovery edge). The dispatch
// fires applyFSM(StateRolloutPrimary, EventReconcileTick, !QuorumOK)
// → T15 emits PrimaryRolloutBlocked.
func TestRecoveryEdge_AbortPrimary(t *testing.T) {
	s := pvcResizeTestScheme(t)
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = "vk"

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&valkeyv1beta1.Valkey{}).
		Build()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:   c,
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	key := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	// Tick 1: state=RolloutPrimary (mid-primary-rollout), no prior.
	runRecoveryTick(t, r, v, key, orchestration.StateRolloutPrimary, true)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("tick1: no events expected, got %v", got)
	}

	// Tick 2: quorum lost — Degraded, prev=RolloutPrimary. The abort
	// transition fires applyFSM(RolloutPrimary, ReconcileTick, !QuorumOK)
	// → T15 emits PrimaryRolloutBlocked. SuspendedFrom must NOT be
	// stamped (primary aborts re-emerge through rollout-trigger).
	runRecoveryTick(t, r, v, key, orchestration.StateDegraded, false)
	abort := drainAllEvents(rec.Events)
	if len(abort) != 1 || !strings.Contains(abort[0], "PrimaryRolloutBlocked") {
		t.Fatalf("tick2 (T15 abort): expected one PrimaryRolloutBlocked, got %v", abort)
	}
	if v.Status.Rollout != nil && v.Status.Rollout.SuspendedFrom != nil {
		t.Errorf("post-T15: SuspendedFrom=%q, want nil (primary aborts must NOT stamp)",
			*v.Status.Rollout.SuspendedFrom)
	}
}

// TestIsFailoverInFlight_UnifiedCriticalSection pins that the one
// canonical predicate enters/exits over BOTH derived signals — the
// durable FailoverDispatch marker (via its in-memory latch mirror) and
// the roll-in-progress FSM state — so the sentinel-roll gate, the
// dual-master self-heal, and the render fallback all read one truth.
// The observer is nil here (post-restart / boot-race: observedAddr ""),
// which is precisely the window where the durable latch is the
// load-bearing half — the FSM tracker is process-local and empty after
// a restart, yet the section must stay closed.
func TestIsFailoverInFlight_UnifiedCriticalSection(t *testing.T) {
	t.Parallel()
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	t.Run("durable latch alone holds the section (FSM tracker empty — post-restart)", func(t *testing.T) {
		r := &ValkeyReconciler{}
		// No FSM tracker at all (fresh process after a crash), but the
		// durable latch was rehydrated from the marker.
		r.failoverLatchSet(cr, "10.0.0.1:6379")
		if !r.IsFailoverInFlight(cr) {
			t.Fatal("the durable latch must hold the critical section even with no FSM observation")
		}
	})

	t.Run("durable latch holds while FSM reads a non-failover state", func(t *testing.T) {
		r := &ValkeyReconciler{}
		r.failoverLatchSet(cr, "10.0.0.1:6379")
		r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateSteady}
		if !r.IsFailoverInFlight(cr) {
			t.Fatal("the durable latch alone must keep the section closed")
		}
	})

	t.Run("roll-in-progress FSM state alone holds the section (no latch)", func(t *testing.T) {
		r := &ValkeyReconciler{}
		r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateFailoverInFlight}
		if !r.IsFailoverInFlight(cr) {
			t.Fatal("the roll-in-progress derivation must hold the section")
		}
	})

	t.Run("neither signal — section open", func(t *testing.T) {
		r := &ValkeyReconciler{}
		r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateSteady}
		if r.IsFailoverInFlight(cr) {
			t.Fatal("with no latch and a steady FSM state the section must be open")
		}
	})

	t.Run("deadline escape — an expired latch does not hold the section", func(t *testing.T) {
		r := &ValkeyReconciler{}
		// Latch present but past its deadline: the timeout/escape releases
		// the section (never an infinite hold). The pure predicate read
		// reports inactive without mutating the latch.
		r.stateFor(cr).setFailoverLatch(&failoverInFlightLatch{
			preStripAddr: "10.0.0.1:6379",
			deadline:     time.Now().Add(-time.Second),
		})
		r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateSteady}
		if r.IsFailoverInFlight(cr) {
			t.Fatal("an expired latch must not hold the critical section")
		}
	})
}
