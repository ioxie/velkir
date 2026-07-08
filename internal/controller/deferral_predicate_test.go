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
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/ioxie/velkir/internal/orchestration"
)

func TestIsFailoverInFlight_NoTrackerReturnsFalse(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if r.IsFailoverInFlight(cr) {
		t.Errorf("expected false for unknown CR, got true")
	}
}

func TestIsFailoverInFlight_ReadsLastStateFromTracker(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	cases := []struct {
		state orchestration.State
		want  bool
	}{
		{orchestration.StateBootstrap, false},
		{orchestration.StateSteady, false},
		{orchestration.StateRolloutPending, false},
		{orchestration.StateRolloutReplicas, false},
		{orchestration.StateRolloutPrimary, false},
		{orchestration.StateFailoverInFlight, true},
		{orchestration.StateRolloutComplete, false},
		{orchestration.StateDegraded, false},
		{orchestration.StateDegradedQuorumLost, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: tc.state}
			if got := r.IsFailoverInFlight(cr); got != tc.want {
				t.Errorf("lastState=%q: IsFailoverInFlight=%v want=%v", tc.state, got, tc.want)
			}
		})
	}
}

func TestIsFailoverInFlight_TracksTransitionEdge(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	// First fsmTransitionEdge call records Steady.
	_ = r.fsmTransitionEdge(cr, orchestration.StateSteady)
	if r.IsFailoverInFlight(cr) {
		t.Errorf("after Steady, expected IsFailoverInFlight=false")
	}

	// Transition into FailoverInFlight.
	_ = r.fsmTransitionEdge(cr, orchestration.StateFailoverInFlight)
	if !r.IsFailoverInFlight(cr) {
		t.Errorf("after FailoverInFlight, expected IsFailoverInFlight=true")
	}

	// Transition out of FailoverInFlight.
	_ = r.fsmTransitionEdge(cr, orchestration.StateSteady)
	if r.IsFailoverInFlight(cr) {
		t.Errorf("after exit to Steady, expected IsFailoverInFlight=false")
	}
}

func TestIsFailoverInFlight_PerCRIsolation(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	k1 := types.NamespacedName{Namespace: "ns", Name: "vk1"}
	k2 := types.NamespacedName{Namespace: "ns", Name: "vk2"}
	r.stateFor(k1).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateFailoverInFlight}
	r.stateFor(k2).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateSteady}
	if !r.IsFailoverInFlight(k1) {
		t.Errorf("k1 expected true (FailoverInFlight)")
	}
	if r.IsFailoverInFlight(k2) {
		t.Errorf("k2 expected false (Steady)")
	}
}

func TestIsFailoverInFlight_ConcurrentReadsSafe(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateFailoverInFlight}

	const readers = 32
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			for range 100 {
				_ = r.IsFailoverInFlight(cr)
			}
		}()
	}
	wg.Wait()
}

// TestDeferralPredicate_TruthTable pins the OR-composition contract.
// The predicate defers when EITHER suppression is active OR the
// last-observed FSM state is StateFailoverInFlight, mirroring the
// production wiring in cmd/main.go.
func TestDeferralPredicate_TruthTable(t *testing.T) {
	t.Parallel()
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	cases := []struct {
		name       string
		suppressed bool
		failover   bool
		wantDefer  bool
	}{
		{name: "neither — allow", suppressed: false, failover: false, wantDefer: false},
		{name: "suppression only — defer", suppressed: true, failover: false, wantDefer: true},
		{name: "failover only — defer", suppressed: false, failover: true, wantDefer: true},
		{name: "both — defer", suppressed: true, failover: true, wantDefer: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &ValkeyReconciler{}
			r.stateFor(cr).quorum = &crQuorumState{suppressionActive: tc.suppressed}
			fsmState := orchestration.StateSteady
			if tc.failover {
				fsmState = orchestration.StateFailoverInFlight
			}
			r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: fsmState}
			if got := r.DeferralPredicate(cr); got != tc.wantDefer {
				t.Errorf("DeferralPredicate(suppressed=%v, failover=%v)=%v want=%v",
					tc.suppressed, tc.failover, got, tc.wantDefer)
			}
		})
	}
}

func TestDeferralPredicate_UnknownCRReturnsFalse(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if r.DeferralPredicate(cr) {
		t.Errorf("expected false for CR with no per-CR state, got true")
	}
}

// TestValkeyRollActive pins the "valkey roll in flight" half of the
// sentinel-roll gate: true only while the operator is actively rolling
// the data plane (RolloutReplicas / RolloutPrimary). RolloutPending is a
// detected-but-not-started rollout and must NOT gate the sentinel roll.
func TestValkeyRollActive(t *testing.T) {
	t.Parallel()
	cases := []struct {
		state orchestration.State
		want  bool
	}{
		{orchestration.StateBootstrap, false},
		{orchestration.StateSteady, false},
		{orchestration.StateRolloutPending, false},
		{orchestration.StateRolloutReplicas, true},
		{orchestration.StateRolloutPrimary, true},
		{orchestration.StateFailoverInFlight, false},
		{orchestration.StateRolloutComplete, false},
		{orchestration.StateDegraded, false},
		{orchestration.StateDegradedQuorumLost, false},
	}
	for _, tc := range cases {
		if got := valkeyRollActive(tc.state); got != tc.want {
			t.Errorf("valkeyRollActive(%q)=%v want %v", tc.state, got, tc.want)
		}
	}
}
