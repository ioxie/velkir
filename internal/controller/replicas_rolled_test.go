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

	"k8s.io/apimachinery/pkg/types"

	"github.com/ioxie/velkir/internal/orchestration"
)

func TestReplicasRolledEdge(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns", Name: "vk"}

	t.Run("non-RolloutPrimary state → no edge", func(t *testing.T) {
		r := &ValkeyReconciler{}
		for _, s := range []orchestration.State{
			orchestration.StateBootstrap,
			orchestration.StateSteady,
			orchestration.StateRolloutPending,
			orchestration.StateRolloutReplicas,
			orchestration.StateFailoverInFlight,
			orchestration.StateRolloutComplete,
			orchestration.StateDegraded,
			orchestration.StateDegradedQuorumLost,
		} {
			if r.replicasRolledEdge(key, s) {
				t.Fatalf("edge=true for state %q, want false", s)
			}
		}
	})

	t.Run("first observation of RolloutPrimary → edge fires", func(t *testing.T) {
		r := &ValkeyReconciler{}
		if !r.replicasRolledEdge(key, orchestration.StateRolloutPrimary) {
			t.Fatalf("edge=false on first RolloutPrimary observation, want true")
		}
	})

	t.Run("second observation while still RolloutPrimary → suppressed", func(t *testing.T) {
		r := &ValkeyReconciler{}
		// Arm.
		_ = r.replicasRolledEdge(key, orchestration.StateRolloutPrimary)
		// Re-observe — must NOT fire.
		if r.replicasRolledEdge(key, orchestration.StateRolloutPrimary) {
			t.Fatalf("edge=true on second RolloutPrimary observation, want false (suppressed)")
		}
	})

	t.Run("RolloutPrimary → Steady → RolloutPrimary re-arms and fires again", func(t *testing.T) {
		r := &ValkeyReconciler{}
		// 1: first observation, fires.
		if !r.replicasRolledEdge(key, orchestration.StateRolloutPrimary) {
			t.Fatalf("step 1 edge=false, want true")
		}
		// 2: rollout completes (state returns to Steady), no fire,
		//    tracker re-arms.
		if r.replicasRolledEdge(key, orchestration.StateSteady) {
			t.Fatalf("step 2 edge=true on Steady, want false")
		}
		// 3: new rollout reaches the same state, edge fires again.
		if !r.replicasRolledEdge(key, orchestration.StateRolloutPrimary) {
			t.Fatalf("step 3 edge=false on second RolloutPrimary entry, want true (re-armed)")
		}
	})

	t.Run("RolloutPrimary → Degraded (quorum loss) → RolloutPrimary re-arms", func(t *testing.T) {
		// Different exit path: instead of completing, the rollout
		// gets interrupted by quorum loss (deriveState returns
		// Degraded). The tracker re-arms because the state left
		// RolloutPrimary; when quorum recovers and state returns
		// to RolloutPrimary, the edge fires fresh.
		r := &ValkeyReconciler{}
		_ = r.replicasRolledEdge(key, orchestration.StateRolloutPrimary)
		if r.replicasRolledEdge(key, orchestration.StateDegraded) {
			t.Fatalf("edge=true on Degraded, want false")
		}
		if !r.replicasRolledEdge(key, orchestration.StateRolloutPrimary) {
			t.Fatalf("edge=false on re-entry after Degraded, want true (re-armed)")
		}
	})
}
