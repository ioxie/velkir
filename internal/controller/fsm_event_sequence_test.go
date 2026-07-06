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
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

// TestApplyFSM_T11ReplicasRolledEmits closes the per-transition coverage
// gap in fsm_test.go (T3, T4, T1 were asserted there; T11 was only
// covered by the FSM-table test, not the wrapper).
func TestApplyFSM_T11ReplicasRolledEmits(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	next, requeue, matched := r.applyFSM(v,
		orchestration.StateRolloutReplicas,
		orchestration.EventAllReplicasRolled,
		orchestration.GuardCtx{QuorumOK: true})
	if !matched {
		t.Fatalf("matched=false, want true (T11)")
	}
	if next != orchestration.StateRolloutPrimary {
		t.Fatalf("nextState=%q, want %q (T11 → RolloutPrimary)", next, orchestration.StateRolloutPrimary)
	}
	if requeue != 0 {
		t.Fatalf("requeue=%v, want 0 (T11 has no requeue)", requeue)
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1: %v", len(got), got)
	}
	if !strings.Contains(got[0], "ReplicasRolled") {
		t.Fatalf("event %q does not contain ReplicasRolled", got[0])
	}
}

// TestFSMEventSequence_FiveNamedEvents drives the production wrappers
// (applyFSM and fsmAbortAndRecoveryDispatch) through one canonical
// rollout + abort + recovery walk and asserts the five named rollout
// events all fire. A future change that drops any one — an FSM-table
// edit losing an EventReason side-effect, or a wrapper edit silently
// swallowing the emit — surfaces as a single failing test rather than
// requiring five separate per-transition tests to catch.
func TestFSMEventSequence_FiveNamedEvents(t *testing.T) {
	s := pvcResizeTestScheme(t)
	v := &valkeyv1beta1.Valkey{}
	v.Namespace = "ns"
	v.Name = "vk"

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&valkeyv1beta1.Valkey{}).
		Build()
	rec := k8sevents.NewFakeRecorder(32)
	r := &ValkeyReconciler{
		Client:   c,
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	key := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	ctx := context.Background()

	// Step 1 — Steady + RolloutTrigger + QuorumOK → T3 RolloutStarted.
	next, _, matched := r.applyFSM(v, orchestration.StateSteady,
		orchestration.EventRolloutTrigger,
		orchestration.GuardCtx{QuorumOK: true})
	if !matched || next != orchestration.StateRolloutPending {
		t.Fatalf("step 1 (T3): matched=%v next=%q, want true + RolloutPending", matched, next)
	}
	step1 := drainAllEvents(rec.Events)
	if len(step1) != 1 || !strings.Contains(step1[0], "RolloutStarted") {
		t.Fatalf("step 1 (T3): expected RolloutStarted, got %v", step1)
	}

	// Step 2 — RolloutPending → Degraded (quorum loss). The dispatcher
	// stamps SuspendedFrom="RolloutPending" so step 3's recovery picks
	// T23 (re-arm) over T22 (clean recovery).
	r.fsmAbortAndRecoveryDispatch(ctx, v, key, orchestration.StateRolloutPending, true)
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("step 2 prologue (RolloutPending observation): expected no events, got %v", got)
	}
	r.fsmAbortAndRecoveryDispatch(ctx, v, key, orchestration.StateDegraded, false)
	step2 := drainAllEvents(rec.Events)
	if len(step2) != 1 || !strings.Contains(step2[0], "RolloutAbortedQuorumLost") {
		t.Fatalf("step 2 (T8): expected RolloutAbortedQuorumLost, got %v", step2)
	}

	// Step 3 — Degraded → RolloutPending. SuspendedFromPending=true →
	// T23 RolloutResumed.
	r.fsmAbortAndRecoveryDispatch(ctx, v, key, orchestration.StateRolloutPending, true)
	step3 := drainAllEvents(rec.Events)
	if len(step3) != 1 || !strings.Contains(step3[0], "RolloutResumed") {
		t.Fatalf("step 3 (T23): expected RolloutResumed, got %v", step3)
	}

	// Step 4 — RolloutReplicas + AllReplicasRolled → T11 ReplicasRolled.
	next, _, matched = r.applyFSM(v, orchestration.StateRolloutReplicas,
		orchestration.EventAllReplicasRolled,
		orchestration.GuardCtx{QuorumOK: true})
	if !matched || next != orchestration.StateRolloutPrimary {
		t.Fatalf("step 4 (T11): matched=%v next=%q, want true + RolloutPrimary", matched, next)
	}
	step4 := drainAllEvents(rec.Events)
	if len(step4) != 1 || !strings.Contains(step4[0], "ReplicasRolled") {
		t.Fatalf("step 4 (T11): expected ReplicasRolled, got %v", step4)
	}

	// Step 5 — Steady + RolloutTrigger + !QuorumOK → T4 RolloutDeferred.
	// Same call site as step 1, different guard branch.
	next, requeue, matched := r.applyFSM(v, orchestration.StateSteady,
		orchestration.EventRolloutTrigger,
		orchestration.GuardCtx{QuorumOK: false})
	if !matched || next != orchestration.StateSteady {
		t.Fatalf("step 5 (T4): matched=%v next=%q, want true + Steady", matched, next)
	}
	if requeue != 10*time.Second {
		t.Fatalf("step 5 (T4): requeue=%v, want 10s", requeue)
	}
	step5 := drainAllEvents(rec.Events)
	if len(step5) != 1 || !strings.Contains(step5[0], "RolloutDeferred") {
		t.Fatalf("step 5 (T4): expected RolloutDeferred, got %v", step5)
	}
}
