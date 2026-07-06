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
	"strings"
	"testing"
	"time"

	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

func TestApplyFSM_NilMachineNoOps(t *testing.T) {
	r := &ValkeyReconciler{}
	v := &valkeyv1beta1.Valkey{}

	next, requeue, matched := r.applyFSM(v, orchestration.StateSteady, orchestration.EventRolloutTrigger, orchestration.GuardCtx{QuorumOK: true})
	if matched {
		t.Fatalf("matched=true with nil FSM, want false")
	}
	if next != orchestration.StateSteady {
		t.Fatalf("nextState=%q, want input state %q", next, orchestration.StateSteady)
	}
	if requeue != 0 {
		t.Fatalf("requeue=%v, want 0", requeue)
	}
}

func TestApplyFSM_T3StartedEmits(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	next, requeue, matched := r.applyFSM(v, orchestration.StateSteady, orchestration.EventRolloutTrigger, orchestration.GuardCtx{QuorumOK: true})
	if !matched {
		t.Fatalf("matched=false, want true (T3)")
	}
	if next != orchestration.StateRolloutPending {
		t.Fatalf("nextState=%q, want %q", next, orchestration.StateRolloutPending)
	}
	if requeue != 0 {
		t.Fatalf("requeue=%v, want 0 (T3 has no requeue)", requeue)
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1: %v", len(got), got)
	}
	if !strings.Contains(got[0], "RolloutStarted") {
		t.Fatalf("event %q does not contain RolloutStarted", got[0])
	}
}

func TestApplyFSM_T4DeferredEmitsAndRequeues(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	next, requeue, matched := r.applyFSM(v, orchestration.StateSteady, orchestration.EventRolloutTrigger, orchestration.GuardCtx{QuorumOK: false})
	if !matched {
		t.Fatalf("matched=false, want true (T4)")
	}
	if next != orchestration.StateSteady {
		t.Fatalf("nextState=%q, want %q (T4 stays in Steady)", next, orchestration.StateSteady)
	}
	if requeue != 10*time.Second {
		t.Fatalf("requeue=%v, want 10s (T4 declared requeue)", requeue)
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1: %v", len(got), got)
	}
	if !strings.Contains(got[0], "RolloutDeferred") {
		t.Fatalf("event %q does not contain RolloutDeferred", got[0])
	}
}

func TestApplyFSM_T1BootstrapRequeueNoEvent(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	next, requeue, matched := r.applyFSM(v, orchestration.StateBootstrap, orchestration.EventCRCreated, orchestration.GuardCtx{})
	if !matched {
		t.Fatalf("matched=false, want true (T1)")
	}
	if next != orchestration.StateBootstrap {
		t.Fatalf("nextState=%q, want %q (T1 stays in Bootstrap)", next, orchestration.StateBootstrap)
	}
	if requeue != 5*time.Second {
		t.Fatalf("requeue=%v, want 5s (T1 declared requeue)", requeue)
	}
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("emitted %d events, want 0 (T1 has no EventReason): %v", len(got), got)
	}
}

func TestApplyFSM_NoMatchingTransitionNoOps(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	// EventQuorumPrimaryAgreed has no row outside Bootstrap.
	next, requeue, matched := r.applyFSM(v, orchestration.StateRolloutPending, orchestration.EventQuorumPrimaryAgreed, orchestration.GuardCtx{})
	if matched {
		t.Fatalf("matched=true on no-row transition, want false")
	}
	if next != orchestration.StateRolloutPending {
		t.Fatalf("nextState=%q, want input state %q", next, orchestration.StateRolloutPending)
	}
	if requeue != 0 {
		t.Fatalf("requeue=%v, want 0", requeue)
	}
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Fatalf("emitted %d events on no-op, want 0: %v", len(got), got)
	}
}

func TestApplyFSM_NilRecorderSilentlyTransitions(t *testing.T) {
	r := &ValkeyReconciler{
		FSM: orchestration.NewMachine(),
	}
	v := &valkeyv1beta1.Valkey{}
	v.Name = "vk"
	v.Namespace = "ns"

	next, requeue, matched := r.applyFSM(v, orchestration.StateSteady, orchestration.EventRolloutTrigger, orchestration.GuardCtx{QuorumOK: true})
	if !matched {
		t.Fatalf("matched=false, want true")
	}
	if next != orchestration.StateRolloutPending {
		t.Fatalf("nextState=%q, want %q", next, orchestration.StateRolloutPending)
	}
	if requeue != 0 {
		t.Fatalf("requeue=%v, want 0", requeue)
	}
}
