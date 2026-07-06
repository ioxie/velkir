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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

// Unit tests for rollout-trigger completion.
//
// Coverage spread:
//
//   - isInRolloutState classifier (this file)
//   - ManualRolloutAnnotation projection into the pod-template
//     annotations (this file)
//   - rolloutTriggerEdge midRolloutChange detection
//     (rollout_trigger_test.go)
//   - T13 SpecChangeDeferred SideEffect on StateRolloutReplicas +
//     EventSpecChanged (fsm_transitions_test.go)
//   - ScaleDeferred temporal-deferral branch in reconcileStatefulSet
//     — the full integration test belongs in the e2e
//     scenario set; envtest cannot easily seed a positive QuorumOK
//     snapshot (the existing sentinel.Manager test scaffold supports
//     only the negative path), so the wiring is verified by the unit
//     tests covering the classifier + the existing scale envtests.

func TestIsInRolloutState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state orchestration.State
		want  bool
	}{
		{orchestration.StateBootstrap, false},
		{orchestration.StateSteady, false},
		{orchestration.StateDegraded, false},
		{orchestration.StateDegradedQuorumLost, false},
		{orchestration.StateRolloutPending, true},
		{orchestration.StateRolloutReplicas, true},
		{orchestration.StateRolloutPrimary, true},
		{orchestration.StateFailoverInFlight, true},
		{orchestration.StateRolloutComplete, true},
	}
	for _, tc := range tests {
		t.Run(string(tc.state), func(t *testing.T) {
			if got := isInRolloutState(tc.state); got != tc.want {
				t.Errorf("isInRolloutState(%q) = %v, want %v", tc.state, got, tc.want)
			}
		})
	}
}

// testCMHash is the canned config-map hash these unit tests pass to
// buildValkeySTS. The exact value doesn't matter — the assertions
// check ManualRolloutAnnotation handling, not the cmHash plumbing.
const testCMHash = "hash-A"

// stsAnnotations returns the pod-template annotation map produced by
// buildValkeySTS. Unwraps the apply-config pointer plumbing so the
// test bodies stay readable.
func stsAnnotations(v *valkeyv1beta1.Valkey) map[string]string {
	ac := buildValkeySTS(v, testCMHash)
	if ac == nil || ac.Spec == nil || ac.Spec.Template == nil {
		return nil
	}
	return ac.Spec.Template.Annotations
}

func minimalCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeStandalone,
			Image: valkeyv1beta1.ImageSpec{
				Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
				Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
				Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
			},
			Valkey: valkeyv1beta1.ValkeyPodSpec{Replicas: 1},
		},
	}
}

func TestBuildValkeySTS_ManualRolloutAnnotation_AbsentByDefault(t *testing.T) {
	t.Parallel()
	v := minimalCR()
	ann := stsAnnotations(v)
	if _, ok := ann[ManualRolloutAnnotation]; ok {
		t.Errorf("pod-template annotation %q must NOT be projected when the CR has no manual-rollout request; got %q",
			ManualRolloutAnnotation, ann[ManualRolloutAnnotation])
	}
	// The companion ConfigHashAnnotation must still land — the manual-
	// rollout projection is additive, never displacing.
	if ann[ConfigHashAnnotation] != testCMHash {
		t.Errorf("ConfigHashAnnotation = %q, want %q", ann[ConfigHashAnnotation], testCMHash)
	}
}

func TestBuildValkeySTS_ManualRolloutAnnotation_ProjectedWhenSet(t *testing.T) {
	t.Parallel()
	v := minimalCR()
	v.Annotations = map[string]string{ManualRolloutAnnotation: "2026-05-11T13:00:00Z"}
	ann := stsAnnotations(v)
	if ann[ManualRolloutAnnotation] != "2026-05-11T13:00:00Z" {
		t.Errorf("pod-template %q = %q, want %q", ManualRolloutAnnotation,
			ann[ManualRolloutAnnotation], "2026-05-11T13:00:00Z")
	}
	if ann[ConfigHashAnnotation] != testCMHash {
		t.Errorf("ConfigHashAnnotation = %q, want %q", ann[ConfigHashAnnotation], testCMHash)
	}
}

func TestBuildValkeySTS_ManualRolloutAnnotation_EmptyValueNotProjected(t *testing.T) {
	t.Parallel()
	v := minimalCR()
	v.Annotations = map[string]string{ManualRolloutAnnotation: ""}
	ann := stsAnnotations(v)
	if _, ok := ann[ManualRolloutAnnotation]; ok {
		t.Errorf("empty manual-rollout value must NOT be projected (would create churn on every reconcile); got %q",
			ann[ManualRolloutAnnotation])
	}
}

func TestBuildValkeySTS_ManualRolloutAnnotation_DistinctValuesProduceDistinctTemplates(t *testing.T) {
	t.Parallel()
	v1 := minimalCR()
	v1.Annotations = map[string]string{ManualRolloutAnnotation: "value-A"}
	v2 := minimalCR()
	v2.Annotations = map[string]string{ManualRolloutAnnotation: "value-B"}

	got1 := stsAnnotations(v1)[ManualRolloutAnnotation]
	got2 := stsAnnotations(v2)[ManualRolloutAnnotation]
	if got1 == got2 {
		t.Errorf("distinct manual-rollout values must surface in pod-template annotations distinctly; got both = %q", got1)
	}
}
