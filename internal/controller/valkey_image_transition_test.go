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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Tests for the runtime image-transition check
// (`checkValkeyImageTransition`). Drives the gate directly with
// hand-built (CR, STS) pairs and asserts (allow|reject, event-fired).

func newCRWithImage(repo, tag string) *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk0", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Image: valkeyv1beta1.ImageSpec{
				Valkey: valkeyv1beta1.ContainerImage{Repository: repo, Tag: tag},
			},
		},
	}
}

func newSTSWithValkeyImage(image string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "vk0", Namespace: "ns"},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "valkey", Image: image},
					},
				},
			},
		},
	}
}

func TestCheckValkeyImageTransition_AllowsEqualImage(t *testing.T) {
	t.Parallel()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "8.1.6-alpine")
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("equal image must allow")
	}
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Errorf("equal image must not emit events; got %v", got)
	}
}

func TestCheckValkeyImageTransition_AllowsMinorBumpByOne(t *testing.T) {
	t.Parallel()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "8.2.0")
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("minor +1 must allow")
	}
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Errorf("minor +1 must not emit events; got %v", got)
	}
}

func TestCheckValkeyImageTransition_WarnsOnSkipMinor(t *testing.T) {
	t.Parallel()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "8.5.0")
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("skip-minor must allow (it is non-blocking)")
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 || !strings.Contains(got[0], "ValkeyImageTransitionWarning") {
		t.Errorf("expected one ValkeyImageTransitionWarning, got %v", got)
	}
}

func TestCheckValkeyImageTransition_RejectsMajorDowngrade(t *testing.T) {
	t.Parallel()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "7.4.0")
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("major downgrade must reject")
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 || !strings.Contains(got[0], "ValkeyImageTransitionRejected") {
		t.Errorf("expected one ValkeyImageTransitionRejected, got %v", got)
	}
}

func TestCheckValkeyImageTransition_AllowsMajorUpgrade(t *testing.T) {
	t.Parallel()
	// Note: the static admission check already rejects unsupported
	// majors (e.g., 9.x today). At runtime, if a 9.x image somehow
	// reached the STS comparison, the transition rules don't gate
	// major-up — only major-DOWN is the blocking rule. The static
	// admission check is the right fence, not the runtime check.
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "9.0.0")
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("major upgrade is not gated by the runtime rules; admission is responsible")
	}
}

func TestCheckValkeyImageTransition_AllowsCustomTagSilently(t *testing.T) {
	t.Parallel()
	// Custom-build tags that don't parse as Valkey major.minor are
	// neither allowed nor rejected by version-compat — the operator
	// declines to enforce rules on tags it can't reason about. Allow
	// silently.
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("internal/valkey", "custom-build-7afef00")
	sts := newSTSWithValkeyImage("internal/valkey:other-custom-build")
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("unparseable tags must allow")
	}
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Errorf("unparseable tags must not emit events; got %v", got)
	}
}

func TestCheckValkeyImageTransition_AllowsSTSWithNoValkeyContainer(t *testing.T) {
	t.Parallel()
	// Defensive: if some other reconciler manages an STS with our name
	// but no `valkey` container (shouldn't happen post-bootstrap),
	// don't gate on a comparison we can't make.
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "7.0.0")
	sts := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "other", Image: "other:1.0"}},
			}},
		},
	}
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("missing valkey container must allow (nothing to compare)")
	}
	if got := drainAllEvents(rec.Events); len(got) != 0 {
		t.Errorf("missing valkey container must not emit events; got %v", got)
	}
}

func TestCheckValkeyImageTransition_NilRecorderDoesNotPanic(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{} // Recorder is nil.
	cr := newCRWithImage("valkey/valkey", "7.4.0")
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	// Should not panic on nil Recorder; should still return false on
	// the major-downgrade rule.
	if r.checkValkeyImageTransition(cr, sts) {
		t.Errorf("major downgrade must reject even with nil Recorder")
	}
}

// TestCheckValkeyImageTransition_UpgradePreflightFalseBypassesDowngrade
// pins the UpgradePreflight gate's opt-out contract: explicit
// `spec.featureGates.UpgradePreflight=false` opts out of the major-
// downgrade rejection. The override path must (a) return true so the
// STS apply proceeds, (b) emit ValkeyImageTransitionOverridden so
// the operator audit trail records the bypass, and (c) NOT emit
// ValkeyImageTransitionRejected (the user explicitly chose to skip
// it; emitting both would be noise).
func TestCheckValkeyImageTransition_UpgradePreflightFalseBypassesDowngrade(t *testing.T) {
	t.Parallel()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "7.4.0")
	cr.Spec.FeatureGates = map[string]bool{FeatureGateUpgradePreflight: false}
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Fatalf("UpgradePreflight=false must bypass downgrade rejection")
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 || !strings.Contains(got[0], "ValkeyImageTransitionOverridden") {
		t.Fatalf("expected one ValkeyImageTransitionOverridden, got %v", got)
	}
	for _, e := range got {
		if strings.Contains(e, "ValkeyImageTransitionRejected") {
			t.Errorf("override path must not emit ValkeyImageTransitionRejected; got %q", e)
		}
	}
}

// TestCheckValkeyImageTransition_UpgradePreflightTrueKeepsDowngradeRejection
// pins the explicit-true case: writing the gate as true is equivalent
// to leaving it absent — the rejection arm still fires. Without this
// pin, a future refactor that flipped the comma-ok check semantics
// (e.g., to "any presence disables") would silently weaken the gate.
func TestCheckValkeyImageTransition_UpgradePreflightTrueKeepsDowngradeRejection(t *testing.T) {
	t.Parallel()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "7.4.0")
	cr.Spec.FeatureGates = map[string]bool{FeatureGateUpgradePreflight: true}
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if r.checkValkeyImageTransition(cr, sts) {
		t.Fatalf("UpgradePreflight=true must keep the downgrade rejection (equivalent to absent)")
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 || !strings.Contains(got[0], "ValkeyImageTransitionRejected") {
		t.Fatalf("expected one ValkeyImageTransitionRejected, got %v", got)
	}
}

// TestCheckValkeyImageTransition_UpgradePreflightFalseDoesNotAffectSkipMinor
// pins the gate's scope: it only governs the downgrade-rejection arm,
// not the skip-minor warning. Skip-minor proceeds with a Warning
// regardless of the gate's state.
func TestCheckValkeyImageTransition_UpgradePreflightFalseDoesNotAffectSkipMinor(t *testing.T) {
	t.Parallel()
	rec := k8sevents.NewFakeRecorder(8)
	r := &ValkeyReconciler{Recorder: rec}
	cr := newCRWithImage("valkey/valkey", "8.5.0")
	cr.Spec.FeatureGates = map[string]bool{FeatureGateUpgradePreflight: false}
	sts := newSTSWithValkeyImage("valkey/valkey:8.1.6-alpine")
	if !r.checkValkeyImageTransition(cr, sts) {
		t.Fatalf("skip-minor must allow (UpgradePreflight=false doesn't change this)")
	}
	got := drainAllEvents(rec.Events)
	if len(got) != 1 || !strings.Contains(got[0], "ValkeyImageTransitionWarning") {
		t.Fatalf("expected one ValkeyImageTransitionWarning on skip-minor; got %v", got)
	}
	for _, e := range got {
		if strings.Contains(e, "ValkeyImageTransitionOverridden") {
			t.Errorf("gate must not produce an Overridden event on skip-minor; got %q", e)
		}
	}
}
