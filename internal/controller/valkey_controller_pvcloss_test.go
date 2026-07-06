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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

const pvcLossTestCRName = "vk0"

// pvcLossCR builds a minimal Valkey CR with the supplied
// pvcRetentionPolicy + finalizer-state. Helper-unit tests start
// from this skeleton and overlay annotations.
func pvcLossCR(policy valkeyv1beta1.PVCRetentionPolicy, withFinalizer bool) *valkeyv1beta1.Valkey {
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: pvcLossTestCRName, Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			PVCRetentionPolicy: policy,
		},
	}
	if withFinalizer {
		v.Finalizers = []string{PVCRetentionFinalizer}
	}
	return v
}

func newPVCForTestCR(podName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "data-" + pvcLossTestCRName + "-" + podName,
			Labels:    map[string]string{CRLabel: pvcLossTestCRName},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
}

func newSTSForTestCR() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      pvcLossTestCRName,
		},
	}
}

// gateReconciler returns a ValkeyReconciler wired against a fake
// client containing the supplied objects (STS, PVCs, etc.). The
// reconciler is intentionally minimal — only the Client field is
// used by detectPVCLossAndGate.
func gateReconciler(t *testing.T, objs ...client.Object) *ValkeyReconciler {
	t.Helper()
	s := pvcResizeTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &ValkeyReconciler{Client: c}
}

func TestDetectPVCLoss_FreshBootstrap_Allows(t *testing.T) {
	// Without the finalizer, the CR is in its first reconcile —
	// STS+PVC absence is the bootstrap shape, not disaster.
	v := pvcLossCR(valkeyv1beta1.PVCRetentionRetain, false)
	r := gateReconciler(t, v)

	proceed, err := r.detectPVCLossAndGate(context.Background(), v, false)
	if !proceed {
		t.Errorf("first-reconcile bootstrap was gated: err=%v", err)
	}
	if err != nil {
		t.Errorf("unexpected statusErr on bootstrap path: %v", err)
	}
}

func TestDetectPVCLoss_DeletePolicy_Allows(t *testing.T) {
	// Delete-policy CRs document PVC loss as a deletion consequence;
	// the gate doesn't apply.
	v := pvcLossCR(valkeyv1beta1.PVCRetentionDelete, true)
	r := gateReconciler(t, v)

	proceed, err := r.detectPVCLossAndGate(context.Background(), v, true)
	if !proceed {
		t.Errorf("Delete-policy CR was gated: err=%v", err)
	}
	if err != nil {
		t.Errorf("unexpected statusErr on Delete-policy path: %v", err)
	}
}

func TestDetectPVCLoss_STSExists_Allows(t *testing.T) {
	// STS present means PVCs are either present or pending —
	// not the disaster shape the gate is for.
	v := pvcLossCR(valkeyv1beta1.PVCRetentionRetain, true)
	r := gateReconciler(t, v, newSTSForTestCR())

	proceed, err := r.detectPVCLossAndGate(context.Background(), v, true)
	if !proceed {
		t.Errorf("STS-present CR was gated: err=%v", err)
	}
	if err != nil {
		t.Errorf("unexpected statusErr with STS present: %v", err)
	}
}

func TestDetectPVCLoss_OrphanPVCsAllowed(t *testing.T) {
	// STS gone, but matching PVCs survive (e.g., user deleted STS
	// only). Recovery would adopt the existing PVCs — no data loss.
	v := pvcLossCR(valkeyv1beta1.PVCRetentionRetain, true)
	r := gateReconciler(t, v, newPVCForTestCR("0"), newPVCForTestCR("1"))

	proceed, err := r.detectPVCLossAndGate(context.Background(), v, true)
	if !proceed {
		t.Errorf("orphan-PVC adoption path was gated: err=%v", err)
	}
	if err != nil {
		t.Errorf("unexpected statusErr on orphan-PVC path: %v", err)
	}
}

func TestDetectPVCLoss_TerminatingPVCsCountAsAbsent(t *testing.T) {
	// User did `kubectl delete sts && kubectl delete pvc` and the
	// reconciler runs while the PVCs are still finalising
	// (DeletionTimestamp set, not yet GC'd). The gate must treat
	// those as absent — they will be gone any moment, and the user
	// has effectively destroyed the data. A naive
	// `len(pvcs.Items) > 0` check would let recovery proceed
	// silently, which is the data-loss footgun the gate exists to
	// prevent.
	v := pvcLossCR(valkeyv1beta1.PVCRetentionRetain, true)
	deletingPVC := newPVCForTestCR("0")
	now := metav1.Now()
	deletingPVC.DeletionTimestamp = &now
	deletingPVC.Finalizers = []string{"kubernetes.io/pvc-protection"}
	r := gateReconciler(t, v, deletingPVC)

	proceed, statusErr := r.detectPVCLossAndGate(context.Background(), v, true)
	if proceed {
		t.Error("Terminating PVC was treated as live; gate did not fire")
	}
	if statusErr == nil {
		t.Fatal("expected statusErr when only Terminating PVCs remain")
	}
}

func TestDetectPVCLoss_NoAnnotation_Refuses(t *testing.T) {
	// Disaster shape (STS gone + PVCs gone) on a Retain CR that was
	// previously bootstrapped — refuse silent recovery.
	v := pvcLossCR(valkeyv1beta1.PVCRetentionRetain, true)
	r := gateReconciler(t, v)

	proceed, statusErr := r.detectPVCLossAndGate(context.Background(), v, true)
	if proceed {
		t.Error("disaster shape without annotation was allowed; expected refuse")
	}
	if statusErr == nil {
		t.Fatal("expected statusErr describing the gate refusal")
	}
	if !strings.Contains(statusErr.Error(), AcceptPVCLossAnnotation) {
		t.Errorf("statusErr does not name the annotation key: %v", statusErr)
	}
}

func TestDetectPVCLoss_AnnotationTrue_Allows(t *testing.T) {
	// Disaster shape WITH `accept-pvc-loss=true` → gate consumes,
	// reconcile proceeds (Phase 2 will create fresh STS+PVCs).
	v := pvcLossCR(valkeyv1beta1.PVCRetentionRetain, true)
	v.Annotations = map[string]string{
		AcceptPVCLossAnnotation:          "true",
		AcceptPVCLossRequestorAnnotation: "alice",
	}
	r := gateReconciler(t, v)

	proceed, err := r.detectPVCLossAndGate(context.Background(), v, true)
	if !proceed {
		t.Errorf("annotation=true was rejected: err=%v", err)
	}
	if err != nil {
		t.Errorf("unexpected statusErr on consume: %v", err)
	}
}

func TestDetectPVCLoss_AnnotationNonTrue_Refuses(t *testing.T) {
	// The webhook validator (Step 1) rejects
	// non-"true" values, so this case shouldn't normally reach the
	// reconciler — but if it does (e.g., webhook bypassed or pre-
	// existing CR), the gate must still refuse silent recovery.
	v := pvcLossCR(valkeyv1beta1.PVCRetentionRetain, true)
	v.Annotations = map[string]string{AcceptPVCLossAnnotation: "True"}
	r := gateReconciler(t, v)

	proceed, statusErr := r.detectPVCLossAndGate(context.Background(), v, true)
	if proceed {
		t.Error("non-\"true\" annotation value was treated as authorization")
	}
	if statusErr == nil {
		t.Fatal("expected statusErr on non-\"true\" annotation path")
	}
}

func TestDetectPVCLoss_DefaultPolicy_Retain(t *testing.T) {
	// Empty PVCRetentionPolicy must default to Retain (the CRD's
	// kubebuilder default). The gate must fire under this default.
	v := pvcLossCR("", true)
	r := gateReconciler(t, v)

	proceed, statusErr := r.detectPVCLossAndGate(context.Background(), v, true)
	if proceed {
		t.Error("default policy (empty) treated as non-Retain; gate did not fire")
	}
	if statusErr == nil {
		t.Fatal("expected statusErr under default policy")
	}
}
