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
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func newDeletionScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 add to scheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("valkeyv1beta1 add to scheme: %v", err)
	}
	return s
}

// deletionTestNS is the single namespace these tests pin objects in;
// reconcileDeletion is namespace-scoped via v.Namespace and there is no
// cross-namespace logic to exercise, so every helper hardcodes this
// namespace internally rather than threading it through as a parameter.
const deletionTestNS = "test-ns"

func makeDeletingCR(name string, policy valkeyv1beta1.PVCRetentionPolicy) *valkeyv1beta1.Valkey {
	now := metav1.Now()
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         deletionTestNS,
			UID:               types.UID("uid-" + name),
			DeletionTimestamp: &now,
			Finalizers:        []string{PVCRetentionFinalizer},
		},
		Spec: valkeyv1beta1.ValkeySpec{PVCRetentionPolicy: policy},
	}
}

func makePVCForCR(name, crName string, withOwnerRef bool, ownerUID types.UID) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: deletionTestNS,
			Labels:    map[string]string{CRLabel: crName},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if withOwnerRef {
		pvc.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: valkeyv1beta1.GroupVersion.String(),
				Kind:       "Valkey",
				Name:       crName,
				UID:        ownerUID,
				Controller: new(true),
			},
		}
	}
	return pvc
}

// TestReconcileDeletion_RetainPolicy_AggregatesErrorsAndContinues verifies
// that under the Retain policy a transient Patch failure on one PVC does
// NOT short-circuit the loop: the remaining PVCs are still visited, the
// error is aggregated, and the finalizer stays on the CR (so the next
// reconcile retries).
func TestReconcileDeletion_RetainPolicy_AggregatesErrorsAndContinues(t *testing.T) {
	const crName = "valkey-retain"

	cr := makeDeletingCR(crName, valkeyv1beta1.PVCRetentionRetain)
	pvcA := makePVCForCR("data-"+crName+"-0", crName, true, cr.UID)
	pvcB := makePVCForCR("data-"+crName+"-1", crName, true, cr.UID)
	pvcC := makePVCForCR("data-"+crName+"-2", crName, true, cr.UID)

	patchedNames := map[string]bool{}
	scheme := newDeletionScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, pvcA, pvcB, pvcC).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					patchedNames[pvc.Name] = true
					if pvc.Name == pvcB.Name {
						return errors.New("synthetic apiserver hiccup on pvcB")
					}
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).Build()

	r := &ValkeyReconciler{Client: c}
	err := r.reconcileDeletion(context.Background(), cr)
	if err == nil {
		t.Fatalf("reconcileDeletion returned nil; want aggregated error from pvcB")
	}
	if !strings.Contains(err.Error(), pvcB.Name) {
		t.Errorf("error %q does not mention failing pvc %q", err, pvcB.Name)
	}
	if !patchedNames[pvcA.Name] || !patchedNames[pvcB.Name] || !patchedNames[pvcC.Name] {
		t.Errorf("not all PVCs were visited: %v", patchedNames)
	}

	// Finalizer must still be on the CR — the apiserver-side object
	// (refreshed via Get) continues to carry it because reconcileDeletion
	// returned before the finalizer-strip step.
	got := &valkeyv1beta1.Valkey{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: crName, Namespace: deletionTestNS}, got); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	if !controllerutil.ContainsFinalizer(got, PVCRetentionFinalizer) {
		t.Errorf("finalizer was stripped despite PVC patch failure")
	}

	// Successful PVCs (A and C) had their owner-ref stripped; the failed
	// one (B) still carries it — the next reconcile will retry just B.
	checkOwnerRef := func(name string, want bool) {
		t.Helper()
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: deletionTestNS}, pvc); err != nil {
			t.Fatalf("get pvc %s: %v", name, err)
		}
		got := false
		for _, o := range pvc.OwnerReferences {
			if o.UID == cr.UID {
				got = true
			}
		}
		if got != want {
			t.Errorf("pvc %s ownerref present=%v want=%v", name, got, want)
		}
	}
	checkOwnerRef(pvcA.Name, false)
	checkOwnerRef(pvcB.Name, true)
	checkOwnerRef(pvcC.Name, false)
}

// TestReconcileDeletion_DeletePolicy_AggregatesErrorsAndContinues mirrors the
// Retain case for the Delete policy: a transient Patch failure on one PVC
// must not abort owner-ref injection on the rest.
func TestReconcileDeletion_DeletePolicy_AggregatesErrorsAndContinues(t *testing.T) {
	const crName = "valkey-delete"

	cr := makeDeletingCR(crName, valkeyv1beta1.PVCRetentionDelete)
	pvcA := makePVCForCR("data-"+crName+"-0", crName, false, "")
	pvcB := makePVCForCR("data-"+crName+"-1", crName, false, "")
	pvcC := makePVCForCR("data-"+crName+"-2", crName, false, "")

	patchedNames := map[string]bool{}
	scheme := newDeletionScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, pvcA, pvcB, pvcC).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					patchedNames[pvc.Name] = true
					if pvc.Name == pvcB.Name {
						return errors.New("synthetic apiserver hiccup on pvcB")
					}
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).Build()

	r := &ValkeyReconciler{Client: c}
	err := r.reconcileDeletion(context.Background(), cr)
	if err == nil {
		t.Fatalf("reconcileDeletion returned nil; want aggregated error from pvcB")
	}
	if !strings.Contains(err.Error(), pvcB.Name) {
		t.Errorf("error %q does not mention failing pvc %q", err, pvcB.Name)
	}
	if !patchedNames[pvcA.Name] || !patchedNames[pvcB.Name] || !patchedNames[pvcC.Name] {
		t.Errorf("not all PVCs were visited: %v", patchedNames)
	}

	got := &valkeyv1beta1.Valkey{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: crName, Namespace: deletionTestNS}, got); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	if !controllerutil.ContainsFinalizer(got, PVCRetentionFinalizer) {
		t.Errorf("finalizer was stripped despite PVC patch failure")
	}
}

// TestReconcileDeletion_AllPVCsSucceed_FinalizerRemoved is regression-
// protection for the success path: when every per-PVC patch lands cleanly
// the finalizer must be stripped so the apiserver can drop the CR.
func TestReconcileDeletion_AllPVCsSucceed_FinalizerRemoved(t *testing.T) {
	const crName = "valkey-clean"

	cr := makeDeletingCR(crName, valkeyv1beta1.PVCRetentionRetain)
	pvcA := makePVCForCR("data-"+crName+"-0", crName, true, cr.UID)
	pvcB := makePVCForCR("data-"+crName+"-1", crName, true, cr.UID)

	scheme := newDeletionScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, pvcA, pvcB).Build()

	r := &ValkeyReconciler{Client: c}
	if err := r.reconcileDeletion(context.Background(), cr); err != nil {
		t.Fatalf("reconcileDeletion: %v", err)
	}

	// With DeletionTimestamp set + finalizer removed the fake client (like
	// the apiserver) GCs the CR. Either NotFound or "still present without
	// the finalizer" both prove the finalizer-strip step ran.
	got := &valkeyv1beta1.Valkey{}
	err := c.Get(context.Background(), types.NamespacedName{Name: crName, Namespace: deletionTestNS}, got)
	if err == nil {
		if controllerutil.ContainsFinalizer(got, PVCRetentionFinalizer) {
			t.Errorf("finalizer should be removed when all PVCs patched cleanly")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("get CR: %v", err)
	}
}

// TestReconcileDeletion_MultipleFailures_AllReported ensures errors.Join
// preserves every per-PVC error so an operator tailing logs sees all
// failure root causes in a single reconcile, not just the first one.
func TestReconcileDeletion_MultipleFailures_AllReported(t *testing.T) {
	const crName = "valkey-multierr"

	cr := makeDeletingCR(crName, valkeyv1beta1.PVCRetentionRetain)
	pvcA := makePVCForCR("data-"+crName+"-0", crName, true, cr.UID)
	pvcB := makePVCForCR("data-"+crName+"-1", crName, true, cr.UID)

	scheme := newDeletionScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, pvcA, pvcB).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return errors.New("synthetic failure on " + obj.GetName())
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).Build()

	r := &ValkeyReconciler{Client: c}
	err := r.reconcileDeletion(context.Background(), cr)
	if err == nil {
		t.Fatalf("reconcileDeletion returned nil; want aggregated errors")
	}
	msg := err.Error()
	if !strings.Contains(msg, pvcA.Name) || !strings.Contains(msg, pvcB.Name) {
		t.Errorf("aggregated error %q must mention both failing PVCs %q and %q", msg, pvcA.Name, pvcB.Name)
	}
}
