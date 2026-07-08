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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ctrl "sigs.k8s.io/controller-runtime"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Tests for the CR-removal cleanup path: when a CR is removed from the
// apiserver (NotFound branch), the per-CR sync.Map prunes must drop the
// deleted CR's in-memory state-bag entry without disturbing other CRs.

func newDeferralScheme(t *testing.T) *runtime.Scheme {
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

// TestReconcile_NotFound_PrunesTrackerMaps pins that the Phase 0a
// NotFound cleanup drops the deleted CR's per-CR state-bag entry
// (consolidated the former per-tracker maps into perCR).
// Fails-before: the sqStatusDigests + missingAuthSecretFirstSeen maps
// were omitted from the cleanup branch and leaked one entry per deleted
// CR. An unrelated CR's state must survive.
func TestReconcile_NotFound_PrunesTrackerMaps(t *testing.T) {
	t.Parallel()
	scheme := newDeferralScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build() // no CR present
	r := &ValkeyReconciler{Client: c, Scheme: scheme}

	gone := types.NamespacedName{Namespace: "ns", Name: "vk-gone"}
	other := types.NamespacedName{Namespace: "ns", Name: "vk-other"}
	goneState := r.stateFor(gone)
	goneState.sqStatusDigest = "digest"
	goneState.missingAuthSeen = time.Now()
	otherState := r.stateFor(other)
	otherState.sqStatusDigest = "keep"
	otherState.missingAuthSeen = time.Now()

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: gone}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if _, ok := r.perCR.Load(gone); ok {
		t.Error("perCR: state for the deleted CR was not pruned")
	}
	if _, ok := r.perCR.Load(other); !ok {
		t.Error("perCR: an unrelated CR's state was wrongly pruned")
	}
}
