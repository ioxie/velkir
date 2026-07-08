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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func newRolloutTriggerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme corev1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme appsv1: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme valkey: %v", err)
	}
	return scheme
}

// newRolloutTriggerFixture constructs a reconciler whose fake client
// holds a StatefulSet with the given UpdateRevision / CurrentRevision
// status. fake.NewClientBuilder ignores Status fields on construction,
// so the status is applied via a follow-up Status().Update.
func newRolloutTriggerFixture(t *testing.T, scheme *runtime.Scheme, target, current string) (*ValkeyReconciler, *appsv1.StatefulSet, client.Client) {
	t.Helper()
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "ns"},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: target, CurrentRevision: current},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	_ = c.Status().Update(context.Background(), sts)
	return &ValkeyReconciler{Client: c, Scheme: scheme}, sts, c
}

func rolloutTriggerCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk", Namespace: "ns"},
	}
}

func TestRolloutTriggerEdge(t *testing.T) {
	scheme := newRolloutTriggerScheme(t)
	v := rolloutTriggerCR()

	t.Run("no STS observed → zero signal, no error", func(t *testing.T) {
		r := &ValkeyReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Scheme: scheme}
		got, err := r.rolloutTriggerEdge(context.Background(), v)
		if err != nil {
			t.Fatalf("err=%v, want nil (NotFound is non-error)", err)
		}
		if got.edge || got.midRolloutChange {
			t.Fatalf("got=%+v with no STS, want zero", got)
		}
	})

	t.Run("first observation, UpdateRevision == CurrentRevision → zero signal", func(t *testing.T) {
		r, _, _ := newRolloutTriggerFixture(t, scheme, "rev-A", "rev-A")
		got, err := r.rolloutTriggerEdge(context.Background(), v)
		if err != nil {
			t.Fatalf("err=%v, want nil", err)
		}
		if got.edge || got.midRolloutChange {
			t.Fatalf("got=%+v on stable STS, want zero (no rollout pending)", got)
		}
	})

	t.Run("first observation, UpdateRevision != CurrentRevision → edge fires", func(t *testing.T) {
		r, _, _ := newRolloutTriggerFixture(t, scheme, "rev-B", "rev-A")
		got, err := r.rolloutTriggerEdge(context.Background(), v)
		if err != nil {
			t.Fatalf("err=%v, want nil", err)
		}
		if !got.edge {
			t.Fatalf("got=%+v on first pending observation, want edge=true", got)
		}
		if got.midRolloutChange {
			t.Fatalf("got=%+v on fresh edge, want midRolloutChange=false", got)
		}
	})

	t.Run("second observation while still pending, same target → no signal (suppressed)", func(t *testing.T) {
		r, _, _ := newRolloutTriggerFixture(t, scheme, "rev-B", "rev-A")
		if _, err := r.rolloutTriggerEdge(context.Background(), v); err != nil {
			t.Fatalf("first call err=%v", err)
		}
		got, err := r.rolloutTriggerEdge(context.Background(), v)
		if err != nil {
			t.Fatalf("err=%v, want nil", err)
		}
		if got.edge || got.midRolloutChange {
			t.Fatalf("got=%+v on second in-flight observation, want zero", got)
		}
	})

	t.Run("UpdateRevision empty (pre-bootstrap) → zero signal", func(t *testing.T) {
		r, _, _ := newRolloutTriggerFixture(t, scheme, "", "")
		got, err := r.rolloutTriggerEdge(context.Background(), v)
		if err != nil {
			t.Fatalf("err=%v, want nil", err)
		}
		if got.edge || got.midRolloutChange {
			t.Fatalf("got=%+v on empty UpdateRevision, want zero", got)
		}
	})
}

// TestRolloutTriggerEdge_ReArmAfterCompletion drives the STS revision
// lifecycle through pending → complete → fresh-pending and checks the
// detector re-arms on the new pending edge.
func TestRolloutTriggerEdge_ReArmAfterCompletion(t *testing.T) {
	scheme := newRolloutTriggerScheme(t)
	v := rolloutTriggerCR()
	r, sts, c := newRolloutTriggerFixture(t, scheme, "rev-B", "rev-A")

	sig1, err := r.rolloutTriggerEdge(context.Background(), v)
	if err != nil {
		t.Fatalf("step 1 err=%v", err)
	}
	if !sig1.edge {
		t.Fatalf("step 1 got=%+v, want edge=true", sig1)
	}

	// Rollout completes (UpdateRevision == CurrentRevision again).
	sts.Status.CurrentRevision = "rev-B"
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("step 2 status update: %v", err)
	}
	sig2, err := r.rolloutTriggerEdge(context.Background(), v)
	if err != nil {
		t.Fatalf("step 2 err=%v", err)
	}
	if sig2.edge || sig2.midRolloutChange {
		t.Fatalf("step 2 got=%+v on rollout completion, want zero", sig2)
	}

	// New spec change lands (UpdateRevision diverges again).
	sts.Status.UpdateRevision = "rev-C"
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("step 3 status update: %v", err)
	}
	sig3, err := r.rolloutTriggerEdge(context.Background(), v)
	if err != nil {
		t.Fatalf("step 3 err=%v", err)
	}
	if !sig3.edge {
		t.Fatalf("step 3 got=%+v on new spec change after completion, want edge=true (re-armed)", sig3)
	}
	if sig3.midRolloutChange {
		t.Fatalf("step 3 got=%+v post-completion re-arm, want midRolloutChange=false", sig3)
	}
}

// TestRolloutTriggerEdge_MidRolloutSwap walks the lifecycle pending
// (rev-B → rev-A), then mid-rollout the user edits again so pending
// becomes (rev-C → rev-A) while CurrentRevision is still climbing.
// The detector must distinguish this from a clean re-arm.
func TestRolloutTriggerEdge_MidRolloutSwap(t *testing.T) {
	scheme := newRolloutTriggerScheme(t)
	v := rolloutTriggerCR()
	r, sts, c := newRolloutTriggerFixture(t, scheme, "rev-B", "rev-A")

	sig1, err := r.rolloutTriggerEdge(context.Background(), v)
	if err != nil {
		t.Fatalf("step 1 err=%v", err)
	}
	if !sig1.edge {
		t.Fatalf("step 1 got=%+v, want edge=true", sig1)
	}

	// Mid-rollout the user edits the CR — STS recomputes a new
	// UpdateRevision while CurrentRevision is still climbing.
	sts.Status.UpdateRevision = "rev-C"
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("step 2 status update: %v", err)
	}
	sig2, err := r.rolloutTriggerEdge(context.Background(), v)
	if err != nil {
		t.Fatalf("step 2 err=%v", err)
	}
	if sig2.edge {
		t.Fatalf("step 2 got=%+v on mid-rollout target swap, want edge=false (still pending)", sig2)
	}
	if !sig2.midRolloutChange {
		t.Fatalf("step 2 got=%+v on mid-rollout target swap, want midRolloutChange=true", sig2)
	}

	// Subsequent observation with the same new target must be
	// suppressed on BOTH flags.
	sig3, err := r.rolloutTriggerEdge(context.Background(), v)
	if err != nil {
		t.Fatalf("step 3 err=%v", err)
	}
	if sig3.edge || sig3.midRolloutChange {
		t.Fatalf("step 3 got=%+v on stable post-swap observation, want zero", sig3)
	}
}
