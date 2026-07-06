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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPatchReplicationCondition_StampsObservedGeneration(t *testing.T) {
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("scheme.AddToScheme: %v", err)
	}

	cases := []struct {
		name       string
		generation int64
		existing   []corev1.PodCondition
		desired    corev1.ConditionStatus
		message    string
	}{
		{
			name:       "fresh stamp on pod with no prior gate condition",
			generation: 7,
			existing:   nil,
			desired:    corev1.ConditionTrue,
			message:    "link up; lag 0 bytes",
		},
		{
			name:       "replace existing condition propagates current generation",
			generation: 12,
			existing: []corev1.PodCondition{{
				Type:               ReplicationReadyGate,
				Status:             corev1.ConditionFalse,
				Message:            "stale",
				ObservedGeneration: 5,
			}},
			desired: corev1.ConditionTrue,
			message: "link up; lag 0 bytes",
		},
		{
			name:       "generation zero stamps as zero, not skipped",
			generation: 0,
			existing:   nil,
			desired:    corev1.ConditionFalse,
			message:    "master_link_status:down",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  "ns",
					Name:       "vk-0",
					Generation: tc.generation,
				},
				Status: corev1.PodStatus{Conditions: tc.existing},
			}
			c := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(p).
				WithStatusSubresource(&corev1.Pod{}).
				Build()
			ctx := context.Background()

			if !patchReplicationCondition(ctx, c, p, tc.desired, tc.message) {
				t.Fatalf("patchReplicationCondition reported no-op; expected a write")
			}

			got := &corev1.Pod{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "vk-0"}, got); err != nil {
				t.Fatalf("Get pod: %v", err)
			}

			cond := findReplicationCondition(got)
			if cond == nil {
				t.Fatalf("ReplicationReadyGate condition missing from refreshed pod")
				return
			}
			if cond.ObservedGeneration != tc.generation {
				t.Errorf("ObservedGeneration = %d, want %d", cond.ObservedGeneration, tc.generation)
			}
			if cond.Status != tc.desired {
				t.Errorf("Status = %q, want %q", cond.Status, tc.desired)
			}
			if cond.Message != tc.message {
				t.Errorf("Message = %q, want %q", cond.Message, tc.message)
			}
		})
	}
}

func TestPatchReplicationCondition_NoOpDoesNotRestampObservedGeneration(t *testing.T) {
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("scheme.AddToScheme: %v", err)
	}
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk-0", Generation: 3},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:               ReplicationReadyGate,
			Status:             corev1.ConditionTrue,
			Message:            "link up; lag 0 bytes",
			ObservedGeneration: 1,
		}}},
	}
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(p).
		WithStatusSubresource(&corev1.Pod{}).
		Build()
	ctx := context.Background()

	if patchReplicationCondition(ctx, c, p, corev1.ConditionTrue, "link up; lag 0 bytes") {
		t.Fatalf("patchReplicationCondition reported a write; expected no-op")
	}

	got := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "vk-0"}, got); err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	cond := findReplicationCondition(got)
	if cond == nil {
		t.Fatalf("condition missing")
		return
	}
	if cond.ObservedGeneration != 1 {
		t.Errorf("ObservedGeneration = %d, want 1 (no-op must not re-stamp)", cond.ObservedGeneration)
	}
}
