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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// Pure-data unit tests for the gate-mutation helpers.
// The integrated `applyRoleTransitionGate` (which calls Patch) is
// envtest territory; these tests pin the slice-shape mutations the
// gate-lifecycle path depends on so a regression in the strip /
// add / contains primitives doesn't sneak in.

func TestPodHasReadinessGate(t *testing.T) {
	cases := []struct {
		name  string
		gates []corev1.PodReadinessGate
		ct    corev1.PodConditionType
		want  bool
	}{
		{"empty slice → false", nil, ReplicationReadyGate, false},
		{"missing → false", []corev1.PodReadinessGate{{ConditionType: "other"}}, ReplicationReadyGate, false},
		{"single match → true", []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}}, ReplicationReadyGate, true},
		{"first of many → true", []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}, {ConditionType: "other"}}, ReplicationReadyGate, true},
		{"last of many → true", []corev1.PodReadinessGate{{ConditionType: "other"}, {ConditionType: ReplicationReadyGate}}, ReplicationReadyGate, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{ReadinessGates: tc.gates}}
			if got := podHasReadinessGate(pod, tc.ct); got != tc.want {
				t.Errorf("podHasReadinessGate = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStripReadinessGate(t *testing.T) {
	cases := []struct {
		name string
		in   []corev1.PodReadinessGate
		ct   corev1.PodConditionType
		want []corev1.PodReadinessGate
	}{
		{
			name: "empty slice → empty result",
			in:   nil,
			ct:   ReplicationReadyGate,
			want: []corev1.PodReadinessGate{},
		},
		{
			name: "no match → input preserved (length)",
			in:   []corev1.PodReadinessGate{{ConditionType: "other"}},
			ct:   ReplicationReadyGate,
			want: []corev1.PodReadinessGate{{ConditionType: "other"}},
		},
		{
			name: "single match → empty result",
			in:   []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}},
			ct:   ReplicationReadyGate,
			want: []corev1.PodReadinessGate{},
		},
		{
			name: "first of many removed",
			in:   []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}, {ConditionType: "other"}},
			ct:   ReplicationReadyGate,
			want: []corev1.PodReadinessGate{{ConditionType: "other"}},
		},
		{
			name: "last of many removed",
			in:   []corev1.PodReadinessGate{{ConditionType: "other"}, {ConditionType: ReplicationReadyGate}},
			ct:   ReplicationReadyGate,
			want: []corev1.PodReadinessGate{{ConditionType: "other"}},
		},
		{
			name: "duplicate match → all removed",
			in:   []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}, {ConditionType: ReplicationReadyGate}, {ConditionType: "other"}},
			ct:   ReplicationReadyGate,
			want: []corev1.PodReadinessGate{{ConditionType: "other"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stripReadinessGate(tc.in, tc.ct)
			if len(got) != len(tc.want) {
				t.Errorf("len = %d, want %d", len(got), len(tc.want))
				return
			}
			if len(got) > 0 && !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestStripPodCondition(t *testing.T) {
	cases := []struct {
		name string
		in   []corev1.PodCondition
		ct   corev1.PodConditionType
		want []corev1.PodCondition
	}{
		{
			name: "empty → empty",
			in:   nil,
			ct:   ReplicationReadyGate,
			want: []corev1.PodCondition{},
		},
		{
			name: "no match → preserved",
			in:   []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ct:   ReplicationReadyGate,
			want: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
		{
			name: "single match removed",
			in:   []corev1.PodCondition{{Type: ReplicationReadyGate, Status: corev1.ConditionTrue}},
			ct:   ReplicationReadyGate,
			want: []corev1.PodCondition{},
		},
		{
			name: "match removed, others preserved",
			in: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				{Type: ReplicationReadyGate, Status: corev1.ConditionTrue},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
			},
			ct: ReplicationReadyGate,
			want: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stripPodCondition(tc.in, tc.ct)
			if len(got) != len(tc.want) {
				t.Errorf("len = %d, want %d", len(got), len(tc.want))
				return
			}
			if len(got) > 0 && !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestStripReadinessGate_DoesNotMutateInput(t *testing.T) {
	// The helper allocates a fresh slice (`gates[:0:0]`) so
	// callers can use the result as the new pod.Spec.ReadinessGates
	// without surprising the original pod object the caller still
	// holds. Pin this contract — a future "in-place compaction"
	// regression would have the caller's `old.DeepCopy()` snapshot
	// silently shift under it, breaking the StrategicMergeFrom diff.
	in := []corev1.PodReadinessGate{
		{ConditionType: ReplicationReadyGate},
		{ConditionType: "other"},
	}
	inCopy := append([]corev1.PodReadinessGate(nil), in...)
	_ = stripReadinessGate(in, ReplicationReadyGate)
	if !reflect.DeepEqual(in, inCopy) {
		t.Errorf("stripReadinessGate mutated input: got %+v, want %+v", in, inCopy)
	}
}

func TestStripPodCondition_DoesNotMutateInput(t *testing.T) {
	in := []corev1.PodCondition{
		{Type: ReplicationReadyGate, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}
	inCopy := append([]corev1.PodCondition(nil), in...)
	_ = stripPodCondition(in, ReplicationReadyGate)
	if !reflect.DeepEqual(in, inCopy) {
		t.Errorf("stripPodCondition mutated input: got %+v, want %+v", in, inCopy)
	}
}
