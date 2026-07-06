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

	corev1 "k8s.io/api/core/v1"
)

// TestPodReplicationHealthy pins the four branches of the replica
// rollout gate. A non-standalone CR's replica is
// "advance-safe" only when both PodReady AND the replication-ready
// gate are True; the gate may be absent (CR opted out) in which
// case the helper reports true so the rollout isn't blocked.
func TestPodReplicationHealthy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		conditions []corev1.PodCondition
		want       bool
	}{
		{
			name:       "no conditions at all → true (gate absent, opt-out path)",
			conditions: nil,
			want:       true,
		},
		{
			name: "only PodReady, no replication gate → true (opt-out CR)",
			conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			want: true,
		},
		{
			name: "replication gate True → true",
			conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: ReplicationReadyGate, Status: corev1.ConditionTrue},
			},
			want: true,
		},
		{
			name: "replication gate False → false (still catching up)",
			conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: ReplicationReadyGate, Status: corev1.ConditionFalse},
			},
			want: false,
		},
		{
			name: "replication gate Unknown → false (treat as not-yet-healthy)",
			conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: ReplicationReadyGate, Status: corev1.ConditionUnknown},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pod := &corev1.Pod{
				Status: corev1.PodStatus{Conditions: tc.conditions},
			}
			if got := podReplicationHealthy(pod); got != tc.want {
				t.Errorf("podReplicationHealthy = %v, want %v", got, tc.want)
			}
		})
	}
}
