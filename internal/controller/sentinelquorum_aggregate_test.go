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
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// TestClampQuorumToPoolMajority pins the operator-side hardening: a
// sub-majority spec.sentinel.quorum is raised to the sentinel pool
// majority before it gates the quorum-lost verdict, while a quorum that
// already meets or exceeds the majority passes through untouched.
func TestClampQuorumToPoolMajority(t *testing.T) {
	cases := []struct {
		name             string
		quorum, replicas int32
		want             int32
	}{
		{"submajority 5/2 raised to 3", 2, 5, 3},
		{"submajority 7/2 raised to 4", 2, 7, 4},
		{"submajority 7/3 raised to 4", 3, 7, 4},
		{"submajority 4/2 raised to 3", 2, 4, 3},
		{"submajority 6/3 raised to 4", 3, 6, 4},
		{"unset quorum 5/0 raised to 3", 0, 5, 3},
		{"unset quorum 7/0 raised to 4", 0, 7, 4},
		{"single-vote 2/1 raised to 2", 1, 2, 2},
		{"majority 3/2 unchanged", 2, 3, 2},
		{"majority 5/3 unchanged", 3, 5, 3},
		{"above-majority 5/4 unchanged", 4, 5, 4},
		{"all-agree 7/7 unchanged", 7, 7, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampQuorumToPoolMajority(tc.quorum, tc.replicas); got != tc.want {
				t.Errorf("clampQuorumToPoolMajority(%d, %d) = %d, want %d",
					tc.quorum, tc.replicas, got, tc.want)
			}
		})
	}
}

// TestAggregateSentinelQuorum_ClampsSubMajorityVerdict pins the wiring:
// aggregateSentinelQuorum must pass the CLAMPED quorum into
// sqaggregate.Aggregate, not the raw spec value. With replicas=5 and a
// sub-majority quorum=2, only 2 of 5 fresh sentinels reporting reachable
// must still aggregate to QuorumLost — because the clamp raises the
// floor to the pool majority (3). Were the clamp removed, the raw
// quorum=2 would be met by the 2 reachable records and the verdict would
// flip to QuorumOK, so this test fails closed on a regression.
func TestAggregateSentinelQuorum_ClampsSubMajorityVerdict(t *testing.T) {
	s := runtime.NewScheme()
	if err := valkeyv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	const (
		ns   = "default"
		name = "vk-clamp"
	)
	v := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Sentinel: &valkeyv1beta1.SentinelPodSpec{
				Replicas:   5,
				Quorum:     2, // sub-majority: pool majority is 3
				MasterName: "mymaster",
			},
		},
	}

	// 5 fresh SentinelQuorum records; exactly 2 report reachable.
	objs := make([]client.Object, 0, 5)
	for i := range 5 {
		reachable := i < 2
		now := metav1.Now()
		objs = append(objs, &valkeyv1beta1.SentinelQuorum{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-sentinel-%d", name, i),
				Namespace: ns,
				Labels: map[string]string{
					CRLabel:        name,
					ComponentLabel: componentSentinel,
				},
			},
			Status: valkeyv1beta1.SentinelQuorumStatus{
				ObservedPrimary:  fmt.Sprintf("%s-0", name),
				QuorumReachable:  &reachable,
				LastObservedTime: &now,
			},
		})
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	r := &ValkeyReconciler{Client: c, Scheme: s}

	got := r.aggregateSentinelQuorum(context.Background(), v)
	if got.Quorum != sentinel.QuorumStatusLost {
		t.Errorf("Quorum = %v, want %v (2-of-5 reachable is below the clamped pool majority of 3)",
			got.Quorum, sentinel.QuorumStatusLost)
	}
	if !got.QuorumLost {
		t.Errorf("QuorumLost = false, want true (sub-majority quorum must be clamped to pool majority)")
	}
}
