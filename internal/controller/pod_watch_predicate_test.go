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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func podWith(ip string, phase corev1.PodPhase, conds ...corev1.PodCondition) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "ns"},
		Status: corev1.PodStatus{
			PodIP:      ip,
			Phase:      phase,
			Conditions: conds,
		},
	}
}

func cond(t corev1.PodConditionType, s corev1.ConditionStatus) corev1.PodCondition {
	return corev1.PodCondition{Type: t, Status: s}
}

func TestPodStatusChangePredicate_CreateDeleteAlwaysPass(t *testing.T) {
	p := podStatusChangePredicate()
	if !p.Create(event.CreateEvent{Object: podWith("", corev1.PodPending)}) {
		t.Error("Create should always pass (a new pod is always actionable)")
	}
	if !p.Delete(event.DeleteEvent{Object: podWith("10.0.0.1", corev1.PodRunning)}) {
		t.Error("Delete should always pass")
	}
	if p.Generic(event.GenericEvent{Object: podWith("10.0.0.1", corev1.PodRunning)}) {
		t.Error("Generic should be dropped (no source feeds pod generics here)")
	}
}

func TestPodStatusChangePredicate_Update(t *testing.T) {
	ready := cond(corev1.PodReady, corev1.ConditionTrue)
	notReady := cond(corev1.PodReady, corev1.ConditionFalse)
	cases := []struct {
		name string
		old  *corev1.Pod
		new  *corev1.Pod
		want bool
	}{
		{"podIP assigned", podWith("", corev1.PodPending), podWith("10.0.0.1", corev1.PodPending), true},
		{"phase change", podWith("10.0.0.1", corev1.PodPending), podWith("10.0.0.1", corev1.PodRunning), true},
		{"condition status flip", podWith("10.0.0.1", corev1.PodRunning, notReady), podWith("10.0.0.1", corev1.PodRunning, ready), true},
		{"no status change (label/annotation churn)", podWith("10.0.0.1", corev1.PodRunning, ready), podWith("10.0.0.1", corev1.PodRunning, ready), false},
	}
	p := podStatusChangePredicate()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new})
			if got != tc.want {
				t.Errorf("Update predicate = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestPodStatusChangePredicate_NonPodObjectFallsThrough(t *testing.T) {
	// A non-pod object (shouldn't happen for an Owns(&Pod{}) watch) is
	// passed through rather than silently dropped.
	p := podStatusChangePredicate()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "ns"}}
	if !p.Update(event.UpdateEvent{ObjectOld: cm, ObjectNew: cm}) {
		t.Error("non-pod update should fall through to true")
	}
}

func TestEqualPodConditions(t *testing.T) {
	ready := cond(corev1.PodReady, corev1.ConditionTrue)
	notReady := cond(corev1.PodReady, corev1.ConditionFalse)
	init := cond(corev1.PodInitialized, corev1.ConditionTrue)
	cases := []struct {
		name string
		a    []corev1.PodCondition
		b    []corev1.PodCondition
		want bool
	}{
		{"identical", []corev1.PodCondition{ready, init}, []corev1.PodCondition{ready, init}, true},
		{"order-independent", []corev1.PodCondition{ready, init}, []corev1.PodCondition{init, ready}, true},
		{"status differs", []corev1.PodCondition{ready}, []corev1.PodCondition{notReady}, false},
		{"length differs", []corev1.PodCondition{ready}, []corev1.PodCondition{ready, init}, false},
		{"both empty", nil, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := equalPodConditions(tc.a, tc.b); got != tc.want {
				t.Errorf("equalPodConditions = %v; want %v", got, tc.want)
			}
		})
	}
}
