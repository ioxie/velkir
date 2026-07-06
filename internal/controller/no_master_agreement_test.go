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
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

func TestObserveNoMasterAgreement_NilCR_ReturnsFalse(t *testing.T) {
	r := &ValkeyReconciler{}
	if r.observeNoMasterAgreement(context.Background(), nil) {
		t.Fatal("nil CR must return false (avoids panic in the deferred status closure)")
	}
}

func TestObserveNoMasterAgreement_NonSentinel_ReturnsFalse(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	// Standalone CR + active anomaly in the observer would still
	// return false — non-sentinel modes don't consult the observer.
	v := newCR(valkeyv1beta1.ModeStandalone)
	if r.observeNoMasterAgreement(context.Background(), v) {
		t.Fatal("standalone mode must always return false")
	}
}

func TestObserveNoMasterAgreement_NilObserver_ReturnsFalse(t *testing.T) {
	r := &ValkeyReconciler{SentinelObserver: nil}
	v := newCR(valkeyv1beta1.ModeSentinel)
	if r.observeNoMasterAgreement(context.Background(), v) {
		t.Fatal("nil observer must always return false")
	}
}

func TestObserveNoMasterAgreement_PreSnapshot_ReturnsFalse(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	mgr, cancel := startedManager(t, rec)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeSentinel)
	// No Ensure call → Snapshot returns Present=false. Helper
	// returns false. Pins the boot-race shape: must NOT flap
	// Degraded=NoMasterAgreement on a fresh CR before the first
	// snapshot lands.
	if r.observeNoMasterAgreement(context.Background(), v) {
		t.Fatal("pre-snapshot (Present=false) must return false")
	}
}

func TestCountPrimaryLabeledPods_NoPrimary(t *testing.T) {
	// Test the production helper directly (not a re-implementation
	// in test code). Zero primary-labeled pods is the
	// "active recovery" signal the stale-replica gate keys on.
	pods := []corev1.Pod{
		labelledPod("vk0-0", roleValueReplica),
		labelledPod("vk0-1", roleValueReplica),
	}
	if got := countPrimaryLabeledPods(pods); got != 0 {
		t.Errorf("countPrimaryLabeledPods = %d; want 0", got)
	}
}

func TestCountPrimaryLabeledPods_OnePrimary(t *testing.T) {
	pods := []corev1.Pod{
		labelledPod("vk0-0", roleValuePrimary),
		labelledPod("vk0-1", roleValueReplica),
	}
	if got := countPrimaryLabeledPods(pods); got != 1 {
		t.Errorf("countPrimaryLabeledPods = %d; want 1", got)
	}
}

func TestCountPrimaryLabeledPods_MultiPrimaryRace(t *testing.T) {
	// Multi-primary is a labelling race or a Phase 7 bug — the
	// counter still reports the count accurately. Phase 8's gate
	// treats >0 as "we have a primary, don't suppress"; the
	// degenerate state recovers on the next reconcile.
	pods := []corev1.Pod{
		labelledPod("vk0-0", roleValuePrimary),
		labelledPod("vk0-1", roleValuePrimary),
		labelledPod("vk0-2", roleValueReplica),
	}
	if got := countPrimaryLabeledPods(pods); got != 2 {
		t.Errorf("countPrimaryLabeledPods = %d; want 2", got)
	}
}

func TestPodIPMatchesAny_HappyPath(t *testing.T) {
	// Phase 7's NoMasterAgreement guard fires when this returns false.
	pods := []corev1.Pod{
		ipPod("vk0-0", "10.0.0.1"),
		ipPod("vk0-1", "10.0.0.2"),
	}
	if !podIPMatchesAny("10.0.0.2", pods) {
		t.Errorf("podIPMatchesAny should match second pod's IP")
	}
}

func TestPodIPMatchesAny_NoMatch(t *testing.T) {
	pods := []corev1.Pod{
		ipPod("vk0-0", "10.0.0.1"),
		ipPod("vk0-1", "10.0.0.2"),
	}
	// 10.0.0.99 is the canonical "defunct master IP" case from the
	// bug — sentinels point at it, no pod has it.
	if podIPMatchesAny("10.0.0.99", pods) {
		t.Errorf("podIPMatchesAny should NOT match a defunct IP (this is the wedge state)")
	}
}

func TestPodIPMatchesAny_SkipsEmptyPodIP(t *testing.T) {
	// Pre-scheduling pods carry empty PodIP. Must be skipped to
	// avoid a false positive on host="" matching pod.Status.PodIP="".
	pods := []corev1.Pod{
		ipPod("vk0-0", ""),
		ipPod("vk0-1", "10.0.0.1"),
	}
	if podIPMatchesAny("", pods) {
		t.Errorf("empty host should always return false (defensive)")
	}
}

func labelledPod(name, role string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				CRLabel:        "vk0",
				ComponentLabel: componentValkey,
				RoleLabel:      role,
			},
		},
	}
}

func ipPod(name, ip string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.PodStatus{PodIP: ip},
	}
}
