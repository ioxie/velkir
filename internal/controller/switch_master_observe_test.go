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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
)

// switchMasterPod builds a pod with a PodIP and an optional role label
// (empty role → no role label, the pre-bootstrap shape). The detector
// reads only PodIP + RoleLabel, so no CR/component labels are needed.
func switchMasterPod(name, ip, role string) corev1.Pod {
	labels := map[string]string{}
	if role != "" {
		labels[RoleLabel] = role
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status:     corev1.PodStatus{PodIP: ip},
	}
}

func quorumSnap(addr string) sentinel.Snapshot {
	return sentinel.Snapshot{
		Present: true,
		Primary: sentinel.ObservedPrimary{Addr: addr, QuorumOK: true},
	}
}

func TestObserveExternalSwitchMaster(t *testing.T) {
	// Canonical topology: pod at .1 is the stale labelled primary,
	// pod at .2 is a replica that sentinel just promoted.
	staleTopology := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValuePrimary),
		switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	tests := []struct {
		name        string
		snap        sentinel.Snapshot
		pods        []corev1.Pod
		latchActive bool
		wantAddr    string
		wantOK      bool
	}{
		{
			name:     "external switch — observer primary disagrees with stale label",
			snap:     quorumSnap("10.0.0.2:6379"),
			pods:     staleTopology,
			wantAddr: "10.0.0.2:6379",
			wantOK:   true,
		},
		{
			name:   "agreement — observer primary already labelled primary",
			snap:   quorumSnap("10.0.0.1:6379"),
			pods:   staleTopology,
			wantOK: false,
		},
		{
			name:        "latch active — operator-initiated, suppress",
			snap:        quorumSnap("10.0.0.2:6379"),
			pods:        staleTopology,
			latchActive: true,
			wantOK:      false,
		},
		{
			name:   "snapshot not present",
			snap:   sentinel.Snapshot{Present: false},
			pods:   staleTopology,
			wantOK: false,
		},
		{
			name:   "quorum not OK — split-brain territory (T6), not T5",
			snap:   sentinel.Snapshot{Present: true, Primary: sentinel.ObservedPrimary{Addr: "10.0.0.2:6379", QuorumOK: false}},
			pods:   staleTopology,
			wantOK: false,
		},
		{
			name: "bootstrap — no pod labelled primary yet (first stamp, not failover)",
			snap: quorumSnap("10.0.0.2:6379"),
			pods: []corev1.Pod{
				switchMasterPod("vk0-0", "10.0.0.1", roleValueReplica),
				switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
			},
			wantOK: false,
		},
		{
			name:   "no-master-agreement — observer Addr matches no pod IP",
			snap:   quorumSnap("10.0.0.99:6379"),
			pods:   staleTopology,
			wantOK: false,
		},
		{
			name:   "malformed Addr (no host:port)",
			snap:   quorumSnap("garbage"),
			pods:   staleTopology,
			wantOK: false,
		},
		{
			name:   "empty Addr",
			snap:   quorumSnap(""),
			pods:   staleTopology,
			wantOK: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotAddr, gotOK := observeExternalSwitchMaster(tc.snap, tc.pods, tc.latchActive)
			if gotOK != tc.wantOK {
				t.Fatalf("detected = %v; want %v", gotOK, tc.wantOK)
			}
			if gotAddr != tc.wantAddr {
				t.Fatalf("addr = %q; want %q", gotAddr, tc.wantAddr)
			}
		})
	}
}

// newSwitchMasterReconciler builds a reconciler with a live FSM and a
// fake recorder — the minimum needed to exercise fsmSwitchMasterDispatch
// without a SentinelObserver (the snapshot is threaded in directly).
func newSwitchMasterReconciler() (*ValkeyReconciler, *k8sevents.FakeRecorder) {
	rec := k8sevents.NewFakeRecorder(8)
	return &ValkeyReconciler{
		FSM:      orchestration.NewMachine(),
		Recorder: rec,
	}, rec
}

func switchMasterCR() (*valkeyv1beta1.Valkey, types.NamespacedName) {
	v := newCR(valkeyv1beta1.ModeSentinel)
	return v, types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
}

func countUnexpectedFailover(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, string("UnexpectedFailover")) {
			n++
		}
	}
	return n
}

func TestFsmSwitchMasterDispatch_ExternalSwitchEmitsOnce(t *testing.T) {
	r, rec := newSwitchMasterReconciler()
	v, key := switchMasterCR()
	pods := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValuePrimary),
		switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	snap := quorumSnap("10.0.0.2:6379")

	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, snap, pods)

	got := drainAllEvents(rec.Events)
	if n := countUnexpectedFailover(got); n != 1 {
		t.Fatalf("emitted %d UnexpectedFailover events, want 1: %v", n, got)
	}
}

func TestFsmSwitchMasterDispatch_OperatorInitiatedSuppressed(t *testing.T) {
	r, rec := newSwitchMasterReconciler()
	v, key := switchMasterCR()
	pods := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValuePrimary),
		switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	snap := quorumSnap("10.0.0.2:6379")
	// Operator just dispatched FAILOVER: latch is active for the
	// observed Addr, so the switch is its own, not external.
	r.failoverLatchSet(key, "10.0.0.2:6379")

	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, snap, pods)

	got := drainAllEvents(rec.Events)
	if n := countUnexpectedFailover(got); n != 0 {
		t.Fatalf("emitted %d UnexpectedFailover events on operator-initiated failover, want 0: %v", n, got)
	}
}

func TestFsmSwitchMasterDispatch_BootstrapNoFire(t *testing.T) {
	r, rec := newSwitchMasterReconciler()
	v, key := switchMasterCR()
	// No pod labelled primary yet — first post-bootstrap stamp, not a
	// failover.
	pods := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValueReplica),
		switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	snap := quorumSnap("10.0.0.2:6379")

	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, snap, pods)

	got := drainAllEvents(rec.Events)
	if n := countUnexpectedFailover(got); n != 0 {
		t.Fatalf("emitted %d UnexpectedFailover events at bootstrap, want 0: %v", n, got)
	}
}

func TestFsmSwitchMasterDispatch_NonSteadyNoFire(t *testing.T) {
	r, rec := newSwitchMasterReconciler()
	v, key := switchMasterCR()
	pods := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValuePrimary),
		switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	snap := quorumSnap("10.0.0.2:6379")

	// Same detectable disagreement, but the FSM is mid-rollout — T5's
	// From:Steady must keep it from firing.
	r.fsmSwitchMasterDispatch(v, key, orchestration.StateRolloutPrimary, snap, pods)

	got := drainAllEvents(rec.Events)
	if n := countUnexpectedFailover(got); n != 0 {
		t.Fatalf("emitted %d UnexpectedFailover events outside Steady, want 0: %v", n, got)
	}
}

func TestFsmSwitchMasterDispatch_EdgeFiresOncePerEpisode(t *testing.T) {
	r, rec := newSwitchMasterReconciler()
	v, key := switchMasterCR()
	pods := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValuePrimary),
		switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	snap := quorumSnap("10.0.0.2:6379")

	// Two reconciles observing the SAME disagreement (Phase 7 has not
	// relabelled yet) must emit exactly one event.
	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, snap, pods)
	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, snap, pods)

	got := drainAllEvents(rec.Events)
	if n := countUnexpectedFailover(got); n != 1 {
		t.Fatalf("emitted %d UnexpectedFailover events across two reconciles of one episode, want 1: %v", n, got)
	}
}

func TestFsmSwitchMasterDispatch_ReArmsAfterRelabel(t *testing.T) {
	r, rec := newSwitchMasterReconciler()
	v, key := switchMasterCR()
	stale := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValuePrimary),
		switchMasterPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	// Episode 1: external switch to .2 → fire.
	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, quorumSnap("10.0.0.2:6379"), stale)

	// Phase 7 relabels: now .2 is labelled primary, agreement → no fire,
	// edge re-arms.
	relabelled := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValueReplica),
		switchMasterPod("vk0-1", "10.0.0.2", roleValuePrimary),
	}
	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, quorumSnap("10.0.0.2:6379"), relabelled)

	// Episode 2: a fresh external switch back to .1 (now stale-labelled
	// .2 is primary) → fire again.
	stale2 := []corev1.Pod{
		switchMasterPod("vk0-0", "10.0.0.1", roleValueReplica),
		switchMasterPod("vk0-1", "10.0.0.2", roleValuePrimary),
	}
	r.fsmSwitchMasterDispatch(v, key, orchestration.StateSteady, quorumSnap("10.0.0.1:6379"), stale2)

	got := drainAllEvents(rec.Events)
	if n := countUnexpectedFailover(got); n != 2 {
		t.Fatalf("emitted %d UnexpectedFailover events across two distinct episodes, want 2: %v", n, got)
	}
}
