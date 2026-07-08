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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// readyRolloutPod builds a pod that clears reconcilePodRollout's in-flight
// gate (PodReady=True; no ReplicationReadyGate condition, so
// podReplicationHealthy is true; no DeletionTimestamp) at the given role
// and controller-revision-hash, so the quorum gate below it is reached.
func readyRolloutPod(name, role, revision string) *corev1.Pod {
	p := newValkeyPod(name, role, revision)
	p.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}}
	return p
}

func rolloutEventsHaveReason(events []string, reason string) bool {
	for _, e := range events {
		if strings.Contains(e, reason) {
			return true
		}
	}
	return false
}

// rolloutGateFixture seeds a mid-rollout shape: primary at the new
// revision, two replicas still on the old revision, all Ready. The STS
// reports UpdateRevision != CurrentRevision so a rollout is genuinely
// due. Returns the reconciler (fake client seeded with the pods + a fake
// recorder), the pod slice to thread into reconcilePodRollout, and the
// STS to pass alongside.
func rolloutGateFixture(t *testing.T) (*ValkeyReconciler, []corev1.Pod, *appsv1.StatefulSet) {
	t.Helper()
	primary := readyRolloutPod("vk-0", roleValuePrimary, "rev-new")
	replica1 := readyRolloutPod("vk-1", roleValueReplica, "rev-old")
	replica2 := readyRolloutPod("vk-2", roleValueReplica, "rev-old")

	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(primary, replica1, replica2).
		Build()

	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
	}

	sts := newSTSWithUpdateRevision("rev-new")
	sts.Status.CurrentRevision = "rev-old"

	pods := []corev1.Pod{*primary, *replica1, *replica2}
	return r, pods, sts
}

// TestReconcilePodRollout_SentinelQuorumNotOK_DefersWithoutDeleting pins
// the invariant: in sentinel mode, when the observed sentinel quorum
// is not OK, the replica rollout must NOT delete a stale replica (that
// could tip the sentinel pool below quorum mid-roll). It must instead
// emit RolloutDeferred and ask for a short requeue.
func TestReconcilePodRollout_SentinelQuorumNotOK_DefersWithoutDeleting(t *testing.T) {
	v := newSentinelCR()
	r, pods, sts := rolloutGateFixture(t)

	requeue, err := r.reconcilePodRollout(context.Background(), v, false /* quorumOK */, pods, sts)
	if err != nil {
		t.Fatalf("reconcilePodRollout returned error: %v", err)
	}
	if requeue != rolloutQuorumDeferRequeue {
		t.Errorf("requeue=%s, want %s (quorum-deferred roll must re-check on the defer cadence)",
			requeue, rolloutQuorumDeferRequeue)
	}

	// The highest-ordinal stale replica must still exist — no delete happened.
	got := &corev1.Pod{}
	if err := r.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vk-2"}, got); err != nil {
		t.Fatalf("vk-2 must NOT be deleted when quorum is not OK; got Get error: %v", err)
	}

	events := drainEvents(r.Recorder.(*k8sevents.FakeRecorder))
	if !rolloutEventsHaveReason(events, "RolloutDeferred") {
		t.Errorf("RolloutDeferred must fire on a quorum-fragile defer; got events: %v", events)
	}
	if rolloutEventsHaveReason(events, "PodRolledForConfig") {
		t.Errorf("PodRolledForConfig must NOT fire when the roll is deferred; got events: %v", events)
	}
}

// TestReconcilePodRollout_SentinelQuorumOK_DeletesHighestOrdinalReplica is
// the contrast case: the SAME sentinel-mode shape, but with QuorumOK=true,
// proceeds to delete the highest-ordinal stale replica. This proves the
// quorum verdict — not some other gate — is what blocks the defer case.
func TestReconcilePodRollout_SentinelQuorumOK_DeletesHighestOrdinalReplica(t *testing.T) {
	v := newSentinelCR()
	r, pods, sts := rolloutGateFixture(t)

	requeue, err := r.reconcilePodRollout(context.Background(), v, true /* quorumOK */, pods, sts)
	if err != nil {
		t.Fatalf("reconcilePodRollout returned error: %v", err)
	}
	if requeue != 0 {
		t.Errorf("requeue=%s, want 0 (delete path arms its own watchdog, no quorum-defer hint)", requeue)
	}

	got := &corev1.Pod{}
	err = r.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vk-2"}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("vk-2 (highest-ordinal stale replica) must be deleted when quorum is OK; Get err=%v", err)
	}

	events := drainEvents(r.Recorder.(*k8sevents.FakeRecorder))
	if !rolloutEventsHaveReason(events, "PodRolledForConfig") {
		t.Errorf("PodRolledForConfig must fire on the delete; got events: %v", events)
	}
}

// TestReconcilePodRollout_ReplicationMode_QuorumNotOK_StillRolls pins the
// load-bearing mode restriction: replication mode has no sentinel pool, so
// its facts.QuorumOK is always false. The quorum gate must NOT apply to it
// — otherwise every replication-mode rollout would wedge forever.
func TestReconcilePodRollout_ReplicationMode_QuorumNotOK_StillRolls(t *testing.T) {
	v := newSentinelCR()
	v.Spec.Mode = valkeyv1beta1.ModeReplication
	r, pods, sts := rolloutGateFixture(t)

	requeue, err := r.reconcilePodRollout(context.Background(), v, false /* quorumOK */, pods, sts)
	if err != nil {
		t.Fatalf("reconcilePodRollout returned error: %v", err)
	}
	if requeue != 0 {
		t.Errorf("requeue=%s, want 0 (replication rollout is not quorum-gated)", requeue)
	}

	got := &corev1.Pod{}
	err = r.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vk-2"}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("vk-2 must be deleted in replication mode regardless of quorumOK; Get err=%v", err)
	}
}

// rolloutLabeledReplica builds a Ready stale replica carrying an explicit
// `apps.kubernetes.io/pod-index` label, so the victim sort exercises the
// label-first ordinal source rather than the name-suffix parse.
func rolloutLabeledReplica(name, ordinal, revision string) *corev1.Pod {
	p := readyRolloutPod(name, roleValueReplica, revision)
	p.Labels[podIndexLabel] = ordinal
	return p
}

// TestReconcilePodRollout_VictimSort_PrefersPodIndexLabel pins the
// label-first victim sort: stale replicas must be ranked by the
// `apps.kubernetes.io/pod-index` label, not the pod-name suffix.
// The two replicas are arranged so name-parse and label orderings
// DISAGREE — vk-1 (name ordinal 1, label 9) outranks vk-2 (name
// ordinal 2, label 2) only under label-first. Deleting vk-1 proves
// the label is the ordinal source of truth; a regression to pure name
// parsing would wrongly delete vk-2 first.
func TestReconcilePodRollout_VictimSort_PrefersPodIndexLabel(t *testing.T) {
	v := newSentinelCR()

	primary := readyRolloutPod("vk-0", roleValuePrimary, "rev-new")
	primary.Labels[podIndexLabel] = "0"
	highOrdinal := rolloutLabeledReplica("vk-1", "9", "rev-old") // name 1, label 9
	lowOrdinal := rolloutLabeledReplica("vk-2", "2", "rev-old")  // name 2, label 2

	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(primary, highOrdinal, lowOrdinal).
		Build()
	r := &ValkeyReconciler{
		Client:   c,
		Recorder: k8sevents.NewFakeRecorder(8),
	}

	sts := newSTSWithUpdateRevision("rev-new")
	sts.Status.CurrentRevision = "rev-old"
	pods := []corev1.Pod{*primary, *highOrdinal, *lowOrdinal}

	requeue, err := r.reconcilePodRollout(context.Background(), v, true /* quorumOK */, pods, sts)
	if err != nil {
		t.Fatalf("reconcilePodRollout returned error: %v", err)
	}
	if requeue != 0 {
		t.Errorf("requeue=%s, want 0 (delete path arms its own watchdog)", requeue)
	}

	// vk-1 carries pod-index label 9 — the highest — so it is the
	// victim under label-first sorting.
	got := &corev1.Pod{}
	if err := r.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vk-1"}, got); !apierrors.IsNotFound(err) {
		t.Errorf("vk-1 (pod-index label 9) must be deleted first; Get err=%v", err)
	}
	// vk-2 has the higher NAME ordinal but a lower label ordinal — a
	// name-parse regression would wrongly delete it. It must survive.
	if err := r.Get(context.Background(),
		types.NamespacedName{Namespace: "ns", Name: "vk-2"}, got); err != nil {
		t.Errorf("vk-2 (name ordinal 2, label 2) must survive the first roll; Get err=%v", err)
	}
}

// TestOrdinalFromPod covers the label-first ordinal helper directly:
// a valid pod-index label wins, the name suffix is the fallback for an
// absent/malformed/negative label, and an unparseable name with no
// label yields -1.
func TestOrdinalFromPod(t *testing.T) {
	tests := []struct {
		name     string
		podName  string
		podIndex string // "" means no pod-index label set
		want     int32
	}{
		{"label wins over name", "vk-2", "5", 5},
		{"absent label falls back to name", "vk-3", "", 3},
		{"malformed label falls back to name", "vk-4", "not-a-number", 4},
		{"negative label falls back to name", "vk-6", "-1", 6},
		{"no label and unparseable name yields -1", "weird-pod", "", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newValkeyPod(tc.podName, "", "")
			if tc.podIndex != "" {
				pod.Labels[podIndexLabel] = tc.podIndex
			}
			if got := ordinalFromPod(pod, "vk"); got != tc.want {
				t.Errorf("ordinalFromPod(name=%q, podIndex=%q) = %d, want %d",
					tc.podName, tc.podIndex, got, tc.want)
			}
		})
	}
}
