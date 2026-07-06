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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

func TestDeriveStateFromFacts(t *testing.T) {
	cases := []struct {
		name string
		in   observedFacts
		want orchestration.State
	}{
		{
			name: "no pods → Bootstrap",
			in:   observedFacts{},
			want: orchestration.StateBootstrap,
		},
		{
			name: "no pods, standalone → Bootstrap (PodCount==0 wins over IsStandalone)",
			in:   observedFacts{IsStandalone: true},
			want: orchestration.StateBootstrap,
		},
		{
			name: "standalone with one pod → Steady",
			in:   observedFacts{IsStandalone: true, PodCount: 1, AllPodsAtTargetRevision: true},
			want: orchestration.StateSteady,
		},
		{
			name: "standalone with one pod, no target match → Steady (FSM doesn't drive standalone)",
			in:   observedFacts{IsStandalone: true, PodCount: 1},
			want: orchestration.StateSteady,
		},
		{
			name: "sentinel mode, quorum lost → Degraded",
			in:   observedFacts{PodCount: 3, QuorumOK: false},
			want: orchestration.StateDegraded,
		},
		{
			name: "sentinel mode, failover in flight → FailoverInFlight",
			in: observedFacts{
				PodCount:                3,
				QuorumOK:                true,
				FailoverInFlight:        true,
				PrimaryAtTargetRevision: true,
			},
			want: orchestration.StateFailoverInFlight,
		},
		{
			name: "sentinel mode, all pods at target → Steady",
			in: observedFacts{
				PodCount:                    3,
				QuorumOK:                    true,
				PrimaryAtTargetRevision:     true,
				AllReplicasAtTargetRevision: true,
				AllPodsAtTargetRevision:     true,
			},
			want: orchestration.StateSteady,
		},
		{
			name: "sentinel mode, replicas caught up + settled, primary stale → RolloutPrimary",
			in: observedFacts{
				PodCount:                    3,
				QuorumOK:                    true,
				PrimaryAtTargetRevision:     false,
				AllReplicasAtTargetRevision: true,
				ReplicasReadyForHandoff:     true,
			},
			want: orchestration.StateRolloutPrimary,
		},
		{
			name: "sentinel mode, replicas at target revision but NOT settled → RolloutReplicas (hand-off deferred)",
			in: observedFacts{
				PodCount:                    3,
				QuorumOK:                    true,
				PrimaryAtTargetRevision:     false,
				AllReplicasAtTargetRevision: true,
				ReplicasReadyForHandoff:     false,
			},
			want: orchestration.StateRolloutReplicas,
		},
		{
			name: "sentinel mode, primary at target, replicas stale → RolloutReplicas",
			in: observedFacts{
				PodCount:                    3,
				QuorumOK:                    true,
				PrimaryAtTargetRevision:     true,
				AllReplicasAtTargetRevision: false,
			},
			want: orchestration.StateRolloutReplicas,
		},
		{
			name: "sentinel mode, mid-rollout (primary stale, some replicas stale) → RolloutReplicas (fallback)",
			in: observedFacts{
				PodCount:                    3,
				QuorumOK:                    true,
				PrimaryAtTargetRevision:     false,
				AllReplicasAtTargetRevision: false,
			},
			want: orchestration.StateRolloutReplicas,
		},
		{
			name: "FailoverInFlight overrides revision-divergence rules",
			in: observedFacts{
				PodCount:                    3,
				QuorumOK:                    true,
				FailoverInFlight:            true,
				PrimaryAtTargetRevision:     false,
				AllReplicasAtTargetRevision: false,
			},
			want: orchestration.StateFailoverInFlight,
		},
		{
			name: "QuorumOK=false dominates failover/revision flags",
			in: observedFacts{
				PodCount:                    3,
				QuorumOK:                    false,
				FailoverInFlight:            true,
				PrimaryAtTargetRevision:     true,
				AllReplicasAtTargetRevision: true,
				AllPodsAtTargetRevision:     true,
			},
			want: orchestration.StateDegraded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveStateFromFacts(tc.in)
			if got != tc.want {
				t.Fatalf("deriveStateFromFacts(%+v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// The tests below exercise the fact-computation layer of `deriveState`
// (the method) — observable facts read from STS UpdateRevision + pod
// role + revision labels + sentinel snapshot — which sits above the
// `deriveStateFromFacts` decision table covered by TestDeriveStateFromFacts.
// Together they pin the post-restart resume path: a freshly-started
// reconciler re-derives state from cluster facts rather than any
// persisted checkpoint.
//
// deriveState is pure with respect to the cluster: the
// reconciler fetches the valkey pods + StatefulSet once at the top of
// the pass and threads them in. These tests mirror that by seeding a
// fake client and fetching the same inputs via deriveTestInputs before
// calling deriveState — which also exercises listValkeyPods /
// getValkeySTS end-to-end.
//
// SentinelObserver is left nil; QuorumOK then defaults to false and the
// Degraded gate dominates regardless of pod shape — the design
// invariant that a fresh leader refuses relabeling until quorum is
// confirmed. Reaching QuorumOK=true would require a fake sentinel TCP
// server (already covered by internal/sentinel/observer_test.go) or an
// interface-faked observer (an out-of-scope refactor here).

// deriveTestInputs lists the valkey pods + StatefulSet the now-pure
// deriveState consumes, mirroring the once-per-reconcile fetch
// the reconciler performs before calling it. A getValkeySTS that
// returns nil models the pre-bootstrap StatefulSet-absent state.
func deriveTestInputs(t *testing.T, r *ValkeyReconciler, v *valkeyv1beta1.Valkey) ([]corev1.Pod, *appsv1.StatefulSet) {
	t.Helper()
	pods, err := r.listValkeyPods(context.Background(), v)
	if err != nil {
		t.Fatalf("listing valkey pods: %v", err)
	}
	sts, err := r.getValkeySTS(context.Background(), v)
	if err != nil {
		t.Fatalf("getting STS: %v", err)
	}
	return pods, sts
}

// newSentinelCR builds a minimal sentinel-mode CR for the method tests.
func newSentinelCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk"},
		Spec:       valkeyv1beta1.ValkeySpec{Mode: valkeyv1beta1.ModeSentinel},
	}
}

// newStandaloneCR builds a minimal standalone-mode CR for the method
// tests.
func newStandaloneCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk"},
		Spec:       valkeyv1beta1.ValkeySpec{Mode: valkeyv1beta1.ModeStandalone},
	}
}

// newSTSWithUpdateRevision builds a StatefulSet whose Status carries
// UpdateRevision=revision so the fake client surfaces the revision the
// method compares pods against. fake.NewClientBuilder ignores Status
// on construction, so callers must additionally Status().Update.
func newSTSWithUpdateRevision(revision string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vk"},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: revision},
	}
}

// newValkeyPod builds a pod carrying the CR + component labels deriveState
// filters on, plus the supplied role and controller-revision-hash labels.
func newValkeyPod(name, role, revision string) *corev1.Pod {
	labels := map[string]string{
		CRLabel:                    "vk",
		ComponentLabel:             componentValkey,
		"controller-revision-hash": revision,
	}
	if role != "" {
		labels[RoleLabel] = role
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      name,
			Labels:    labels,
		},
	}
}

// TestDeriveState_STSNotFound_ReturnsBootstrap pins the STS-NotFound
// branch of deriveState: pre-STS state is treated as a legitimate
// transient (Bootstrap + nil error), not a reconcile failure. Without
// this check a freshly-created CR would surface a spurious error every
// reconcile until the STS lands.
func TestDeriveState_STSNotFound_ReturnsBootstrap(t *testing.T) {
	v := newSentinelCR()
	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(v).
		Build()
	r := &ValkeyReconciler{Client: c}

	pods, derSTS := deriveTestInputs(t, r, v)
	state, facts := r.deriveState(v, pods, derSTS)
	if state != orchestration.StateBootstrap {
		t.Fatalf("state=%q, want %q", state, orchestration.StateBootstrap)
	}
	if facts.PodCount != 0 {
		t.Fatalf("PodCount=%d, want 0 (no pods read on the NotFound branch)", facts.PodCount)
	}
}

// TestDeriveState_NoPods_ReturnsBootstrap pins the PodCount==0
// short-circuit: STS exists but the pod list is empty (apiserver
// consistent reads should surface this when the STS is mid-Create).
// deriveStateFromFacts's PodCount==0 branch wins regardless of mode,
// so sentinel-mode pre-pod state reads as Bootstrap rather than
// Degraded.
func TestDeriveState_NoPods_ReturnsBootstrap(t *testing.T) {
	v := newSentinelCR()
	sts := newSTSWithUpdateRevision("rev-1")

	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(v, sts).
		WithStatusSubresource(sts).
		Build()
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("seeding STS Status: %v", err)
	}
	r := &ValkeyReconciler{Client: c}

	pods, derSTS := deriveTestInputs(t, r, v)
	state, facts := r.deriveState(v, pods, derSTS)
	if state != orchestration.StateBootstrap {
		t.Fatalf("state=%q, want %q", state, orchestration.StateBootstrap)
	}
	if facts.PodCount != 0 {
		t.Fatalf("PodCount=%d, want 0", facts.PodCount)
	}
}

// TestDeriveState_Standalone_WithPod_ReturnsSteady pins the standalone
// short-circuit: deriveState sets IsStandalone from Spec.Mode, and
// deriveStateFromFacts's IsStandalone branch returns Steady once
// PodCount>0. Standalone has no sentinel and no rollout choreography,
// so the FSM treats any pod-present standalone CR as Steady regardless
// of revision matches.
func TestDeriveState_Standalone_WithPod_ReturnsSteady(t *testing.T) {
	v := newStandaloneCR()
	sts := newSTSWithUpdateRevision("rev-1")
	// Phase 7 stamps role=primary on pod-0 for standalone (see
	// TestDesiredRolesForCR_Standalone_BootstrapRule). Without that
	// label the "no primary + any non-primary pod seen" early-return
	// in deriveState fires and short-circuits to Bootstrap before the
	// deriveStateFromFacts standalone branch runs.
	pod := newValkeyPod("vk-0", roleValuePrimary, "rev-1")

	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(v, sts, pod).
		WithStatusSubresource(sts).
		Build()
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("seeding STS Status: %v", err)
	}
	r := &ValkeyReconciler{Client: c}

	pods, derSTS := deriveTestInputs(t, r, v)
	state, facts := r.deriveState(v, pods, derSTS)
	if state != orchestration.StateSteady {
		t.Fatalf("state=%q, want %q", state, orchestration.StateSteady)
	}
	if !facts.IsStandalone {
		t.Fatalf("IsStandalone=false, want true")
	}
	if facts.PodCount != 1 {
		t.Fatalf("PodCount=%d, want 1", facts.PodCount)
	}
}

// TestDeriveState_Sentinel_NoObserverWired_MidRollout_ReturnsDegraded
// pins the snapshot-absent path: when SentinelObserver is nil (test
// injection or pre-startup boot), QuorumOK stays at zero-value (false),
// which deriveStateFromFacts treats as Degraded — even when the pod
// shape (primary-at-target, replica-stale) would otherwise indicate
// RolloutReplicas. The Degraded gate must dominate so the reconciler
// refuses relabeling until quorum is confirmed; this test pins that
// invariant on the freshly-started-reconciler path before the observer
// has published its first poll.
func TestDeriveState_Sentinel_NoObserverWired_MidRollout_ReturnsDegraded(t *testing.T) {
	v := newSentinelCR()
	sts := newSTSWithUpdateRevision("rev-new")
	primary := newValkeyPod("vk-0", roleValuePrimary, "rev-new")
	replicaRolled := newValkeyPod("vk-1", roleValueReplica, "rev-new")
	replicaStale := newValkeyPod("vk-2", roleValueReplica, "rev-old")

	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(v, sts, primary, replicaRolled, replicaStale).
		WithStatusSubresource(sts).
		Build()
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("seeding STS Status: %v", err)
	}
	r := &ValkeyReconciler{Client: c}

	pods, derSTS := deriveTestInputs(t, r, v)
	state, facts := r.deriveState(v, pods, derSTS)
	if state != orchestration.StateDegraded {
		t.Fatalf("state=%q, want %q (no observer → QuorumOK=false dominates)",
			state, orchestration.StateDegraded)
	}
	// Pod-layer observations must still populate correctly so a
	// subsequent reconcile (with QuorumOK=true) lands at the right
	// rollout state without re-reading.
	if facts.PodCount != 3 {
		t.Fatalf("PodCount=%d, want 3", facts.PodCount)
	}
	if !facts.PrimaryAtTargetRevision {
		t.Fatalf("PrimaryAtTargetRevision=false, want true (primary labelled, at rev-new)")
	}
	if facts.AllReplicasAtTargetRevision {
		t.Fatalf("AllReplicasAtTargetRevision=true, want false (vk-2 still at rev-old)")
	}
	if facts.AllPodsAtTargetRevision {
		t.Fatalf("AllPodsAtTargetRevision=true, want false")
	}
	if facts.QuorumOK {
		t.Fatalf("QuorumOK=true, want false (no observer wired)")
	}
	// The failoverLatchActive lookup runs unconditionally and reads
	// from the reconciler's failoverInFlightLatches sync.Map. An empty
	// map (test construction) must leave FailoverInFlight false; a
	// future regression in the latch logic that fired on an absent
	// entry would otherwise slip through the QuorumOK-dominated state
	// gate.
	if facts.FailoverInFlight {
		t.Fatalf("FailoverInFlight=true, want false (empty failover-latch map on a fresh reconciler)")
	}
	if facts.IsStandalone {
		t.Fatalf("IsStandalone=true, want false (Mode=Sentinel CR)")
	}
}

// TestDeriveState_Sentinel_BootstrapInProgress_NoPrimaryLabelled pins
// the "no primary + any non-primary pod seen" early-return: a CR
// mid-bootstrap (sentinel quorum not yet agreed; Phase 7 hasn't
// stamped role=primary on any pod) reads as Bootstrap rather than
// Degraded, even though QuorumOK=false would otherwise route via
// deriveStateFromFacts to Degraded. The early-return is what lets the
// bootstrap-complete edge fire on the next reconcile (Phase 7 stamps
// the primary label → primarySeen=true → fall-through to
// deriveStateFromFacts).
func TestDeriveState_Sentinel_BootstrapInProgress_NoPrimaryLabelled(t *testing.T) {
	v := newSentinelCR()
	sts := newSTSWithUpdateRevision("rev-1")
	// Two replicas, no primary labelled. Mid-bootstrap shape.
	replica0 := newValkeyPod("vk-0", roleValueReplica, "rev-1")
	replica1 := newValkeyPod("vk-1", roleValueReplica, "rev-1")

	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(v, sts, replica0, replica1).
		WithStatusSubresource(sts).
		Build()
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("seeding STS Status: %v", err)
	}
	r := &ValkeyReconciler{Client: c}

	pods, derSTS := deriveTestInputs(t, r, v)
	state, _ := r.deriveState(v, pods, derSTS)
	if state != orchestration.StateBootstrap {
		t.Fatalf("state=%q, want %q (no primary labelled → bootstrap early-return)",
			state, orchestration.StateBootstrap)
	}
}

// TestDeriveState_Sentinel_PodsAtTargetRevision_PrimaryLabelled pins
// the steady-shape observation layer: every pod (primary + replicas)
// carries the STS UpdateRevision label, so the pod-layer facts compute
// AllPodsAtTargetRevision=true. With QuorumOK=false (no observer
// wired) the gate still routes via Degraded, but the observation
// itself is correct — a future reconcile with QuorumOK=true would
// land at Steady without re-reading any pod.
func TestDeriveState_Sentinel_PodsAtTargetRevision_PrimaryLabelled(t *testing.T) {
	v := newSentinelCR()
	sts := newSTSWithUpdateRevision("rev-1")
	primary := newValkeyPod("vk-0", roleValuePrimary, "rev-1")
	replica := newValkeyPod("vk-1", roleValueReplica, "rev-1")

	c := fake.NewClientBuilder().
		WithScheme(pvcResizeTestScheme(t)).
		WithObjects(v, sts, primary, replica).
		WithStatusSubresource(sts).
		Build()
	if err := c.Status().Update(context.Background(), sts); err != nil {
		t.Fatalf("seeding STS Status: %v", err)
	}
	r := &ValkeyReconciler{Client: c}

	pods, derSTS := deriveTestInputs(t, r, v)
	state, facts := r.deriveState(v, pods, derSTS)
	// Gate: QuorumOK=false → Degraded.
	if state != orchestration.StateDegraded {
		t.Fatalf("state=%q, want %q", state, orchestration.StateDegraded)
	}
	// Observation: pods are correctly classified as at-target.
	if !facts.PrimaryAtTargetRevision {
		t.Fatalf("PrimaryAtTargetRevision=false, want true")
	}
	if !facts.AllReplicasAtTargetRevision {
		t.Fatalf("AllReplicasAtTargetRevision=false, want true")
	}
	if !facts.AllPodsAtTargetRevision {
		t.Fatalf("AllPodsAtTargetRevision=false, want true")
	}
}

// TestDeriveState_ReplicasReadyForHandoff pins the hand-off gate:
// revision labels alone must not flip the FSM into RolloutPrimary — the
// replica set must be complete (spec replicas minus the primary) and
// every replica non-terminating, Ready, and replication-healthy before
// the primary hand-off (and its SENTINEL FAILOVER) may dispatch.
// deriveState is pure with respect to the cluster, so the pod set is
// hand-built per case.
func TestDeriveState_ReplicasReadyForHandoff(t *testing.T) {
	now := metav1.Now()
	mkPod := func(name, role string, ready bool, mutate func(*corev1.Pod)) corev1.Pod {
		p := newValkeyPod(name, role, "rev-2")
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
		if mutate != nil {
			mutate(p)
		}
		return *p
	}

	cr := newSentinelCR()
	cr.Spec.Valkey.Replicas = 3
	sts := newSTSWithUpdateRevision("rev-2")

	cases := []struct {
		name string
		pods []corev1.Pod
		want bool
	}{
		{
			name: "complete + settled replica set → handoff ready",
			pods: []corev1.Pod{
				mkPod("vk-0", roleValuePrimary, true, nil),
				mkPod("vk-1", roleValueReplica, true, nil),
				mkPod("vk-2", roleValueReplica, true, nil),
			},
			want: true,
		},
		{
			name: "replica missing (mid-recreate, not yet listed) → not ready",
			pods: []corev1.Pod{
				mkPod("vk-0", roleValuePrimary, true, nil),
				mkPod("vk-2", roleValueReplica, true, nil),
			},
			want: false,
		},
		{
			name: "replica not Ready (fresh recreate still booting) → not ready",
			pods: []corev1.Pod{
				mkPod("vk-0", roleValuePrimary, true, nil),
				mkPod("vk-1", roleValueReplica, false, nil),
				mkPod("vk-2", roleValueReplica, true, nil),
			},
			want: false,
		},
		{
			name: "replica terminating → not ready",
			pods: []corev1.Pod{
				mkPod("vk-0", roleValuePrimary, true, nil),
				mkPod("vk-1", roleValueReplica, true, func(p *corev1.Pod) {
					p.DeletionTimestamp = &now
				}),
				mkPod("vk-2", roleValueReplica, true, nil),
			},
			want: false,
		},
		{
			name: "replica replication-gate False (mid initial sync) → not ready",
			pods: []corev1.Pod{
				mkPod("vk-0", roleValuePrimary, true, nil),
				mkPod("vk-1", roleValueReplica, true, func(p *corev1.Pod) {
					p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
						Type: ReplicationReadyGate, Status: corev1.ConditionFalse,
					})
				}),
				mkPod("vk-2", roleValueReplica, true, nil),
			},
			want: false,
		},
	}
	r := &ValkeyReconciler{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, facts := r.deriveState(cr, tc.pods, sts)
			if facts.ReplicasReadyForHandoff != tc.want {
				t.Fatalf("ReplicasReadyForHandoff=%v, want %v", facts.ReplicasReadyForHandoff, tc.want)
			}
		})
	}
}
