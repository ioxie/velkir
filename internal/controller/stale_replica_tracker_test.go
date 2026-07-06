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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/valkey"
)

// Phase 8 staleReplicaTrackers semantics. The per-CR sync.Map records
// when THIS operator instance first observed each replica's
// replication-ready gate as False, keyed by pod UID. Staleness for the
// stuck-replica-recovery delete is measured against the in-memory
// timestamp, not the apiserver-side PodCondition.LastTransitionTime —
// the apiserver clock keeps running while the operator is down, so
// the window must restart after an operator restart.

var _ = Describe("Phase 8 staleReplicaTrackers", func() {
	ctx := context.Background()

	makeReplicationCR := func(name string, replicas int32) *valkeyv1beta1.Valkey {
		return &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeReplication,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Valkey: valkeyv1beta1.ValkeyPodSpec{
					Replicas: replicas,
					Persistence: &valkeyv1beta1.PersistenceSpec{
						Size: resource.MustParse("1Gi"),
					},
					ReadinessGate: valkeyv1beta1.ReadinessGateSpec{
						Enabled:     new(true),
						MaxLagBytes: new(int64(1 << 20)),
					},
				},
			},
		}
	}

	seedReplica := func(name, crName, ip string, gateFalseAgo time.Duration) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels: map[string]string{
					CRLabel:        crName,
					ComponentLabel: componentValkey,
					RoleLabel:      roleValueReplica,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "valkey",
					Image: "valkey/valkey:8.1.6-alpine",
				}},
				ReadinessGates: []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		if gateFalseAgo > 0 {
			stamped := metav1.NewTime(time.Now().Add(-gateFalseAgo))
			p.Status.Conditions = []corev1.PodCondition{{
				Type:               ReplicationReadyGate,
				Status:             corev1.ConditionFalse,
				LastTransitionTime: stamped,
				LastProbeTime:      stamped,
				Message:            "seeded stale",
			}}
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
		return p
	}

	// seedPrimary creates a role=primary-labeled valkey pod with a
	// PodIP. Stale-replica deletion in Phase 8 is gated on the
	// presence of at least one primary-labeled pod — without this,
	// the operator is in active-recovery state and skips deletions
	// to preserve failover candidates for sentinel. Tests that
	// exercise the deletion path need a primary in the fixture.
	seedPrimary := func(name, crName, ip string) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels: map[string]string{
					CRLabel:        crName,
					ComponentLabel: componentValkey,
					RoleLabel:      roleValuePrimary,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "valkey",
					Image: "valkey/valkey:8.1.6-alpine",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
		return p
	}

	cleanupPodsAndCR := func(crName string, podNames ...string) {
		for _, n := range podNames {
			p := &corev1.Pod{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: n, Namespace: "default"}, p); err == nil {
				_ = k8sClient.Delete(ctx, p, client.GracePeriodSeconds(0))
			}
		}
		cr := &valkeyv1beta1.Valkey{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: crName, Namespace: "default"}, cr); err == nil {
			_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
		}
	}

	innerFor := func(r *ValkeyReconciler, crKey types.NamespacedName) *sync.Map {
		ps, ok := r.stateForIfPresent(crKey)
		if !ok {
			return nil
		}
		ps.mu.Lock()
		defer ps.mu.Unlock()
		return ps.staleReplicas
	}

	It("operator-restart: empty tracker + apiserver gate >100s stale → no delete", func() {
		const name = "tracker-restart"
		// Replication mode requires spec.replicas >= 2; this test only
		// seeds one replica pod (the case under test is "single stale
		// pod, fresh operator"), so the count is for the CR validator,
		// not the seeded fleet.
		cr := makeReplicationCR(name, 2)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		// The apiserver-side LastTransitionTime claims 150s of
		// staleness, but the operator has never observed this pod
		// before. Without the in-memory tracker, the prior code would
		// dereference the apiserver timestamp and delete on the first
		// reconcile after restart.
		pod := seedReplica(name+"-1", name, "10.0.0.21", 150*time.Second)
		DeferCleanup(cleanupPodsAndCR, name, name+"-1")

		fake := newFakeLagChecker()
		fake.errAddr["10.0.0.21:6379"] = context.DeadlineExceeded
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake}

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())

		got := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-1", Namespace: "default"}, got)).
			To(Succeed(), "pod must survive — empty tracker means this is the first observation")
		Expect(got.DeletionTimestamp).To(BeNil(), "operator-restart guard violated: pod was deleted")

		inner := innerFor(r, types.NamespacedName{Name: name, Namespace: "default"})
		Expect(inner).NotTo(BeNil(), "tracker inner map must exist after first observation")
		firstSeenAny, ok := inner.Load(pod.UID)
		Expect(ok).To(BeTrue(), "first-seen entry not recorded")
		// The apiserver-side timestamp is 150s in the past; a 2s window
		// is wide enough to absorb envtest scheduling jitter yet narrow
		// enough that the test fails loudly if the code accidentally
		// adopted the stale apiserver timestamp.
		Expect(firstSeenAny.(time.Time)).To(BeTemporally("~", time.Now(), 2*time.Second),
			"first-seen must restart at now, not adopt the apiserver-side timestamp")
	})

	It("rate-cap: three stale replicas → exactly one delete per reconcile pass", func() {
		const name = "tracker-ratecap"
		cr := makeReplicationCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		// Seed a role=primary pod so Phase 8's NoMasterAgreement
		// guard doesn't suppress deletion — the rate-cap path under
		// test only fires when the cluster has an agreed primary
		// (the recovery state preserves replicas as failover
		// candidates instead).
		seedPrimary(name+"-0", name, "10.0.0.30")
		pod1 := seedReplica(name+"-1", name, "10.0.0.31", 0)
		pod2 := seedReplica(name+"-2", name, "10.0.0.32", 0)
		pod3 := seedReplica(name+"-3", name, "10.0.0.33", 0)
		DeferCleanup(cleanupPodsAndCR, name, name+"-0", name+"-1", name+"-2", name+"-3")

		fake := newFakeLagChecker()
		for _, addr := range []string{"10.0.0.31:6379", "10.0.0.32:6379", "10.0.0.33:6379"} {
			fake.errAddr[addr] = context.DeadlineExceeded
		}
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake}

		past := time.Now().Add(-100 * time.Second)
		inner := &sync.Map{}
		inner.Store(pod1.UID, past)
		inner.Store(pod2.UID, past)
		inner.Store(pod3.UID, past)
		r.stateFor(client.ObjectKeyFromObject(cr)).staleReplicas = inner

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())

		alive := 0
		for _, n := range []string{name + "-1", name + "-2", name + "-3"} {
			got := &corev1.Pod{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: n, Namespace: "default"}, got)
			if apierrors.IsNotFound(err) {
				continue
			}
			Expect(err).NotTo(HaveOccurred())
			if got.DeletionTimestamp == nil {
				alive++
			}
		}
		Expect(alive).To(Equal(2),
			"rate-cap violated: expected exactly one of three stale replicas to be deleted per pass, got alive=%d", alive)

		// The reconciler clears the tracker entry for the pod it
		// deleted (so the next pass doesn't re-evaluate a UID for a
		// pod that no longer exists). Asserting exactly one of the
		// three pre-populated entries was cleared pins the delete
		// path — not just "some pod is missing for unrelated reasons".
		clearedEntries := 0
		for _, uid := range []types.UID{pod1.UID, pod2.UID, pod3.UID} {
			if _, present := inner.Load(uid); !present {
				clearedEntries++
			}
		}
		Expect(clearedEntries).To(Equal(1),
			"delete path didn't clear the tracker entry for the deleted pod (cleared=%d, want 1)", clearedEntries)
	})

	It("recovery: gate flips True → tracker entry cleared", func() {
		const name = "tracker-recovery"
		cr := makeReplicationCR(name, 2)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		pod := seedReplica(name+"-1", name, "10.0.0.41", 0)
		DeferCleanup(cleanupPodsAndCR, name, name+"-1")

		fake := newFakeLagChecker()
		fake.byAddr["10.0.0.41:6379"] = valkey.LagState{Role: "slave", LinkUp: true, LagBytes: 0}
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake}

		// Pre-existing entry — the replica was observed False before
		// recovery. After this reconcile, the timer must be wiped so a
		// future False → True → False bounce restarts from scratch
		// instead of resuming the old countdown.
		inner := &sync.Map{}
		inner.Store(pod.UID, time.Now().Add(-30*time.Second))
		r.stateFor(client.ObjectKeyFromObject(cr)).staleReplicas = inner

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())

		got := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-1", Namespace: "default"}, got)).To(Succeed())
		cond := findReplicationCondition(got)
		Expect(cond).NotTo(BeNil(), "gate condition must be present after the patch")
		Expect(cond.Status).To(Equal(corev1.ConditionTrue),
			"healthy replica's gate should be patched True; got %s (%q)", cond.Status, cond.Message)

		_, stillTracked := inner.Load(pod.UID)
		Expect(stillTracked).To(BeFalse(),
			"tracker entry must be cleared on recovery — otherwise a future regression resumes the countdown from the old firstSeen")
	})

	It("CR delete: NotFound path evicts the per-CR inner map", func() {
		const name = "tracker-crdelete"
		crKey := types.NamespacedName{Name: name, Namespace: "default"}

		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		inner := &sync.Map{}
		inner.Store(types.UID("dead-pod-uid"), time.Now())
		r.stateFor(crKey).staleReplicas = inner
		Expect(innerFor(r, crKey)).NotTo(BeNil(), "sanity: tracker entry must exist before reconcile")

		// Pin the precondition: the CR must NOT exist, otherwise
		// Reconcile would fall through to the live-path and the
		// cleanup we're testing wouldn't fire — the test would still
		// pass by coincidence and silently invert.
		probe := &valkeyv1beta1.Valkey{}
		probeErr := k8sClient.Get(ctx, crKey, probe)
		Expect(apierrors.IsNotFound(probeErr)).To(BeTrue(),
			"CR must not exist before this case — got Get err=%v", probeErr)

		// No CR with this name exists — the Phase 0a NotFound branch
		// inside Reconcile must invoke the staleReplicaTrackers
		// cleanup. Mirrors the per-CR cleanup pattern used for the
		// other reconciler-owned sync.Maps.
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: crKey})
		Expect(err).NotTo(HaveOccurred())

		Expect(innerFor(r, crKey)).To(BeNil(),
			"NotFound cleanup must evict the per-CR staleReplicaTrackers entry")
	})
})
