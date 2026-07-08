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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// Phase 7 envtest gates for the sentinel-mode exit criteria. The unit tests
// in phase7_test.go cover the desiredRolesForCR helper at the
// pure-function boundary; these specs drive the integrated
// reconcileRoleLabels path against a live envtest apiserver + a real
// sentinel.Manager so both halves of the suppression / recovery
// contract are pinned end to end:
//   - split-brain injection: observer publishes QuorumOK=false, no
//     role labels written (the SplitBrainDetected event + counter are
//     emitted by updateQuorumSuppressionGate, once per episode — not
//     by Phase 7).
//   - quorum recovery: observer republishes QuorumOK=true after the
//     fault clears, role labels resume from the snapshot's primary IP
//     (recoveringSentinel scripts the wire protocol so the observer
//     dial+poll succeeds against a real TCP listener).
//   - pre-snapshot fallback: bootstrap rule applies when no observer
//     is wired (boot-race window).

var _ = Describe("Phase 7 sentinel-mode envtest gates (#26 exit criteria)", func() {
	ctx := context.Background()

	makeSentinelCR := func(name string) *valkeyv1beta1.Valkey {
		return &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeSentinel,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Valkey:   valkeyv1beta1.ValkeyPodSpec{Replicas: 3},
				Sentinel: &valkeyv1beta1.SentinelPodSpec{Replicas: 3, Quorum: 2, MasterName: "mymaster"},
			},
		}
	}

	cleanupCR := func(name string) {
		cr := &valkeyv1beta1.Valkey{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, cr); err == nil {
			_ = k8sClient.Delete(ctx, cr, client.GracePeriodSeconds(0))
		}
	}

	cleanupPods := func(crName string) {
		_ = k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
			client.InNamespace("default"),
			client.MatchingLabels{CRLabel: crName},
			client.GracePeriodSeconds(0))
	}

	// startedManager spins up a sentinel.Manager so the test can
	// Ensure an observer for a CR; the observer publishes
	// QuorumOK=false when its endpoints are unreachable (closed-port
	// dial returns fast and the snapshot lands with
	// Source != SourceNone + QuorumOK=false). Mirrors the helper of
	// the same name in phase7_test.go.
	startedManager := func() (*sentinel.Manager, context.CancelFunc) {
		rec := k8sevents.NewFakeRecorder(64)
		m := sentinel.NewManager(rec, sentinel.Options{
			PollInterval:       50 * time.Millisecond,
			PubsubReadDeadline: 30 * time.Second,
			PingTimeout:        time.Second,
		})
		mctx, mcancel := context.WithCancel(context.Background())
		go func() { _ = m.Start(mctx) }()
		probe := types.NamespacedName{Namespace: "_probe", Name: "_probe"}
		Eventually(func() error {
			return m.Ensure(mctx, probe, "_probe", "",
				[]sentinel.Endpoint{{Name: "_probe", Addr: "127.0.0.1:1"}})
		}, 2*time.Second, 10*time.Millisecond).Should(Succeed())
		m.Remove(probe)
		return m, mcancel
	}

	seedValkeyPod := func(crName, podName, ip string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Labels: map[string]string{
					podIndexLabel:     podOrdinal(podName),
					CRLabel:           crName,
					ComponentLabel:    componentValkey,
					ManagedByLabel:    ManagedByValue,
					AppNameLabel:      "valkey",
					AppInstanceLabel:  crName,
					AppComponentLabel: componentValkey,
					AppPartOfLabel:    "velkir",
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
	}

	drainEvents := func(rec *k8sevents.FakeRecorder) []string {
		var out []string
		for {
			select {
			case e := <-rec.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	It("quorum-unknown: suppresses role-label writes WITHOUT emitting SplitBrainDetected (#557)", func() {
		const name = "i26-splitbrain"
		cr := makeSentinelCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanupCR, name)
		DeferCleanup(cleanupPods, name)

		mgr, mcancel := startedManager()
		DeferCleanup(mcancel)

		// Ensure the observer for our CR with two unreachable
		// sentinels — the first poll tick lands a snapshot with
		// Quorum=Unknown (reachable=0 < threshold) + Source !=
		// SourceNone (the observer published, not the boot-time
		// placeholder). Phase 7 must suppress the relabel (refuse to act
		// on incomplete agreement) but must NOT emit SplitBrainDetected:
		// Unknown is "no data yet", not a real split-brain.
		crKey := types.NamespacedName{Namespace: "default", Name: name}
		Expect(mgr.Ensure(ctx, crKey, "mymaster", "",
			[]sentinel.Endpoint{
				{Name: name + "-sentinel-0", Addr: "127.0.0.1:1"},
				{Name: name + "-sentinel-1", Addr: "127.0.0.1:2"},
			})).To(Succeed())

		Eventually(func() bool {
			s := mgr.Snapshot(crKey)
			return s.Present && s.Primary.Quorum == sentinel.QuorumStatusUnknown && s.Primary.Source != sentinel.SourceNone
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(),
			"observer never published a Present + Quorum=Unknown snapshot")

		seedValkeyPod(name, name+"-0", "10.0.0.1")
		seedValkeyPod(name, name+"-1", "10.0.0.2")
		seedValkeyPod(name, name+"-2", "10.0.0.3")

		rec := k8sevents.NewFakeRecorder(32)
		r := &ValkeyReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			Recorder:         rec,
			SentinelObserver: mgr,
		}
		fetched := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())

		Expect(r.reconcileRoleLabels(ctx, fetched, "")).To(Succeed())

		for _, suffix := range []string{"-0", "-1", "-2"} {
			p := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffix, Namespace: "default"}, p)).To(Succeed())
			Expect(p.Labels).NotTo(HaveKey(RoleLabel),
				"pod %s must not carry %s under split-brain suppression", p.Name, RoleLabel)
		}

		events := drainEvents(rec)
		for _, e := range events {
			Expect(e).NotTo(ContainSubstring("SplitBrainDetected"),
				"Quorum=Unknown must NOT emit SplitBrainDetected (#557); got event %q", e)
		}
	})

	It("quorum recovery: relabel resumes when the observer republishes QuorumOK=true", func() {
		const name = "i26-recovery"
		cr := makeSentinelCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanupCR, name)
		DeferCleanup(cleanupPods, name)

		mgr, mcancel := startedManager()
		DeferCleanup(mcancel)

		// Phase A — split-brain: ensure the observer with two
		// unreachable endpoints. The first poll publishes a
		// Present + QuorumOK=false snapshot (closed-port dial fails
		// fast with reachable=0 < threshold=2). Phase 7 must
		// suppress the relabel.
		crKey := types.NamespacedName{Namespace: "default", Name: name}
		Expect(mgr.Ensure(ctx, crKey, "mymaster", "",
			[]sentinel.Endpoint{
				{Name: name + "-sentinel-0", Addr: "127.0.0.1:1"},
				{Name: name + "-sentinel-1", Addr: "127.0.0.1:2"},
			})).To(Succeed())

		Eventually(func() bool {
			s := mgr.Snapshot(crKey)
			return s.Present && !s.Primary.QuorumOK && s.Primary.Source != sentinel.SourceNone
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(),
			"observer never published a Present + QuorumOK=false snapshot")

		// Pod-1 (10.0.0.2) will be the recovery-phase primary —
		// having pod-1 be primary (not pod-0, which the bootstrap
		// rule would pick) lets the assertion distinguish the
		// snapshot-driven assignment from a bootstrap fallback.
		seedValkeyPod(name, name+"-0", "10.0.0.1")
		seedValkeyPod(name, name+"-1", "10.0.0.2")
		seedValkeyPod(name, name+"-2", "10.0.0.3")

		rec := k8sevents.NewFakeRecorder(64)
		r := &ValkeyReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			Recorder:         rec,
			SentinelObserver: mgr,
		}
		fetched := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())

		Expect(r.reconcileRoleLabels(ctx, fetched, "")).To(Succeed())
		for _, suffix := range []string{"-0", "-1", "-2"} {
			p := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffix, Namespace: "default"}, p)).To(Succeed())
			Expect(p.Labels).NotTo(HaveKey(RoleLabel),
				"phase A: pod %s must not carry %s under split-brain suppression", p.Name, RoleLabel)
		}

		_ = drainEvents(rec)

		// Phase B — recovery: replace the unreachable endpoints
		// with two recoveringSentinel TCP fakes that script
		// QuorumOK=true with master addr matching pod-1 (10.0.0.2).
		// QuorumThreshold(2)==2 so both must agree before the
		// observer publishes QuorumOK=true.
		fake1, err := newRecoveringSentinel("10.0.0.2", 100)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(fake1.Stop)
		fake2, err := newRecoveringSentinel("10.0.0.2", 100)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(fake2.Stop)

		Expect(mgr.Ensure(ctx, crKey, "mymaster", "",
			[]sentinel.Endpoint{
				{Name: name + "-sentinel-0", Addr: fake1.Addr()},
				{Name: name + "-sentinel-1", Addr: fake2.Addr()},
			})).To(Succeed())

		Eventually(func() bool {
			s := mgr.Snapshot(crKey)
			return s.Present && s.Primary.QuorumOK && s.Primary.Addr == "10.0.0.2:6379"
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(),
			"observer never republished a QuorumOK=true snapshot pinned to 10.0.0.2:6379")

		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())
		Expect(r.reconcileRoleLabels(ctx, fetched, "")).To(Succeed())

		pod1 := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-1", Namespace: "default"}, pod1)).To(Succeed())
		Expect(pod1.Labels).To(HaveKeyWithValue(RoleLabel, roleValuePrimary),
			"phase B: pod-1 (observer-elected primary at 10.0.0.2) must carry role=primary after recovery")
		for _, suffix := range []string{"-0", "-2"} {
			p := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffix, Namespace: "default"}, p)).To(Succeed())
			Expect(p.Labels).To(HaveKeyWithValue(RoleLabel, roleValueReplica),
				"phase B: non-primary pod %s must carry role=replica after recovery", p.Name)
		}
	})

	It("pre-snapshot fallback: stamps bootstrap labels until the observer publishes", func() {
		const name = "i26-presnapshot"
		cr := makeSentinelCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanupCR, name)
		DeferCleanup(cleanupPods, name)

		// No observer manager → desiredRolesForCR falls back to the
		// bootstrap rule (pod-0 = primary, others = replica) so the
		// ro-Service selector keeps working until the observer joins
		// and publishes its first snapshot. Pinning the boot-race
		// path at envtest level catches a regression where the
		// fallback mistakenly suppresses.
		seedValkeyPod(name, name+"-0", "10.0.0.1")
		seedValkeyPod(name, name+"-1", "10.0.0.2")
		seedValkeyPod(name, name+"-2", "10.0.0.3")

		crKey := types.NamespacedName{Namespace: "default", Name: name}
		rec := k8sevents.NewFakeRecorder(32)
		r := &ValkeyReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			Recorder:         rec,
			SentinelObserver: nil,
		}
		fetched := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())

		Expect(r.reconcileRoleLabels(ctx, fetched, "")).To(Succeed())

		pod0 := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-0", Namespace: "default"}, pod0)).To(Succeed())
		Expect(pod0.Labels).To(HaveKeyWithValue(RoleLabel, roleValuePrimary),
			"pod-0 must carry role=primary under bootstrap fallback")
		for _, suffix := range []string{"-1", "-2"} {
			p := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + suffix, Namespace: "default"}, p)).To(Succeed())
			Expect(p.Labels).To(HaveKeyWithValue(RoleLabel, roleValueReplica),
				"non-zero ordinal pod must carry role=replica under bootstrap fallback")
		}

		for _, e := range drainEvents(rec) {
			Expect(e).NotTo(ContainSubstring("SplitBrainDetected"),
				"SplitBrainDetected must not fire on the pre-snapshot fallback path")
		}
	})

	// The operator-driven primary rollout strips role=primary off
	// the outgoing primary BEFORE issuing SENTINEL FAILOVER, suppressing
	// re-stamping via an in-memory latch until the observer reports the
	// new primary. The latch is lost on operator restart; the durable
	// Status.Rollout.FailoverDispatch marker (written before the strip)
	// is what lets a fresh operator keep suppressing the pre-strip primary
	// mid-election instead of re-stamping it (which would reopen the
	// write-to-old-primary window). This spec models the crash-then-
	// restart path end to end and asserts count(role=primary) <= 1
	// throughout, with no persistent zero-primary state.
	It("persist strip-intent: a crash mid-failover suppresses re-stamping the pre-strip primary until +switch-master (#548)", func() {
		const name = "i548-crashrestart"
		cr := makeSentinelCR(name)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		DeferCleanup(cleanupCR, name)
		DeferCleanup(cleanupPods, name)
		crKey := types.NamespacedName{Namespace: "default", Name: name}

		// RFC 5737 TEST-NET-2 addresses, used only by this spec so the
		// marker addr does not collide with the package's "10.0.0.x:6379"
		// literals (goconst). oldIP is the pre-strip primary (pod-0);
		// newIP is the post-+switch-master primary (pod-1).
		oldIP, newIP, thirdIP := "198.51.100.1", "198.51.100.2", "198.51.100.3"
		oldAddr, newAddr := "198.51.100.1:6379", "198.51.100.2:6379"

		// Model the post-strip, pre-+switch-master crash state:
		//   - pod-0 (oldIP) WAS primary; the operator stripped its role
		//     label, then crashed → pod-0 carries no role label.
		//   - pods 1,2 are labelled replica.
		//   - the durable marker records the in-flight strip+FAILOVER
		//     (PreStripAddr = pod-0's addr).
		seedValkeyPod(name, name+"-0", oldIP)
		seedValkeyPod(name, name+"-1", newIP)
		seedValkeyPod(name, name+"-2", thirdIP)
		setRole := func(podName, role string) {
			p := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: "default"}, p)).To(Succeed())
			p.Labels[RoleLabel] = role
			Expect(k8sClient.Update(ctx, p)).To(Succeed())
		}
		setRole(name+"-1", roleValueReplica)
		setRole(name+"-2", roleValueReplica)

		fetched := &valkeyv1beta1.Valkey{}
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())
		fetched.Status.Rollout = &valkeyv1beta1.RolloutStatus{
			FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{
				PreStripAddr: oldAddr,
				Deadline:     &metav1.Time{Time: time.Now().Add(failoverInFlightLatchTTL)},
			},
		}
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())

		countPrimary := func() int {
			pl := &corev1.PodList{}
			Expect(k8sClient.List(ctx, pl, client.InNamespace("default"),
				client.MatchingLabels{CRLabel: name})).To(Succeed())
			n := 0
			for i := range pl.Items {
				if pl.Items[i].Labels[RoleLabel] == roleValuePrimary {
					n++
				}
			}
			return n
		}

		// The NEW operator after restart: a fresh reconciler (no in-memory
		// latch) + an observer that still reports the OLD primary (oldIP)
		// as the QuorumOK master — +switch-master has not yet landed.
		mgr, mcancel := startedManager()
		DeferCleanup(mcancel)
		oldPrimary1, err := newRecoveringSentinel(oldIP, 100)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(oldPrimary1.Stop)
		oldPrimary2, err := newRecoveringSentinel(oldIP, 100)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(oldPrimary2.Stop)
		Expect(mgr.Ensure(ctx, crKey, "mymaster", "",
			[]sentinel.Endpoint{
				{Name: name + "-sentinel-0", Addr: oldPrimary1.Addr()},
				{Name: name + "-sentinel-1", Addr: oldPrimary2.Addr()},
			})).To(Succeed())
		Eventually(func() bool {
			s := mgr.Snapshot(crKey)
			return s.Present && s.Primary.QuorumOK && s.Primary.Addr == oldAddr
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(),
			"observer never published the pre-strip primary (oldIP) as QuorumOK")

		rec := k8sevents.NewFakeRecorder(64)
		r := &ValkeyReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			Recorder:         rec,
			SentinelObserver: mgr,
		}

		// Reconcile 1 (post-restart): the durable marker rehydrates the
		// suppression latch, so the pre-strip primary (pod-0) is NOT
		// re-stamped role=primary even though the observer still reports
		// oldIP.
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())
		Expect(r.reconcileRoleLabels(ctx, fetched, "")).To(Succeed())

		pod0 := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-0", Namespace: "default"}, pod0)).To(Succeed())
		Expect(pod0.Labels).NotTo(HaveKeyWithValue(RoleLabel, roleValuePrimary),
			"pre-strip primary must NOT be re-stamped role=primary mid-election after a crash")
		Expect(countPrimary()).To(BeNumerically("<=", 1),
			"count(role=primary) must be <= 1 during the in-flight window")
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())
		Expect(fetched.Status.Rollout).NotTo(BeNil())
		Expect(fetched.Status.Rollout.FailoverDispatch).NotTo(BeNil(),
			"durable marker must persist while the election is in progress")

		// +switch-master lands: the observer now reports the new primary
		// (pod-1, newIP).
		newPrimary1, err := newRecoveringSentinel(newIP, 101)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(newPrimary1.Stop)
		newPrimary2, err := newRecoveringSentinel(newIP, 101)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(newPrimary2.Stop)
		Expect(mgr.Ensure(ctx, crKey, "mymaster", "",
			[]sentinel.Endpoint{
				{Name: name + "-sentinel-0", Addr: newPrimary1.Addr()},
				{Name: name + "-sentinel-1", Addr: newPrimary2.Addr()},
			})).To(Succeed())
		Eventually(func() bool {
			s := mgr.Snapshot(crKey)
			return s.Present && s.Primary.QuorumOK && s.Primary.Addr == newAddr
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue(),
			"observer never moved to the new primary (newIP) after +switch-master")

		// Reconcile 2: the window is over — the marker clears, the new
		// primary (pod-1) is stamped, exactly one primary, no persistent
		// zero-primary state.
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())
		Expect(r.reconcileRoleLabels(ctx, fetched, "")).To(Succeed())

		pod1 := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-1", Namespace: "default"}, pod1)).To(Succeed())
		Expect(pod1.Labels).To(HaveKeyWithValue(RoleLabel, roleValuePrimary),
			"the observer-elected new primary (pod-1) must carry role=primary once +switch-master lands")
		Expect(countPrimary()).To(Equal(1),
			"exactly one primary after the failover completes (no persistent zero-primary state)")
		Expect(k8sClient.Get(ctx, crKey, fetched)).To(Succeed())
		if fetched.Status.Rollout != nil {
			Expect(fetched.Status.Rollout.FailoverDispatch).To(BeNil(),
				"durable marker must be cleared once +switch-master lands")
		}
	})
})
