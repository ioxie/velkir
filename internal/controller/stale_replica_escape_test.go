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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/valkey"
)

// escapeDeadMaster is a corpse IP no surveyed pod carries, so a replica
// reporting it as master_host reads as a dead lineage.
const escapeDeadMaster = "10.0.0.99"

// escapeDeadMasterB is a SECOND corpse IP, for divergent-lineage specs
// where two replicas point at different dead masters.
const escapeDeadMasterB = "10.0.0.98"

func escapeReplica(name string, offset int64, haveOffset bool) surveyedPod {
	return surveyedPod{name: name, ip: name + "-ip", state: valkey.LagState{
		Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster,
		SlaveReplOffset: offset, HaveSlaveOffset: haveOffset,
	}}
}

// TestChooseEscapeVictim_ProtectsHighestOffset pins that the victim is
// the lowest-offset eligible replica and the promotion candidate (the
// highest offset) is never chosen.
func TestChooseEscapeVictim_ProtectsHighestOffset(t *testing.T) {
	t.Parallel()
	reps := []surveyedPod{escapeReplica("a", 100, true), escapeReplica("b", 50, true), escapeReplica("c", 75, true)}
	elig := []bool{true, true, true}
	protectedIdx := choosePromotionCandidate(reps)
	if protectedIdx != 0 {
		t.Fatalf("precondition: protected should be a (idx 0, offset 100), got %d", protectedIdx)
	}
	if v := chooseEscapeVictim(reps, elig, protectedIdx); v != 1 {
		t.Fatalf("victim = %d (%s); want idx 1 (b, offset 50)", v, reps[v].name)
	}
}

// TestChooseEscapeVictim_OnlyEligibleIsHighestOffset_ReturnsMinus1 pins
// that when the sole eligible replica is also the promotion candidate,
// there is no victim (the escape must not delete the candidate).
func TestChooseEscapeVictim_OnlyEligibleIsHighestOffset_ReturnsMinus1(t *testing.T) {
	t.Parallel()
	reps := []surveyedPod{escapeReplica("a", 100, true), escapeReplica("b", 50, true)}
	elig := []bool{true, false}
	protectedIdx := choosePromotionCandidate(reps)
	if v := chooseEscapeVictim(reps, elig, protectedIdx); v != -1 {
		t.Fatalf("want -1 (sole eligible is the protected candidate), got %d", v)
	}
}

// TestChooseEscapeVictim_TieOffset_KeepsLowestName pins the tie-break:
// equal offsets delete the lexically-higher name, preserving the
// lowest-name pod (the promotion path's own tie-break winner).
func TestChooseEscapeVictim_TieOffset_KeepsLowestName(t *testing.T) {
	t.Parallel()
	reps := []surveyedPod{escapeReplica("a", 50, true), escapeReplica("b", 50, true), escapeReplica("c", 100, true)}
	elig := []bool{true, true, false}
	protectedIdx := choosePromotionCandidate(reps) // c (100)
	v := chooseEscapeVictim(reps, elig, protectedIdx)
	if reps[v].name != "b" {
		t.Fatalf("want victim b (lexically higher; keeps a), got %s", reps[v].name)
	}
}

// TestChooseEscapeVictim_UnrankableSet_ReturnsMinus1 pins that an
// eligible set with no readable offsets (every HaveSlaveOffset false)
// yields NO victim: chooseEscapeVictim skips offset-less candidates
// (their true offset is unknown; deleting one could discard the freshest
// data), so an all-unrankable set has no rankable victim. The escape
// disarms this case upstream at guard 9.
func TestChooseEscapeVictim_UnrankableSet_ReturnsMinus1(t *testing.T) {
	t.Parallel()
	reps := []surveyedPod{escapeReplica("a", 0, false), escapeReplica("b", 0, false), escapeReplica("c", 0, false)}
	elig := []bool{true, true, true}
	protectedIdx := choosePromotionCandidate(reps)
	if protectedIdx != -1 {
		t.Fatalf("precondition: unrankable set → -1, got %d", protectedIdx)
	}
	if v := chooseEscapeVictim(reps, elig, protectedIdx); v != -1 {
		t.Fatalf("want -1 (no rankable eligible victim), got %d", v)
	}
}

// TestChooseEscapeVictim_NoEligible_ReturnsMinus1 pins that no eligible
// replica yields no victim.
func TestChooseEscapeVictim_NoEligible_ReturnsMinus1(t *testing.T) {
	t.Parallel()
	reps := []surveyedPod{escapeReplica("a", 100, true), escapeReplica("b", 50, true)}
	elig := []bool{false, false}
	if v := chooseEscapeVictim(reps, elig, choosePromotionCandidate(reps)); v != -1 {
		t.Fatalf("want -1 (none eligible), got %d", v)
	}
}

// TestObserveNoPrimarySince_SetsOnceAndClears pins the sustained-dwell
// test-and-set: it seeds once, never overwrites while set, and re-seeds
// after a clear (so the dwell cannot latch across a primary reappearance).
func TestObserveNoPrimarySince_SetsOnceAndClears(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	t0 := time.Now()
	if got := s.observeNoPrimarySince(t0); !got.Equal(t0) {
		t.Fatalf("first observe should set to t0, got %v", got)
	}
	if got := s.observeNoPrimarySince(t0.Add(time.Minute)); !got.Equal(t0) {
		t.Fatalf("observe must not overwrite a set dwell; got %v want %v", got, t0)
	}
	s.clearNoPrimarySince()
	later := t0.Add(2 * time.Minute)
	if got := s.observeNoPrimarySince(later); !got.Equal(later) {
		t.Fatalf("after clear, observe must re-seed to the new now; got %v want %v", got, later)
	}
}

// TestStaleReplicaEscapeAllowed_CooldownGate pins the per-CR escape rate
// bound: never-fired is allowed, within-cooldown blocked, after allowed.
func TestStaleReplicaEscapeAllowed_CooldownGate(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	now := time.Now()
	if !s.staleReplicaEscapeAllowed(now) {
		t.Fatalf("never-fired must be allowed")
	}
	s.recordStaleReplicaEscape(now)
	// staleReplicaEscapeCooldown is 10m: 5m in is blocked, 11m allowed.
	if s.staleReplicaEscapeAllowed(now.Add(5 * time.Minute)) {
		t.Fatalf("within cooldown must be blocked")
	}
	if !s.staleReplicaEscapeAllowed(now.Add(11 * time.Minute)) {
		t.Fatalf("after cooldown must be allowed")
	}
}

// TestStaleReplicaEscapeAllowed_ExactBoundary pins the inclusive >=
// cooldown semantics: exactly staleReplicaEscapeCooldown after the last
// escape is allowed; one tick before is still blocked.
func TestStaleReplicaEscapeAllowed_ExactBoundary(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	now := time.Now()
	s.recordStaleReplicaEscape(now)
	if s.staleReplicaEscapeAllowed(now.Add(staleReplicaEscapeCooldown - time.Nanosecond)) {
		t.Fatalf("one tick before the cooldown boundary must be blocked")
	}
	if !s.staleReplicaEscapeAllowed(now.Add(staleReplicaEscapeCooldown)) {
		t.Fatalf("exactly at the cooldown boundary must be allowed (>= semantics)")
	}
}

// TestStaleReplicaEscape_DeleteCarriesUIDPrecondition drives the firing
// escape through a fake client whose Delete is intercepted, and asserts
// the escape's pod delete carries a client.Preconditions{UID} equal to
// the victim's UID. This is the negative-path guard for the
// same-name-recreate race: a mutant dropping the precondition survives
// every behavioural test (the pod still deletes) but fails this one.
func TestStaleReplicaEscape_DeleteCarriesUIDPrecondition(t *testing.T) {
	const (
		crName     = "escape-uidprecond"
		victimName = crName + "-b"
	)
	victimUID := types.UID("victim-uid-b")

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 add to scheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("valkeyv1beta1 add to scheme: %v", err)
	}

	cr := &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: "default"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode:     valkeyv1beta1.ModeSentinel,
			Sentinel: &valkeyv1beta1.SentinelPodSpec{MasterName: "mymaster", Replicas: 3, Quorum: 2},
			Valkey: valkeyv1beta1.ValkeyPodSpec{
				Replicas: 2,
				ReadinessGate: valkeyv1beta1.ReadinessGateSpec{
					Enabled: new(true), MaxLagBytes: new(int64(1 << 20)),
				},
			},
		},
	}

	gatedReplica := func(name, ip string, uid types.UID) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default", UID: uid,
				Labels: map[string]string{
					CRLabel: crName, ComponentLabel: componentValkey, RoleLabel: roleValueReplica,
				},
			},
			Spec:   corev1.PodSpec{ReadinessGates: []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}}},
			Status: corev1.PodStatus{PodIP: ip},
		}
	}
	a := gatedReplica(crName+"-a", "10.0.0.221", types.UID("uid-a")) // offset 100 (protected)
	b := gatedReplica(victimName, "10.0.0.222", victimUID)           // offset 50 (victim)
	sentinelPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: crName + "-s0", Namespace: "default",
			Labels: map[string]string{CRLabel: crName, ComponentLabel: componentSentinel},
		},
		Status: corev1.PodStatus{PodIP: "10.1.0.1"},
	}

	var capturedPreconditions *metav1.Preconditions
	captured := false
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cr, a, b, sentinelPod).
		WithStatusSubresource(&corev1.Pod{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cli client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if obj.GetName() == victimName {
					do := (&client.DeleteOptions{}).ApplyOptions(opts)
					capturedPreconditions = do.Preconditions
					captured = true
				}
				return cli.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	fakeChecker := newFakeLagChecker()
	fakeChecker.byAddr["10.0.0.221:6379"] = valkey.LagState{
		Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster, SlaveReplOffset: 100, HaveSlaveOffset: true,
	}
	fakeChecker.byAddr["10.0.0.222:6379"] = valkey.LagState{
		Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster, SlaveReplOffset: 50, HaveSlaveOffset: true,
	}

	r := &ValkeyReconciler{Client: c, Scheme: scheme, LagChecker: fakeChecker, Recorder: k8sevents.NewFakeRecorder(16)}
	st := r.stateFor(client.ObjectKeyFromObject(cr))
	past := time.Now().Add(-(staleReplicaEscapeDwell + time.Minute))
	st.observeNoPrimarySince(past)
	inner := &sync.Map{}
	inner.Store(a.UID, past)
	inner.Store(b.UID, past)
	st.staleReplicas = inner

	if _, err := r.reconcileReadinessGates(context.Background(), cr, ""); err != nil {
		t.Fatalf("reconcileReadinessGates: %v", err)
	}
	if !captured {
		t.Fatalf("victim delete was never issued (escape did not fire)")
	}
	if capturedPreconditions == nil || capturedPreconditions.UID == nil {
		t.Fatalf("escape delete carried no UID precondition (mutant: precondition dropped)")
	}
	if *capturedPreconditions.UID != victimUID {
		t.Fatalf("UID precondition = %q; want victim UID %q", *capturedPreconditions.UID, victimUID)
	}
}

// Phase-8 bounded-escape integration: reconcileReadinessGates deletes ONE
// least-fresh stale replica under a long sustained no-primary window,
// only in a state the recovery-promotion path itself would admit.
var _ = Describe("Phase 8 stale-replica escape", func() {
	ctx := context.Background()

	makeSentinelCR := func(name string, replicas int32) *valkeyv1beta1.Valkey {
		return &valkeyv1beta1.Valkey{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: valkeyv1beta1.ValkeySpec{
				Mode: valkeyv1beta1.ModeSentinel,
				Image: valkeyv1beta1.ImageSpec{
					Valkey:   valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Sentinel: valkeyv1beta1.ContainerImage{Repository: "valkey/valkey", Tag: "8.1.6-alpine"},
					Exporter: valkeyv1beta1.ContainerImage{Repository: "oliver006/redis_exporter", Tag: "v1.62.0"},
				},
				Sentinel: &valkeyv1beta1.SentinelPodSpec{MasterName: "mymaster", Replicas: 3, Quorum: 2},
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

	makeReplicationCR := func(name string, replicas int32) *valkeyv1beta1.Valkey {
		cr := makeSentinelCR(name, replicas)
		cr.Spec.Mode = valkeyv1beta1.ModeReplication
		cr.Spec.Sentinel = nil
		return cr
	}

	seedReplica := func(name, crName, ip string) *corev1.Pod {
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
				Containers:     []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
				ReadinessGates: []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
		return p
	}

	// seedReplicaNoIP creates a gated role=replica pod that never gets a
	// PodIP (Pending / ContainerCreating) — a potential master-in-waiting
	// the escape must defer to (survey pendingPods > 0).
	seedReplicaNoIP := func(name, crName string) *corev1.Pod {
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
				Containers:     []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
				ReadinessGates: []corev1.PodReadinessGate{{ConditionType: ReplicationReadyGate}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		return p
	}

	// seedGatelessPod creates a valkey pod with NO role label and NO
	// ReplicationReadyGate but a live PodIP — a just-stripped ex-primary
	// whose gate/role were removed during a wedged failover. The Phase-8
	// loop skips it (gate-less), but its IP must still fold into the
	// escape survey's live-pod set.
	seedGatelessPod := func(name, crName, ip string) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels: map[string]string{
					CRLabel:        crName,
					ComponentLabel: componentValkey,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
		return p
	}

	seedSentinelPod := func(name, crName, ip string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels: map[string]string{
					CRLabel:        crName,
					ComponentLabel: componentSentinel,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "sentinel", Image: "valkey/valkey:8.1.6-alpine"}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}

	seedPrimary := func(name, crName, ip string) {
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
				Containers: []corev1.Container{{Name: "valkey", Image: "valkey/valkey:8.1.6-alpine"}},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}

	cleanup := func(crName string, podNames ...string) {
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

	// deadLineageChecker returns a fake whose replicas are all link-down
	// pointing at escapeDeadMaster with the given per-addr offsets.
	deadLineageChecker := func(offsetByAddr map[string]int64) *fakeLagChecker {
		fake := newFakeLagChecker()
		for addr, off := range offsetByAddr {
			fake.byAddr[addr] = valkey.LagState{
				Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster,
				SlaveReplOffset: off, HaveSlaveOffset: true,
			}
		}
		return fake
	}

	// primeReconciler wires a reconciler and pre-seeds the per-CR dwell +
	// stale-replica firstSeen so the pods are past the escape threshold.
	primeReconciler := func(cr *valkeyv1beta1.Valkey, fake *fakeLagChecker, pods ...*corev1.Pod) (*ValkeyReconciler, *k8sevents.FakeRecorder) {
		rec := k8sevents.NewFakeRecorder(32)
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake, Recorder: rec}
		crKey := client.ObjectKeyFromObject(cr)
		st := r.stateFor(crKey)
		past := time.Now().Add(-(staleReplicaEscapeDwell + time.Minute))
		st.observeNoPrimarySince(past)
		inner := &sync.Map{}
		for _, p := range pods {
			inner.Store(p.UID, past)
		}
		st.staleReplicas = inner
		return r, rec
	}

	alive := func(name string) bool {
		p := &corev1.Pod{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, p)
		return err == nil && p.DeletionTimestamp == nil
	}

	It("fires after sustained no-primary: deletes the lowest-offset replica, preserves the highest", func() {
		const name = "escape-fire"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.41") // offset 100 (protected)
		b := seedReplica(name+"-b", name, "10.0.0.42") // offset 50 (victim)
		c := seedReplica(name+"-c", name, "10.0.0.43") // offset 75
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		seedSentinelPod(name+"-s1", name, "10.1.0.2")
		seedSentinelPod(name+"-s2", name, "10.1.0.3")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-c", name+"-s0", name+"-s1", name+"-s2")

		fake := deadLineageChecker(map[string]int64{
			"10.0.0.41:6379": 100, "10.0.0.42:6379": 50, "10.0.0.43:6379": 75,
		})
		r, rec := primeReconciler(cr, fake, a, b, c)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())

		Expect(alive(name+"-b")).To(BeFalse(), "lowest-offset replica (50) must be the victim")
		Expect(alive(name+"-a")).To(BeTrue(), "highest-offset replica (100) must be preserved")
		Expect(alive(name+"-c")).To(BeTrue(), "non-victim replica must survive (one-delete-per-pass)")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(1))
		Expect(r.stateFor(client.ObjectKeyFromObject(cr)).staleReplicaEscapeAllowed(time.Now())).
			To(BeFalse(), "escape cooldown must be stamped after firing")
	})

	It("stays suppressed below the sustained-no-primary dwell", func() {
		const name = "escape-belowdwell"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.51")
		b := seedReplica(name+"-b", name, "10.0.0.52")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.51:6379": 100, "10.0.0.52:6379": 50})
		rec := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake, Recorder: rec}
		st := r.stateFor(client.ObjectKeyFromObject(cr))
		// Dwell only 5 min — below the 10 min threshold.
		st.observeNoPrimarySince(time.Now().Add(-5 * time.Minute))
		inner := &sync.Map{}
		inner.Store(a.UID, time.Now().Add(-(staleReplicaEscapeVictimDwell + time.Minute)))
		inner.Store(b.UID, time.Now().Add(-(staleReplicaEscapeVictimDwell + time.Minute)))
		st.staleReplicas = inner

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("does not dial a gate-less pod while the escape dwell is unexpired", func() {
		const name = "escape-nodial"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		const gatelessIP = "10.0.0.230"
		seedGatelessPod(name+"-gl", name, gatelessIP)
		a := seedReplica(name+"-a", name, "10.0.0.231")
		b := seedReplica(name+"-b", name, "10.0.0.232")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-gl", name+"-a", name+"-b", name+"-s0")

		// Suppression holds (no primary) but the sustained dwell is only
		// half-spent: every gate-less classification reading would be
		// discarded by the escape's dwell guard, so the classification
		// dial itself must not run — this is the survey's only NEW wire
		// call, and an ordinary failover window sits in exactly this
		// state at the 5s cadence for minutes.
		fake := deadLineageChecker(map[string]int64{"10.0.0.231:6379": 100, "10.0.0.232:6379": 50})
		rec := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake, Recorder: rec}
		st := r.stateFor(client.ObjectKeyFromObject(cr))
		st.observeNoPrimarySince(time.Now().Add(-staleReplicaEscapeDwell / 2))
		inner := &sync.Map{}
		inner.Store(a.UID, time.Now().Add(-(staleReplicaEscapeVictimDwell + time.Minute)))
		inner.Store(b.UID, time.Now().Add(-(staleReplicaEscapeVictimDwell + time.Minute)))
		st.staleReplicas = inner

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(fake.callCount(gatelessIP+":6379")).To(Equal(0),
			"a gate-less pod must not be dialed while the escape dwell is unexpired")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("does not dial a gate-less pod while the escape cooldown is active", func() {
		const name = "escape-nodialcooldown"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		const gatelessIP = "10.0.0.240"
		seedGatelessPod(name+"-gl", name, gatelessIP)
		a := seedReplica(name+"-a", name, "10.0.0.241")
		b := seedReplica(name+"-b", name, "10.0.0.242")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-gl", name+"-a", name+"-b", name+"-s0")

		// Dwell satisfied but a recent escape holds the per-CR cooldown:
		// the escape cannot fire this pass, so the gate-less
		// classification dial must not run either.
		fake := deadLineageChecker(map[string]int64{"10.0.0.241:6379": 100, "10.0.0.242:6379": 50})
		r, rec := primeReconciler(cr, fake, a, b)
		r.stateFor(client.ObjectKeyFromObject(cr)).recordStaleReplicaEscape(time.Now().Add(-time.Minute))

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(fake.callCount(gatelessIP+":6379")).To(Equal(0),
			"a gate-less pod must not be dialed while the escape cooldown is active")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("stays suppressed while every victim's own gate-False dwell is fresh", func() {
		const name = "escape-victimdwell"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.251")
		b := seedReplica(name+"-b", name, "10.0.0.252")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		// The sustained no-primary dwell IS satisfied, but both replicas'
		// gate-False first-seen stamps are fresh (a just-recreated pod's
		// new UID resets its tracker entry): neither is escape-eligible,
		// so no victim exists and nothing is deleted — the per-victim
		// anti-thrash guard, independent of the CR-level dwell.
		fake := deadLineageChecker(map[string]int64{"10.0.0.251:6379": 100, "10.0.0.252:6379": 50})
		rec := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake, Recorder: rec}
		st := r.stateFor(client.ObjectKeyFromObject(cr))
		st.observeNoPrimarySince(time.Now().Add(-(staleReplicaEscapeDwell + time.Minute)))
		inner := &sync.Map{}
		inner.Store(a.UID, time.Now().Add(-time.Minute))
		inner.Store(b.UID, time.Now().Add(-time.Minute))
		st.staleReplicas = inner

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name+"-b")).To(BeTrue(),
			"a fresh per-victim gate-False dwell must block the escape even past the sustained no-primary dwell")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("never deletes the last replica", func() {
		const name = "escape-lastreplica"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.61")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.61:6379": 100})
		r, rec := primeReconciler(cr, fake, a)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name+"-a")).To(BeTrue(), "the sole replica must never be deleted")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("is disarmed by a reachable de-facto master", func() {
		const name = "escape-reachablemaster"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.71")
		b := seedReplica(name+"-b", name, "10.0.0.72")
		c := seedReplica(name+"-c", name, "10.0.0.73")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-c", name+"-s0")

		// a and b are clean dead-lineage eligible victims — >= 2 so guard 8
		// ("never the last replica") is satisfied and does NOT mask the
		// named guard. c self-reports as master (a sentinel-driven flip the
		// operator hasn't relabelled): a reachable master disarms the escape.
		fake := deadLineageChecker(map[string]int64{"10.0.0.71:6379": 100, "10.0.0.72:6379": 75})
		fake.byAddr["10.0.0.73:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 200, HaveMasterOffset: true}
		r, rec := primeReconciler(cr, fake, a, b, c)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(alive(name + "-c")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("is disarmed when any replica dial errors", func() {
		const name = "escape-dialerr"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.81")
		b := seedReplica(name+"-b", name, "10.0.0.82")
		c := seedReplica(name+"-c", name, "10.0.0.83")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-c", name+"-s0")

		// a and b are clean dead-lineage eligible victims (>= 2 so guard 8
		// does not mask the named guard). c's dial errors (topology
		// uncertainty) → dialFailures > 0 disarms the escape.
		fake := deadLineageChecker(map[string]int64{"10.0.0.81:6379": 100, "10.0.0.82:6379": 75})
		fake.errAddr["10.0.0.83:6379"] = context.DeadlineExceeded
		r, rec := primeReconciler(cr, fake, a, b, c)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(alive(name + "-c")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("never fires in replication mode even with sentinel endpoints present (manual-failover contract)", func() {
		const name = "escape-replmode"
		cr := makeReplicationCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.91")
		b := seedReplica(name+"-b", name, "10.0.0.92")
		// Seed a sentinel pod so guard 6 (empty endpoints) is satisfied
		// and the sentinel-MODE gate (guard 2) is the SOLE reason the
		// escape defers — otherwise guard 6 would mask a regression of
		// guard 2, the manual-failover-contract guard this spec names.
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.91:6379": 100, "10.0.0.92:6379": 50})
		r, rec := primeReconciler(cr, fake, a, b)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name+"-b")).To(BeTrue(), "replication-mode no-primary is the manual-failover contract; escape must not fire")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("is bounded by the per-CR escape cooldown", func() {
		const name = "escape-cooldown"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.101")
		b := seedReplica(name+"-b", name, "10.0.0.102")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.101:6379": 100, "10.0.0.102:6379": 50})
		r, rec := primeReconciler(cr, fake, a, b)
		// A recent escape blocks a second within the cooldown window.
		r.stateFor(client.ObjectKeyFromObject(cr)).recordStaleReplicaEscape(time.Now().Add(-time.Minute))

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("defers while an operator-driven failover is in flight", func() {
		const name = "escape-failover"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.121")
		b := seedReplica(name+"-b", name, "10.0.0.122")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.121:6379": 100, "10.0.0.122:6379": 50})
		r, rec := primeReconciler(cr, fake, a, b)
		// An in-flight failover latch must defer the escape (guard 4) so it
		// never disturbs the pod set mid-election.
		crKey := client.ObjectKeyFromObject(cr)
		r.failoverLatchSet(crKey, escapeDeadMaster+":6379")
		Expect(r.IsFailoverInFlight(crKey)).To(BeTrue(), "precondition: latch must read as in-flight")

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("defers within the recovery-promotion cooldown (Phase-8-precedes-Phase-11 interlock)", func() {
		const name = "escape-promocooldown"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.131")
		b := seedReplica(name+"-b", name, "10.0.0.132")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.131:6379": 100, "10.0.0.132:6379": 50})
		r, rec := primeReconciler(cr, fake, a, b)
		// A REPLICAOF NO ONE issued last pass (Phase 11) must defer the
		// escape until its cooldown elapses (guard 5) so a landing
		// promotion is not disturbed by a pod delete.
		r.stateFor(client.ObjectKeyFromObject(cr)).quorumTracker().recoveryPromotionLastFired = time.Now()

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("does not disturb the primary-present inline stale-delete path", func() {
		const name = "escape-normalpath"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		seedPrimary(name+"-0", name, "10.0.0.110")
		b := seedReplica(name+"-b", name, "10.0.0.112")
		DeferCleanup(cleanup, name, name+"-0", name+"-b")

		fake := newFakeLagChecker()
		fake.errAddr["10.0.0.112:6379"] = context.DeadlineExceeded
		rec := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake, Recorder: rec}
		// Pre-seed the inline stale tracker past the 90s threshold; a
		// primary IS present, so suppressStaleDelete is false and the
		// ordinary inline delete (not the escape) fires.
		inner := &sync.Map{}
		inner.Store(b.UID, time.Now().Add(-2*staleReplicaThreshold))
		r.stateFor(client.ObjectKeyFromObject(cr)).staleReplicas = inner

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name+"-b")).To(BeFalse(), "primary present → ordinary inline stale-delete still fires")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0), "the escape event must not fire on the normal path")
	})

	It("clears the sustained-no-primary dwell when a primary reappears (anti-latch)", func() {
		const name = "escape-antilatch"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		seedPrimary(name+"-0", name, "10.0.0.140")
		seedReplica(name+"-b", name, "10.0.0.141")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-0", name+"-b", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.141:6379": 50})
		rec := k8sevents.NewFakeRecorder(16)
		r := &ValkeyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), LagChecker: fake, Recorder: rec}
		st := r.stateFor(client.ObjectKeyFromObject(cr))
		// Prime a long-standing no-primary dwell that a returning primary
		// must clear — otherwise the dwell would latch across the recovery
		// and re-arm the escape the instant a primary later vanishes again.
		st.observeNoPrimarySince(time.Now().Add(-(staleReplicaEscapeDwell + time.Minute)))

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())

		st.mu.Lock()
		noPrimarySince := st.noPrimarySince
		st.mu.Unlock()
		Expect(noPrimarySince.IsZero()).To(BeTrue(),
			"a primary-labeled pod must clear the sustained-no-primary dwell")
	})

	It("is disarmed by a pending (IP-less) gated replica (potential master-in-waiting)", func() {
		const name = "escape-pending"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.151")
		b := seedReplica(name+"-b", name, "10.0.0.152")
		pending := seedReplicaNoIP(name+"-c", name)
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-c", name+"-s0")

		// a and b are clean dead-lineage eligible victims; the IP-less
		// pending replica is a potential master-in-waiting → pendingPods > 0
		// disarms the escape.
		fake := deadLineageChecker(map[string]int64{"10.0.0.151:6379": 100, "10.0.0.152:6379": 75})
		r, rec := primeReconciler(cr, fake, a, b, pending)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(alive(name + "-c")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("is disarmed by a link-up replica (replication recovering)", func() {
		const name = "escape-linkup"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.161")
		b := seedReplica(name+"-b", name, "10.0.0.162")
		c := seedReplica(name+"-c", name, "10.0.0.163")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-c", name+"-s0")

		// a and b are clean dead-lineage eligible victims (>= 2 for guard 8).
		// c's replication link is up: any link-up replica means the topology
		// is recovering, so the escape must disarm.
		fake := deadLineageChecker(map[string]int64{"10.0.0.161:6379": 100, "10.0.0.162:6379": 75})
		fake.byAddr["10.0.0.163:6379"] = valkey.LagState{
			Role: "slave", LinkUp: true, MasterHost: escapeDeadMaster,
			SlaveReplOffset: 90, HaveSlaveOffset: true,
		}
		r, rec := primeReconciler(cr, fake, a, b, c)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(alive(name + "-c")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("is disarmed by divergent dead lineages (incomparable replication histories)", func() {
		const name = "escape-divergent"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.171")
		b := seedReplica(name+"-b", name, "10.0.0.172")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		// a and b point at DIFFERENT dead masters — ranking offsets across
		// divergent lineages could delete the wrong data, so the escape must
		// defer to a human (guard 7's single-lineage check).
		fake := newFakeLagChecker()
		fake.byAddr["10.0.0.171:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster,
			SlaveReplOffset: 100, HaveSlaveOffset: true,
		}
		fake.byAddr["10.0.0.172:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: escapeDeadMasterB,
			SlaveReplOffset: 50, HaveSlaveOffset: true,
		}
		r, rec := primeReconciler(cr, fake, a, b)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("fires at the exactly-two-eligible minimum (deletes one, preserves the candidate)", func() {
		const name = "escape-mintwo"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.181") // offset 100 (protected)
		b := seedReplica(name+"-b", name, "10.0.0.182") // offset 50 (victim)
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		fake := deadLineageChecker(map[string]int64{"10.0.0.181:6379": 100, "10.0.0.182:6379": 50})
		r, rec := primeReconciler(cr, fake, a, b)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name+"-b")).To(BeFalse(), "lowest-offset of the two is the victim")
		Expect(alive(name+"-a")).To(BeTrue(),
			"the promotion candidate (highest offset) is preserved even at the 2-replica minimum")
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(1))
	})

	It("disarms when the admitted survey is unrankable (no replica reports an offset)", func() {
		const name = "escape-unrankable"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		a := seedReplica(name+"-a", name, "10.0.0.191")
		b := seedReplica(name+"-b", name, "10.0.0.192")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-a", name+"-b", name+"-s0")

		// Both link-down at the SAME dead master (a single admissible dead
		// lineage) but NEITHER reports slave_repl_offset — the set is
		// unrankable, so there is no provably-safe least-fresh victim and
		// the escape disarms (guard 9) rather than guess.
		fake := newFakeLagChecker()
		fake.byAddr["10.0.0.191:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster, HaveSlaveOffset: false,
		}
		fake.byAddr["10.0.0.192:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster, HaveSlaveOffset: false,
		}
		r, rec := primeReconciler(cr, fake, a, b)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0))
	})

	It("does not fire while a gate-less live pod backs the replicas' lineage (survey covers gate-less pods)", func() {
		const name = "escape-gateless"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		const gatelessIP = "10.0.0.200"
		seedGatelessPod(name+"-gl", name, gatelessIP)
		a := seedReplica(name+"-a", name, "10.0.0.201")
		b := seedReplica(name+"-b", name, "10.0.0.202")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-gl", name+"-a", name+"-b", name+"-s0")

		// a and b are link-down, offset-rankable, past the victim dwell —
		// clean escape victims in isolation. But their master_host is the
		// gate-less pod's LIVE IP: because the survey folds EVERY pod's IP
		// (gate-less included) into livePodIPs, the lineage reads live and
		// promotionSurveyAdmits disarms the escape. A gate-less-blind survey
		// would miss the IP, read the lineage as dead, and wrongly delete.
		fake := newFakeLagChecker()
		fake.byAddr["10.0.0.201:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: gatelessIP,
			SlaveReplOffset: 100, HaveSlaveOffset: true,
		}
		fake.byAddr["10.0.0.202:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: gatelessIP,
			SlaveReplOffset: 50, HaveSlaveOffset: true,
		}
		r, rec := primeReconciler(cr, fake, a, b)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0),
			"a gate-less live pod's IP must keep the lineage live and disarm the escape")
	})

	It("does not fire when a gate-less live master exists but replicas follow a different dead master (masters classification parity)", func() {
		const name = "escape-glmaster"
		cr := makeSentinelCR(name, 3)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		const gatelessMasterIP = "10.0.0.210"
		seedGatelessPod(name+"-gl", name, gatelessMasterIP)
		a := seedReplica(name+"-a", name, "10.0.0.211")
		b := seedReplica(name+"-b", name, "10.0.0.212")
		seedSentinelPod(name+"-s0", name, "10.1.0.1")
		DeferCleanup(cleanup, name, name+"-gl", name+"-a", name+"-b", name+"-s0")

		// The gate-less pod is a stripped ex-primary still running AS master
		// (INFO role:master). a and b are link-down, offset-rankable, past
		// the victim dwell — and they point at a DIFFERENT dead master
		// (escapeDeadMaster), a corpse in NO pod, so the live-lineage IP
		// check would NOT catch the gate-less master. Only dialing and
		// classifying the gate-less pod into escapeMasters trips
		// promotionSurveyAdmits' len(masters)!=0 disarm — the parity the
		// escape needs so it never deletes a replica while a reachable
		// master exists.
		fake := newFakeLagChecker()
		fake.byAddr[gatelessMasterIP+":6379"] = valkey.LagState{
			Role: valkey.RoleMaster, MasterReplOffset: 300, HaveMasterOffset: true,
		}
		fake.byAddr["10.0.0.211:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster,
			SlaveReplOffset: 100, HaveSlaveOffset: true,
		}
		fake.byAddr["10.0.0.212:6379"] = valkey.LagState{
			Role: "slave", LinkUp: false, MasterHost: escapeDeadMaster,
			SlaveReplOffset: 50, HaveSlaveOffset: true,
		}
		r, rec := primeReconciler(cr, fake, a, b)

		_, err := r.reconcileReadinessGates(ctx, cr, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(alive(name + "-a")).To(BeTrue())
		Expect(alive(name + "-b")).To(BeTrue())
		Expect(countEvent(rec, "StaleReplicaEscapeDeleted")).To(Equal(0),
			"a gate-less live master must be classified into escapeMasters and disarm the escape")
	})
})
