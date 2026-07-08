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
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/valkey"
)

// fakeReplicaOfIssuer records every IssueReplicaOf call. Tests
// inspect the recorded calls to assert on REPLICAOF target IP +
// port + replica address. Optionally returns scripted errors per
// replica address for the failure-path tests.
type fakeReplicaOfIssuer struct {
	mu       sync.Mutex
	calls    []fakeReplicaOfCall
	errByPod map[string]error
}

type fakeReplicaOfCall struct {
	addr, masterIP string
	masterPort     int
}

func newFakeReplicaOfIssuer() *fakeReplicaOfIssuer {
	return &fakeReplicaOfIssuer{
		errByPod: map[string]error{},
	}
}

func (f *fakeReplicaOfIssuer) IssueReplicaOf(_ context.Context, addr, _, masterIP string, masterPort int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeReplicaOfCall{addr: addr, masterIP: masterIP, masterPort: masterPort})
	if err, ok := f.errByPod[addr]; ok {
		return err
	}
	return nil
}

func (f *fakeReplicaOfIssuer) recorded() []fakeReplicaOfCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeReplicaOfCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// orphanTestScheme builds a runtime.Scheme that knows about
// corev1.Pod and the Valkey CR. Used to construct the fake client
// without a global side-effect.
func orphanTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := valkeyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("valkeyv1beta1.AddToScheme: %v", err)
	}
	return scheme
}

func orphanTestCR() *valkeyv1beta1.Valkey {
	return &valkeyv1beta1.Valkey{
		ObjectMeta: metav1.ObjectMeta{Name: "vk0", Namespace: "ns"},
		Spec: valkeyv1beta1.ValkeySpec{
			Mode: valkeyv1beta1.ModeSentinel,
			Valkey: valkeyv1beta1.ValkeyPodSpec{
				Replicas: 3,
			},
		},
	}
}

func labelledValkeyPod(name, podIP, role string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				CRLabel:        "vk0",
				ComponentLabel: componentValkey,
				RoleLabel:      role,
			},
		},
		Status: corev1.PodStatus{PodIP: podIP},
	}
}

// orphanPods lists the valkey pods the (now pod-threaded) orphan-master
// phase consumes, mirroring the once-per-reconcile fetch. Returns
// nil when the reconciler has no client wired (the standalone no-op
// test, which returns before reading pods).
func orphanPods(t *testing.T, r *ValkeyReconciler, v *valkeyv1beta1.Valkey) []corev1.Pod {
	t.Helper()
	if r.Client == nil {
		return nil
	}
	pods, err := r.listValkeyPods(context.Background(), v)
	if err != nil {
		t.Fatalf("listing valkey pods: %v", err)
	}
	return pods
}

func TestReconcileOrphanMasters_StandaloneIsNoOp(t *testing.T) {
	r := &ValkeyReconciler{}
	v := orphanTestCR()
	v.Spec.Mode = valkeyv1beta1.ModeStandalone
	// No fake client, no LagChecker — function must not even attempt I/O.
	// Standalone mode must return without doing any work (no client, no
	// LagChecker wired — a stray I/O attempt would panic).
	r.reconcileOrphanMasters(context.Background(), v, orphanPods(t, r, v), "")
}

func TestReconcileOrphanMasters_NoElectedPrimary_NoFailover_Skips(t *testing.T) {
	scheme := orphanTestScheme(t)
	// All pods labelled replica; no primary. With NO failover in flight the
	// dual-master self-heal is a no-op (gate 1), so the no-primary path
	// still skips without any I/O — the prior behaviour, now gated.
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica),
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	checker := newFakeLagChecker()
	issuer := newFakeReplicaOfIssuer()
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
	}
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	// No primary AND no failover in flight → skip without any I/O.
	if len(issuer.recorded()) != 0 {
		t.Errorf("expected zero REPLICAOF calls when no primary exists; got %d", len(issuer.recorded()))
	}
	if checker.callCount("10.0.0.1:6379") != 0 || checker.callCount("10.0.0.2:6379") != 0 {
		t.Errorf("expected zero CheckLag calls when no primary; got %d, %d",
			checker.callCount("10.0.0.1:6379"), checker.callCount("10.0.0.2:6379"))
	}
}

// Test addresses for the dual-master self-heal cases, routed through
// consts so the repeated literals don't trip goconst. Survivor holds the
// higher master_repl_offset; loser is demoted onto it.
const (
	dmIPSurvivor   = "10.0.5.2"
	dmIPLoser      = "10.0.5.1"
	dmAddrSurvivor = "10.0.5.2:6379"
	dmAddrLoser    = "10.0.5.1:6379"
	dmDeadIP       = "10.0.5.99"
	dmDeadAddr     = "10.0.5.99:6379"
	dmSentinel0    = "vk0-sentinel-0"
	dmSentinel1    = "vk0-sentinel-1"
	dmPod0         = "vk0-0"
	dmPod1         = "vk0-1"
	dmPod2         = "vk0-2"
	dmIPPod2       = "10.0.5.3"
	dmAddrPod2     = "10.0.5.3:6379"
)

// replicationOrphanCR is orphanTestCR in replication mode — the mode
// whose no-labeled-primary dual-master split routes to
// observeDualMasterNoPrimary (surface-only, no sentinel self-heal).
func replicationOrphanCR() *valkeyv1beta1.Valkey {
	v := orphanTestCR()
	v.Spec.Mode = valkeyv1beta1.ModeReplication
	return v
}

// selfHealReconciler builds a reconciler primed for the dual-master
// self-heal trigger: no SentinelObserver (so quorumViewUnusable is true)
// and a failover latch set (so IsFailoverInFlight is true — observedAddr
// is "" with no observer, so the latch never exits on an addr change).
// objs are seeded into the fake client; pods must carry no role=primary
// label so reconcileOrphanMasters routes into the self-heal.
func selfHealReconciler(t *testing.T, checker *fakeLagChecker, issuer *fakeReplicaOfIssuer, killer *fakeClientKillIssuer, objs ...client.Object) *ValkeyReconciler {
	t.Helper()
	scheme := orphanTestScheme(t)
	r := &ValkeyReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		LagChecker:       checker,
		ReplicaOfIssuer:  issuer,
		ClientKillIssuer: killer,
		Recorder:         k8sevents.NewFakeRecorder(16),
	}
	r.failoverLatchSet(types.NamespacedName{Namespace: "ns", Name: "vk0"}, "10.9.9.9:6379")
	return r
}

// twoMasterPods seeds two pods (loser at dmIPLoser, survivor at
// dmIPSurvivor), both labelled replica so reconcileOrphanMasters sees no
// elected primary and routes into the self-heal.
func twoMasterPods() []client.Object {
	return []client.Object{
		labelledValkeyPod(dmPod0, dmIPLoser, roleValueReplica),
		labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
	}
}

// waitForSnapshot blocks until the observer snapshot for cr satisfies
// pred, or fails the test after a 5s deadline.
func waitForSnapshot(t *testing.T, mgr *sentinel.Manager, cr types.NamespacedName, pred func(sentinel.Snapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred(mgr.Snapshot(cr)) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("snapshot predicate not satisfied within deadline; last=%+v", mgr.Snapshot(cr))
}

// twoMasterChecker preloads role=master LagStates with the given offsets.
func twoMasterChecker(loserOffset, survivorOffset int64) *fakeLagChecker {
	c := newFakeLagChecker()
	c.byAddr[dmAddrLoser] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: loserOffset, HaveMasterOffset: true}
	c.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: survivorOffset, HaveMasterOffset: true}
	return c
}

func TestDualMasterSelfHeal_HighestOffsetSurvives(t *testing.T) {
	// Two de-facto masters (both labelled replica but reporting
	// role=master). The survivor holds the strictly-higher
	// master_repl_offset; the loser is demoted onto it.
	checker := twoMasterChecker(100, 1_000_000)
	issuer := newFakeReplicaOfIssuer()
	r := selfHealReconciler(t, checker, issuer, &fakeClientKillIssuer{}, twoMasterPods()...)

	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")

	calls := issuer.recorded()
	if len(calls) != 1 {
		t.Fatalf("expected exactly one REPLICAOF (demote the loser); got %d: %+v", len(calls), calls)
	}
	if calls[0].addr != dmAddrLoser {
		t.Errorf("expected loser (%s) demoted; got addr=%q", dmAddrLoser, calls[0].addr)
	}
	if calls[0].masterIP != dmIPSurvivor {
		t.Errorf("expected demotion onto survivor (%s); got masterIP=%q", dmIPSurvivor, calls[0].masterIP)
	}
}

func TestDualMasterSelfHeal_EpochFenceRejectsLower(t *testing.T) {
	// The observer is present + QuorumOK but names a defunct primary IP
	// (NoMasterAgreement) carrying config-epoch 2, below the failover's
	// PreStripEpoch=5. The fence is meaningful here — the observer epoch is
	// trustworthy — so the self-heal refuses to act on the stale view.
	mgr, cancel := startedManager(t, k8sevents.NewFakeRecorder(16))
	defer cancel()
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	s1, err := newRecoveringSentinel(dmDeadIP, 2)
	if err != nil {
		t.Fatalf("recoveringSentinel: %v", err)
	}
	defer s1.Stop()
	s2, err := newRecoveringSentinel(dmDeadIP, 2)
	if err != nil {
		t.Fatalf("recoveringSentinel: %v", err)
	}
	defer s2.Stop()
	if err := mgr.Ensure(context.Background(), cr, "mymaster", "",
		[]sentinel.Endpoint{
			{Name: dmSentinel0, Addr: s1.Addr()},
			{Name: dmSentinel1, Addr: s2.Addr()},
		}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	waitForSnapshot(t, mgr, cr, func(s sentinel.Snapshot) bool {
		return s.Present && s.Primary.QuorumOK && s.Primary.Addr == dmDeadAddr
	})

	checker := twoMasterChecker(100, 1_000_000)
	issuer := newFakeReplicaOfIssuer()
	r := &ValkeyReconciler{
		Client:           fake.NewClientBuilder().WithScheme(orphanTestScheme(t)).WithObjects(twoMasterPods()...).Build(),
		LagChecker:       checker,
		ReplicaOfIssuer:  issuer,
		ClientKillIssuer: &fakeClientKillIssuer{},
		SentinelObserver: mgr,
		Recorder:         k8sevents.NewFakeRecorder(16),
	}
	r.stateFor(cr).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateFailoverInFlight}
	v := orphanTestCR()
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{PreStripEpoch: 5},
	}

	r.reconcileOrphanMasters(context.Background(), v, orphanPods(t, r, v), "")

	if n := len(issuer.recorded()); n != 0 {
		t.Errorf("epoch fence (observer epoch 2 < PreStripEpoch 5) must refuse to demote; got %d REPLICAOF calls", n)
	}
}

func TestDualMasterSelfHeal_QuorumLostFiresDespiteEpoch(t *testing.T) {
	// In a genuine quorum-LOST split (no usable observer snapshot) the
	// observer epoch carries no fencing signal, so a recorded PreStripEpoch
	// must NOT block the self-heal — otherwise the feature is inert in
	// exactly its target scenario. The loser is demoted despite
	// PreStripEpoch=5.
	checker := twoMasterChecker(100, 1_000_000)
	issuer := newFakeReplicaOfIssuer()
	r := selfHealReconciler(t, checker, issuer, &fakeClientKillIssuer{}, twoMasterPods()...)
	v := orphanTestCR()
	v.Status.Rollout = &valkeyv1beta1.RolloutStatus{
		FailoverDispatch: &valkeyv1beta1.FailoverDispatchStatus{PreStripEpoch: 5},
	}

	r.reconcileOrphanMasters(context.Background(), v, orphanPods(t, r, v), "")

	if n := len(issuer.recorded()); n != 1 {
		t.Errorf("quorum-lost split must fire despite PreStripEpoch>0 (fence skipped when observer epoch unusable); got %d REPLICAOF calls", n)
	}
}

func TestDualMasterSelfHeal_TieWithinEpsilonDefers(t *testing.T) {
	// The two highest offsets are within dualMasterOffsetEpsilon — no
	// unambiguous survivor, so the self-heal refuses to demote either
	// (never demote the more-advanced master on a coin-flip).
	checker := twoMasterChecker(5000, 5000+dualMasterOffsetEpsilon-1)
	issuer := newFakeReplicaOfIssuer()
	r := selfHealReconciler(t, checker, issuer, &fakeClientKillIssuer{}, twoMasterPods()...)

	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")

	if n := len(issuer.recorded()); n != 0 {
		t.Errorf("within-epsilon offset tie must defer (no demotion); got %d REPLICAOF calls", n)
	}
}

func TestDualMasterSelfHeal_NeverPromotesWithoutQuorum(t *testing.T) {
	// Promote-side boundary: the self-heal demotes the loser but NEVER
	// stamps role=primary on the survivor (no quorum). The survivor is
	// seeded role=replica; after the pass it must STILL be role=replica —
	// a future change that promoted it would flip the label in the fake
	// client and fail this — while the loser is demoted via REPLICAOF.
	checker := twoMasterChecker(100, 1_000_000)
	issuer := newFakeReplicaOfIssuer()
	r := selfHealReconciler(t, checker, issuer, &fakeClientKillIssuer{}, twoMasterPods()...)

	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")

	if n := len(issuer.recorded()); n != 1 {
		t.Fatalf("expected the loser to be demoted; got %d REPLICAOF calls", n)
	}
	// The survivor (dmPod1, the higher-offset pod a buggy promotion would
	// target) must remain role=replica — the self-heal writes no labels.
	survivor := &corev1.Pod{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: dmPod1}, survivor); err != nil {
		t.Fatalf("getting survivor pod: %v", err)
	}
	if got := survivor.Labels[RoleLabel]; got != roleValueReplica {
		t.Errorf("survivor must remain role=replica (never promoted without quorum); got %q", got)
	}
}

func TestDesiredRolesForCR_SettlingDamp_MoveWaitsForTwoPolls(t *testing.T) {
	// A primary MOVE (dmPod0 currently labeled primary, observer names
	// dmPod1) must NOT relabel on the first fresh poll — the 2-poll
	// settling damp suppresses it — and must relabel once a second fresh
	// poll confirms the same Addr. Kill criterion: flipping
	// newPrimaryStableMinPolls to 1 makes the first-call suppression fail.
	mgr, cancel := startedManager(t, k8sevents.NewFakeRecorder(16))
	defer cancel()
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	s1, err := newRecoveringSentinel(dmIPSurvivor, 1)
	if err != nil {
		t.Fatalf("recoveringSentinel: %v", err)
	}
	defer s1.Stop()
	s2, err := newRecoveringSentinel(dmIPSurvivor, 1)
	if err != nil {
		t.Fatalf("recoveringSentinel: %v", err)
	}
	defer s2.Stop()
	if err := mgr.Ensure(context.Background(), cr, "mymaster", "",
		[]sentinel.Endpoint{
			{Name: dmSentinel0, Addr: s1.Addr()},
			{Name: dmSentinel1, Addr: s2.Addr()},
		}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	waitForSnapshot(t, mgr, cr, func(s sentinel.Snapshot) bool {
		return s.Present && s.Primary.QuorumOK && s.Primary.Addr == dmAddrSurvivor
	})

	r := &ValkeyReconciler{SentinelObserver: mgr, Recorder: k8sevents.NewFakeRecorder(16)}
	v := orphanTestCR()
	// dmPod0 currently labeled primary; observer names dmPod1 (dmIPSurvivor).
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod0, dmIPLoser, roleValuePrimary),
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
	}

	// First fresh poll: the MOVE is suppressed (freshPolls == 1 < 2).
	roles, suppress := r.desiredRolesForCR(v, pods)
	if !suppress || roles != nil {
		t.Fatalf("first fresh poll on a primary MOVE must suppress; got roles=%v suppress=%v", roles, suppress)
	}

	// Subsequent fresh polls (the observer keeps polling, advancing
	// LastPolledAt) release the suppression and relabel dmPod1.
	deadline := time.Now().Add(5 * time.Second)
	var moved map[string]string
	for time.Now().Before(deadline) {
		roles, suppress = r.desiredRolesForCR(v, pods)
		if !suppress && roles[dmPod1] == roleValuePrimary {
			moved = roles
			break
		}
		time.Sleep(60 * time.Millisecond)
	}
	if moved == nil {
		t.Fatalf("after >=2 fresh polls the MOVE must relabel dmPod1 primary; last roles=%v suppress=%v", roles, suppress)
	}
	if moved[dmPod0] != roleValueReplica {
		t.Errorf("old primary dmPod0 must become replica; got %q", moved[dmPod0])
	}
}

// advancingMasterChecker reports the seeded de-facto masters with
// master_repl_offsets that both advance by step on every CheckLag call,
// holding their gap constant within epsilon — modelling the live
// replication-PING / write offset drift that must not defeat the
// deferral edge-gate.
type advancingMasterChecker struct {
	mu   sync.Mutex
	off  map[string]int64
	step int64
}

func (c *advancingMasterChecker) CheckLag(_ context.Context, addr, _ string) (valkey.LagState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, ok := c.off[addr]
	if !ok {
		return valkey.LagState{}, nil
	}
	c.off[addr] = cur + c.step
	return valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: cur, HaveMasterOffset: true}, nil
}

func TestDualMasterSelfHeal_DeferredEventEdgeGated(t *testing.T) {
	// A steady within-epsilon tie recurs every reconcile while both
	// master_repl_offsets drift upward (live writes / the periodic
	// replication PING). The DualMasterSelfHealDeferred Warning must still
	// fire once per episode — the edge-gate keys on the tied pod pair, not
	// the advancing offsets. Regression for an offset-keyed signature,
	// which would re-fire every pass.
	checker := &advancingMasterChecker{
		off:  map[string]int64{dmAddrLoser: 5000, dmAddrSurvivor: 5000 + dualMasterOffsetEpsilon - 1},
		step: 1000,
	}
	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(orphanTestScheme(t)).WithObjects(twoMasterPods()...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
		Recorder:        rec,
	}
	r.failoverLatchSet(types.NamespacedName{Namespace: "ns", Name: "vk0"}, "10.9.9.9:6379")

	for range 3 {
		r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	}
	if n := len(issuer.recorded()); n != 0 {
		t.Errorf("within-epsilon tie must never demote; got %d REPLICAOF calls", n)
	}
	deferred := 0
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "DualMasterSelfHealDeferred") {
			deferred++
		}
	}
	if deferred != 1 {
		t.Errorf("DualMasterSelfHealDeferred must be edge-gated to one emit per episode even as offsets advance; got %d across 3 passes", deferred)
	}
}

// crossoverMasterChecker reports two de-facto masters whose offsets stay
// within epsilon but ALTERNATE which one leads on each reconcile (the
// non-atomic offset reads crossing between passes). Each reconcile reads
// both addrs once; the lead flips per reconcile, so the offset-ranked
// survivor swaps between the two pods. Used to pin that the deferral
// edge-gate signature is order-independent.
type crossoverMasterChecker struct {
	mu    sync.Mutex
	addr0 string
	addr1 string
	base  int64
	delta int64
	calls int
}

func (c *crossoverMasterChecker) CheckLag(_ context.Context, addr, _ string) (valkey.LagState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	round := c.calls / 2 // two addr reads per reconcile
	c.calls++
	if addr != c.addr0 && addr != c.addr1 {
		return valkey.LagState{}, nil
	}
	leaderIsAddr0 := round%2 == 0
	off := c.base
	if (addr == c.addr0 && leaderIsAddr0) || (addr == c.addr1 && !leaderIsAddr0) {
		off = c.base + c.delta
	}
	return valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: off, HaveMasterOffset: true}, nil
}

func TestDualMasterSelfHeal_DeferredEventEdgeGated_Crossover(t *testing.T) {
	// A steady within-epsilon split whose two offsets CROSS between passes
	// (pod A ahead, then pod B ahead) swaps the offset-ranked survivor and
	// runnerUp. The deferral signature must be order-independent so the
	// Warning still fires once per episode. Regression for an ordered
	// epsilon:<survivor>:<runnerUp> signature, which would flip on a cross.
	checker := &crossoverMasterChecker{
		addr0: dmAddrLoser,
		addr1: dmAddrSurvivor,
		base:  5000,
		delta: 100, // < dualMasterOffsetEpsilon, so the gate always defers
	}
	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(orphanTestScheme(t)).WithObjects(twoMasterPods()...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
		Recorder:        rec,
	}
	r.failoverLatchSet(types.NamespacedName{Namespace: "ns", Name: "vk0"}, "10.9.9.9:6379")

	for range 4 {
		r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	}
	if n := len(issuer.recorded()); n != 0 {
		t.Errorf("within-epsilon tie must never demote; got %d REPLICAOF calls", n)
	}
	deferred := 0
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "DualMasterSelfHealDeferred") {
			deferred++
		}
	}
	if deferred != 1 {
		t.Errorf("DualMasterSelfHealDeferred must be edge-gated to one emit per episode even when the survivor/runnerUp order crosses; got %d across 4 passes", deferred)
	}
}

func TestReconcileOrphanMasters_HealthyCluster_NoREPLICAOF(t *testing.T) {
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValuePrimary),
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValueReplica),
		labelledValkeyPod("vk0-2", "10.0.0.3", roleValueReplica),
	}
	checker := newFakeLagChecker()
	// Primary reports role=master; replicas correctly report role=slave.
	checker.byAddr["10.0.0.1:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 100}
	checker.byAddr["10.0.0.2:6379"] = valkey.LagState{Role: "slave", LinkUp: true}
	checker.byAddr["10.0.0.3:6379"] = valkey.LagState{Role: "slave", LinkUp: true}

	issuer := newFakeReplicaOfIssuer()
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
	}
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	if len(issuer.recorded()) != 0 {
		t.Errorf("expected zero REPLICAOF calls in healthy cluster; got %d: %+v", len(issuer.recorded()), issuer.recorded())
	}
}

// TestReconcileOrphanMasters_UsesThreadedPassword pins that the phase
// uses the auth password threaded in from the single Phase-0d
// resolution instead of re-reading the auth Secret itself. A
// distinctive password handed to the phase must reach the LagChecker
// unchanged. Before the fix the phase ignored any threaded value and
// re-read the Secret (resolving "" for an auth-less CR), so the checker
// saw "" — this test fails-before, passes-after.
func TestReconcileOrphanMasters_UsesThreadedPassword(t *testing.T) {
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValuePrimary),
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValueReplica),
	}
	checker := newFakeLagChecker()
	checker.byAddr["10.0.0.1:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 100}
	checker.byAddr["10.0.0.2:6379"] = valkey.LagState{Role: "slave", LinkUp: true}

	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: newFakeReplicaOfIssuer(),
	}
	const threaded = "threaded-secret-pw"
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), threaded)
	if got := checker.recordedPassword(); got != threaded {
		t.Errorf("CheckLag password = %q; want %q — phase must use the threaded password, not re-read the Secret", got, threaded)
	}
}

func TestReconcileOrphanMasters_OrphanDetected_FiresREPLICAOF(t *testing.T) {
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica), // ORPHAN — labelled replica
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValuePrimary), // elected master
		labelledValkeyPod("vk0-2", "10.0.0.3", roleValueReplica),
	}
	checker := newFakeLagChecker()
	checker.byAddr["10.0.0.2:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 13746728, HaveMasterOffset: true}
	// vk0-0 is the orphan: labelled replica, reports role=master with HIGHER offset.
	checker.byAddr["10.0.0.1:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 14007200, HaveMasterOffset: true}
	checker.byAddr["10.0.0.3:6379"] = valkey.LagState{Role: "slave", LinkUp: true}

	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
		Recorder:        rec,
	}
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	recorded := issuer.recorded()
	if len(recorded) != 1 {
		t.Fatalf("expected exactly 1 REPLICAOF call; got %d: %+v", len(recorded), recorded)
	}
	if recorded[0].addr != "10.0.0.1:6379" {
		t.Errorf("REPLICAOF target = %q; want 10.0.0.1:6379 (the orphan pod)", recorded[0].addr)
	}
	if recorded[0].masterIP != "10.0.0.2" || recorded[0].masterPort != 6379 {
		t.Errorf("REPLICAOF master = %s:%d; want 10.0.0.2:6379 (the elected primary)",
			recorded[0].masterIP, recorded[0].masterPort)
	}
	// Two events must fire: OrphanMasterDataDivergence (Warning, because orphan
	// offset > elected master) and OrphanMasterDemoted (Normal).
	events := drainEventsString(rec.Events)
	if !containsSubstring(events, "OrphanMasterDataDivergence") {
		t.Errorf("expected OrphanMasterDataDivergence event (orphan offset exceeds elected master); got events: %v", events)
	}
	if !containsSubstring(events, "OrphanMasterDemoted") {
		t.Errorf("expected OrphanMasterDemoted event; got events: %v", events)
	}
}

func TestReconcileOrphanMasters_OrphanNoOffsetDivergence_StillDemotes(t *testing.T) {
	// Orphan reports role=master but with LOWER offset (e.g., it
	// just rebooted and hasn't taken writes). REPLICAOF still fires
	// to demote it, but the OrphanMasterDataDivergence Warning event
	// must NOT fire (no data is being discarded).
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica),
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValuePrimary),
	}
	checker := newFakeLagChecker()
	checker.byAddr["10.0.0.2:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 14000000, HaveMasterOffset: true}
	checker.byAddr["10.0.0.1:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 1000, HaveMasterOffset: true} // freshly booted

	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
		Recorder:        rec,
	}
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	if len(issuer.recorded()) != 1 {
		t.Fatalf("expected 1 REPLICAOF (orphan still demoted), got %d", len(issuer.recorded()))
	}
	events := drainEventsString(rec.Events)
	// Explicit dual-assertion: DataDivergence must NOT fire (orphan
	// had less data than the elected master, so the resync discards
	// nothing); Demoted MUST fire (the orphan was still in a wrong-
	// role state that the operator must reconcile).
	if containsSubstring(events, "OrphanMasterDataDivergence") {
		t.Errorf("DataDivergence event must NOT fire when orphan offset < elected master (no data lost); got events: %v", events)
	}
	if !containsSubstring(events, "OrphanMasterDemoted") {
		t.Errorf("OrphanMasterDemoted MUST fire on every successful REPLICAOF, divergence or not; got: %v", events)
	}
}

func TestReconcileOrphanMasters_MissingOffset_SkipsDivergence(t *testing.T) {
	// The orphan reports role=master with a raw offset that EXCEEDS the
	// elected primary's — which would normally fire DataDivergence —
	// but its INFO reply omitted master_repl_offset (HaveMasterOffset
	// false), so the value is not trustworthy. The divergence event
	// must be suppressed (a truncated reply must not fabricate a
	// data-loss warning) while the orphan is still demoted.
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica), // orphan
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValuePrimary), // elected master
	}
	checker := newFakeLagChecker()
	checker.byAddr["10.0.0.2:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 100, HaveMasterOffset: true}
	// Orphan's INFO was truncated: a high raw offset but the field was
	// absent, so HaveMasterOffset is false and it must not be compared.
	checker.byAddr["10.0.0.1:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 9_000_000, HaveMasterOffset: false}

	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
		Recorder:        rec,
	}
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	if len(issuer.recorded()) != 1 {
		t.Fatalf("expected 1 REPLICAOF (orphan still demoted despite missing offset), got %d", len(issuer.recorded()))
	}
	events := drainEventsString(rec.Events)
	if containsSubstring(events, "OrphanMasterDataDivergence") {
		t.Errorf("DataDivergence must NOT fire when master_repl_offset was absent from an INFO reply (untrustworthy); got: %v", events)
	}
	if !containsSubstring(events, "OrphanMasterDemoted") {
		t.Errorf("OrphanMasterDemoted MUST still fire; got: %v", events)
	}
}

func TestReconcileOrphanMasters_REPLICAOFFailure_EmitsWarning(t *testing.T) {
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica), // orphan
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValuePrimary),
	}
	checker := newFakeLagChecker()
	checker.byAddr["10.0.0.2:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 100}
	checker.byAddr["10.0.0.1:6379"] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 50}

	issuer := newFakeReplicaOfIssuer()
	issuer.errByPod["10.0.0.1:6379"] = errors.New("ECONNREFUSED")
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
		Recorder:        rec,
	}
	// REPLICAOF failure must not fail the reconcile — the phase is
	// best-effort + void; the next pass retries.
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	events := drainEventsString(rec.Events)
	if !containsSubstring(events, "OrphanMasterDemotionFailed") {
		t.Errorf("expected OrphanMasterDemotionFailed event; got: %v", events)
	}
	// Demoted event must NOT fire on the failure path.
	if containsSubstring(events, "OrphanMasterDemoted ") || strings.HasSuffix(strings.Join(events, "|"), "OrphanMasterDemoted") {
		t.Errorf("OrphanMasterDemoted must NOT fire when REPLICAOF errored; got: %v", events)
	}
}

func TestReconcileOrphanMasters_ElectedMasterReportsSlave_Skips(t *testing.T) {
	// Defensive: if the elected primary's INFO reports role=slave
	// (e.g., labels stale or in-flight failover), we must NOT
	// demote anything — pointing orphans at a non-master would
	// make things worse. Phase 7 NoMasterAgreement surfaces this
	// case elsewhere.
	scheme := orphanTestScheme(t)
	pods := []client.Object{
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica),
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValuePrimary),
	}
	checker := newFakeLagChecker()
	checker.byAddr["10.0.0.2:6379"] = valkey.LagState{Role: "slave", LinkUp: true} // ← anomaly
	checker.byAddr["10.0.0.1:6379"] = valkey.LagState{Role: valkey.RoleMaster}

	issuer := newFakeReplicaOfIssuer()
	r := &ValkeyReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		LagChecker:      checker,
		ReplicaOfIssuer: issuer,
	}
	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")
	if len(issuer.recorded()) != 0 {
		t.Errorf("expected zero REPLICAOF when elected primary doesn't report role=master; got %d", len(issuer.recorded()))
	}
}

// countDualMasterObserved drains rec and returns how many
// DualMasterObserved events it saw.
func countDualMasterObserved(rec *k8sevents.FakeRecorder) int {
	n := 0
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "DualMasterObserved") {
			n++
		}
	}
	return n
}

func TestReconcileOrphanMasters_ReplicationNoPrimary_StampsAndFiresEvent(t *testing.T) {
	// Replication CR, no labeled primary, two pods both self-reporting
	// role=master → the no-primary observation stamps the condition and
	// fires exactly one DualMasterObserved Warning, and the gauge reads
	// active. Pins that the former blind spot now surfaces.
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 2048, HaveMasterOffset: true}
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 1024, HaveMasterOffset: true}
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: newFakeReplicaOfIssuer(), Recorder: rec}
	v := replicationOrphanCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	r.reconcileOrphanMasters(context.Background(), v, pods, "")

	obs := r.stateFor(cr).dualMasterObservation()
	if obs == nil || len(obs.pods) != 2 {
		t.Fatalf("expected a two-pod stamp, got %+v", obs)
	}
	if obs.pods[0] != dmPod1 || obs.pods[1] != dmPod2 {
		t.Errorf("stamp pods = %v; want [%s %s]", obs.pods, dmPod1, dmPod2)
	}
	var dmBodies []string
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "DualMasterObserved") {
			dmBodies = append(dmBodies, e)
		}
	}
	if len(dmBodies) != 1 {
		t.Fatalf("expected exactly one DualMasterObserved Warning, got %d", len(dmBodies))
	}
	// Pin the operator-facing payload, not just the reason: both pod names
	// and each master_repl_offset must render in the event body.
	for _, want := range []string{dmPod1, dmPod2, "master_repl_offset=2048", "master_repl_offset=1024"} {
		if !strings.Contains(dmBodies[0], want) {
			t.Errorf("DualMasterObserved body missing %q; got %q", want, dmBodies[0])
		}
	}
	if !r.stateFor(cr).dualMasterActiveOrExpire(activeAt(time.Now())) {
		t.Errorf("dual-master observation must read active immediately after the stamp")
	}
}

func TestReconcileOrphanMasters_ReplicationNoPrimary_UnknownOffsetRendered(t *testing.T) {
	// A de-facto master whose INFO omitted master_repl_offset still counts
	// toward the split and renders "master_repl_offset=unknown" in the event
	// body (the no-primary observation never demotes, so a missing offset
	// does not gate surfacing — unlike the labeled-primary self-heal path).
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 2048, HaveMasterOffset: true}
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: false}
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: newFakeReplicaOfIssuer(), Recorder: rec}
	v := replicationOrphanCR()
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	r.reconcileOrphanMasters(context.Background(), v, pods, "")

	var body string
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "DualMasterObserved") {
			body = e
		}
	}
	if body == "" {
		t.Fatalf("expected a DualMasterObserved event even with one offset absent")
	}
	if !strings.Contains(body, "master_repl_offset=unknown") {
		t.Errorf("the offset-absent master must render master_repl_offset=unknown; got %q", body)
	}
	if !strings.Contains(body, "master_repl_offset=2048") {
		t.Errorf("the offset-bearing master must still render its value; got %q", body)
	}
}

func TestReconcileOrphanMasters_ReplicationNoPrimary_ChurnKeyedOnUnionNoRefire(t *testing.T) {
	// The replication no-primary producer keys the DualMasterObserved event
	// on the accumulated de-facto-master UNION (not the current scan's exact
	// set), so role churn within one split pages once per genuinely-new pod,
	// not once per membership permutation. Mirrors the survey producer's
	// TestDualMasterObservedEdge_ChurnKeyedOnUnionNoRefire but drives the
	// replication reconcileOrphanMasters path: {a,b}->{a,c}->{b,c} across a
	// shared reconciler (a=vk0-0, b=vk0-1, c=vk0-2), mutating the checker
	// between passes, asserting exactly two events. A 3-set permutation is
	// required — a 2-set flap debounces under last-set keying too, so this is
	// what kills the "revert to last-set keying" mutant on the replication
	// producer specifically.
	master := valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: true}
	slave := valkey.LagState{Role: "slave", LinkUp: true}
	checker := newFakeLagChecker()
	rec := k8sevents.NewFakeRecorder(32)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: newFakeReplicaOfIssuer(), Recorder: rec}
	v := replicationOrphanCR()
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod0, dmIPLoser, roleValueReplica),
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	// A: {vk0-0, vk0-1} de-facto master → fire (union {vk0-0,vk0-1}).
	checker.byAddr[dmAddrLoser] = master
	checker.byAddr[dmAddrSurvivor] = master
	checker.byAddr[dmAddrPod2] = slave
	r.reconcileOrphanMasters(context.Background(), v, pods, "")
	// B: {vk0-0, vk0-2} → vk0-2 joins the union → fire.
	checker.byAddr[dmAddrLoser] = master
	checker.byAddr[dmAddrSurvivor] = slave
	checker.byAddr[dmAddrPod2] = master
	r.reconcileOrphanMasters(context.Background(), v, pods, "")
	// C: {vk0-1, vk0-2} → no genuinely-new pod (both already in the union) → NO fire.
	checker.byAddr[dmAddrLoser] = slave
	checker.byAddr[dmAddrSurvivor] = master
	checker.byAddr[dmAddrPod2] = master
	r.reconcileOrphanMasters(context.Background(), v, pods, "")

	if n := countDualMasterObserved(rec); n != 2 {
		t.Fatalf("union-keyed replication producer must fire twice ({a,b}, then when c joins) and NOT on {b,c}; got %d", n)
	}
}

func TestReconcileOrphanMasters_ReplicationNoPrimary_NoDemotion(t *testing.T) {
	// Surface-only: the no-primary observation never issues REPLICAOF —
	// replication has no elected primary to fence a demotion against.
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: true}
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: true}
	issuer := newFakeReplicaOfIssuer()
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: issuer, Recorder: k8sevents.NewFakeRecorder(16)}
	v := replicationOrphanCR()
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	r.reconcileOrphanMasters(context.Background(), v, pods, "")

	if n := len(issuer.recorded()); n != 0 {
		t.Errorf("replication no-primary observation must never demote; got %d REPLICAOF calls", n)
	}
}

func TestReconcileOrphanMasters_ReplicationNoPrimary_SingleMasterCompleteSweepClears(t *testing.T) {
	// A complete, clean sweep seeing <2 masters clears a prior stamp and
	// fires nothing.
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: true}
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: "slave", LinkUp: true}
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: newFakeReplicaOfIssuer(), Recorder: rec}
	v := replicationOrphanCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	r.stateFor(cr).stampDualMasterObserved([]string{dmPod1, dmPod2}, time.Now())
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	r.reconcileOrphanMasters(context.Background(), v, pods, "")

	if r.stateFor(cr).dualMasterObservation() != nil {
		t.Errorf("complete clean single-master sweep must clear the stamp")
	}
	if n := countDualMasterObserved(rec); n != 0 {
		t.Errorf("single-master sweep must not fire DualMasterObserved; got %d", n)
	}
}

func TestReconcileOrphanMasters_ReplicationNoPrimary_IncompleteSweepLeavesStamp(t *testing.T) {
	// A sweep seeing <2 masters but with a dial failure (incomplete
	// coverage) records no verdict: it neither clears nor fires, leaving
	// the stamp to age out via the freshness window.
	checker := newFakeLagChecker()
	checker.errAddr[dmAddrSurvivor] = context.DeadlineExceeded
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: true}
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: newFakeReplicaOfIssuer(), Recorder: rec}
	v := replicationOrphanCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	r.stateFor(cr).stampDualMasterObserved([]string{dmPod1, dmPod2}, time.Now())
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	r.reconcileOrphanMasters(context.Background(), v, pods, "")

	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("incomplete sweep (dial failure) seeing <2 masters must not clear the stamp")
	}
	if n := countDualMasterObserved(rec); n != 0 {
		t.Errorf("incomplete <2-master sweep must not fire DualMasterObserved; got %d", n)
	}
}

func TestReconcileOrphanMasters_SentinelNoPrimary_StillRoutesToSelfHeal(t *testing.T) {
	// The mode split must not divert the sentinel path: a sentinel CR with
	// no labeled primary, failover in flight and two masters with an
	// unambiguous offset gap still runs the self-heal (demotes the loser,
	// emits DualMasterSelfHealInitiated).
	checker := twoMasterChecker(100, 1_000_000)
	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(16)
	r := selfHealReconciler(t, checker, issuer, &fakeClientKillIssuer{}, twoMasterPods()...)
	r.Recorder = rec

	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")

	if n := len(issuer.recorded()); n != 1 {
		t.Fatalf("sentinel no-primary must still route to the self-heal (demote the loser); got %d REPLICAOF calls", n)
	}
	if !containsSubstring(drainEvents(rec), "DualMasterSelfHealInitiated") {
		t.Errorf("sentinel self-heal path must emit DualMasterSelfHealInitiated")
	}
	if n := countDualMasterObserved(rec); n != 0 {
		t.Errorf("sentinel self-heal path must not fire DualMasterObserved (its Initiated/Deferred events own the messaging); got %d", n)
	}
}

func TestReconcileOrphanMasters_ReplicationFlap_NoPrimaryContaminationRefire(t *testing.T) {
	// The load-bearing union-isolation test. A shared reconciler drives
	// three passes across a pod-0 flap:
	//   A (no primary):  vk0-1,vk0-2 both master → producer-4 fires (1).
	//   B (pod-0 primary): vk0-0(primary)+vk0-1+vk0-2 → producer-1 stamps
	//                      {vk0-0,vk0-1,vk0-2} condition-only, no event.
	//   C (no primary):  vk0-1,vk0-2 again → union still {vk0-1,vk0-2}
	//                      (NOT contaminated by vk0-0) → no new event.
	// If producer-1 fed the event union, C would re-fire. Assert exactly 1.
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrLoser] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 100, HaveMasterOffset: true}     // vk0-0
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 5000, HaveMasterOffset: true} // vk0-1
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 6000, HaveMasterOffset: true}     // vk0-2
	// The issuer records REPLICAOF calls but does not mutate checker state,
	// so pass B's faked demotion leaves vk0-1/vk0-2 self-reporting master.
	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(32)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: issuer, Recorder: rec}
	v := replicationOrphanCR()

	noPrimary := []corev1.Pod{
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}
	withPrimary := []corev1.Pod{
		*labelledValkeyPod(dmPod0, dmIPLoser, roleValuePrimary),
		*labelledValkeyPod(dmPod1, dmIPSurvivor, roleValueReplica),
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	r.reconcileOrphanMasters(context.Background(), v, noPrimary, "")   // A
	r.reconcileOrphanMasters(context.Background(), v, withPrimary, "") // B

	// Pass-B premise, asserted directly (not merely inferred from the final
	// count): the labeled-primary orphan scan stamped the CONDITION including
	// the primary vk0-0, but did NOT feed vk0-0 into the EVENT union — so C
	// keys to the same {vk0-1,vk0-2} union and debounces. A refactor that made
	// pass B a condition-only no-op (or that leaked vk0-0 into the union)
	// would trip one of the two assertions below even though the final count
	// could still read 1.
	ps := r.stateFor(cr)
	if obs := ps.dualMasterObservation(); obs == nil || !slices.Contains(obs.pods, dmPod0) {
		t.Fatalf("pass B: condition producer must stamp the labeled primary %s; got %+v", dmPod0, obs)
	}
	ps.mu.Lock()
	union := append([]string(nil), ps.dualMasterEventUnion...)
	ps.mu.Unlock()
	if slices.Contains(union, dmPod0) {
		t.Fatalf("pass B: labeled primary %s must NOT contaminate the event union; got %v", dmPod0, union)
	}

	r.reconcileOrphanMasters(context.Background(), v, noPrimary, "") // C

	if n := countDualMasterObserved(rec); n != 1 {
		t.Fatalf("union isolation broken: expected exactly 1 DualMasterObserved across the flap (A fires, B condition-only, C debounced); got %d", n)
	}
}

func drainEventsString(ch chan string) []string {
	var out []string
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

func containsSubstring(haystack []string, needle string) bool {
	for _, s := range haystack {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

func TestReconcileOrphanMasters_ReplicationNoPrimary_PendingPodLeavesStamp(t *testing.T) {
	// A sweep seeing <2 masters while a listed pod is still IP-less
	// (Pending / recreating) never evaluated that pod — it may boot off
	// an intact PVC as role:master — so the sweep records no clear
	// verdict: the stamp is retained (aging out via the freshness
	// window) and nothing fires, mirroring the dial-failure posture and
	// the survey producer's pendingPods guard.
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: true}
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: newFakeReplicaOfIssuer(), Recorder: rec}
	v := replicationOrphanCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	r.stateFor(cr).stampDualMasterObserved([]string{dmPod1, dmPod2}, time.Now())
	pods := []corev1.Pod{
		*labelledValkeyPod(dmPod1, "", roleValueReplica), // recreated, still Pending
		*labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica),
	}

	r.reconcileOrphanMasters(context.Background(), v, pods, "")

	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("a <2-master sweep with a Pending (IP-less) pod must not clear the stamp")
	}
	if n := countDualMasterObserved(rec); n != 0 {
		t.Errorf("an incomplete <2-master sweep must not fire DualMasterObserved; got %d", n)
	}
}

func TestReconcileOrphanMasters_LabeledPrimary_PendingPodLeavesStamp(t *testing.T) {
	// The labeled-primary orphan scan shares the clear-verdict posture: a
	// clean single-master pass with a listed IP-less pod is incomplete
	// coverage (the Pending pod was never evaluated and could be a rogue
	// master), so a prior stamp survives; the same pass without the
	// Pending pod is complete and clears it.
	checker := newFakeLagChecker()
	checker.byAddr[dmAddrSurvivor] = valkey.LagState{Role: valkey.RoleMaster, HaveMasterOffset: true}
	checker.byAddr[dmAddrPod2] = valkey.LagState{Role: "slave", LinkUp: true}
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{LagChecker: checker, ReplicaOfIssuer: newFakeReplicaOfIssuer(), Recorder: rec}
	v := orphanTestCR()
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	r.stateFor(cr).stampDualMasterObserved([]string{dmPod1, dmPod2}, time.Now())
	pending := labelledValkeyPod(dmPod0, "", roleValueReplica) // recreated, still Pending
	primary := labelledValkeyPod(dmPod1, dmIPSurvivor, roleValuePrimary)
	replica := labelledValkeyPod(dmPod2, dmIPPod2, roleValueReplica)

	r.reconcileOrphanMasters(context.Background(), v, []corev1.Pod{*pending, *primary, *replica}, "")
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("a single-master labeled-primary scan with a Pending pod must not clear the stamp")
	}

	// The pod gets its IP... nothing else changes: same scan is now a
	// complete clean sweep and the clear verdict lands.
	pending2 := labelledValkeyPod(dmPod0, "10.0.5.4", roleValueReplica)
	checker.byAddr["10.0.5.4:6379"] = valkey.LagState{Role: "slave", LinkUp: true}
	r.reconcileOrphanMasters(context.Background(), v, []corev1.Pod{*pending2, *primary, *replica}, "")
	if r.stateFor(cr).dualMasterObservation() != nil {
		t.Errorf("a complete clean single-master scan must clear the stamp")
	}
}

// TestFireDualMasterDeferEdge_AlternatingSignatureRateBound pins the
// emission bound for a deferral reason that CHANGES pass-to-pass: the
// same-signature suppression alone cannot gate an alternating pair, so
// a signature change re-fires only after dualMasterDeferMinInterval —
// bounding the producer to ~1 Warning per interval at the 5s wedge
// reconcile cadence instead of one per flip.
func TestFireDualMasterDeferEdge_AlternatingSignatureRateBound(t *testing.T) {
	t.Parallel()
	s := &perCRState{}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	if !s.fireDualMasterDeferEdge("no-offset", t0) {
		t.Fatalf("first deferral must fire")
	}
	// Alternating signatures at reconcile cadence: every flip inside the
	// interval is suppressed (changed sig → rate bound; repeated sig →
	// edge), and the suppressed flips must not consume the edge state.
	for i := 1; i <= 5; i++ {
		at := t0.Add(time.Duration(i) * 5 * time.Second)
		sig := "no-offset"
		if i%2 == 1 {
			sig = "epsilon:vk0-1:vk0-2"
		}
		if s.fireDualMasterDeferEdge(sig, at) {
			t.Fatalf("flip %d (%s) within the interval must be suppressed", i, sig)
		}
	}
	// Past the interval the still-alternating reason fires once more...
	if !s.fireDualMasterDeferEdge("epsilon:vk0-1:vk0-2", t0.Add(dualMasterDeferMinInterval)) {
		t.Fatalf("a changed signature past the interval must fire")
	}
	// ...and the following flips are bounded again.
	if s.fireDualMasterDeferEdge("no-offset", t0.Add(dualMasterDeferMinInterval+5*time.Second)) {
		t.Fatalf("the next flip inside the fresh interval must be suppressed")
	}
	// A steady identical deferral never re-fires regardless of elapsed
	// time — the per-episode edge, unchanged by the rate bound.
	if s.fireDualMasterDeferEdge("epsilon:vk0-1:vk0-2", t0.Add(10*dualMasterDeferMinInterval)) {
		t.Fatalf("a repeated signature must never re-fire within the episode")
	}
}

func TestDualMasterSelfHeal_PendingPodDefersAndLeavesStamp(t *testing.T) {
	// A listed pod with no IP (Pending / recreating) was never dialed and
	// may itself boot off an intact PVC as the most-advanced de-facto
	// master, so the self-heal must neither elect a survivor (no
	// REPLICAOF — an unranked master could be wrongly demoted onto a
	// behind survivor) nor claim a coverage-complete clear verdict —
	// aligning the fourth producer with the other three.

	// Part 1: a visible two-master split PLUS a pending pod → the stamp
	// records the split, a Deferred Warning fires, and NO demotion runs.
	checker := twoMasterChecker(100, 1_000_000)
	issuer := newFakeReplicaOfIssuer()
	rec := k8sevents.NewFakeRecorder(16)
	pods := append(twoMasterPods(), labelledValkeyPod(dmPod2, "", roleValueReplica))
	r := selfHealReconciler(t, checker, issuer, &fakeClientKillIssuer{}, pods...)
	r.Recorder = rec
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	r.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r, orphanTestCR()), "")

	if n := len(issuer.recorded()); n != 0 {
		t.Fatalf("a pending pod must defer the self-heal (no demotion); got %d REPLICAOF calls", n)
	}
	events := drainEvents(rec)
	deferred := 0
	for _, e := range events {
		if strings.Contains(e, "DualMasterSelfHealDeferred") {
			deferred++
		}
	}
	if deferred != 1 {
		t.Errorf("a deferred visible split must emit exactly one DualMasterSelfHealDeferred; got %d", deferred)
	}
	if containsSubstring(events, "DualMasterSelfHealInitiated") {
		t.Errorf("a pending pod must block the heal before DualMasterSelfHealInitiated")
	}
	if r.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("the visible split must still stamp the observation")
	}

	// Part 2: a single dialed master plus a pending pod is incomplete
	// coverage — a prior stamp survives instead of being cleared by a
	// verdict the scan could not prove, and NO deferral Warning fires:
	// with at most one master visible there is no split being deferred,
	// so an ordinary failover window with a pod still coming up must
	// not page (the <2-master no-page clause of the pending gate).
	checker2 := newFakeLagChecker()
	checker2.byAddr[dmAddrLoser] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 100, HaveMasterOffset: true}
	issuer2 := newFakeReplicaOfIssuer()
	rec2 := k8sevents.NewFakeRecorder(16)
	pods2 := []client.Object{
		labelledValkeyPod(dmPod0, dmIPLoser, roleValueReplica),
		labelledValkeyPod(dmPod2, "", roleValueReplica),
	}
	r2 := selfHealReconciler(t, checker2, issuer2, &fakeClientKillIssuer{}, pods2...)
	r2.Recorder = rec2
	r2.stateFor(cr).stampDualMasterObserved([]string{dmPod0, dmPod1}, time.Now())

	r2.reconcileOrphanMasters(context.Background(), orphanTestCR(), orphanPods(t, r2, orphanTestCR()), "")

	if r2.stateFor(cr).dualMasterObservation() == nil {
		t.Errorf("a <2-master scan with a pending pod must not clear the stamp (incomplete coverage)")
	}
	if n := len(issuer2.recorded()); n != 0 {
		t.Errorf("part 2 must not demote either; got %d REPLICAOF calls", n)
	}
	if containsSubstring(drainEvents(rec2), "DualMasterSelfHealDeferred") {
		t.Errorf("a <2-master pending scan must not page DualMasterSelfHealDeferred (no visible split to defer)")
	}
}
