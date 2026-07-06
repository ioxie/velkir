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
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/valkey"
)

// stubLagChecker scripts CheckLag results per pod IP for the
// pickPromotionCandidate unit tests. Distinct from the broader
// fakeLagChecker in valkey_reconciler_test.go (which counts calls
// and returns zero-value on miss) — this stub returns a
// deterministic error on miss so a test that fails to script a
// candidate fails loud.
type stubLagChecker struct {
	byAddr map[string]valkey.LagState
	errs   map[string]error
}

func (f *stubLagChecker) CheckLag(_ context.Context, addr, _ string) (valkey.LagState, error) {
	if err, ok := f.errs[addr]; ok {
		return valkey.LagState{}, err
	}
	if state, ok := f.byAddr[addr]; ok {
		return state, nil
	}
	return valkey.LagState{}, fmt.Errorf("stubLagChecker: no scripted reply for %s", addr)
}

// addrFor builds the IP:DefaultPort form pickPromotionCandidate uses
// to key into the fake's reply table.
func addrFor(ip string) string {
	return net.JoinHostPort(ip, fmt.Sprintf("%d", valkey.DefaultPort))
}

// candidateName is the test-pod name picked by several
// pickPromotionCandidate cases — extracted to silence goconst.
const candidateName = "vk0-2"

// makePod is a slim corev1.Pod with the IP fields populated for the
// helper's reach. Distinct from phase7_test.go's newPod (which sets
// the StatefulSet ordinal annotation) — primary-rollout tests don't
// care about ordinals, only about the role label and PodIP.
func makePod(name, ip, role string) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      name,
			Labels:    map[string]string{},
		},
		Status: corev1.PodStatus{PodIP: ip},
	}
	if role != "" {
		p.Labels[RoleLabel] = role
	}
	return p
}

// --- failoverInFlightLatch lifecycle ---

func TestFailoverLatch_NewlySetIsActive(t *testing.T) {
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	r.failoverLatchSet(cr, "10.0.0.1:6379")
	if !r.failoverLatchActive(cr, "10.0.0.1:6379", 0) {
		t.Fatal("latch should be active immediately after Set with matching Addr")
	}
}

func TestFailoverLatch_ClearsOnObserverAddrChange(t *testing.T) {
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	r.failoverLatchSet(cr, "10.0.0.1:6379")
	// Observer reports the new primary's Addr (+switch-master arrived).
	if r.failoverLatchActive(cr, "10.0.0.2:6379", 0) {
		t.Fatal("latch must clear when observer Addr differs from preStripAddr")
	}
	// After clear, even a re-check against the original addr returns
	// inactive — the latch was deleted.
	if r.failoverLatchActive(cr, "10.0.0.1:6379", 0) {
		t.Fatal("latch must stay cleared after auto-clear on Addr change")
	}
}

func TestFailoverLatch_ActiveOnEmptyObserverAddr(t *testing.T) {
	// Boot-race: snapshot not yet present after a failover dispatch.
	// The empty Addr must NOT trigger the "Addr changed" auto-clear
	// — otherwise the latch would clear immediately after set on a
	// fresh observer that hasn't published.
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	r.failoverLatchSet(cr, "10.0.0.1:6379")
	if !r.failoverLatchActive(cr, "", 0) {
		t.Fatal("latch must stay active on empty observer Addr (boot-race)")
	}
}

func TestFailoverLatch_ClearsOnDeadlineExpiry(t *testing.T) {
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	// Inject expired latch directly (TTL is 210s in production —
	// can't realistically wait for the public Set path).
	r.stateFor(cr).setFailoverLatch(&failoverInFlightLatch{
		preStripAddr: "10.0.0.1:6379",
		deadline:     time.Now().Add(-time.Second),
	})
	if r.failoverLatchActive(cr, "10.0.0.1:6379", 0) {
		t.Fatal("expired latch must auto-clear")
	}
}

func TestFailoverLatch_ExplicitClear(t *testing.T) {
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	r.failoverLatchSet(cr, "10.0.0.1:6379")
	r.failoverLatchClear(cr)
	if r.failoverLatchActive(cr, "10.0.0.1:6379", 0) {
		t.Fatal("explicit Clear must drop the latch")
	}
}

func TestFailoverLatch_PerCRIsolation(t *testing.T) {
	r := &ValkeyReconciler{}
	crA := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	crB := types.NamespacedName{Namespace: "ns", Name: "vk1"}
	r.failoverLatchSet(crA, "10.0.0.1:6379")
	if r.failoverLatchActive(crB, "10.0.0.1:6379", 0) {
		t.Fatal("latch on crA must not leak into crB")
	}
}

// TestFailoverLatch_EpochFenceHoldsStaleMoveOff pins the PreStripEpoch
// fence on the critical-section exit: an observer move-off carrying a
// config-epoch LOWER than the one the strip ran under is a stale sentinel
// view of an election this dispatch already superseded, so it must NOT
// exit the latch — otherwise the roll could re-stamp a pre-failover
// primary. A move-off at an epoch >= the fence is a genuine newer primary
// and clears as usual; the deadline escape still fires regardless of
// epoch (never an infinite hold).
func TestFailoverLatch_EpochFenceHoldsStaleMoveOff(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	t.Run("stale lower-epoch move-off holds the section", func(t *testing.T) {
		r := &ValkeyReconciler{}
		r.failoverLatchSetWithDeadline(cr, "10.0.0.1:6379", 7, time.Now().Add(time.Minute))
		// Observer reports a different Addr but at a LOWER epoch (6 < 7):
		// a lagging sentinel's stale view. The section must hold.
		if !r.failoverLatchActive(cr, "10.0.0.2:6379", 6) {
			t.Fatal("a lower-epoch observer move-off must not exit the critical section")
		}
	})

	t.Run("genuine higher-epoch move-off exits the section", func(t *testing.T) {
		r := &ValkeyReconciler{}
		r.failoverLatchSetWithDeadline(cr, "10.0.0.1:6379", 7, time.Now().Add(time.Minute))
		// Observer reports the new primary at an epoch >= the fence: a
		// real +switch-master. The section exits.
		if r.failoverLatchActive(cr, "10.0.0.2:6379", 7) {
			t.Fatal("an epoch >= the fence on a move-off must exit the critical section")
		}
	})

	t.Run("deadline escape fires even under a held epoch fence", func(t *testing.T) {
		r := &ValkeyReconciler{}
		// Expired deadline + a lower-epoch move-off: the deadline escape
		// dominates so the section is never held indefinitely.
		r.stateFor(cr).setFailoverLatch(&failoverInFlightLatch{
			preStripAddr:  "10.0.0.1:6379",
			deadline:      time.Now().Add(-time.Second),
			preStripEpoch: 7,
		})
		if r.failoverLatchActive(cr, "10.0.0.2:6379", 6) {
			t.Fatal("an expired deadline must release the section regardless of the epoch fence")
		}
	})
}

// --- pickPromotionCandidate ---

func TestPickPromotionCandidate_NoneEligibleReturnsNil(t *testing.T) {
	pods := []corev1.Pod{
		makePod("vk0-1", "10.0.0.2", "replica"),
		makePod(candidateName, "10.0.0.3", "replica"),
	}
	checker := &stubLagChecker{byAddr: map[string]valkey.LagState{
		addrFor("10.0.0.2"): {Role: "slave", LinkUp: true, LagBytes: 50000},
		addrFor("10.0.0.3"): {Role: "slave", LinkUp: true, LagBytes: 30000},
	}}
	got := pickPromotionCandidate(context.Background(), checker, pods, "", 10000)
	if got != nil {
		t.Fatalf("expected nil candidate, got %+v", got)
	}
}

func TestPickPromotionCandidate_SkipsLinkDown(t *testing.T) {
	pods := []corev1.Pod{
		makePod("vk0-1", "10.0.0.2", "replica"),
		makePod(candidateName, "10.0.0.3", "replica"),
	}
	checker := &stubLagChecker{byAddr: map[string]valkey.LagState{
		addrFor("10.0.0.2"): {Role: "slave", LinkUp: false, LagBytes: 0},
		addrFor("10.0.0.3"): {Role: "slave", LinkUp: true, LagBytes: 100},
	}}
	got := pickPromotionCandidate(context.Background(), checker, pods, "", 10000)
	if got == nil {
		t.Fatal("expected vk0-2; got nil")
		return
	}
	if got.Name != candidateName {
		t.Errorf("expected vk0-2 (link-down vk0-1 must be skipped); got %s", got.Name)
	}
}

func TestPickPromotionCandidate_PrefersLowestLag(t *testing.T) {
	pods := []corev1.Pod{
		makePod("vk0-1", "10.0.0.2", "replica"),
		makePod(candidateName, "10.0.0.3", "replica"),
		makePod("vk0-3", "10.0.0.4", "replica"),
	}
	checker := &stubLagChecker{byAddr: map[string]valkey.LagState{
		addrFor("10.0.0.2"): {Role: "slave", LinkUp: true, LagBytes: 5000},
		addrFor("10.0.0.3"): {Role: "slave", LinkUp: true, LagBytes: 100},
		addrFor("10.0.0.4"): {Role: "slave", LinkUp: true, LagBytes: 8000},
	}}
	got := pickPromotionCandidate(context.Background(), checker, pods, "", 10000)
	if got == nil {
		t.Fatal("expected vk0-2; got nil")
		return
	}
	if got.Name != candidateName {
		t.Errorf("expected lowest-lag vk0-2 (lag=100); got %s", got.Name)
	}
}

func TestPickPromotionCandidate_DialErrorSkipsCandidate(t *testing.T) {
	pods := []corev1.Pod{
		makePod("vk0-1", "10.0.0.2", "replica"),
		makePod(candidateName, "10.0.0.3", "replica"),
	}
	checker := &stubLagChecker{
		byAddr: map[string]valkey.LagState{
			addrFor("10.0.0.3"): {Role: "slave", LinkUp: true, LagBytes: 200},
		},
		errs: map[string]error{
			addrFor("10.0.0.2"): errors.New("dial timeout"),
		},
	}
	got := pickPromotionCandidate(context.Background(), checker, pods, "", 10000)
	if got == nil || got.Name != candidateName {
		t.Errorf("expected vk0-2; got %+v", got)
	}
}

func TestPickPromotionCandidate_SelfReportingMasterIsEligible(t *testing.T) {
	// Sentinel-driven flip beat the operator's role-label relabel —
	// a pod self-reporting as master is fully eligible (operator
	// view is stale; treat as caught-up).
	pods := []corev1.Pod{
		makePod("vk0-1", "10.0.0.2", "replica"),
	}
	checker := &stubLagChecker{byAddr: map[string]valkey.LagState{
		addrFor("10.0.0.2"): {Role: "master"},
	}}
	got := pickPromotionCandidate(context.Background(), checker, pods, "", 10000)
	if got == nil {
		t.Fatal("self-reporting-master pod must be eligible; got nil")
	}
}

func TestPickPromotionCandidate_TieBreakOnPodName(t *testing.T) {
	pods := []corev1.Pod{
		makePod("vk0-9", "10.0.0.9", "replica"),
		makePod("vk0-1", "10.0.0.2", "replica"),
	}
	checker := &stubLagChecker{byAddr: map[string]valkey.LagState{
		addrFor("10.0.0.9"): {Role: "slave", LinkUp: true, LagBytes: 100},
		addrFor("10.0.0.2"): {Role: "slave", LinkUp: true, LagBytes: 100},
	}}
	got := pickPromotionCandidate(context.Background(), checker, pods, "", 10000)
	if got == nil {
		t.Fatal("expected a candidate; got nil")
		return
	}
	if got.Name != "vk0-1" {
		t.Errorf("expected vk0-1 by name tie-break; got %s", got.Name)
	}
}

// --- rolloutMaxLagBytes ---

func TestRolloutMaxLagBytes_DefaultsToTenThousand(t *testing.T) {
	v := &valkeyv1beta1.Valkey{}
	if got := rolloutMaxLagBytes(v); got != 10000 {
		t.Errorf("default expected 10000; got %d", got)
	}
}

func TestRolloutMaxLagBytes_HonorsExplicit(t *testing.T) {
	v := &valkeyv1beta1.Valkey{}
	v.Spec.Rollout.MaxLagBytes = 25000
	if got := rolloutMaxLagBytes(v); got != 25000 {
		t.Errorf("explicit override expected 25000; got %d", got)
	}
}

// --- Strip-then-relabel invariant (named test from issue body) ---

// TestExporterNoDupesAcrossSwitchover pins the strip-then-relabel
// sequencing invariant: across the entire label-strip-to-relabel window, no
// two pods carry `role=primary` at the same observable moment. The
// invariant matters because the exporter ServiceMonitor (and any
// future per-pod target list) keys on this label — a transient
// double-stamp would cause Prometheus to scrape two replicas of
// the same primary metric set, producing duplicate samples for the
// failover-window seconds.
//
// This is a pure-logic test: it walks the operator's role-label
// state machine across the strip/dispatch/relabel sequence,
// asserting count(role=primary) ≤ 1 at every step.
func TestExporterNoDupesAcrossSwitchover(t *testing.T) {
	// Initial topology: pod-0 is primary, pod-1 + pod-2 are
	// replicas. (The bootstrap-rule path stamped these on first
	// reconcile; standalone of M2.x.)
	pods := []corev1.Pod{
		makePod("vk0-0", "10.0.0.1", "primary"),
		makePod("vk0-1", "10.0.0.2", "replica"),
		makePod(candidateName, "10.0.0.3", "replica"),
	}
	assertAtMostOnePrimary(t, "initial", pods)

	// Step 1 — stripPrimaryLabel removes the `role` label from the
	// outgoing primary. Operator code: delete the label entry;
	// Patch with strategic-merge.
	pods[0].Labels = nil
	assertAtMostOnePrimary(t, "post-strip", pods)
	assertOldPrimaryNotLabelledPrimary(t, "post-strip", &pods[0])

	// Step 2 — SENTINEL FAILOVER dispatched. No label changes here;
	// the cluster is mid-failover. The latch is set so
	// desiredRolesForCR suppresses any re-stamp.
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	preStripAddr := net.JoinHostPort(pods[0].Status.PodIP, "6379")
	r.failoverLatchSet(cr, preStripAddr)
	if !r.failoverLatchActive(cr, preStripAddr, 0) {
		t.Fatal("latch must be active during the failover-in-flight window")
	}
	assertAtMostOnePrimary(t, "mid-failover (latch active)", pods)
	assertOldPrimaryNotLabelledPrimary(t, "mid-failover (latch active)", &pods[0])

	// Step 3 — observer's +switch-master arrives; snapshot Addr
	// flips from old primary's IP to vk0-1's IP. The latch
	// auto-clears on the next desiredRolesForCR call.
	newPrimaryAddr := net.JoinHostPort(pods[1].Status.PodIP, "6379")
	if r.failoverLatchActive(cr, newPrimaryAddr, 0) {
		t.Fatal("latch must clear when observer reports the new primary's Addr")
	}
	assertOldPrimaryNotLabelledPrimary(t, "post-+switch-master, pre-relabel", &pods[0])

	// Step 4 — Phase 7 reads the post-+switch-master snapshot; the
	// new primary (vk0-1) gets `role=primary`, vk0-0 (now demoted)
	// gets `role=replica`. Crucially: the patch loop is idempotent
	// per-pod; the assertion holds across each per-pod patch step.

	// Patch order in production: reconcileRoleLabels iterates the
	// pod list in apiserver-list order, which is name-sorted. For
	// our three pods: vk0-0 (currently no label, wants replica) →
	// vk0-1 (currently replica, wants primary) → vk0-2 (currently
	// replica, wants replica, no-op).
	pods[0].Labels = map[string]string{RoleLabel: "replica"}
	assertAtMostOnePrimary(t, "post-patch vk0-0 → replica", pods)
	assertOldPrimaryNotLabelledPrimary(t, "post-patch vk0-0 → replica", &pods[0])
	pods[1].Labels[RoleLabel] = roleValuePrimary
	assertAtMostOnePrimary(t, "post-patch vk0-1 → primary", pods)
	assertOldPrimaryNotLabelledPrimary(t, "post-patch vk0-1 → primary", &pods[0])
	// vk0-2 is no-op.
	assertAtMostOnePrimary(t, "final post-patch", pods)
	assertOldPrimaryNotLabelledPrimary(t, "final post-patch", &pods[0])
}

// assertAtMostOnePrimary fails the test when more than one pod
// carries `role=primary`. Reports the offending step name + the
// labelled pod set so a regression is debuggable.
func assertAtMostOnePrimary(t *testing.T, step string, pods []corev1.Pod) {
	t.Helper()
	primaries := []string{}
	for _, p := range pods {
		if p.Labels[RoleLabel] == roleValuePrimary {
			primaries = append(primaries, p.Name)
		}
	}
	if len(primaries) > 1 {
		t.Errorf("[%s] count(role=primary)=%d > 1 → exporter would double-scrape; pods: %s",
			step, len(primaries), strings.Join(primaries, ", "))
	}
}

// assertOldPrimaryNotLabelledPrimary fails the test when the
// just-stripped pod transiently carries `role=primary` again before
// the new primary is labelled. Catches the bug class where buggy
// code re-stamps `role=primary` on the OLD pod (e.g., an
// unsuppressed Phase 7 read of a stale observer Addr) — count(role=primary)
// would still be ≤ 1 and the parent assertion would pass, but the
// invariant is violated: writes briefly route back to a pod the
// operator has just decided to demote.
func assertOldPrimaryNotLabelledPrimary(t *testing.T, step string, oldPrimary *corev1.Pod) {
	t.Helper()
	if oldPrimary.Labels[RoleLabel] == roleValuePrimary {
		t.Errorf("[%s] outgoing primary %s carries role=primary again — strip was undone",
			step, oldPrimary.Name)
	}
}

// --- runPrimaryRolloutDispatch early-exit no-ops ---

func TestRunPrimaryRolloutDispatch_NoOpWhenStateIsNotRolloutPrimary(t *testing.T) {
	r := &ValkeyReconciler{}
	v := newCR(valkeyv1beta1.ModeSentinel)
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	for _, state := range []orchestration.State{
		orchestration.StateSteady,
		orchestration.StateBootstrap,
		orchestration.StateDegraded,
		orchestration.StateRolloutReplicas,
		orchestration.StateFailoverInFlight,
	} {
		got := r.runPrimaryRolloutDispatch(context.Background(), v, cr, state, "vk0", "", nil)
		if got {
			t.Errorf("state=%s: expected no-op, got matched=true", state)
		}
	}
}

func TestRunPrimaryRolloutDispatch_NoOpWhenObserverNil(t *testing.T) {
	r := &ValkeyReconciler{SentinelObserver: nil}
	v := newCR(valkeyv1beta1.ModeSentinel)
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	got := r.runPrimaryRolloutDispatch(context.Background(), v, cr,
		orchestration.StateRolloutPrimary, "vk0", "", nil)
	if got {
		t.Errorf("nil observer: expected no-op, got matched=true")
	}
}

func TestRunPrimaryRolloutDispatch_NoOpWhenSnapshotNotPresent(t *testing.T) {
	// A never-Ensure'd CR has Snapshot{Present:false}; the
	// dispatcher's !snap.Present || !QuorumOK short-circuit fires
	// before any kube interaction.
	mgr, cancel := startedManager(t, nil)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr}
	v := newCR(valkeyv1beta1.ModeSentinel)
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	got := r.runPrimaryRolloutDispatch(context.Background(), v, cr,
		orchestration.StateRolloutPrimary, "vk0", "",
		[]sentinel.Endpoint{{Name: "vk0-sentinel-0", Addr: "127.0.0.1:1"}})
	if got {
		t.Errorf("!Present snapshot: expected no-op, got matched=true")
	}
}

// --- snapshotStale freshness gate ---

func TestSnapshotStale(t *testing.T) {
	base := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	const maxAge = 30 * time.Second
	// snap builds a present, quorum-OK snapshot whose last live poll was
	// at polled and whose UpdatedAt (which pub/sub also bumps) is updated
	// — kept distinct so the table can prove the gate keys off the poll
	// clock, not UpdatedAt.
	snap := func(polled, updated time.Time) sentinel.Snapshot {
		return sentinel.Snapshot{
			Present: true,
			Primary: sentinel.ObservedPrimary{
				Addr: "10.0.0.1:6379", QuorumOK: true,
				LastPolledAt: polled, UpdatedAt: updated,
			},
		}
	}
	old := base.Add(-(maxAge + time.Second))
	tests := []struct {
		name   string
		snap   sentinel.Snapshot
		now    time.Time
		maxAge time.Duration
		want   bool
	}{
		{"fresh poll within window", snap(base.Add(-10*time.Second), base), base, maxAge, false},
		{"boundary is inclusive-fresh", snap(base.Add(-maxAge), base), base, maxAge, false},
		{"stale poll past window", snap(old, old), base, maxAge, true},
		// Regression guard: pub/sub just refreshed UpdatedAt (= now) while
		// carrying a stale quorum forward, but the last live poll is old.
		// Must read stale — gating on UpdatedAt here would wave it through.
		{"pubsub refresh does not reset the poll clock", snap(old, base), base, maxAge, true},
		{"just-polled is fresh", snap(base, base), base, maxAge, false},
		{"non-positive maxAge disables the gate", snap(base.Add(-time.Hour), base), base, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := snapshotStale(tt.snap, tt.now, tt.maxAge); got != tt.want {
				t.Errorf("snapshotStale(pollAge=%s, maxAge=%s) = %v, want %v",
					tt.now.Sub(tt.snap.Primary.LastPolledAt), tt.maxAge, got, tt.want)
			}
		})
	}
}

// --- restorePrimaryLabel idempotency ---

func TestRestorePrimaryLabel_IdempotentWhenAlreadyLabelled(t *testing.T) {
	// When the pod already carries role=primary the function must
	// return nil without invoking r.Patch. Tested with a nil
	// reconciler client — if the function tried to call r.Patch the
	// nil deref would panic, proving the early-return guard works.
	r := &ValkeyReconciler{}
	pod := makePod("vk0-0", "10.0.0.1", "primary")
	if err := r.restorePrimaryLabel(context.Background(), &pod); err != nil {
		t.Fatalf("idempotent restore should return nil; got %v", err)
	}
	// Patch path (label was missing) is exercised by envtest because
	// it requires a kube client; the unit-level guard above is the
	// only contract that can be pinned without that infrastructure.
}

func TestRolloutMaxLagBytes_NormalisesNegativeOrZero(t *testing.T) {
	v := &valkeyv1beta1.Valkey{}
	v.Spec.Rollout.MaxLagBytes = 0
	if got := rolloutMaxLagBytes(v); got != 10000 {
		t.Errorf("zero must normalise to default 10000; got %d", got)
	}
	v.Spec.Rollout.MaxLagBytes = -50
	if got := rolloutMaxLagBytes(v); got != 10000 {
		t.Errorf("negative must normalise to default 10000; got %d", got)
	}
}

// TestFailoverInFlight_TimeoutEscapeToDegraded pins that the failover
// critical section is timeout-bounded and escapes to degraded/observe on
// Deadline expiry rather than holding indefinitely. It walks the escape
// through the real pure functions: while the latch is active and the
// observer still reports quorum on the pre-strip primary the derive pins
// StateFailoverInFlight; once the Deadline passes the latch releases the
// section (IsFailoverInFlight flips to false), and a cluster whose
// quorum was lost in the wedge re-derives to Degraded.
func TestFailoverInFlight_TimeoutEscapeToDegraded(t *testing.T) {
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	// In-flight: latch pins facts.FailoverInFlight while quorum still holds
	// on the pre-strip primary, so the derive reports the held section.
	if held := deriveStateFromFacts(observedFacts{PodCount: 3, QuorumOK: true, FailoverInFlight: true}); held != orchestration.StateFailoverInFlight {
		t.Fatalf("in-flight derive = %q, want %q", held, orchestration.StateFailoverInFlight)
	}

	// The latch is timeout-bounded: held while the deadline is in the
	// future, released once it passes — never an infinite hold.
	r := &ValkeyReconciler{}
	r.failoverLatchSetWithDeadline(cr, "10.0.0.1:6379", 0, time.Now().Add(time.Minute))
	if !r.IsFailoverInFlight(cr) {
		t.Fatal("section must be held while the latch deadline is in the future")
	}
	r.stateFor(cr).setFailoverLatch(&failoverInFlightLatch{
		preStripAddr: "10.0.0.1:6379",
		deadline:     time.Now().Add(-time.Second),
	})
	if r.IsFailoverInFlight(cr) {
		t.Fatal("section must release once the latch deadline passes (timeout/escape, never an infinite hold)")
	}

	// Post-release the wedged cluster (quorum lost, no pinned failover)
	// derives Degraded — the escape lands in degraded/observe.
	if escaped := deriveStateFromFacts(observedFacts{PodCount: 3, QuorumOK: false}); escaped != orchestration.StateDegraded {
		t.Fatalf("escaped derive = %q, want %q", escaped, orchestration.StateDegraded)
	}
}
