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

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/valkey"
)

// fakePromoteIssuer records IssuePromote calls without opening sockets.
type fakePromoteIssuer struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakePromoteIssuer) IssuePromote(_ context.Context, addr, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, addr)
	return f.err
}

func (f *fakePromoteIssuer) callAddrs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// promotionTestObserver spins a sentinel.Manager whose snapshot names
// snapIP as the quorum-backed primary, mirroring the apparatus the
// snapshot fast-path tests use, and waits for the first poll.
func promotionTestObserver(t *testing.T, cr types.NamespacedName, snapIP string) (*sentinel.Manager, func()) {
	t.Helper()
	mgr, cancel := startedManager(t, nil)
	rs, err := newRecoveringSentinel(snapIP, 1)
	if err != nil {
		cancel()
		t.Fatalf("newRecoveringSentinel: %v", err)
	}
	if err := mgr.Ensure(context.Background(), cr, "vk0", "",
		[]sentinel.Endpoint{{Name: "vk0-sentinel-0", Addr: rs.Addr()}}); err != nil {
		rs.Stop()
		cancel()
		t.Fatalf("Ensure: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		snap := mgr.Snapshot(cr)
		if snap.Present && snap.Primary.QuorumOK && snap.Primary.Addr == snapIP+":6379" {
			break
		}
		if time.Now().After(deadline) {
			rs.Stop()
			cancel()
			t.Fatalf("observer never published a fresh quorum snapshot; got %+v", mgr.Snapshot(cr))
		}
		time.Sleep(10 * time.Millisecond)
	}
	return mgr, func() { rs.Stop(); cancel() }
}

// wedgeASurvey is the zero-master total-wedge reading: two reachable
// replicas, both link-down, both replicating from a corpse.
func wedgeASurvey() *valkeyPodSurvey {
	return &valkeyPodSurvey{
		livePodIPs: map[string]struct{}{"10.0.0.1": {}, "10.0.0.2": {}},
		dialed:     true,
		replicas: []surveyedPod{
			{name: "vk0-0", ip: "10.0.0.1", state: valkey.LagState{
				Role: "slave", MasterHost: "10.0.0.9",
				SlaveReplOffset: 100, HaveSlaveOffset: true,
			}},
			{name: "vk0-1", ip: "10.0.0.2", state: valkey.LagState{
				Role: "slave", MasterHost: "10.0.0.9",
				SlaveReplOffset: 200, HaveSlaveOffset: true,
			}},
		},
	}
}

// wedgeAEndpoints is the sentinel endpoint set the dispatcher passes
// through — the addrs are never dialed in these tests (the replica
// views come from the injected fake).
func wedgeAEndpoints() []sentinel.Endpoint {
	return []sentinel.Endpoint{
		{Name: "vk0-sentinel-0", Addr: "10.1.0.1:26379"},
		{Name: "vk0-sentinel-1", Addr: "10.1.0.2:26379"},
		{Name: "vk0-sentinel-2", Addr: "10.1.0.3:26379"},
	}
}

// promoteAddrPodA is the wire address of the lowest-named wedge-A
// candidate (vk0-0 / 10.0.0.1) — the tie-break winner.
const promoteAddrPodA = "10.0.0.1:6379"

// promoteAddrPodB is the wire address of the highest-offset wedge-A
// candidate (vk0-1 / 10.0.0.2) — the offset-order winner.
const promoteAddrPodB = "10.0.0.2:6379"

// electionDoomedStub returns an ElectionDoomedFn with a fixed verdict.
// Wire-level doomed-election coverage lives in the sentinel package
// (Manager.QuorumElectionDoomed tests); the controller seam only needs
// the boolean.
func electionDoomedStub(doomed bool) func(context.Context, []sentinel.Endpoint, string, string, map[string]struct{}) bool {
	return func(context.Context, []sentinel.Endpoint, string, string, map[string]struct{}) bool {
		return doomed
	}
}

// promotionTestClient backs promotionPodViewConfirmed's strong-read
// with the same two pods the wedge-A survey reports, so the confirm
// passes unless a test deliberately drifts the views apart.
func promotionTestClient(t *testing.T) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(orphanTestScheme(t)).WithObjects(
		labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica),
		labelledValkeyPod("vk0-1", "10.0.0.2", roleValueReplica),
	).Build()
}

// promotionTestCR is orphanTestCR plus the sentinel spec the promotion
// path dereferences (the dispatcher guarantees it in production).
func promotionTestCR() *valkeyv1beta1.Valkey {
	cr := orphanTestCR()
	cr.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{MasterName: "vk0", Quorum: 2}
	return cr
}

// TestMaybeRecoveryPromote_PromotesHighestOffset pins the happy path of
// the zero-master recovery election: with every gate satisfied it
// promotes exactly the replica with the highest applied offset, and the
// per-CR cooldown blocks an immediate second election.
func TestMaybeRecoveryPromote_PromotesHighestOffset(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}

	r.maybeRecoveryPromote(context.Background(), cr, crKey, "", wedgeASurvey(), wedgeAEndpoints())
	if got := issuer.callAddrs(); len(got) != 1 || got[0] != promoteAddrPodB {
		t.Fatalf("expected exactly one promotion of 10.0.0.2 (offset 200), got %v", got)
	}

	// Immediate re-entry: the cooldown must hold even though every
	// other gate still passes.
	r.maybeRecoveryPromote(context.Background(), cr, crKey, "", wedgeASurvey(), wedgeAEndpoints())
	if got := issuer.callAddrs(); len(got) != 1 {
		t.Fatalf("promotion cooldown must block an immediate re-election, got %v", got)
	}
}

// TestDetectAndRecoverStrandedSentinels_CompoundFailureStillPromotes pins
// that the stranded-surgery cooldown gate — even backed off to its max —
// does NOT pace the zero-master recovery election. A compound failure
// (a sentinel linkup-stuck AND the cluster's master lost) must still
// recover write-availability on the election's own cadence, not wait out
// the stranded backoff. Regression guard: the gate used to sit above the
// election, so a backed-off cooldown suppressed recovery for up to ~4min.
func TestDetectAndRecoverStrandedSentinels_CompoundFailureStillPromotes(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
		// Zero-master survey with no live dials → the dispatcher takes
		// the recovery-election branch (masterIP == "").
		observedMasterFn: func(context.Context, *valkeyv1beta1.Valkey, string) (string, *valkeyPodSurvey) {
			return "", wedgeASurvey()
		},
	}

	// A linkup-stuck sentinel is deeply backed off and the probe gate just
	// fired — the stranded surgery is firmly cooling, but the recovery
	// election must run regardless (it sits above the gate).
	state := r.stateFor(crKey).quorumTracker()
	state.strandedNoProgress = map[string]strandedAddrState{
		"10.1.0.1:26379": {
			noProgress: strandedSurgeryStuckThreshold + strandedSurgeryMaxBackoffLevel,
			lastWiped:  time.Now(),
		},
	}
	state.strandedRecoveryLastFired = time.Now()
	if !state.strandedProbeCoolingDown(time.Now()) {
		t.Fatalf("precondition: the stranded surgery probe gate must be cooling")
	}

	r.detectAndRecoverStrandedSentinels(context.Background(), cr, crKey, "", wedgeAEndpoints())

	if got := issuer.callAddrs(); len(got) != 1 || got[0] != promoteAddrPodB {
		t.Fatalf("compound failure must still promote under an active stranded cooldown; got %v", got)
	}
}

// TestDetectAndRecoverStrandedSentinels_PassesComputedSkipSet pins the
// wiring seam: the dispatcher computes the per-address skip-set from its
// own tracker and passes it to the manager via
// InitialResetTarget.SkipStrandedAddrs. A stuck addr (past-threshold +
// recent lastWiped) must be in the passed set; a fresh (untracked) addr
// must NOT be. Kills the "computes-but-passes-nil" regression class.
func TestDetectAndRecoverStrandedSentinels_PassesComputedSkipSet(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	const stuckAddr = "10.2.0.1:26379"
	const freshAddr = "10.2.0.2:26379"

	var gotSkip map[string]struct{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		SentinelObserver: mgr,
		// A live master so the dispatcher proceeds past the masterIP==""
		// recovery-election branch to the surgery gate.
		observedMasterFn: func(context.Context, *valkeyv1beta1.Valkey, string) (string, *valkeyPodSurvey) {
			return "10.0.0.9", wedgeASurvey()
		},
		// Record the skip-set the dispatcher computes and passes — the seam
		// under test. Returns a bare probed result (no wipe) so the outcome
		// fold takes the gate-defer branch.
		recoverStrandedFn: func(_ context.Context, target sentinel.InitialResetTarget, _ bool) sentinel.StrandedRecoveryResult {
			gotSkip = target.SkipStrandedAddrs
			return sentinel.StrandedRecoveryResult{Probed: true}
		},
	}

	// One stuck addr (level 1, wiped just now → within its window) and one
	// fresh addr left untracked; the probe gate is open (zero last-fired).
	state := r.stateFor(crKey).quorumTracker()
	state.strandedNoProgress = map[string]strandedAddrState{
		stuckAddr: {noProgress: strandedSurgeryStuckThreshold, lastWiped: time.Now()},
	}

	r.detectAndRecoverStrandedSentinels(context.Background(), cr, crKey, "", wedgeAEndpoints())

	if _, ok := gotSkip[stuckAddr]; !ok {
		t.Errorf("the dispatcher must pass the stuck addr in SkipStrandedAddrs, got %v", gotSkip)
	}
	if _, ok := gotSkip[freshAddr]; ok {
		t.Errorf("a fresh (untracked) addr must NOT be in SkipStrandedAddrs, got %v", gotSkip)
	}
}

// TestMaybeRecoveryPromote_TieBreaksOnLowestName pins the tie-break:
// equal applied offsets promote the lowest-named pod.
func TestMaybeRecoveryPromote_TieBreaksOnLowestName(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}
	survey := wedgeASurvey()
	survey.replicas[1].state.SlaveReplOffset = survey.replicas[0].state.SlaveReplOffset
	// The lowest-named pod must NOT be first in iteration order —
	// otherwise first-seen seeding alone satisfies the assertion and
	// the `name <` tie-break clause is unproven.
	survey.replicas[0], survey.replicas[1] = survey.replicas[1], survey.replicas[0]
	r.maybeRecoveryPromote(context.Background(), cr, crKey, "", survey, wedgeAEndpoints())
	if got := issuer.callAddrs(); len(got) != 1 || got[0] != promoteAddrPodA {
		t.Fatalf("equal offsets must promote the lowest-named pod (vk0-0 / 10.0.0.1), got %v", got)
	}
}

// TestMaybeRecoveryPromote_Guards pins each independent precondition:
// any single unmet gate must block the promotion outright.
func TestMaybeRecoveryPromote_Guards(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	cases := []struct {
		name      string
		mutate    func(s *valkeyPodSurvey) *valkeyPodSurvey
		endpoints func() []sentinel.Endpoint
		doomedFn  func(context.Context, []sentinel.Endpoint, string, string, map[string]struct{}) bool
	}{
		{name: "nil survey", mutate: func(*valkeyPodSurvey) *valkeyPodSurvey { return nil }},
		{name: "dial sweep did not run", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.dialed = false
			return s
		}},
		{name: "a pod failed INFO (could be a hidden master)", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.dialFailures = 1
			return s
		}},
		{name: "a pod is still IP-less (could be the recreated master)", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.pendingPods = 1
			return s
		}},
		{name: "a live self-reported master exists", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.masters = append(s.masters, surveyedPod{name: "vk0-2", ip: "10.0.0.3"})
			return s
		}},
		{name: "no replicas to elect from", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.replicas = nil
			return s
		}},
		{name: "a replica has its link up (its master lives)", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.replicas[0].state.LinkUp = true
			return s
		}},
		{name: "a replica points at a LIVE pod", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.replicas[0].state.MasterHost = "10.0.0.2"
			return s
		}},
		{name: "replicas point at DIFFERENT dead masters (divergent lineages)", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.replicas[0].state.MasterHost = "10.0.0.8"
			return s
		}},
		{name: "a replica reports an empty master_host (unconfirmable lineage)", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.replicas[0].state.MasterHost = ""
			return s
		}},
		{name: "no replica reports slave_repl_offset (unrankable)", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			s.replicas[0].state.HaveSlaveOffset = false
			s.replicas[1].state.HaveSlaveOffset = false
			return s
		}},
		{name: "no sentinel endpoints", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey { return s },
			endpoints: func() []sentinel.Endpoint { return nil }},
		{name: "sentinel election not provably doomed (live candidate or unreadable tables)", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey { return s },
			doomedFn: electionDoomedStub(false)},
		{name: "strong-read pod view drifted from the survey", mutate: func(s *valkeyPodSurvey) *valkeyPodSurvey {
			// The cache-derived survey is missing a pod the API server
			// knows about (watch lag) — the confirm must defer.
			delete(s.livePodIPs, "10.0.0.1")
			s.replicas = s.replicas[1:]
			return s
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issuer := &fakePromoteIssuer{}
			doomedFn := tc.doomedFn
			if doomedFn == nil {
				doomedFn = electionDoomedStub(true)
			}
			r := &ValkeyReconciler{
				Client:           promotionTestClient(t),
				SentinelObserver: mgr,
				PromoteIssuer:    issuer,
				ElectionDoomedFn: doomedFn,
			}
			eps := wedgeAEndpoints()
			if tc.endpoints != nil {
				eps = tc.endpoints()
			}
			r.maybeRecoveryPromote(context.Background(), cr, crKey, "", tc.mutate(wedgeASurvey()), eps)
			if got := issuer.callAddrs(); len(got) != 0 {
				t.Fatalf("gate %q must block promotion, got %v", tc.name, got)
			}
		})
	}
}

// TestGhostReapAllowed pins the TTL-bounded +odown veto: a fresh
// +odown vetoes (an election may be brewing), a stale one does not
// (pubsub-only map, a lost -odown frame must not latch the veto
// forever), and !Present always vetoes.
func TestGhostReapAllowed(t *testing.T) {
	now := time.Unix(5000, 0)
	const ftMS = int32(180000)
	snap := func(present bool, odown map[string]time.Time) sentinel.Snapshot {
		s := sentinel.Snapshot{Present: present}
		s.Primary.ODown = odown
		return s
	}
	if ghostReapAllowed(snap(false, nil), ftMS, now) {
		t.Error("!Present must veto")
	}
	if !ghostReapAllowed(snap(true, nil), ftMS, now) {
		t.Error("empty odown map must allow")
	}
	fresh := map[string]time.Time{"s1": now.Add(-time.Minute)}
	if ghostReapAllowed(snap(true, fresh), ftMS, now) {
		t.Error("a fresh +odown (within failover-timeout) must veto")
	}
	stale := map[string]time.Time{"s1": now.Add(-4 * time.Minute)}
	if !ghostReapAllowed(snap(true, stale), ftMS, now) {
		t.Error("a stale +odown (past failover-timeout) must not veto")
	}
	// Non-positive failover-timeout falls back to the 180s floor.
	if ghostReapAllowed(snap(true, fresh), 0, now) {
		t.Error("zero failover-timeout must fall back to the 180s floor and veto a fresh entry")
	}
}

// TestGhostReapAllowed_PullSideODownSource pins the second veto source:
// a fresh pull-side o_down (ODownPull) within one failover-timeout must
// veto even when the pubsub ODown map is empty, and a stale ODownPull
// entry (past failover-timeout) must NOT veto — the rising-edge escape
// valve. The existing TestGhostReapAllowed (nil ODownPull) proves the
// new loop is a no-op when the pull source is absent.
func TestGhostReapAllowed_PullSideODownSource(t *testing.T) {
	now := time.Unix(5000, 0)
	const ftMS = int32(180000)
	snap := func(pull map[string]time.Time) sentinel.Snapshot {
		s := sentinel.Snapshot{Present: true}
		s.Primary.ODownPull = pull
		return s
	}
	// Empty ODown, fresh ODownPull (within failover-timeout) → veto.
	fresh := map[string]time.Time{"s1": now.Add(-time.Minute)}
	if ghostReapAllowed(snap(fresh), ftMS, now) {
		t.Error("a fresh pull-side o_down must veto even with an empty pubsub ODown map")
	}
	// Empty ODown, stale ODownPull (past failover-timeout) → allow.
	stale := map[string]time.Time{"s1": now.Add(-4 * time.Minute)}
	if !ghostReapAllowed(snap(stale), ftMS, now) {
		t.Error("a stale pull-side o_down (past failover-timeout) must not veto — the escape valve")
	}
	// Age-out boundary: ftMS=180000ms => ttl=3m, and the loop predicate
	// is `now.Sub(ts) < ttl` (exclusive). At elapsed == ttl exactly the
	// entry must NOT veto; one nanosecond inside it must. Pins the `<`
	// and kills a `<=` mutant.
	atBoundary := map[string]time.Time{"s1": now.Add(-3 * time.Minute)}
	if !ghostReapAllowed(snap(atBoundary), ftMS, now) {
		t.Error("at exactly one failover-timeout the entry must not veto — the bound is exclusive")
	}
	justInside := map[string]time.Time{"s1": now.Add(-3*time.Minute + time.Nanosecond)}
	if ghostReapAllowed(snap(justInside), ftMS, now) {
		t.Error("one nanosecond inside one failover-timeout must veto")
	}
}

// TestMaybeRecoveryPromote_SnapshotNamesLivePod pins the quorum-evidence
// gate: when the sentinels' quorum-backed primary address belongs to a
// LIVE pod, the master exists (Phase 7 / re-point own that state) and
// the election must not fire — even though the dial survey saw only
// link-down replicas.
func TestMaybeRecoveryPromote_SnapshotNamesLivePod(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	// The snapshot names 10.0.0.1 — which IS in the survey's live set.
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.1")
	defer stop()

	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}
	r.maybeRecoveryPromote(context.Background(), cr, crKey, "", wedgeASurvey(), wedgeAEndpoints())
	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("a live quorum-named primary must block promotion, got %v", got)
	}
}

// TestRepointClassArmed pins the hot-path gate for the dead-master
// re-point fan-out: it arms only on corpse-monitoring suspicion.
func TestRepointClassArmed(t *testing.T) {
	live := map[string]struct{}{"10.0.0.1": {}, "10.0.0.2": {}}
	snap := func(present, quorumOK bool, addr string) sentinel.Snapshot {
		s := sentinel.Snapshot{Present: present}
		s.Primary.QuorumOK = quorumOK
		s.Primary.Addr = addr
		return s
	}
	cases := []struct {
		name string
		snap sentinel.Snapshot
		want bool
	}{
		{"healthy quorum on a live master disarms", snap(true, true, "10.0.0.1:6379"), false},
		{"quorum-agreed dead address arms", snap(true, true, "10.0.0.9:6379"), true},
		{"no quorum agreement arms", snap(true, false, ""), true},
		{"unparseable agreed addr arms", snap(true, true, "not-an-addr"), true},
		{"absent snapshot disarms (boot race)", snap(false, true, "10.0.0.9:6379"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := repointClassArmed(tc.snap, live); got != tc.want {
				t.Errorf("repointClassArmed = %v, want %v", got, tc.want)
			}
		})
	}
}

// armSuppression flips the per-CR quorum-suppression gate on, mirroring a
// sustained CKQUORUM=NOQUORUM dwell — the arm the observer-side recovery
// path keys off.
func armSuppression(r *ValkeyReconciler, crKey types.NamespacedName) {
	st := r.stateFor(crKey).quorumTracker()
	st.mu.Lock()
	st.suppressionActive = true
	st.mu.Unlock()
}

// wedgeALagChecker seeds a fakeLagChecker with the wedge-A readings keyed by
// wire address, so a forced dial-sweep rebuilds the same zero-master survey
// wedgeASurvey hand-constructs (two link-down replicas replicating from one
// dead master, offsets 100 and 200).
func wedgeALagChecker() *fakeLagChecker {
	checker := newFakeLagChecker()
	checker.byAddr[promoteAddrPodA] = valkey.LagState{
		Role: "slave", MasterHost: "10.0.0.9", LinkUp: false,
		SlaveReplOffset: 100, HaveSlaveOffset: true,
	}
	checker.byAddr[promoteAddrPodB] = valkey.LagState{
		Role: "slave", MasterHost: "10.0.0.9", LinkUp: false,
		SlaveReplOffset: 200, HaveSlaveOffset: true,
	}
	return checker
}

// nonDialedSurvey is the shape a cheap resolver short-circuit hands the
// dispatcher: the live-IP set is known but no INFO sweep has run.
func nonDialedSurvey() *valkeyPodSurvey {
	return &valkeyPodSurvey{
		livePodIPs: map[string]struct{}{"10.0.0.1": {}, "10.0.0.2": {}},
		dialed:     false,
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_ForcesDetectionAndPromotes pins the
// core of the sustained-quorum-loss arming path: when the resolver
// short-circuited (a non-dialed survey) but the suppression gate is armed,
// the path forces a fresh dialed survey and runs the full promotion guard
// chain, promoting the highest-offset replica.
func TestMaybeRecoveryPromoteOnQuorumLoss_ForcesDetectionAndPromotes(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	checker := wedgeALagChecker()
	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}
	armSuppression(r, crKey)

	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())

	if got := issuer.callAddrs(); len(got) != 1 || got[0] != promoteAddrPodB {
		t.Fatalf("the CKQUORUM arm must force detection and promote the highest-offset replica (%s); got %v", promoteAddrPodB, got)
	}
	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n == 0 {
		t.Fatalf("the arm must force a fresh dial-sweep when the resolver short-circuited; got zero CheckLag calls")
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_NotSuppressedIsCheapNoOp pins the
// round-trip-budget invariant: with the suppression gate clear (a healthy
// cluster) the path is a cheap no-op — no forced dial, no promotion.
func TestMaybeRecoveryPromoteOnQuorumLoss_NotSuppressedIsCheapNoOp(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	checker := wedgeALagChecker()
	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}
	// suppressionActive stays false — the arm is disengaged.

	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())

	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n != 0 {
		t.Fatalf("a healthy (unsuppressed) cluster must not pay the forced dial-sweep; got %d CheckLag calls", n)
	}
	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("no promotion may fire without the quorum-loss arm; got %v", got)
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_CooldownBoundsProbe pins that
// recoveryDetectionCooldown makes the forced probe periodic: a probe within
// the cooldown window is suppressed; one past it runs. Uses a frozen clock so
// the cooldown is exercised independently of the promotion cooldown.
func TestMaybeRecoveryPromoteOnQuorumLoss_CooldownBoundsProbe(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	checker := wedgeALagChecker()
	t0 := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	nowVal := t0
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    &fakePromoteIssuer{},
		ElectionDoomedFn: electionDoomedStub(true),
		nowFunc:          func() time.Time { return nowVal },
	}
	armSuppression(r, crKey)
	state := r.stateFor(crKey).quorumTracker()
	state.mu.Lock()
	state.recoveryDetectionLastProbed = t0.Add(-10 * time.Second)
	state.mu.Unlock()

	// Only 10s since the last probe (< 30s cooldown) → suppressed.
	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())
	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n != 0 {
		t.Fatalf("within recoveryDetectionCooldown the forced dial must not run; got %d CheckLag calls", n)
	}

	// Advance to 35s elapsed (>= 30s) → the probe runs.
	nowVal = t0.Add(25 * time.Second)
	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())
	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n == 0 {
		t.Fatalf("past recoveryDetectionCooldown the forced dial must run; got zero CheckLag calls")
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_DebounceCommitSuppressesSecondCall
// pins that the arming path COMMITS its probe-cadence stamp. Two calls
// inside recoveryDetectionCooldown with NO manual stamp seeding must dial
// exactly once: call 1 (no prior stamp) runs the forced N-pod sweep AND
// writes recoveryDetectionLastProbed; call 2, a few seconds later, reads
// that committed stamp and is suppressed. This kills the mutant that drops
// the stamp-write — which would leave the stamp unwritten so every
// reconcile re-dials the full INFO sweep under sustained quorum loss,
// violating the round-trip budget. CooldownBoundsProbe pre-seeds the stamp
// and so exercises only the READ side; this exercises the WRITE.
func TestMaybeRecoveryPromoteOnQuorumLoss_DebounceCommitSuppressesSecondCall(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	checker := wedgeALagChecker()
	t0 := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	nowVal := t0
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    &fakePromoteIssuer{},
		ElectionDoomedFn: electionDoomedStub(true),
		nowFunc:          func() time.Time { return nowVal },
	}
	armSuppression(r, crKey)
	// No manual recoveryDetectionLastProbed seeding: the zero stamp forces
	// call 1 to run the sweep and (correctly) commit the probe time.

	// Call 1: dials + must write the stamp.
	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())
	after1 := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB)
	if after1 == 0 {
		t.Fatalf("call 1 must run the forced dial-sweep; got zero CheckLag calls")
	}

	// Call 2, 5s later (well inside the 30s cooldown): the committed stamp
	// must suppress a second sweep. Without the stamp-write the stamp stays
	// zero and call 2 re-dials — the mutant this test kills.
	nowVal = t0.Add(5 * time.Second)
	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())
	if after2 := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); after2 != after1 {
		t.Fatalf("call 2 inside recoveryDetectionCooldown must be suppressed by the committed stamp; sweep count went %d -> %d (stamp not committed?)", after1, after2)
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_LiveMasterBlocks pins the safety
// point: under the arm the forced dial still runs, but a pod self-reporting
// master makes promotionSurveyAdmits refuse — a still-served cluster is never
// promoted over, even though the live-snapshot gate is inert under NOQUORUM.
func TestMaybeRecoveryPromoteOnQuorumLoss_LiveMasterBlocks(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	checker := newFakeLagChecker()
	checker.byAddr[promoteAddrPodA] = valkey.LagState{Role: valkey.RoleMaster}
	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}
	armSuppression(r, crKey)

	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())

	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("a live self-reported master must block promotion under the arm; got %v", got)
	}
	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n == 0 {
		t.Fatalf("the forced dial must run (it is what reveals the live master); got zero CheckLag calls")
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_ReusesDialedSurvey pins that an
// already-dialed survey is threaded straight to the guard chain — no re-List,
// no re-dial — and that maybeRecoveryPromote still refuses on a survey that
// carries a master.
func TestMaybeRecoveryPromoteOnQuorumLoss_ReusesDialedSurvey(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	checker := wedgeALagChecker()
	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}
	armSuppression(r, crKey)

	survey := &valkeyPodSurvey{
		livePodIPs: map[string]struct{}{"10.0.0.1": {}},
		dialed:     true,
		masters: []surveyedPod{
			{name: "vk0-0", ip: "10.0.0.1", state: valkey.LagState{Role: valkey.RoleMaster}},
		},
	}
	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", survey, wedgeAEndpoints())

	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n != 0 {
		t.Fatalf("an already-dialed survey must be reused, not re-probed; got %d CheckLag calls", n)
	}
	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("maybeRecoveryPromote must refuse on a survey carrying a master; got %v", got)
	}
}

// TestDetectAndRecoverStrandedSentinels_QuorumLossTriggerAboveStrandedCooldown
// pins the dispatcher hoist: with a resolved masterIP (the masterIP!=""
// branch) and a firmly-active stranded-surgery cooldown, the sustained-
// quorum-loss detection still forces a dial-sweep — it runs on the election's
// own cadence, above the base-cadence surgery probe gate.
func TestDetectAndRecoverStrandedSentinels_QuorumLossTriggerAboveStrandedCooldown(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	checker := wedgeALagChecker()
	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
		// A resolved masterIP routes the dispatcher into the masterIP!=""
		// branch (below the masterIP=="" recovery block), with a non-dialed
		// survey standing in for a cheap resolver short-circuit.
		observedMasterFn: func(context.Context, *valkeyv1beta1.Valkey, string) (string, *valkeyPodSurvey) {
			return "10.0.0.1", nonDialedSurvey()
		},
	}
	armSuppression(r, crKey)

	// Active stranded-surgery probe gate: fired recently, so the base-cadence
	// probe cooldown would block surgery this pass — the detection election
	// must run above it regardless.
	state := r.stateFor(crKey).quorumTracker()
	state.strandedRecoveryLastFired = time.Now().Add(-5 * time.Second)
	if !state.strandedProbeCoolingDown(time.Now()) {
		t.Fatalf("precondition: the stranded surgery probe cooldown must be active")
	}

	r.detectAndRecoverStrandedSentinels(context.Background(), cr, crKey, "", wedgeAEndpoints())

	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n == 0 {
		t.Fatalf("quorum-loss detection must force a dial-sweep above the active stranded cooldown; got zero CheckLag calls")
	}
}

// TestMaybeRecoveryPromote_FailoverInFlightBlocks pins the failover-in-flight
// refusal the arming path leans on: with an otherwise-promotable wedge-A
// survey (the exact input that promotes in _PromotesHighestOffset), a CR
// observed in StateFailoverInFlight must NOT be promoted — a recovery
// election must never race a sentinel-driven failover's config-epoch
// propagation. Deleting the guard lets a promotion fire here (mutation-tested).
func TestMaybeRecoveryPromote_FailoverInFlightBlocks(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}
	// Stamp the FSM tracker into failover-in-flight — the same idiom the
	// deferral-predicate tests use to drive IsFailoverInFlight true.
	r.stateFor(crKey).fsmTransition = &fsmTransitionTracker{lastState: orchestration.StateFailoverInFlight}

	r.maybeRecoveryPromote(context.Background(), cr, crKey, "", wedgeASurvey(), wedgeAEndpoints())

	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("a failover in flight must block recovery promotion; got %v", got)
	}
}

// TestMaybeRecoveryPromote_BootRaceSnapshotBlocks pins the boot-race guard:
// a not-yet-Present observer snapshot (a manager that has not published a
// quorum view for this CR) must block promotion outright, even with an
// otherwise-promotable wedge-A survey. A boot-racing observer must never
// authorize an irreversible REPLICAOF NO ONE. Deleting the guard lets a
// promotion fire here (mutation-tested).
func TestMaybeRecoveryPromote_BootRaceSnapshotBlocks(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	// A started manager that never Ensured this CR → Snapshot(cr) is not
	// Present (the boot-race window before the first quorum poll).
	mgr, cancel := startedManager(t, nil)
	defer cancel()

	issuer := &fakePromoteIssuer{}
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
	}

	r.maybeRecoveryPromote(context.Background(), cr, crKey, "", wedgeASurvey(), wedgeAEndpoints())

	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("a not-Present (boot-race) snapshot must block recovery promotion; got %v", got)
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_ForcedDialFailureRefusesAndStamps pins
// the forced-dial CheckLag-error path: when a probed pod fails INFO the
// survey records a dialFailure, and promotionSurveyAdmits refuses on
// incomplete coverage (a failed pod could be a hidden master). The debounce
// stamp is still committed, so the failing sweep does not hot-loop.
func TestMaybeRecoveryPromoteOnQuorumLoss_ForcedDialFailureRefusesAndStamps(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}

	checker := wedgeALagChecker()
	// The forced dial hits an INFO failure on one pod → dialFailures++.
	checker.errAddr[promoteAddrPodA] = context.DeadlineExceeded
	issuer := &fakePromoteIssuer{}
	t0 := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	r := &ValkeyReconciler{
		Client:        promotionTestClient(t),
		LagChecker:    checker,
		PromoteIssuer: issuer,
		nowFunc:       func() time.Time { return t0 },
	}
	armSuppression(r, crKey)

	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())

	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("an incomplete forced dial (a pod failed INFO) must refuse promotion; got %v", got)
	}
	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n == 0 {
		t.Fatalf("the forced dial must have run; got zero CheckLag calls")
	}
	state := r.stateFor(crKey).quorumTracker()
	state.mu.Lock()
	stamped := state.recoveryDetectionLastProbed
	state.mu.Unlock()
	if !stamped.Equal(t0) {
		t.Fatalf("the probe stamp must be committed even when the dial fails; got %v want %v", stamped, t0)
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_ListErrorStampsAndDebounces pins the
// listValkeyPods-error early return: the debounce stamp is committed BEFORE
// the List, so a List error refuses this pass AND a second call inside the
// cooldown does not re-List — the failing sweep cannot hot-loop at reconcile
// cadence.
func TestMaybeRecoveryPromoteOnQuorumLoss_ListErrorStampsAndDebounces(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}

	checker := wedgeALagChecker()
	issuer := &fakePromoteIssuer{}
	listCalls := 0
	cl := fake.NewClientBuilder().WithScheme(orphanTestScheme(t)).
		WithObjects(
			labelledValkeyPod("vk0-0", "10.0.0.1", roleValueReplica),
			labelledValkeyPod("vk0-1", "10.0.0.2", roleValueReplica),
		).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				listCalls++
				return context.DeadlineExceeded
			},
		}).Build()

	t0 := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	nowVal := t0
	r := &ValkeyReconciler{
		Client:        cl,
		LagChecker:    checker,
		PromoteIssuer: issuer,
		nowFunc:       func() time.Time { return nowVal },
	}
	armSuppression(r, crKey)

	// Call 1: stamps, then the forced List errors → early return, no dial.
	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())
	if listCalls != 1 {
		t.Fatalf("call 1 must attempt exactly one List; got %d", listCalls)
	}
	if n := checker.callCount(promoteAddrPodA) + checker.callCount(promoteAddrPodB); n != 0 {
		t.Fatalf("a List error must return before any dial; got %d CheckLag calls", n)
	}
	state := r.stateFor(crKey).quorumTracker()
	state.mu.Lock()
	stamped := state.recoveryDetectionLastProbed
	state.mu.Unlock()
	if !stamped.Equal(t0) {
		t.Fatalf("the probe stamp must be committed before the List; got %v want %v", stamped, t0)
	}

	// Call 2, 5s later (inside the 30s cooldown): the committed stamp must
	// debounce it before the List — no re-List, no hot loop.
	nowVal = t0.Add(5 * time.Second)
	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())
	if listCalls != 1 {
		t.Fatalf("call 2 inside the cooldown must be debounced before the List; List count went to %d", listCalls)
	}
	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("no promotion may fire when the forced List errors; got %v", got)
	}
}

// TestMaybeRecoveryPromoteOnQuorumLoss_ForcedDialSurfacesDualMaster pins
// the forced sweep's dual-master feed: under the sustained-quorum-loss
// arm with a resolver short-circuit (non-dialed survey), a forced dial
// revealing >=2 self-reported masters must stamp the dual-master
// observation AND fire the DualMasterObserved Warning — not just refuse
// promotion silently. Without the feed this compound wedge (NOQUORUM
// suppression + labeled-primary short-circuit + data-plane split) is
// invisible to the condition/gauge/event surface.
func TestMaybeRecoveryPromoteOnQuorumLoss_ForcedDialSurfacesDualMaster(t *testing.T) {
	cr := promotionTestCR()
	crKey := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	mgr, stop := promotionTestObserver(t, crKey, "10.0.0.9")
	defer stop()

	// Both pods self-report role:master — the split the short-circuited
	// resolver never dialed for.
	checker := newFakeLagChecker()
	checker.byAddr[promoteAddrPodA] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 100, HaveMasterOffset: true}
	checker.byAddr[promoteAddrPodB] = valkey.LagState{Role: valkey.RoleMaster, MasterReplOffset: 200, HaveMasterOffset: true}
	issuer := &fakePromoteIssuer{}
	rec := k8sevents.NewFakeRecorder(16)
	r := &ValkeyReconciler{
		Client:           promotionTestClient(t),
		LagChecker:       checker,
		SentinelObserver: mgr,
		PromoteIssuer:    issuer,
		ElectionDoomedFn: electionDoomedStub(true),
		Recorder:         rec,
	}
	armSuppression(r, crKey)

	r.maybeRecoveryPromoteOnQuorumLoss(context.Background(), cr, crKey, "", nonDialedSurvey(), wedgeAEndpoints())

	if got := issuer.callAddrs(); len(got) != 0 {
		t.Fatalf("two self-reported masters must block promotion; got %v", got)
	}
	obs := r.stateFor(crKey).dualMasterObservation()
	if obs == nil || len(obs.pods) != 2 {
		t.Fatalf("the forced dial must stamp the dual-master observation; got %+v", obs)
	}
	if n := countDualMasterObserved(rec); n != 1 {
		t.Fatalf("the forced dial must fire exactly one DualMasterObserved Warning; got %d", n)
	}
}
