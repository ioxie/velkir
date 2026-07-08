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

package sentinel

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

// TestReconcileODownPull_RisingEdgeStampsOnce pins the escape-valve-
// preserving rising-edge semantics: the first o_down stamps `now`, and
// a later tick with o_down still set KEEPS the original stamp (no
// refresh) so the entry ages out one failover-timeout after o_down
// FIRST appeared.
func TestReconcileODownPull_RisingEdgeStampsOnce(t *testing.T) {
	m := map[string]time.Time{}
	t0 := time.Unix(1000, 0)
	reconcileODownPull(m, []odownPullObs{{name: "s1", flags: &MasterFlags{ODown: true}}}, t0)
	if got, ok := m["s1"]; !ok || !got.Equal(t0) {
		t.Fatalf("first o_down must stamp t0; got %v ok=%v", got, ok)
	}
	// A later tick with o_down still set must NOT refresh the stamp.
	t1 := t0.Add(30 * time.Second)
	reconcileODownPull(m, []odownPullObs{{name: "s1", flags: &MasterFlags{ODown: true}}}, t1)
	if got := m["s1"]; !got.Equal(t0) {
		t.Fatalf("persisting o_down must keep the original stamp %v, got %v", t0, got)
	}
}

// TestReconcileODownPull_UnreachableLeavesEntryUntouched pins
// absence-of-evidence != evidence-of-absence: a nil-flags observation
// (the sentinel was unreachable this tick) leaves the existing entry
// exactly as it was — neither stamped nor cleared.
func TestReconcileODownPull_UnreachableLeavesEntryUntouched(t *testing.T) {
	t0 := time.Unix(1000, 0)
	m := map[string]time.Time{"s1": t0}
	reconcileODownPull(m, []odownPullObs{{name: "s1", flags: nil}}, t0.Add(time.Minute))
	if got, ok := m["s1"]; !ok || !got.Equal(t0) {
		t.Fatalf("nil flags (unreachable) must leave the entry untouched; got %v ok=%v", got, ok)
	}
}

// TestReconcileODownPull_ParseErrorLeavesEntryUntouched pins the
// parse-error provenance of the untouched rule: a sentinel that WAS
// reached but whose SENTINEL MASTER reply had an unparseable flags
// field surfaces as nil flags too, and must likewise leave the entry
// untouched — a lost -odown frame must never clear the pull entry.
func TestReconcileODownPull_ParseErrorLeavesEntryUntouched(t *testing.T) {
	t0 := time.Unix(2000, 0)
	m := map[string]time.Time{"s1": t0}
	reconcileODownPull(m, []odownPullObs{{name: "s1", flags: nil}}, t0.Add(2*time.Minute))
	if got, ok := m["s1"]; !ok || !got.Equal(t0) {
		t.Fatalf("parse-error nil flags must leave the entry untouched; got %v ok=%v", got, ok)
	}
}

// TestReconcileODownPull_ClearOnConfirmedAbsence pins the
// pull-confirmed clear: a REACHED sentinel reporting o_down=false
// deletes the entry, unlike an unreachable sentinel or a lost -odown
// frame.
func TestReconcileODownPull_ClearOnConfirmedAbsence(t *testing.T) {
	t0 := time.Unix(3000, 0)
	m := map[string]time.Time{"s1": t0}
	reconcileODownPull(m, []odownPullObs{{name: "s1", flags: &MasterFlags{ODown: false}}}, t0.Add(time.Minute))
	if _, ok := m["s1"]; ok {
		t.Fatal("a reached sentinel reporting o_down=false must delete the entry (pull-confirmed clear)")
	}
}

// TestReconcileODownPull_NoODownNoEntryIsNoOp pins that a reached
// sentinel reporting no o_down (with no prior entry) leaves the map
// empty — including the s_down-only case, since a lone subjective
// s_down is not a stamp input.
func TestReconcileODownPull_NoODownNoEntryIsNoOp(t *testing.T) {
	m := map[string]time.Time{}
	reconcileODownPull(m, []odownPullObs{
		{name: "s1", flags: &MasterFlags{}},
		{name: "s2", flags: &MasterFlags{SDown: true}},
	}, time.Unix(4000, 0))
	if len(m) != 0 {
		t.Fatalf("reached-and-clear (incl. s_down-only) with no prior entry must leave the map empty; got %v", m)
	}
}

// TestReconcileODownPull_SDownOnlyClearsPreExistingStamp pins the two
// halves of the s_down rule precisely: s_down is never a stamp INPUT,
// so an s_down-only reply on an ABSENT key stays absent; but an
// s_down-only reply is still o_down-clear, so on a key that already
// holds an o_down stamp it DELETES the entry — a pull-confirmed o_down
// clear (the o_down -> s_down downgrade means the quorum no longer
// agrees the master is down). Seeding a pre-existing entry is what
// distinguishes delete from leave-alone.
func TestReconcileODownPull_SDownOnlyClearsPreExistingStamp(t *testing.T) {
	t0 := time.Unix(6000, 0)
	m := map[string]time.Time{"s1": t0}
	reconcileODownPull(m, []odownPullObs{
		// s1: carried an o_down stamp, now reads s_down-only (o_down
		//     cleared) -> delete (pull-confirmed clear).
		{name: "s1", flags: &MasterFlags{SDown: true}},
		// s2: absent key, s_down-only -> no stamp created (s_down is
		//     not a stamp input).
		{name: "s2", flags: &MasterFlags{SDown: true}},
	}, t0.Add(time.Minute))
	if _, ok := m["s1"]; ok {
		t.Error("an s_down-only reply on a pre-existing o_down stamp must delete it (o_down confirmed clear)")
	}
	if _, ok := m["s2"]; ok {
		t.Error("an s_down-only reply on an absent key must not create a stamp")
	}
	if len(m) != 0 {
		t.Errorf("map must be empty after the s_down-only reconcile; got %v", m)
	}
}

// TestObserver_PublishCopiesODownPull pins the lock-free-read copy for
// the new field: publishLocked must snapshot o.odownPull into a
// distinct map so mutating either side never affects the other.
func TestObserver_PublishCopiesODownPull(t *testing.T) {
	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{{Name: "vk0-sentinel-0", Addr: "10.0.0.1:26379"}},
		Options{}, nil, nil,
	)
	t0 := time.Unix(5000, 0)
	o.mu.Lock()
	o.odownPull["vk0-sentinel-0"] = t0
	o.publishLocked(QuorumStatusOK, testPrimaryAddr, 1, SourcePoll, time.Now())
	o.mu.Unlock()

	snap, ok := o.snapshot()
	if !ok {
		t.Fatal("snapshot must be present after publish")
	}
	if got, ok := snap.ODownPull["vk0-sentinel-0"]; !ok || !got.Equal(t0) {
		t.Fatalf("published ODownPull must carry the stamp; got %v ok=%v", got, ok)
	}

	// Mutating the observer's live map must not affect the published copy.
	o.mu.Lock()
	delete(o.odownPull, "vk0-sentinel-0")
	o.mu.Unlock()
	if got, ok := snap.ODownPull["vk0-sentinel-0"]; !ok || !got.Equal(t0) {
		t.Fatalf("published copy must be independent of the live map; got %v ok=%v", got, ok)
	}

	// Mutating the published copy must not leak into the live map.
	snap.ODownPull["injected"] = t0
	o.mu.Lock()
	_, leaked := o.odownPull["injected"]
	o.mu.Unlock()
	if leaked {
		t.Fatal("mutating the snapshot copy must not leak into the live map")
	}
}

// queuePollRepliesWithFlags is queuePollReplies with a `flags` field
// added to the SENTINEL MASTER reply so pull-side o_down derivation can
// be exercised end-to-end. Epoch is fixed at the happy-path value.
func queuePollRepliesWithFlags(fs *fakeSentinel, flags string, quorumOK bool) {
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", buildArrayReply("10.0.0.7", "6379"))
	fs.QueueReply("SENTINEL MASTER", buildArrayReply(
		"name", "vk0",
		"ip", "10.0.0.7",
		"port", "6379",
		"flags", flags,
		"config-epoch", "42",
	))
	if quorumOK {
		fs.QueueReply("SENTINEL CKQUORUM", "+OK 3 usable Sentinels\r\n")
	} else {
		fs.QueueReply("SENTINEL CKQUORUM", "-NOQUORUM Quorum not reached\r\n")
	}
}

// TestObserver_PullTickStampsODownPull pins the end-to-end pull path,
// envtest-free: two fake sentinels report flags=master,s_down,o_down in
// their SENTINEL MASTER replies, and the published snapshot's ODownPull
// gets a stamped entry keyed by each sentinel pod name — zero extra
// round-trips, riding the existing pull tick.
func TestObserver_PullTickStampsODownPull(t *testing.T) {
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	t.Cleanup(func() { fs1.Stop(); fs2.Stop() })
	for _, fs := range []*fakeSentinel{fs1, fs2} {
		queuePsubscribeAcks(fs)
		for range 5 {
			queuePollRepliesWithFlags(fs, "master,s_down,o_down", true)
		}
	}

	opts := Options{
		PollInterval:       50 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	}
	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
			{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
		},
		opts,
		k8sevents.NewFakeRecorder(64),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); o.stop() }()
	o.start(ctx)

	snap := waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		_, ok := p.ODownPull["vk0-sentinel-0"]
		return ok
	})
	if _, ok := snap.ODownPull["vk0-sentinel-1"]; !ok {
		t.Errorf("both reached sentinels reporting o_down must be stamped; got %v", snap.ODownPull)
	}
}

// TestObserver_PullTickStampsODownPullUnderNoQuorum pins the degraded
// seam the pull-side veto exists for: a reached sentinel reports
// flags=master,o_down while CKQUORUM answers -NOQUORUM (the cluster is
// sub-quorum). queryOne surfaces the CKQUORUM error as quorumOK=false
// but still returns the flags parsed from the SAME SENTINEL MASTER
// reply, so ODownPull must be stamped even though the published quorum
// is Lost — a reached sentinel's o_down is honored regardless of
// quorum. Without this, reverting queryOne's CKQUORUM-error return to a
// nil flags leaves the suite green (a surviving mutant).
func TestObserver_PullTickStampsODownPullUnderNoQuorum(t *testing.T) {
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	t.Cleanup(func() { fs1.Stop(); fs2.Stop() })
	for _, fs := range []*fakeSentinel{fs1, fs2} {
		queuePsubscribeAcks(fs)
		for range 5 {
			// false => CKQUORUM answers -NOQUORUM; the SENTINEL MASTER
			// reply still carries flags=master,o_down from a reached pod.
			queuePollRepliesWithFlags(fs, "master,o_down", false)
		}
	}

	opts := Options{
		PollInterval:       50 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	}
	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
			{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
		},
		opts,
		k8sevents.NewFakeRecorder(64),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); o.stop() }()
	o.start(ctx)

	snap := waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		_, ok := p.ODownPull["vk0-sentinel-0"]
		return ok
	})
	// The stamp must land on the very snapshot that reports Lost quorum:
	// the pull-side o_down truth is honored despite -NOQUORUM.
	if snap.QuorumOK {
		t.Errorf("expected QuorumOK=false under -NOQUORUM, got true")
	}
	if _, ok := snap.ODownPull["vk0-sentinel-1"]; !ok {
		t.Errorf("both reached sentinels' o_down must be stamped despite NOQUORUM; got %v", snap.ODownPull)
	}
}

// TestObserver_DegradedPublishCarriesReconciledODownPull pins the
// degraded publish path (reachable < threshold). With ONE reachable
// sentinel reporting flags=o_down and ONE unreachable (reachable=1 <
// threshold=2), pollOnce takes the early QuorumStatusUnknown return —
// which still copies the reconciled o.odownPull. The published snapshot
// must be Unknown AND carry the reachable pod's stamp. This is the added
// conservatism the pull-side veto buys: a single reachable sentinel's
// o_down now vetoes ghost-reap under sub-quorum, bounded by the
// failover-timeout age-out. Reconciling BELOW the degraded return (a
// refactor mutant) would publish an un-reconciled map and fail here.
func TestObserver_DegradedPublishCarriesReconciledODownPull(t *testing.T) {
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	t.Cleanup(func() { fs1.Stop(); fs2.Stop() })

	// fs1 reachable, reporting o_down. fs2 gets only psubscribe acks and
	// NO poll replies, so every pull tick errors on GET-MASTER —
	// unreachable for the aggregate (reachable=1 < threshold=2).
	queuePsubscribeAcks(fs1)
	queuePsubscribeAcks(fs2)
	for range 5 {
		queuePollRepliesWithFlags(fs1, "master,o_down", true)
	}

	opts := Options{
		PollInterval:       50 * time.Millisecond,
		PubsubReadDeadline: 30 * time.Second,
		PingTimeout:        time.Second,
	}
	o := newObserver(
		types.NamespacedName{Namespace: "ns", Name: "vk0"},
		"vk0", "",
		[]Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
			{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
		},
		opts,
		k8sevents.NewFakeRecorder(64),
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); o.stop() }()
	o.start(ctx)

	snap := waitForSnapshot(t, o, func(p ObservedPrimary) bool {
		_, ok := p.ODownPull["vk0-sentinel-0"]
		return p.Quorum == QuorumStatusUnknown && ok
	})
	if snap.Quorum != QuorumStatusUnknown {
		t.Errorf("expected Quorum=Unknown under sub-quorum, got %v", snap.Quorum)
	}
	// The unreachable sentinel never contributes a stamp (untouched).
	if _, ok := snap.ODownPull["vk0-sentinel-1"]; ok {
		t.Errorf("unreachable sentinel must not be stamped; got %v", snap.ODownPull)
	}
}
