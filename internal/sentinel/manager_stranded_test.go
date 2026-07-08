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
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

// TestRecoverStrandedSentinels_SequenceOrderAndTuning verifies the
// load-bearing fix: REMOVE precedes MONITOR (MONITOR on a
// still-registered master returns -ERR Duplicated master name) and
// SetMasterTuningAll fires AFTER MONITOR (MONITOR's default-population
// erases the operator's tuning).
func TestRecoverStrandedSentinels_SequenceOrderAndTuning(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())
	queueSentinelsReply(fs /* empty — stranded */)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	// Tuning issues three SETs; queue +OK for each.
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   masterHost,
			Port:       masterPort,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000, FailoverTimeout: 60000, ParallelSyncs: 1},
			Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
			Password:   "",
		},
		false)

	if len(out.Stranded) != 1 {
		t.Fatalf("expected 1 stranded sentinel, got %v", out.Stranded)
	}
	if !out.Probed {
		t.Errorf("a pass that ran classification must set Probed=true")
	}

	// Walk the wire log: REMOVE must precede MONITOR, MONITOR must
	// precede the first SET (tuning). Ordering is load-bearing.
	var removeIdx, monitorIdx, firstSetIdx = -1, -1, -1
	for i, line := range fs.Sent() {
		if removeIdx < 0 && strings.HasPrefix(line, "SENTINEL REMOVE") {
			removeIdx = i
		}
		if monitorIdx < 0 && strings.HasPrefix(line, "SENTINEL MONITOR") {
			monitorIdx = i
		}
		if firstSetIdx < 0 && strings.HasPrefix(line, "SENTINEL SET") {
			firstSetIdx = i
		}
	}
	if removeIdx < 0 || monitorIdx < 0 || firstSetIdx < 0 {
		t.Fatalf("expected REMOVE, MONITOR, SET; sent: %v", fs.Sent())
	}
	if removeIdx >= monitorIdx || monitorIdx >= firstSetIdx {
		t.Errorf("expected REMOVE(%d) < MONITOR(%d) < SET(%d); sent: %v",
			removeIdx, monitorIdx, firstSetIdx, fs.Sent())
	}
}

// TestRecoverStrandedSentinels_TuningFiltersOnRemoveSucceeded verifies
// SetMasterTuningAll only targets sentinels whose REMOVE succeeded —
// running tuning against a sentinel where REMOVE failed would race
// the next reconcile's retry and could leak SET commands against
// stale master entries.
func TestRecoverStrandedSentinels_TuningFiltersOnRemoveSucceeded(t *testing.T) {
	fsOK := newFakeSentinel(t)   // REMOVE succeeds → tuning expected
	fsFail := newFakeSentinel(t) // REMOVE fails → tuning must NOT fire
	defer fsOK.Stop()
	defer fsFail.Stop()
	queueSentinelsReply(fsOK /* empty — stranded */)
	queueSentinelsReply(fsFail /* empty — stranded */)
	fsOK.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fsOK.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	fsOK.QueueReply("SENTINEL SET", "+OK\r\n")
	fsFail.QueueReply("SENTINEL REMOVE", "-ERR wedged\r\n")
	// No subsequent replies queued for fsFail — the test fails if
	// MONITOR or SET hits it.
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   masterHost,
			Port:       masterPort,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-ok", Addr: fsOK.Addr()},
				{Name: "vk0-sentinel-fail", Addr: fsFail.Addr()},
			},
			Password: "",
		},
		false)

	for _, line := range fsFail.Sent() {
		if strings.HasPrefix(line, "SENTINEL MONITOR") || strings.HasPrefix(line, "SENTINEL SET") {
			t.Errorf("REMOVE-failed sentinel must NOT receive follow-up commands; sent: %q", line)
		}
	}
	var sawSet bool
	for _, line := range fsOK.Sent() {
		if strings.HasPrefix(line, "SENTINEL SET vk0 down-after-milliseconds 3000") {
			sawSet = true
		}
	}
	if !sawSet {
		t.Errorf("REMOVE-succeeded sentinel must receive tuning SET; sent: %v", fsOK.Sent())
	}
}

// TestRecoverStrandedSentinels_PredicateSnapshotNoSelfDeadlock pins the
// fix for a self-deadlock: RecoverStrandedSentinels holds the
// manager mutex (mu) across the deferral predicate, while the
// controller's real predicate (IsFailoverInFlight ->
// observedPrimaryAddrEpoch) reads an observer Snapshot. When the observer
// registry shared that same mutex, Snapshot re-locked it and the
// reconcile worker hung forever. The registry now has its own lock
// (obsMu), distinct from the predicate lock (mu), so a predicate that
// takes a Snapshot completes. The timeout guard turns a regression into a
// failure instead of a hung run.
func TestRecoverStrandedSentinels_PredicateSnapshotNoSelfDeadlock(t *testing.T) {
	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	// Predicate that re-enters the manager via Snapshot — the exact shape
	// of the controller's IsFailoverInFlight check. Returns true (defer)
	// so RecoverStrandedSentinels exercises the predicate under mu with
	// Snapshot taking obsMu in between.
	m.SetDeferralPredicate(func(c observerKey) bool {
		_ = m.Snapshot(c)
		return true
	})

	// Dummy endpoint: the predicate defers before any dial, so this
	// address is never contacted.
	target := InitialResetTarget{
		CR:         cr,
		MasterName: "mymaster",
		MasterIP:   "10.0.0.5",
		Port:       6379,
		Quorum:     2,
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: "10.255.255.1:26379"}},
	}

	done := make(chan struct{})
	go func() {
		out := m.RecoverStrandedSentinels(context.Background(), target, false)
		if len(out.Stranded) != 0 {
			t.Errorf("predicate must short-circuit before wire I/O; got Stranded=%v", out.Stranded)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("RecoverStrandedSentinels deadlocked: the deferral predicate re-entered the manager mutex via Snapshot")
	}
}

// TestRecoverStrandedSentinels_HonoursDeferralPredicate verifies the
// per-reconcile wedge-recovery path is gated by the deferral predicate —
// REMOVE + MONITOR + tuning mutate sentinel config state and must not
// race a failover's config-epoch propagation.
func TestRecoverStrandedSentinels_HonoursDeferralPredicate(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs /* empty — would be stranded */)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	m.SetDeferralPredicate(func(_ observerKey) bool { return true })

	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   "10.0.0.5",
			Port:       6379,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
			Password:   "",
		},
		false)

	if len(out.Stranded) != 0 {
		t.Errorf("deferral predicate must short-circuit before any wire I/O; got Stranded=%v", out.Stranded)
	}
	if out.Probed {
		t.Errorf("the deferral predicate is a PRE-classification early-return; Probed must be false")
	}
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL") {
			t.Errorf("deferral predicate must prevent SENTINEL commands; sent: %q", line)
		}
	}
}

// TestRecoverStrandedSentinels_SkipsReachableMinority pins the
// fix on the per-reconcile wedge-recovery path. With a 3-sentinel
// quorum where 2 are unreachable and the lone reachable one is
// stranded, recovery must NOT fire: acting on a reachable minority
// would point that sentinel at our (possibly stale) masterIP while
// the unreachable majority may hold a higher config-epoch view that
// resurfaces on rejoin — a split-brain contributor. Before the guard
// this fired REMOVE + MONITOR on the single reachable sentinel.
func TestRecoverStrandedSentinels_SkipsReachableMinority(t *testing.T) {
	fs := newFakeSentinel(t) // reachable + stranded (empty peer-list)
	defer fs.Stop()
	queueSentinelsReply(fs /* empty — stranded */)
	// Queue REMOVE/MONITOR so a regression fails loudly on the wire
	// rather than blocking on an unscripted reply.
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   "10.0.0.5",
			Port:       6379,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-0", Addr: fs.Addr()},
				{Name: "vk0-sentinel-1", Addr: "127.0.0.1:1"}, // unreachable
				{Name: "vk0-sentinel-2", Addr: "127.0.0.1:1"}, // unreachable
			},
			Password: "",
		},
		false)

	if len(out.Stranded) != 0 {
		t.Errorf("minority-reachable pass must report no recovery; got Stranded=%v", out.Stranded)
	}
	if !out.Probed {
		t.Errorf("the minority guard is a POST-classification defer; Probed must be true to debounce the probe")
	}
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("recovery must NOT fire on a reachable minority (split-brain guard); sent: %q", line)
		}
	}
}

// splitHostPort splits a fake listener address into the (host, port)
// pair RecoverStrandedSentinels takes for the master.
func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %q: %v", portStr, err)
	}
	return host, port
}

// TestRecoverStrandedSentinels_DefersWhenMasterUnreachable pins the
// surgery gate: REMOVE + MONITOR re-registers a stranded sentinel
// with NO replica or peer knowledge — it can only relearn them from
// the master. Registering against a dead master strands the sentinel
// permanently, so the pass must defer when the resolved master does
// not answer PING.
func TestRecoverStrandedSentinels_DefersWhenMasterUnreachable(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs /* empty — stranded */)
	// Queue REMOVE/MONITOR so a regression fails loudly on the wire.
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   "127.0.0.1",
			Port:       1, // closed port — master dead
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
			Password:   "",
		},
		false)

	if len(out.ResetResults) != 0 || len(out.MonitorResults) != 0 {
		t.Errorf("dead-master pass must not fire surgery; got %+v", out)
	}
	if !out.Probed {
		t.Errorf("the master-PING gate is a POST-classification defer; Probed must be true to debounce the classification probe")
	}
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("REMOVE/MONITOR must not fire against a dead master; sent: %q", line)
		}
	}
}

// TestRecoverStrandedSentinels_DefersOnSurvivorFailoverInProgress pins
// the second surgery gate: a healthy survivor mid-election
// (flags contain failover_in_progress) vetoes the pass — wiping and
// re-pointing stranded sentinels during an election races the
// config-epoch the election is propagating.
func TestRecoverStrandedSentinels_DefersOnSurvivorFailoverInProgress(t *testing.T) {
	stranded := newFakeSentinel(t)
	defer stranded.Stop()
	queueSentinelsReply(stranded /* empty — stranded */)
	stranded.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	stranded.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	healthy := newFakeSentinel(t)
	defer healthy.Stop()
	queueSentinelsReply(healthy, PeerInfo{Name: "peer-1", IP: "10.0.0.9", Port: 26379, RunID: "r1"})
	healthy.QueueReply("SENTINEL MASTER", masterReplyWithFlags("master,failover_in_progress"))

	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   masterHost,
			Port:       masterPort,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-stranded", Addr: stranded.Addr()},
				{Name: "vk0-sentinel-healthy", Addr: healthy.Addr()},
			},
			Password: "",
		},
		false)

	if len(out.ResetResults) != 0 || len(out.MonitorResults) != 0 {
		t.Errorf("election-in-progress pass must not fire surgery; got %+v", out)
	}
	if !out.Probed {
		t.Errorf("the survivor-failover-in-progress gate is a POST-classification defer; Probed must be true to debounce the classification probe")
	}
	for _, line := range stranded.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("REMOVE/MONITOR must not race a live election; sent: %q", line)
		}
	}
}

// TestRecoverStrandedSentinels_BypassQuorumDeferral pins the
// deadlock fix: with the deferral predicate active (the quorum-loss
// suppression gate) and bypassQuorumDeferral=true, the pass proceeds —
// the surgery gates (PING, election check) still apply, but sustained
// quorum loss alone no longer blocks the only path that can repair the
// electorate.
func TestRecoverStrandedSentinels_BypassQuorumDeferral(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs /* empty — stranded */)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")

	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	m.SetDeferralPredicate(func(_ observerKey) bool { return true })

	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   masterHost,
			Port:       masterPort,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
			Password:   "",
		},
		true)

	if len(out.Stranded) != 1 || len(out.MonitorResults) != 1 {
		t.Fatalf("bypass pass must fire REMOVE + MONITOR; got %+v", out)
	}
	var sawMonitor bool
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL MONITOR") {
			sawMonitor = true
		}
	}
	if !sawMonitor {
		t.Errorf("bypass pass must reach MONITOR; sent: %v", fs.Sent())
	}
}

// masterReplyWithFlags builds the flat key/value array reply for
// `SENTINEL MASTER <name>` carrying the supplied flags value.
func masterReplyWithFlags(flags string) string {
	var b strings.Builder
	b.WriteString("*4\r\n")
	writeBulk(&b, "name")
	writeBulk(&b, "vk0")
	writeBulk(&b, "flags")
	writeBulk(&b, flags)
	return b.String()
}

// TestRecoverStrandedSentinels_NoSuchMasterCountsAsStranded pins the
// self-heal: a sentinel that ANSWERS SENTINEL SENTINELS with
// "-ERR No such master with that name" (the signature of a recovery
// pass interrupted between REMOVE and MONITOR) is reachable AND
// stranded — not unreachable. Counting it as unreachable would drop
// the reachable total below quorum and wedge the only repair path. The
// pass must fire REMOVE + MONITOR against it.
func TestRecoverStrandedSentinels_NoSuchMasterCountsAsStranded(t *testing.T) {
	mk := func() *fakeSentinel {
		fs := newFakeSentinel(t)
		fs.QueueReply("SENTINEL SENTINELS", "-ERR No such master with that name\r\n")
		fs.QueueReply("SENTINEL REMOVE", "-ERR No such master with that name\r\n") // removeOne treats as success
		fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")
		fs.QueueReply("SENTINEL SET", "+OK\r\n")
		fs.QueueReply("SENTINEL SET", "+OK\r\n")
		fs.QueueReply("SENTINEL SET", "+OK\r\n")
		return fs
	}
	s0 := mk()
	defer s0.Stop()
	s1 := mk()
	defer s1.Stop()

	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   masterHost,
			Port:       masterPort,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-0", Addr: s0.Addr()},
				{Name: "vk0-sentinel-1", Addr: s1.Addr()},
			},
			Password: "",
		},
		false)

	if len(out.Stranded) != 2 {
		t.Fatalf("both no-such-master sentinels must classify as stranded; got %v", out.Stranded)
	}
	if len(out.MonitorResults) != 2 {
		t.Fatalf("recovery must MONITOR both; got %d monitor results", len(out.MonitorResults))
	}
	for _, fs := range []*fakeSentinel{s0, s1} {
		var sawMonitor bool
		for _, line := range fs.Sent() {
			if strings.HasPrefix(line, "SENTINEL MONITOR") {
				sawMonitor = true
			}
		}
		if !sawMonitor {
			t.Errorf("each no-master sentinel must be re-MONITORed; sent: %v", fs.Sent())
		}
	}
}

// TestRecoverStrandedSentinels_SkipSetExcludesFromWipe pins the manager
// half of the per-address pacing: with two empty-peer stranded sentinels
// and one listed in SkipStrandedAddrs, only the non-skipped one is
// REMOVE + MONITOR'd. The skipped one is reported in SkippedStranded,
// absent from EmptyPeerStranded/Stranded, and received NO wire surgery,
// while the pass is Probed=true, Healthy=false.
func TestRecoverStrandedSentinels_SkipSetExcludesFromWipe(t *testing.T) {
	fsWiped := newFakeSentinel(t)
	defer fsWiped.Stop()
	queueSentinelsReply(fsWiped /* empty — stranded */)
	fsWiped.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fsWiped.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	fsWiped.QueueReply("SENTINEL SET", "+OK\r\n")

	fsSkipped := newFakeSentinel(t)
	defer fsSkipped.Stop()
	queueSentinelsReply(fsSkipped /* empty — stranded, but paced */)
	// No REMOVE/MONITOR queued: the wire assertion fails loudly if surgery
	// hits the skipped sentinel.

	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   masterHost,
			Port:       masterPort,
			Quorum:     2,
			Tuning:     MasterTuning{DownAfterMilliseconds: 3000},
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-wiped", Addr: fsWiped.Addr()},
				{Name: "vk0-sentinel-skipped", Addr: fsSkipped.Addr()},
			},
			SkipStrandedAddrs: map[string]struct{}{fsSkipped.Addr(): {}},
			Password:          "",
		},
		false)

	if !out.Probed {
		t.Errorf("a classified pass must set Probed=true")
	}
	if out.Healthy {
		t.Errorf("a pass with a wiped strand must not be Healthy")
	}
	if len(out.SkippedStranded) != 1 || out.SkippedStranded[0] != "vk0-sentinel-skipped" {
		t.Fatalf("the skipped sentinel must be in SkippedStranded, got %v", out.SkippedStranded)
	}
	if len(out.Stranded) != 1 || out.Stranded[0] != "vk0-sentinel-wiped" {
		t.Fatalf("only the non-skipped sentinel may be in Stranded, got %v", out.Stranded)
	}
	for _, n := range out.EmptyPeerStranded {
		if n == "vk0-sentinel-skipped" {
			t.Errorf("the skipped sentinel must be absent from EmptyPeerStranded, got %v", out.EmptyPeerStranded)
		}
	}
	for _, line := range fsSkipped.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("a skipped sentinel must never be REMOVE/MONITOR'd; sent: %q", line)
		}
	}
	var sawRemove, sawMonitor bool
	for _, line := range fsWiped.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			sawRemove = true
		}
		if strings.HasPrefix(line, "SENTINEL MONITOR") {
			sawMonitor = true
		}
	}
	if !sawRemove || !sawMonitor {
		t.Errorf("the non-skipped sentinel must be REMOVE'd + MONITOR'd; sent: %v", fsWiped.Sent())
	}
}

// TestRecoverStrandedSentinels_AllSkippedNotHealthy pins the skip-only
// short-circuit: with every empty-peer sentinel paced, the pass returns
// SkippedStranded=both, empty EmptyPeerStranded/Stranded, Healthy=false,
// Probed=true, fires no surgery, and needs no reachable master (the
// healthy-return short-circuits before the PING fan-out).
func TestRecoverStrandedSentinels_AllSkippedNotHealthy(t *testing.T) {
	fs0 := newFakeSentinel(t)
	defer fs0.Stop()
	queueSentinelsReply(fs0 /* empty — stranded, paced */)
	fs1 := newFakeSentinel(t)
	defer fs1.Stop()
	queueSentinelsReply(fs1 /* empty — stranded, paced */)
	// No fake master, no REMOVE/MONITOR queued.

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(),
		InitialResetTarget{
			CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
			MasterName: "vk0",
			MasterIP:   "10.0.0.5",
			Port:       6379,
			Quorum:     2,
			Endpoints: []Endpoint{
				{Name: "vk0-sentinel-0", Addr: fs0.Addr()},
				{Name: "vk0-sentinel-1", Addr: fs1.Addr()},
			},
			SkipStrandedAddrs: map[string]struct{}{fs0.Addr(): {}, fs1.Addr(): {}},
			Password:          "",
		},
		false)

	if !out.Probed {
		t.Errorf("a classified pass must set Probed=true")
	}
	if out.Healthy {
		t.Errorf("an all-skipped pass must NOT be Healthy (the wedge persists)")
	}
	if len(out.SkippedStranded) != 2 {
		t.Fatalf("both stranded sentinels must be in SkippedStranded, got %v", out.SkippedStranded)
	}
	if len(out.EmptyPeerStranded) != 0 || len(out.Stranded) != 0 {
		t.Errorf("nothing wiped: EmptyPeerStranded=%v Stranded=%v", out.EmptyPeerStranded, out.Stranded)
	}
	if len(out.ResetResults) != 0 || len(out.MonitorResults) != 0 {
		t.Errorf("an all-skipped pass must fire no surgery; got %+v", out)
	}
	for _, fs := range []*fakeSentinel{fs0, fs1} {
		for _, line := range fs.Sent() {
			if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
				t.Errorf("a skipped sentinel must never be REMOVE/MONITOR'd; sent: %q", line)
			}
		}
	}
}

// TestRecoverStrandedSentinels_SkippedCorpseMonitorNotRepointed is the 2a
// guard: a PACED empty-peer sentinel that also happens to monitor a dead
// master with an all-dead replica table (the re-point signature) must NOT
// be re-classified into the re-point class and wiped behind the skip's
// back. beingWiped is seeded from the FULL empty-peer class (wiped OR
// skipped), so selectRepointTargets excludes it. It stays in
// SkippedStranded only, never Repointed, and receives no wire surgery —
// even though a reachable master and an armed live set would let a
// re-point fire.
func TestRecoverStrandedSentinels_SkippedCorpseMonitorNotRepointed(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	// Empty peer-list → empty-peer stranded class.
	queueSentinelsReply(fs /* empty — stranded, paced */)
	// Its monitored master is a corpse (10.0.0.9, no live valkey pod), so
	// the re-point classifier would flag it were it not paced...
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME",
		"*2\r\n$8\r\n10.0.0.9\r\n$4\r\n6379\r\n")
	// ...and its replica table is all-dead (vacuously doomed).
	fs.QueueReply("SENTINEL REPLICAS", "*0\r\n")
	// Queue surgery replies so a REGRESSION (re-point behind the skip)
	// fires them and is caught by the wire assertion rather than blocking.
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	fs.QueueReply("SENTINEL SET", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   masterHost,
		Port:       masterPort,
		Quorum:     1,
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-corpse", Addr: fs.Addr()}},
		// Arm the re-point class (non-empty live set).
		LiveValkeyIPs: map[string]struct{}{masterHost: {}},
		// Pace this same empty-peer sentinel.
		SkipStrandedAddrs: map[string]struct{}{fs.Addr(): {}},
	}, false)

	if len(out.SkippedStranded) != 1 || out.SkippedStranded[0] != "vk0-sentinel-corpse" {
		t.Fatalf("the paced corpse-monitoring sentinel must be in SkippedStranded, got %v", out.SkippedStranded)
	}
	if len(out.Repointed) != 0 {
		t.Errorf("a paced empty-peer sentinel must NEVER be re-classified into Repointed, got %v", out.Repointed)
	}
	if len(out.Stranded) != 0 || len(out.EmptyPeerStranded) != 0 {
		t.Errorf("nothing wiped: Stranded=%v EmptyPeerStranded=%v", out.Stranded, out.EmptyPeerStranded)
	}
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("a paced sentinel must never be REMOVE/MONITOR'd behind the skip's back; sent: %q", line)
		}
	}
}
