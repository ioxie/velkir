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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

// TestRunInitialReset_SkipsWhenAllPeerListsNonEmpty: when every
// reachable sentinel reports a non-empty peer-list, the cluster is
// participating in gossip and the startup safety-net must NOT
// disturb it. Wiping a healthy peer-list (rc.10 / rc.11 behavior)
// was the root cause of the wedge — silence is the success signal.
func TestRunInitialReset_SkipsWhenAllPeerListsNonEmpty(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	// Peer-list shows one peer → non-empty → healthy → skip.
	queueSentinelsReply(fs, PeerInfo{Name: "s2", IP: "10.0.0.12", Port: 26379, RunID: "abc"})

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		Password:   "",
		MasterIP:   "10.0.0.5",
		Port:       6379,
		Quorum:     2,
	}})

	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Errorf("REMOVE should NOT fire on a healthy peer-list; got command: %q", line)
		}
		if strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("MONITOR should NOT fire on a healthy peer-list; got command: %q", line)
		}
	}

	select {
	case ev := <-rec.Events:
		t.Errorf("unexpected event on healthy-peer-list path: %s", ev)
	default:
	}
}

// TestRunInitialReset_OutcomeReportsResetCRs verifies the return value the
// controller audits on: a CR that actually fires REMOVE+MONITOR produces
// one outcome listing the stranded targets, while a healthy (no-op) CR
// produces none — so a bare call-site audit can't over-report cold_start
// resets that never hit the wire.
func TestRunInitialReset_OutcomeReportsResetCRs(t *testing.T) {
	fsStranded := newFakeSentinel(t)
	fsSurvivor := newFakeSentinel(t)
	defer fsStranded.Stop()
	defer fsSurvivor.Stop()
	queueSentinelsReply(fsStranded /* stranded: no peers */)
	queueSentinelsReply(fsSurvivor, PeerInfo{Name: "s-other", IP: "10.0.0.12", Port: 26379, RunID: "abc"})
	fsStranded.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fsStranded.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	m := NewManager(k8sevents.NewFakeRecorder(32), Options{})
	outcomes := m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         cr,
		MasterName: "vk0",
		Endpoints: []Endpoint{
			{Name: "vk0-sentinel-0", Addr: fsStranded.Addr()},
			{Name: "vk0-sentinel-1", Addr: fsSurvivor.Addr()},
		},
		MasterIP: "10.0.0.5",
		Port:     6379,
		Quorum:   2,
	}})
	if len(outcomes) != 1 {
		t.Fatalf("expected 1 outcome for the CR that reset, got %d: %+v", len(outcomes), outcomes)
	}
	if outcomes[0].CR != cr {
		t.Errorf("outcome CR = %v, want %v", outcomes[0].CR, cr)
	}
	if len(outcomes[0].Targets) != 1 {
		t.Errorf("expected 1 stranded target in outcome, got %v", outcomes[0].Targets)
	}
}

func TestRunInitialReset_OutcomeEmptyOnHealthyCluster(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs, PeerInfo{Name: "s2", IP: "10.0.0.12", Port: 26379, RunID: "abc"})

	m := NewManager(k8sevents.NewFakeRecorder(8), Options{})
	outcomes := m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		MasterIP:   "10.0.0.5",
		Port:       6379,
		Quorum:     2,
	}})
	if len(outcomes) != 0 {
		t.Errorf("healthy cluster must produce no reset outcome; got %+v", outcomes)
	}
}

// TestRunInitialReset_FiresOnStrandedSentinel: when one sentinel
// reports an empty peer-list (rebuilt pod with no preserved peer
// state), it must be REMOVE + MONITORed so it can rejoin the gossip
// ring. The surviving sentinel — which has a non-empty peer-list —
// must be left alone.
//
// REMOVE (not RESET) is the load-bearing primitive: plain RESET
// keeps the master entry at its prior (possibly stale) IP, so a
// follow-up MONITOR returns "-ERR Duplicated master name" and
// the rebuilt sentinel stays pointed at the stale master forever.
// REMOVE clears the entry entirely so MONITOR registers the new
// master IP cleanly.
func TestRunInitialReset_FiresOnStrandedSentinel(t *testing.T) {
	fsStranded := newFakeSentinel(t) // rebuilt pod, empty peer-list
	fsSurvivor := newFakeSentinel(t) // original, peer-list intact
	defer fsStranded.Stop()
	defer fsSurvivor.Stop()

	// Stranded sentinel: empty SENTINELS reply.
	queueSentinelsReply(fsStranded /* no peers */)
	// Survivor sentinel: one peer in its view.
	queueSentinelsReply(fsSurvivor, PeerInfo{Name: "s-other", IP: "10.0.0.12", Port: 26379, RunID: "abc"})
	// Stranded receives REMOVE + MONITOR.
	fsStranded.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fsStranded.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints: []Endpoint{
			{Name: "vk0-sentinel-0", Addr: fsStranded.Addr()},
			{Name: "vk0-sentinel-1", Addr: fsSurvivor.Addr()},
		},
		Password: "",
		MasterIP: "10.0.0.5",
		Port:     6379,
		Quorum:   2,
	}})

	// Survivor must NOT receive REMOVE (the load-bearing fix).
	for _, line := range fsSurvivor.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Errorf("survivor sentinel must NOT receive REMOVE (would wipe healthy peer-list, re-create #456 wedge); sent: %q", line)
		}
	}
	// Stranded must receive REMOVE + MONITOR carrying operator's
	// MasterIP, and REMOVE must precede MONITOR — MONITOR on a
	// not-yet-removed master returns "-ERR Duplicated master name"
	// (the rc.10/rc.11/rc.12 wedge). Order verification is the
	// load-bearing assertion here, not just presence.
	var removeIdx, monitorIdx = -1, -1
	for i, line := range fsStranded.Sent() {
		if removeIdx < 0 && strings.HasPrefix(line, "SENTINEL REMOVE") {
			removeIdx = i
		}
		if monitorIdx < 0 && strings.Contains(line, "SENTINEL MONITOR vk0 10.0.0.5 6379 2") {
			monitorIdx = i
		}
	}
	if removeIdx < 0 {
		t.Errorf("stranded sentinel must receive REMOVE; fsStranded sent: %v", fsStranded.Sent())
	}
	if monitorIdx < 0 {
		t.Errorf("stranded sentinel must receive MONITOR carrying operator MasterIP; fsStranded sent: %v", fsStranded.Sent())
	}
	if removeIdx >= 0 && monitorIdx >= 0 && removeIdx > monitorIdx {
		t.Errorf("REMOVE (idx=%d) must precede MONITOR (idx=%d); MONITOR on a still-registered master returns -ERR Duplicated master name; sent: %v",
			removeIdx, monitorIdx, fsStranded.Sent())
	}
}

// TestRunInitialReset_NeverSendsSetKnownSentinel is a regression
// guard: the prior fix attempt (rc.11) sent SENTINEL SET <name>
// known-sentinel <ip> <port> <runid> after RESET, which is not a
// valid runtime option in Valkey or Redis (sentinelSetCommand
// rejects unknown options). The rebuilt sentinel now relies on
// Pub/Sub gossip via __sentinel__:hello to rejoin its peers —
// no manual peer-registration command should ever hit the wire.
func TestRunInitialReset_NeverSendsSetKnownSentinel(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs /* empty — triggers recovery path */)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	// Pair the stranded sentinel with a reachable survivor so a
	// quorum of sentinels is reachable and the recovery path runs.
	fsSurvivor := newFakeSentinel(t)
	defer fsSurvivor.Stop()
	queueSentinelsReply(fsSurvivor, PeerInfo{Name: "p", IP: "10.0.0.99", Port: 26379, RunID: "p1"})

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints: []Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs.Addr()},
			{Name: "vk0-sentinel-1", Addr: fsSurvivor.Addr()},
		},
		Password: "",
		MasterIP: "10.0.0.5",
		Port:     6379,
		Quorum:   2,
	}})

	for _, line := range fs.Sent() {
		if strings.Contains(line, "known-sentinel") {
			t.Errorf("SENTINEL SET known-sentinel must never hit the wire (not a valid runtime command); sent: %q", line)
		}
	}
}

// TestRunInitialReset_FiresOnAllStranded: when every reachable
// sentinel is stranded (whole-STS-recreate during operator
// downtime), each one is REMOVE + MONITORed. The empty-peer-list
// selectivity prevents the rc.11 cascade (the cascade was caused
// by RESETting healthy sentinels with intact peer-lists, not by
// touching empty ones). After REMOVE + MONITOR, all sentinels
// subscribe to __sentinel__:hello on the (now-known) master and
// gossip rebuilds the peer-list within ~2s.
func TestRunInitialReset_FiresOnAllStranded(t *testing.T) {
	fs1 := newFakeSentinel(t)
	fs2 := newFakeSentinel(t)
	defer fs1.Stop()
	defer fs2.Stop()
	queueSentinelsReply(fs1 /* empty */)
	queueSentinelsReply(fs2 /* empty */)
	fs1.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs1.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	fs2.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs2.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(32)
	m := NewManager(rec, Options{})
	m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints: []Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs1.Addr()},
			{Name: "vk0-sentinel-1", Addr: fs2.Addr()},
		},
		Password: "",
		MasterIP: "10.0.0.5",
		Port:     6379,
		Quorum:   2,
	}})

	var removeCount, monitorCount int
	for _, line := range append(fs1.Sent(), fs2.Sent()...) {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			removeCount++
		}
		if strings.Contains(line, "SENTINEL MONITOR vk0 10.0.0.5 6379 2") {
			monitorCount++
		}
	}
	if removeCount != 2 {
		t.Errorf("expected REMOVE on both stranded sentinels (count=2), got %d", removeCount)
	}
	if monitorCount != 2 {
		t.Errorf("expected MONITOR on both stranded sentinels (count=2), got %d", monitorCount)
	}
}

// TestRunInitialReset_SkipsUnreachableSentinel: a sentinel pod
// unreachable during the safety-net pass cannot be classified.
// Skipping it (vs. blind-firing recovery) prevents the rc.10
// cascade where unreachability was treated as "needs RESET" and
// wiped every survivor's peer-list.
func TestRunInitialReset_SkipsUnreachableSentinel(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs, PeerInfo{Name: "p", IP: "10.0.0.12", Port: 26379, RunID: "p1"})

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints: []Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs.Addr()},
			{Name: "vk0-sentinel-dead", Addr: "127.0.0.1:1"},
		},
		Password: "",
		MasterIP: "10.0.0.5",
		Port:     6379,
		Quorum:   2,
	}})

	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Errorf("REMOVE must NOT fire just because a peer was unreachable; sent: %q", line)
		}
	}
}

// TestRunInitialReset_SkipsWhenMasterIPUnknown: the operator's
// startup safety-net cannot fire recovery without a known-good
// master IP — REMOVE wipes the master entry, and without a
// follow-up MONITOR pointing at a real master, the sentinels are
// left without a master to monitor. Recovery is deferred to the
// per-reconcile detector once observedMasterIP resolves.
func TestRunInitialReset_SkipsWhenMasterIPUnknown(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	queueSentinelsReply(fs /* empty */)
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		Password:   "",
		MasterIP:   "", // operator can't determine — must skip
		Port:       6379,
		Quorum:     2,
	}})

	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Errorf("REMOVE MUST NOT fire when MasterIP is empty; sent: %q", line)
		}
		if strings.HasPrefix(line, "SENTINEL SENTINELS") {
			t.Errorf("Probe MUST NOT fire when MasterIP is empty — skip happens before any wire I/O; sent: %q", line)
		}
	}
}

// TestRunInitialReset_SkipsReachableMinority pins the fix on the
// startup safety-net path. With a 3-sentinel quorum where 2 are
// unreachable and the lone reachable one is stranded, recovery must
// NOT fire: a reachable minority pointed at the operator's
// (possibly stale) MasterIP while the unreachable majority may hold a
// higher config-epoch is a split-brain contributor. Before the guard
// this fired REMOVE + MONITOR on the single reachable sentinel.
func TestRunInitialReset_SkipsReachableMinority(t *testing.T) {
	fs := newFakeSentinel(t) // reachable + stranded (empty peer-list)
	defer fs.Stop()
	queueSentinelsReply(fs /* empty — stranded */)
	// Queue REMOVE/MONITOR so a regression fails loudly on the wire
	// rather than blocking on an unscripted reply.
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	rec := k8sevents.NewFakeRecorder(8)
	m := NewManager(rec, Options{})
	m.RunInitialReset(context.Background(), []InitialResetTarget{{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		Endpoints: []Endpoint{
			{Name: "vk0-sentinel-0", Addr: fs.Addr()},
			{Name: "vk0-sentinel-1", Addr: "127.0.0.1:1"}, // unreachable
			{Name: "vk0-sentinel-2", Addr: "127.0.0.1:1"}, // unreachable
		},
		Password: "",
		MasterIP: "10.0.0.5",
		Port:     6379,
		Quorum:   2,
	}})

	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("recovery must NOT fire on a reachable minority (#477 split-brain guard); sent: %q", line)
		}
	}
}
