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
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
)

// TestClassifyRepointTargets pins the dead-master re-point selection:
// only reachable sentinels whose monitored master addr matches no live
// valkey pod qualify; sentinels on the resolved master or on any other
// LIVE pod are left alone, and a nil live set disables the class.
func TestClassifyRepointTargets(t *testing.T) {
	eps := []Endpoint{
		{Name: "s0", Addr: "127.0.0.1:26379"},
		{Name: "s1", Addr: "127.0.0.2:26379"},
		{Name: "s2", Addr: "127.0.0.3:26379"},
	}
	live := map[string]struct{}{"10.0.0.1": {}, "10.0.0.2": {}}
	const masterIP = "10.0.0.1"

	cases := []struct {
		name  string
		probe []ProbeResult
		live  map[string]struct{}
		want  []string
	}{
		{
			name: "corpse-monitoring sentinel is selected",
			probe: []ProbeResult{
				{Name: "s0", Addr: "10.0.0.9:6379"},
				{Name: "s1", Addr: masterIP + ":6379"},
				{Name: "s2", Addr: "10.0.0.2:6379"},
			},
			live: live,
			// s0 monitors a corpse → selected. s1 is on the resolved
			// master → skip. s2 is on a DIFFERENT live pod (mid-failover
			// view) → skip; wiping it could race a legitimate election.
			want: []string{"s0"},
		},
		{
			name: "probe error and empty addr are skipped",
			probe: []ProbeResult{
				{Name: "s0", Err: context.DeadlineExceeded},
				{Name: "s1", Addr: ""},
				{Name: "s2", Addr: "10.0.0.9:6379"},
			},
			live: live,
			want: []string{"s2"},
		},
		{
			name: "all corpse-monitoring (the storm end-state)",
			probe: []ProbeResult{
				{Name: "s0", Addr: "10.0.0.9:6379"},
				{Name: "s1", Addr: "10.0.0.9:6379"},
				{Name: "s2", Addr: "10.0.0.9:6379"},
			},
			live: live,
			want: []string{"s0", "s1", "s2"},
		},
		{
			name: "nil live set disables the class",
			probe: []ProbeResult{
				{Name: "s0", Addr: "10.0.0.9:6379"},
			},
			live: nil,
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// allowStaleEpoch=false: only the DeadMaster (corpse) class
			// selects, and it must equal the pre-2b behaviour exactly.
			got := classifyRepointTargets(tc.probe, eps[:len(tc.probe)], tc.live, masterIP, false)
			if len(got.StaleEpoch) != 0 {
				t.Fatalf("StaleEpoch must be empty with allowStaleEpoch=false, got %v", got.StaleEpoch)
			}
			names := make([]string, 0, len(got.DeadMaster))
			for _, ep := range got.DeadMaster {
				names = append(names, ep.Name)
			}
			if len(names) != len(tc.want) {
				t.Fatalf("targets = %v, want %v", names, tc.want)
			}
			for i := range names {
				if names[i] != tc.want[i] {
					t.Fatalf("targets = %v, want %v", names, tc.want)
				}
			}
		})
	}

	t.Run("empty masterIP disables the class", func(t *testing.T) {
		got := classifyRepointTargets([]ProbeResult{{Name: "s0", Addr: "10.0.0.9:6379"}}, eps[:1], live, "", true)
		if len(got.DeadMaster) != 0 || len(got.StaleEpoch) != 0 {
			t.Fatalf("expected empty classification with empty masterIP, got %+v", got)
		}
	})
}

// Shared fixture identifiers for the stale-epoch classify tests. The
// operator's resolved master is seStaleMasterIP; seStaleDivergentAddr is
// a DIFFERENT live pod; seStaleCorpseAddr is a dead pod. Named once so
// goconst stays quiet and the guard intent reads clearly.
const (
	seStaleMasterIP      = "10.0.0.1"
	seStaleMasterAddr    = "10.0.0.1:6379"
	seStaleDivergentAddr = "10.0.0.2:6379"
	seStaleCorpseAddr    = "10.0.0.9:6379"
	seStaleFrontierIP    = "10.0.0.3"
	seStaleFrontierAddr  = "10.0.0.3:6379"
	seFlagsClean         = "master"
	seFlagsElection      = "master,o_down"
)

// staleEps are three sentinel endpoints; only their Names matter to
// classifyRepointTargets (it returns endpoints[i] for a selected probe).
var staleEps = []Endpoint{
	{Name: "s0", Addr: "127.0.0.1:26379"},
	{Name: "s1", Addr: "127.0.0.2:26379"},
	{Name: "s2", Addr: "127.0.0.3:26379"},
}

// staleLive is the live-pod set: the resolved master + the divergent pod.
var staleLive = map[string]struct{}{"10.0.0.1": {}, "10.0.0.2": {}}

// classifyStaleNames runs classifyRepointTargets with allowStaleEpoch=true
// and returns the selected StaleEpoch Names for terse assertions.
func classifyStaleNames(probe []ProbeResult) []string {
	c := classifyRepointTargets(probe, staleEps[:len(probe)], staleLive, seStaleMasterIP, true)
	stale := make([]string, 0, len(c.StaleEpoch))
	for _, ep := range c.StaleEpoch {
		stale = append(stale, ep.Name)
	}
	return stale
}

func eqNames(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestClassifyRepointTargets_StaleEpochSelectedWhenStrictlyBehindFrontier
// pins Guards A + B: the operator's masterIP view sits at the frontier
// (epoch 5) and a divergent live sentinel is strictly behind (epoch 3)
// with clean flags → it is the StaleEpoch target; DeadMaster is empty.
func TestClassifyRepointTargets_StaleEpochSelectedWhenStrictlyBehindFrontier(t *testing.T) {
	stale := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
	})
	if !eqNames(stale, []string{"s1"}) {
		t.Fatalf("StaleEpoch = %v, want [s1]", stale)
	}
}

// TestClassifyRepointTargets_StaleEpochSkippedWhenAtOrAheadOfFrontier
// pins the strict wrong-direction guard: a divergent sentinel at exactly
// agreeEpoch (may be ahead) is not selected, and one above it disarms via
// Guard A.
func TestClassifyRepointTargets_StaleEpochSkippedWhenAtOrAheadOfFrontier(t *testing.T) {
	atFrontier := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
	})
	if len(atFrontier) != 0 {
		t.Fatalf("a divergent sentinel at agreeEpoch must not be selected, got %v", atFrontier)
	}
	above := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 7, EpochOK: true, Flags: seFlagsClean},
	})
	if len(above) != 0 {
		t.Fatalf("a divergent sentinel above the frontier must not be selected, got %v", above)
	}
}

// TestClassifyRepointTargets_StaleEpochDisarmedWhenOperatorViewStale pins
// Guard A: if any epoch-eligible sentinel holds an epoch above the
// masterIP cohort, the operator's view is possibly stale → disarm.
func TestClassifyRepointTargets_StaleEpochDisarmedWhenOperatorViewStale(t *testing.T) {
	stale := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
	})
	if len(stale) != 0 {
		t.Fatalf("agreeEpoch(3) < frontier(5) must disarm the class, got %v", stale)
	}
}

// TestClassifyRepointTargets_StaleEpochDisarmedWhenThirdSentinelHoldsFrontier
// pins the wrong-direction clause of Guard A (agreeEpoch != frontierEpoch):
// a THIRD live sentinel holding a config-epoch (5) ABOVE the masterIP
// cohort's agreeEpoch (3) means the operator's masterIP view is stale —
// masterIP may already have been deposed — so the whole StaleEpoch class
// must disarm. The divergent target sitting strictly behind agreeEpoch
// (epoch 1) is NOT sufficient to re-point onto masterIP; the operator's
// own view must sit at the frontier. This is distinct from the
// operator-view-stale case above, which puts the above-cohort epoch ON the
// divergent target itself, where Guard B (r.Epoch >= agreeEpoch) already
// skips it — here the frontier lives on a third sentinel Guard B never
// reaches, so only the frontier clause can disarm.
func TestClassifyRepointTargets_StaleEpochDisarmedWhenThirdSentinelHoldsFrontier(t *testing.T) {
	live := map[string]struct{}{seStaleMasterIP: {}, "10.0.0.2": {}, seStaleFrontierIP: {}}
	c := classifyRepointTargets([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 1, EpochOK: true, Flags: seFlagsClean},
		{Name: "s2", Addr: seStaleFrontierAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
	}, staleEps, live, seStaleMasterIP, true)
	if len(c.StaleEpoch) != 0 {
		t.Fatalf("a third sentinel above the masterIP cohort epoch must disarm StaleEpoch, got %v", c.StaleEpoch)
	}
	if len(c.DeadMaster) != 0 {
		t.Fatalf("no corpse present, DeadMaster must stay empty, got %v", c.DeadMaster)
	}
}

// TestClassifyRepointTargets_StaleEpochVetoedByOwnElectionFlags pins
// self-election guard: a strictly-behind divergent sentinel mid-election on its own
// master is skipped (a straggler cannot be told from a legit advancer).
func TestClassifyRepointTargets_StaleEpochVetoedByOwnElectionFlags(t *testing.T) {
	stale := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsElection},
	})
	if len(stale) != 0 {
		t.Fatalf("a target mid-election must be self-election-skipped, got %v", stale)
	}
}

// TestClassifyRepointTargets_StaleEpochVetoedByDestinationCohortElection
// pins the dest-election guard: a masterIP-cohort sentinel reporting an election on its
// own master disarms the whole class (the quorum is vote-gathering to
// depose the destination — the wrong-direction case).
func TestClassifyRepointTargets_StaleEpochVetoedByDestinationCohortElection(t *testing.T) {
	stale := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsElection},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
	})
	if len(stale) != 0 {
		t.Fatalf("a destination-cohort election must disarm the class, got %v", stale)
	}
}

// TestClassifyRepointTargets_StaleEpochRequiresEpochOK pins Guard B's
// EpochOK requirement AND the Pre-A conservative fail-safe: a monitoring
// sentinel with an unprovable epoch disarms the whole class.
func TestClassifyRepointTargets_StaleEpochRequiresEpochOK(t *testing.T) {
	// (a) the behind sentinel itself has EpochOK=false — it is monitoring
	// (non-empty addr) so Pre-A disarms the class (it could be ahead).
	staleA := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: false, Flags: seFlagsClean},
	})
	if len(staleA) != 0 {
		t.Fatalf("a behind sentinel with EpochOK=false must not be selected, got %v", staleA)
	}
	// (b) an unrelated monitoring sentinel with EpochOK=false disarms the
	// whole class (conservative fail-safe).
	staleB := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
		{Name: "s2", Addr: "10.0.0.2:6380", Epoch: 0, EpochOK: false, Flags: seFlagsClean},
	})
	if len(staleB) != 0 {
		t.Fatalf("any monitoring sentinel with EpochOK=false must disarm the class, got %v", staleB)
	}
}

// TestClassifyRepointTargets_StaleEpochDisabledWhenAllowFalse pins the dest-election veto
// consumption + corpse-class independence: allowStaleEpoch=false leaves
// StaleEpoch empty while DeadMaster still selects.
func TestClassifyRepointTargets_StaleEpochDisabledWhenAllowFalse(t *testing.T) {
	probe := []ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
		{Name: "s2", Addr: seStaleCorpseAddr, Epoch: 4, EpochOK: true, Flags: seFlagsClean},
	}
	c := classifyRepointTargets(probe, staleEps, staleLive, seStaleMasterIP, false)
	if len(c.StaleEpoch) != 0 {
		t.Fatalf("allowStaleEpoch=false must leave StaleEpoch empty, got %v", c.StaleEpoch)
	}
	if len(c.DeadMaster) != 1 || c.DeadMaster[0].Name != "s2" {
		t.Fatalf("DeadMaster must still select the corpse s2, got %v", c.DeadMaster)
	}
}

// TestClassifyRepointTargets_StaleEpochDomainIgnoresEmptyAddrStragglers
// pins the epoch domain: a reachable empty-addr probe (EpochOK=false)
// neither participates in the frontier NOR trips the conservative disarm.
func TestClassifyRepointTargets_StaleEpochDomainIgnoresEmptyAddrStragglers(t *testing.T) {
	stale := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
		{Name: "s2", Addr: "", Epoch: 0, EpochOK: false},
	})
	if !eqNames(stale, []string{"s1"}) {
		t.Fatalf("an empty-addr straggler must not disarm the class; StaleEpoch = %v, want [s1]", stale)
	}
}

// TestClassifyRepointTargets_StaleEpochNoOpWhenAgreeEpochZero pins the
// documented safe no-op through operator-MONITOR-reset states: the
// masterIP cohort at epoch 0 while a divergent sentinel holds a higher
// pre-storm epoch → Guard A (frontier>agree) disarms.
func TestClassifyRepointTargets_StaleEpochNoOpWhenAgreeEpochZero(t *testing.T) {
	stale := classifyStaleNames([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 0, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
	})
	if len(stale) != 0 {
		t.Fatalf("agreeEpoch==0 must no-op the class, got %v", stale)
	}
}

// TestClassifyRepointTargets_StaleEpochDisarmedWhenCorpseHoldsFrontier pins
// the frontier clause of Guard A against a DEAD-host (corpse) probe: the
// masterIP cohort sits at epoch 3, a divergent live target is behind at
// epoch 2, but a corpse carries the HIGHEST epoch (5). The corpse feeds
// frontierEpoch (5) without joining the cohort's agreeEpoch (3), so
// agreeEpoch != frontierEpoch disarms the whole StaleEpoch class. The
// corpse still selects into DeadMaster.
func TestClassifyRepointTargets_StaleEpochDisarmedWhenCorpseHoldsFrontier(t *testing.T) {
	c := classifyRepointTargets([]ProbeResult{
		{Name: "s0", Addr: seStaleMasterAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 2, EpochOK: true, Flags: seFlagsClean},
		{Name: "s2", Addr: seStaleCorpseAddr, Epoch: 5, EpochOK: true, Flags: seFlagsClean},
	}, staleEps, staleLive, seStaleMasterIP, true)
	if len(c.StaleEpoch) != 0 {
		t.Fatalf("a corpse above the cohort frontier must disarm StaleEpoch via Guard A, got %v", c.StaleEpoch)
	}
	if len(c.DeadMaster) != 1 || c.DeadMaster[0].Name != "s2" {
		t.Fatalf("the corpse must still select into DeadMaster, got %v", c.DeadMaster)
	}
}

// TestClassifyRepointTargets_StaleEpochDisarmedWhenNoMasterIPCohort pins the
// !masterIPCohortExists clause: masterIP is non-empty but NO probe's Addr
// resolves to it (every sentinel monitors a different live pod). With no
// cohort at the frontier the operator cannot prove its own masterIP view is
// current, so the class disarms; both targets being live keeps DeadMaster
// empty too.
func TestClassifyRepointTargets_StaleEpochDisarmedWhenNoMasterIPCohort(t *testing.T) {
	c := classifyRepointTargets([]ProbeResult{
		{Name: "s0", Addr: seStaleDivergentAddr, Epoch: 3, EpochOK: true, Flags: seFlagsClean},
		{Name: "s1", Addr: seStaleDivergentAddr, Epoch: 2, EpochOK: true, Flags: seFlagsClean},
	}, staleEps, staleLive, seStaleMasterIP, true)
	if len(c.StaleEpoch) != 0 {
		t.Fatalf("no masterIP cohort must disarm StaleEpoch, got %v", c.StaleEpoch)
	}
	if len(c.DeadMaster) != 0 {
		t.Fatalf("both targets are live, DeadMaster must stay empty, got %v", c.DeadMaster)
	}
}

// TestFlagsIndicateElection pins the flag-token discriminator.
func TestFlagsIndicateElection(t *testing.T) {
	cases := map[string]bool{
		"master":                      false,
		"master,o_down":               true,
		"master,s_down":               true,
		"master,failover_in_progress": true,
		"":                            false,
	}
	for flags, want := range cases {
		if got := flagsIndicateElection(flags); got != want {
			t.Errorf("flagsIndicateElection(%q) = %v, want %v", flags, got, want)
		}
	}
}

// TestRecoverStrandedSentinels_DeadMasterRepoint drives the wedge-B
// end-state through the full per-reconcile path: a gossiping sentinel
// (intact peer-list — the empty-peer class must NOT match) monitors a
// master addr belonging to no live valkey pod, and the pass re-points
// it (REMOVE + MONITOR at the resolved master), reporting it in
// out.Repointed rather than out.Stranded.
func TestRecoverStrandedSentinels_DeadMasterRepoint(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	// Intact peer-list: one live peer whose IP is the sentinel's own
	// endpoint host — present in liveSentinelIPSet, so no ghost class.
	sentinelHost, _ := splitHostPort(t, fs.Addr())
	peer := PeerInfo{Name: "peer", IP: sentinelHost, Port: 26379, RunID: "rid-live"}
	queueSentinelsReply(fs, peer)
	// The monitored master is a corpse: no live valkey pod has 10.0.0.9.
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME",
		"*2\r\n$8\r\n10.0.0.9\r\n$4\r\n6379\r\n")
	// Doomed-election discriminator: the sentinel knows no replicas
	// (empty table = vacuously doomed), so the re-point may proceed.
	fs.QueueReply("SENTINEL REPLICAS", "*0\r\n")
	fs.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	fs.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   masterHost,
		Port:       masterPort,
		Quorum:     1,
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		LiveValkeyIPs: map[string]struct{}{
			masterHost: {},
		},
	}, false)

	if len(out.Stranded) != 0 {
		t.Fatalf("intact peer-list must not classify as Stranded, got %v", out.Stranded)
	}
	if len(out.Repointed) != 1 || out.Repointed[0] != "vk0-sentinel-0" {
		t.Fatalf("expected the corpse-monitoring sentinel in out.Repointed, got %v", out.Repointed)
	}
	var sawRemove, sawMonitorAtMaster bool
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			sawRemove = true
		}
		// The MONITOR must target the resolved LIVE master — a
		// re-point at any other address (the corpse included) is the
		// exact failure the class exists to repair.
		if strings.HasPrefix(line, "SENTINEL MONITOR") &&
			strings.Contains(line, masterHost) &&
			strings.Contains(line, strconv.Itoa(masterPort)) {
			sawMonitorAtMaster = true
		}
	}
	if !sawRemove || !sawMonitorAtMaster {
		t.Fatalf("re-point must REMOVE then MONITOR at %s:%d; sent: %v", masterHost, masterPort, fs.Sent())
	}
}

// TestRecoverStrandedSentinels_NoRepointWhenElectionViable pins the
// doomed-election discriminator: a corpse-monitoring sentinel whose
// replica table still names a LIVE valkey pod has a viable candidate —
// its own election can succeed, so the operator must NOT wipe it.
func TestRecoverStrandedSentinels_NoRepointWhenElectionViable(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	sentinelHost, _ := splitHostPort(t, fs.Addr())
	queueSentinelsReply(fs, PeerInfo{Name: "peer", IP: sentinelHost, Port: 26379, RunID: "rid-live"})
	fs.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME",
		"*2\r\n$8\r\n10.0.0.9\r\n$4\r\n6379\r\n")
	// The sentinel's replica table names a LIVE pod (10.0.0.7) — its
	// election has a viable candidate.
	fs.QueueReply("SENTINEL REPLICAS",
		"*1\r\n*4\r\n$2\r\nip\r\n$8\r\n10.0.0.7\r\n$4\r\nport\r\n$4\r\n6379\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(8), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   masterHost,
		Port:       masterPort,
		Quorum:     1,
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
		LiveValkeyIPs: map[string]struct{}{
			masterHost: {},
			"10.0.0.7": {},
		},
	}, false)

	if len(out.Repointed) != 0 {
		t.Fatalf("a sentinel with a live known candidate must not be re-pointed, got %v", out.Repointed)
	}
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Fatalf("viable-election sentinel must never be REMOVEd; sent: %v", fs.Sent())
		}
	}
}

// TestRecoverStrandedSentinels_NoRepointWithoutLiveSet pins the
// fail-safe: without LiveValkeyIPs the pass never probes master addrs
// and never re-points — a degraded pod-list read must not mass-touch
// healthy sentinels.
func TestRecoverStrandedSentinels_NoRepointWithoutLiveSet(t *testing.T) {
	fs := newFakeSentinel(t)
	defer fs.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	sentinelHost, _ := splitHostPort(t, fs.Addr())
	queueSentinelsReply(fs, PeerInfo{Name: "peer", IP: sentinelHost, Port: 26379, RunID: "rid-live"})

	m := NewManager(k8sevents.NewFakeRecorder(8), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   masterHost,
		Port:       masterPort,
		Quorum:     1,
		Endpoints:  []Endpoint{{Name: "vk0-sentinel-0", Addr: fs.Addr()}},
	}, false)

	if len(out.Repointed) != 0 || len(out.Stranded) != 0 {
		t.Fatalf("expected a no-op pass, got stranded=%v repointed=%v", out.Stranded, out.Repointed)
	}
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL GET-MASTER-ADDR-BY-NAME") {
			t.Fatalf("nil LiveValkeyIPs must skip the master-addr probe; sent: %v", fs.Sent())
		}
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			t.Fatalf("nil LiveValkeyIPs must never REMOVE; sent: %v", fs.Sent())
		}
	}
}

// masterEpochFlagsReply builds a SENTINEL MASTER <name> reply carrying
// the given config-epoch and flags — the fields probeOne pulls
// best-effort for the stale-epoch total order.
func masterEpochFlagsReply(epoch int, flags string) string {
	return buildArrayReply("name", "vk0", "config-epoch", itoa(epoch), "flags", flags)
}

// getMasterAddrReply builds a SENTINEL get-master-addr-by-name reply.
func getMasterAddrReply(host string, port int) string {
	return buildArrayReply(host, itoa(port))
}

func assertRepointSurgery(t *testing.T, fs *fakeSentinel, masterHost string, masterPort int) {
	t.Helper()
	var sawRemove, sawMonitor bool
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") {
			sawRemove = true
		}
		if strings.HasPrefix(line, "SENTINEL MONITOR") &&
			strings.Contains(line, masterHost) &&
			strings.Contains(line, strconv.Itoa(masterPort)) {
			sawMonitor = true
		}
	}
	if !sawRemove || !sawMonitor {
		t.Fatalf("expected REMOVE then MONITOR at %s:%d; sent: %v", masterHost, masterPort, fs.Sent())
	}
}

func assertNoRepointSurgery(t *testing.T, fs *fakeSentinel) {
	t.Helper()
	for _, line := range fs.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Fatalf("expected no REMOVE/MONITOR; sent: %v", fs.Sent())
		}
	}
}

// seTargetName is the divergent stale-epoch sentinel's pod name shared
// by the stale-epoch integration tests (the endpoint staleEpochEndpoints
// and the paced-skip variant wire as `target`).
const seTargetName = "vk0-sentinel-target"

// staleEpochEndpoints wires the two-sentinel topology the manager
// integration tests share: a `target` monitoring a divergent live pod
// and an `agree` cohort sentinel on the resolved master. Both carry an
// intact peer-list (own-host peer) so neither is empty-peer stranded.
func staleEpochEndpoints(t *testing.T, target, agree *fakeSentinel) []Endpoint {
	t.Helper()
	tHost, _ := splitHostPort(t, target.Addr())
	aHost, _ := splitHostPort(t, agree.Addr())
	queueSentinelsReply(target, PeerInfo{Name: "p", IP: tHost, Port: 26379, RunID: "rid-t"})
	queueSentinelsReply(agree, PeerInfo{Name: "p", IP: aHost, Port: 26379, RunID: "rid-a"})
	return []Endpoint{
		{Name: seTargetName, Addr: target.Addr()},
		{Name: "vk0-sentinel-agree", Addr: agree.Addr()},
	}
}

// TestRecoverStrandedSentinels_StaleEpochRepoint proves the
// replicaTableDoomed BYPASS end-to-end: a gossiping sentinel monitoring a
// LIVE-but-different pod at a stale (but readable) config-epoch, while a
// masterIP-cohort sentinel sits at a higher clean epoch, is re-pointed at
// the resolved master and reported in StaleEpochRepointed (not Repointed).
func TestRecoverStrandedSentinels_StaleEpochRepoint(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	eps := staleEpochEndpoints(t, target, agree)

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // gate-2 survivor read
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // pre-surgery destination re-check

	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // probeOne
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // pre-surgery target re-check
	target.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	target.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.Repointed) != 0 {
		t.Fatalf("dead-master class must be empty, got %v", out.Repointed)
	}
	if len(out.StaleEpochRepointed) != 1 || out.StaleEpochRepointed[0] != seTargetName {
		t.Fatalf("expected the target in StaleEpochRepointed, got %v", out.StaleEpochRepointed)
	}
	assertRepointSurgery(t, target, masterHost, masterPort)
}

// TestRecoverStrandedSentinels_StaleEpochRepoint_DeferredOnOwnElection
// pins the self-election guard end-to-end: the target's probeOne SENTINEL MASTER shows
// o_down, so it is never classified into the StaleEpoch set.
func TestRecoverStrandedSentinels_StaleEpochRepoint_DeferredOnOwnElection(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	eps := staleEpochEndpoints(t, target, agree)

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean))

	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsElection))

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.StaleEpochRepointed) != 0 {
		t.Fatalf("a mid-election target must not be re-pointed, got %v", out.StaleEpochRepointed)
	}
	assertNoRepointSurgery(t, target)
}

// TestRecoverStrandedSentinels_StaleEpoch_PreSurgeryRecheckDrops pins the
// intra-pass TOCTOU close: the target is clean at classify (selected) but
// its pre-surgery re-check now shows failover_in_progress → dropped
// before RemoveAll, no surgery, empty StaleEpochRepointed.
func TestRecoverStrandedSentinels_StaleEpoch_PreSurgeryRecheckDrops(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	eps := staleEpochEndpoints(t, target, agree)

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // gate-2

	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean))                  // probeOne (clean, selected)
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, "master,failover_in_progress")) // re-check (now electing)
	target.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	target.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.StaleEpochRepointed) != 0 {
		t.Fatalf("a target that began an election before surgery must be dropped, got %v", out.StaleEpochRepointed)
	}
	assertNoRepointSurgery(t, target)
}

// TestRecoverStrandedSentinels_StaleEpochVetoedByDestinationCohortODown
// pins the dest-election guard end-to-end: the masterIP-cohort sentinel's probeOne
// SENTINEL MASTER carries o_down → the whole class disarms with
// AllowStaleEpochRepoint=true (proving the pull-side destination guard,
// not a snapshot veto).
func TestRecoverStrandedSentinels_StaleEpochVetoedByDestinationCohortODown(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	eps := staleEpochEndpoints(t, target, agree)

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsElection)) // destination election

	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean))

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.StaleEpochRepointed) != 0 {
		t.Fatalf("a destination-cohort election must disarm the class, got %v", out.StaleEpochRepointed)
	}
	assertNoRepointSurgery(t, target)
}

// TestRecoverStrandedSentinels_StaleEpoch_SurvivorFailoverDefersWholePass
// pins the gate-2 survivor-veto seam: a StaleEpoch-eligible target plus an
// untouched survivor whose gate-2 SENTINEL MASTER returns
// failover_in_progress → the whole pass defers, target untouched. The
// survivor is clean at classify (at the frontier, so Guard B skips it)
// and only reports the election on its second (gate-2) read.
func TestRecoverStrandedSentinels_StaleEpoch_SurvivorFailoverDefersWholePass(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	survivor := newFakeSentinel(t)
	defer survivor.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	tHost, _ := splitHostPort(t, target.Addr())
	aHost, _ := splitHostPort(t, agree.Addr())
	sHost, _ := splitHostPort(t, survivor.Addr())
	queueSentinelsReply(target, PeerInfo{Name: "p", IP: tHost, Port: 26379, RunID: "rid-t"})
	queueSentinelsReply(agree, PeerInfo{Name: "p", IP: aHost, Port: 26379, RunID: "rid-a"})
	queueSentinelsReply(survivor, PeerInfo{Name: "p", IP: sHost, Port: 26379, RunID: "rid-s"})

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // gate-2

	// survivor monitors a THIRD live pod at the frontier epoch (clean at
	// classify → Guard B skips it → not wiped → healthy survivor).
	survivor.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.3", 6379))
	survivor.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean))                  // probeOne
	survivor.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, "master,failover_in_progress")) // gate-2 veto

	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean))
	target.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	target.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	eps := []Endpoint{
		{Name: seTargetName, Addr: target.Addr()},
		{Name: "vk0-sentinel-agree", Addr: agree.Addr()},
		{Name: "vk0-sentinel-survivor", Addr: survivor.Addr()},
	}
	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}, "10.0.0.3": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.StaleEpochRepointed) != 0 {
		t.Fatalf("a survivor mid-failover must defer the whole pass, got %v", out.StaleEpochRepointed)
	}
	if !out.Probed {
		t.Errorf("the survivor-failover gate is a post-classification defer; Probed must be true")
	}
	assertNoRepointSurgery(t, target)
}

// TestRecoverStrandedSentinels_StaleEpoch_PreSurgeryRecheckFailSafeDrops pins
// the fail-safe half of the per-target re-check: the target is CLEAN at
// classify (selected) but its pre-surgery SENTINEL MASTER re-read returns a
// wire error → it cannot be confirmed quiet → dropped, no surgery, empty
// StaleEpochRepointed. The agree cohort + target surgery replies are queued so
// that a mutant weakening the re-check predicate would fire surgery (and this
// test would then catch it).
func TestRecoverStrandedSentinels_StaleEpoch_PreSurgeryRecheckFailSafeDrops(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	eps := staleEpochEndpoints(t, target, agree)

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // gate-2
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // destination re-check (reached only under a weakened predicate)

	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // probeOne (clean, selected)
	target.QueueReply("SENTINEL MASTER", "-ERR boom\r\n")                        // re-check errors → cannot confirm quiet → fail-safe drop
	target.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	target.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.StaleEpochRepointed) != 0 {
		t.Fatalf("a target that errors on re-check (cannot confirm quiet) must be dropped, got %v", out.StaleEpochRepointed)
	}
	assertNoRepointSurgery(t, target)
}

// TestRecoverStrandedSentinels_StaleEpoch_DestinationRecheckDropsWholeClass
// pins the symmetric DESTINATION re-check: the masterIP-cohort sentinel is
// clean at classify AND at the gate-2 survivor read, but its pre-surgery
// destination re-check now shows o_down (the quorum is deposing the
// destination between classify and RemoveAll) → the ENTIRE surviving
// stale-epoch subset is disarmed, no surgery. The target itself stays clean
// throughout, so only the destination re-check can drop it — a mutant that
// skips the destination re-check would wrongly wipe it.
func TestRecoverStrandedSentinels_StaleEpoch_DestinationRecheckDropsWholeClass(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t)
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	eps := staleEpochEndpoints(t, target, agree)

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean))    // probeOne (clean)
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean))    // gate-2 (no failover_in_progress → passes)
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsElection)) // destination re-check → o_down → disarm

	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // probeOne (clean, selected)
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // per-target re-check (clean → survives)
	target.QueueReply("SENTINEL REMOVE", "+OK\r\n")                              // reached only under a skip-destination-re-check mutant
	target.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.StaleEpochRepointed) != 0 {
		t.Fatalf("a destination-cohort election on the pre-surgery re-check must disarm the whole class, got %v", out.StaleEpochRepointed)
	}
	assertNoRepointSurgery(t, target)
}

// TestRecoverStrandedSentinels_DeadMasterAndStaleEpochBothRepointed pins the
// fold path carrying BOTH sub-classes in one pass: a corpse-monitoring
// sentinel (empty replica table → DeadMaster → Repointed) AND a strictly-behind
// live-but-different sentinel (StaleEpoch → StaleEpochRepointed) both ride the
// same REMOVE + MONITOR surgery, with both out fields populated and both names
// wiped.
func TestRecoverStrandedSentinels_DeadMasterAndStaleEpochBothRepointed(t *testing.T) {
	corpse := newFakeSentinel(t)
	defer corpse.Stop()
	stale := newFakeSentinel(t)
	defer stale.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	// Intact peer-lists (own-host peer) → none is empty-peer stranded, and the
	// peer IP is a live sentinel → none is a ghost.
	corpseHost, _ := splitHostPort(t, corpse.Addr())
	staleHost, _ := splitHostPort(t, stale.Addr())
	agreeHost, _ := splitHostPort(t, agree.Addr())
	queueSentinelsReply(corpse, PeerInfo{Name: "p", IP: corpseHost, Port: 26379, RunID: "rid-c"})
	queueSentinelsReply(stale, PeerInfo{Name: "p", IP: staleHost, Port: 26379, RunID: "rid-s"})
	queueSentinelsReply(agree, PeerInfo{Name: "p", IP: agreeHost, Port: 26379, RunID: "rid-a"})

	// agree — masterIP cohort / re-point destination at the frontier.
	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // gate-2 survivor read
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // destination re-check

	// corpse — monitors a dead pod; empty (vacuously doomed) replica table →
	// DeadMaster. Its epoch equals the frontier so it never trips Guard A.
	// Only probeOne reads its SENTINEL MASTER (not a stale target, not cohort).
	corpse.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.9", 6379))
	corpse.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	corpse.QueueReply("SENTINEL REPLICAS", "*0\r\n")
	corpse.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	corpse.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	// stale — monitors a DIFFERENT live pod strictly behind the frontier →
	// StaleEpoch. probeOne + per-target re-check both clean → survives.
	stale.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	stale.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // probeOne
	stale.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // per-target re-check
	stale.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	stale.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	eps := []Endpoint{
		{Name: "vk0-sentinel-corpse", Addr: corpse.Addr()},
		{Name: "vk0-sentinel-stale", Addr: stale.Addr()},
		{Name: "vk0-sentinel-agree", Addr: agree.Addr()},
	}
	m := NewManager(k8sevents.NewFakeRecorder(32), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.Repointed) != 1 || out.Repointed[0] != "vk0-sentinel-corpse" {
		t.Fatalf("the corpse must land in Repointed, got %v", out.Repointed)
	}
	if len(out.StaleEpochRepointed) != 1 || out.StaleEpochRepointed[0] != "vk0-sentinel-stale" {
		t.Fatalf("the strictly-behind target must land in StaleEpochRepointed, got %v", out.StaleEpochRepointed)
	}
	assertRepointSurgery(t, corpse, masterHost, masterPort)
	assertRepointSurgery(t, stale, masterHost, masterPort)
}

// TestRecoverStrandedSentinels_StaleEpoch_BelowMaxCohortMemberCompletes pins
// the max-based destination re-check end-to-end: a masterIP cohort split across
// epochs {5, 0} (a member freshly SENTINEL MONITOR-reset onto masterIP sits at
// config-epoch 0 until monotonic gossip convergence catches it up), clean and
// UNCHANGED between classify and re-check, must NOT disarm — the cohort MAX (5)
// still holds the classify-time frontier — so the strictly-behind target @3 is
// re-pointed to completion. An all-members `epoch >= agreeEpoch` re-check would
// wrongly disarm on the @0 member and self-defeat the feature in exactly the
// post-failover / post-surgery convergence window it exists to repair.
func TestRecoverStrandedSentinels_StaleEpoch_BelowMaxCohortMemberCompletes(t *testing.T) {
	agreeHi := newFakeSentinel(t)
	defer agreeHi.Stop()
	agreeLo := newFakeSentinel(t)
	defer agreeLo.Stop()
	target := newFakeSentinel(t)
	defer target.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	// Intact peer-lists (own-host peer) → none empty-peer stranded / ghost.
	hiHost, _ := splitHostPort(t, agreeHi.Addr())
	loHost, _ := splitHostPort(t, agreeLo.Addr())
	tHost, _ := splitHostPort(t, target.Addr())
	queueSentinelsReply(agreeHi, PeerInfo{Name: "p", IP: hiHost, Port: 26379, RunID: "rid-hi"})
	queueSentinelsReply(agreeLo, PeerInfo{Name: "p", IP: loHost, Port: 26379, RunID: "rid-lo"})
	queueSentinelsReply(target, PeerInfo{Name: "p", IP: tHost, Port: 26379, RunID: "rid-t"})

	// agreeHi — masterIP cohort at the frontier (epoch 5).
	agreeHi.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agreeHi.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	agreeHi.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // gate-2
	agreeHi.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // destination re-check

	// agreeLo — masterIP cohort, freshly MONITOR-reset → below-max (epoch 0),
	// clean and unchanged. Must NOT disarm (the max still holds the frontier).
	agreeLo.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agreeLo.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(0, seFlagsClean)) // probeOne
	agreeLo.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(0, seFlagsClean)) // gate-2
	agreeLo.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(0, seFlagsClean)) // destination re-check

	// target — divergent live pod strictly behind the frontier (epoch 3).
	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // probeOne
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // per-target re-check
	target.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	target.QueueReply("SENTINEL MONITOR", "+OK\r\n")

	eps := []Endpoint{
		{Name: "vk0-sentinel-agree-hi", Addr: agreeHi.Addr()},
		{Name: "vk0-sentinel-agree-lo", Addr: agreeLo.Addr()},
		{Name: seTargetName, Addr: target.Addr()},
	}
	m := NewManager(k8sevents.NewFakeRecorder(32), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:                     types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName:             "vk0",
		MasterIP:               masterHost,
		Port:                   masterPort,
		Quorum:                 2,
		Endpoints:              eps,
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
	}, false)

	if len(out.StaleEpochRepointed) != 1 || out.StaleEpochRepointed[0] != seTargetName {
		t.Fatalf("a below-max cohort member must not disarm; expected the target re-pointed, got %v", out.StaleEpochRepointed)
	}
	assertRepointSurgery(t, target, masterHost, masterPort)
}

// cohortMember spins a fake sentinel that answers exactly one SENTINEL MASTER
// read with the given reply — the single read destinationCohortQuiet issues per
// cohort member — and returns its endpoint. The caller defers fs.Stop().
func cohortMember(t *testing.T, name, masterReply string) (Endpoint, *fakeSentinel) {
	t.Helper()
	fs := newFakeSentinel(t)
	fs.QueueReply("SENTINEL MASTER", masterReply)
	return Endpoint{Name: name, Addr: fs.Addr()}, fs
}

// TestDestinationCohortQuiet pins the destination pre-surgery re-check's disarm
// logic directly, isolating the max-based frontier from the classify path.
func TestDestinationCohortQuiet(t *testing.T) {
	// A below-max cohort member (freshly MONITOR-reset @0) does NOT disarm
	// while some member still holds the classify frontier (max 5 >= 5).
	t.Run("below-max member does not disarm", func(t *testing.T) {
		hi, fhi := cohortMember(t, "s0", masterEpochFlagsReply(5, seFlagsClean))
		defer fhi.Stop()
		lo, flo := cohortMember(t, "s1", masterEpochFlagsReply(0, seFlagsClean))
		defer flo.Stop()
		if !destinationCohortQuiet(context.Background(), []Endpoint{hi, lo}, "vk0", "", 5) {
			t.Fatal("a below-max cohort member must not disarm while the max holds the frontier")
		}
	})
	// The whole cohort fell below the classify frontier → disarm (frontier fail-safe).
	t.Run("cohort max below agreeEpoch disarms", func(t *testing.T) {
		a, fa := cohortMember(t, "s0", masterEpochFlagsReply(4, seFlagsClean))
		defer fa.Stop()
		b, fb := cohortMember(t, "s1", masterEpochFlagsReply(3, seFlagsClean))
		defer fb.Stop()
		if destinationCohortQuiet(context.Background(), []Endpoint{a, b}, "vk0", "", 5) {
			t.Fatal("a cohort whose max epoch fell below agreeEpoch must disarm")
		}
	})
	// An electing member disarms per-member regardless of the frontier.
	t.Run("electing member disarms", func(t *testing.T) {
		a, fa := cohortMember(t, "s0", masterEpochFlagsReply(5, seFlagsClean))
		defer fa.Stop()
		b, fb := cohortMember(t, "s1", masterEpochFlagsReply(5, seFlagsElection))
		defer fb.Stop()
		if destinationCohortQuiet(context.Background(), []Endpoint{a, b}, "vk0", "", 5) {
			t.Fatal("an electing cohort member must disarm")
		}
	})
	// No member returned a readable epoch (flags present, no config-epoch) → the
	// frontier can't be established → disarm.
	t.Run("no readable epoch disarms", func(t *testing.T) {
		a, fa := cohortMember(t, "s0", buildArrayReply("name", "vk0", "flags", seFlagsClean))
		defer fa.Stop()
		if destinationCohortQuiet(context.Background(), []Endpoint{a}, "vk0", "", 5) {
			t.Fatal("a cohort with no readable epoch must disarm (frontier fail-safe)")
		}
	})
	// An empty cohort cannot confirm the destination → disarm.
	t.Run("empty cohort disarms", func(t *testing.T) {
		if destinationCohortQuiet(context.Background(), nil, "vk0", "", 5) {
			t.Fatal("an empty cohort must disarm")
		}
	})
}

// TestRecheckElectionQuiet_DropsErroringEndpoint pins the per-target re-check
// primitive directly: a reachable-quiet endpoint is returned and an
// unreachable one (cannot be confirmed quiet) is dropped.
func TestRecheckElectionQuiet_DropsErroringEndpoint(t *testing.T) {
	quiet := newFakeSentinel(t)
	defer quiet.Stop()
	quiet.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean))

	targets := []Endpoint{
		{Name: "s0", Addr: quiet.Addr()},
		{Name: "s1", Addr: "127.0.0.1:1"}, // unreachable → cannot confirm quiet → dropped
	}
	got := recheckElectionQuiet(context.Background(), targets, "vk0", "")
	if len(got) != 1 || got[0].Name != "s0" {
		t.Fatalf("recheckElectionQuiet must return only the reachable-quiet endpoint, got %v", got)
	}
}

// TestParseReplicasReply pins the defensive branches of the SENTINEL
// REPLICAS reply decoder.
func TestParseReplicasReply(t *testing.T) {
	t.Run("non-array top-level reply yields nil", func(t *testing.T) {
		if got := ParseReplicasReply("nope"); got != nil {
			t.Fatalf("expected nil for a non-array reply, got %v", got)
		}
	})
	t.Run("well-formed multi-row table", func(t *testing.T) {
		reply := []any{
			[]any{"ip", "10.0.0.7", "port", "6379", "flags", "slave"},
			[]any{"ip", "10.0.0.8", "port", "6379", "flags", "slave,s_down"},
		}
		got := ParseReplicasReply(reply)
		if len(got) != 2 || got[0].IP != "10.0.0.7" || got[1].IP != "10.0.0.8" {
			t.Fatalf("parsed = %+v, want two rows 10.0.0.7/10.0.0.8", got)
		}
		if got[1].Flags != "slave,s_down" || got[0].Port != 6379 {
			t.Fatalf("parsed = %+v, want flags/port carried through", got)
		}
	})
	t.Run("row without ip is dropped; non-array item skipped", func(t *testing.T) {
		reply := []any{
			"not-an-array",
			[]any{"port", "6379", "flags", "slave"},
			[]any{"ip", "10.0.0.9", "port", "6379"},
		}
		got := ParseReplicasReply(reply)
		if len(got) != 1 || got[0].IP != "10.0.0.9" {
			t.Fatalf("parsed = %+v, want only the ip-carrying row", got)
		}
	})
	t.Run("non-numeric port keeps Port zero", func(t *testing.T) {
		got := ParseReplicasReply([]any{[]any{"ip", "10.0.0.9", "port", "not-a-port"}})
		if len(got) != 1 || got[0].Port != 0 {
			t.Fatalf("parsed = %+v, want one row with Port=0", got)
		}
	})
}

// TestQuorumElectionDoomed pins the single authority for the
// doomed-election rule at the wire level.
func TestQuorumElectionDoomed(t *testing.T) {
	live := map[string]struct{}{"10.0.0.1": {}, "10.0.0.2": {}}
	deadTable := "*1\r\n*4\r\n$2\r\nip\r\n$8\r\n10.0.0.9\r\n$4\r\nport\r\n$4\r\n6379\r\n"
	liveTable := "*1\r\n*4\r\n$2\r\nip\r\n$8\r\n10.0.0.1\r\n$4\r\nport\r\n$4\r\n6379\r\n"

	t.Run("quorum of doomed tables is doomed", func(t *testing.T) {
		fs1, fs2 := newFakeSentinel(t), newFakeSentinel(t)
		defer fs1.Stop()
		defer fs2.Stop()
		fs1.QueueReply("SENTINEL REPLICAS", deadTable)
		fs2.QueueReply("SENTINEL REPLICAS", deadTable)
		m := NewManager(nil, Options{})
		eps := []Endpoint{{Name: "s0", Addr: fs1.Addr()}, {Name: "s1", Addr: fs2.Addr()}}
		if !m.QuorumElectionDoomed(context.Background(), eps, "vk0", "", live) {
			t.Fatal("all-dead tables at quorum must be doomed")
		}
	})
	t.Run("one live known replica is not doomed", func(t *testing.T) {
		fs1, fs2 := newFakeSentinel(t), newFakeSentinel(t)
		defer fs1.Stop()
		defer fs2.Stop()
		fs1.QueueReply("SENTINEL REPLICAS", deadTable)
		fs2.QueueReply("SENTINEL REPLICAS", liveTable)
		m := NewManager(nil, Options{})
		eps := []Endpoint{{Name: "s0", Addr: fs1.Addr()}, {Name: "s1", Addr: fs2.Addr()}}
		if m.QuorumElectionDoomed(context.Background(), eps, "vk0", "", live) {
			t.Fatal("a sentinel knowing a live replica must not be doomed")
		}
	})
	t.Run("sub-quorum reachability is not doomed", func(t *testing.T) {
		fs1 := newFakeSentinel(t)
		defer fs1.Stop()
		fs1.QueueReply("SENTINEL REPLICAS", deadTable)
		m := NewManager(nil, Options{})
		// Second endpoint unreachable: reachable=1 < QuorumThreshold(2)=2.
		eps := []Endpoint{{Name: "s0", Addr: fs1.Addr()}, {Name: "s1", Addr: "127.0.0.1:1"}}
		if m.QuorumElectionDoomed(context.Background(), eps, "vk0", "", live) {
			t.Fatal("an unreadable table must count as unreachable, never doomed")
		}
	})
	t.Run("empty endpoints or live set is never doomed", func(t *testing.T) {
		m := NewManager(nil, Options{})
		if m.QuorumElectionDoomed(context.Background(), nil, "vk0", "", live) {
			t.Fatal("no endpoints must not be doomed")
		}
		if m.QuorumElectionDoomed(context.Background(), []Endpoint{{Name: "s0", Addr: "127.0.0.1:1"}}, "vk0", "", nil) {
			t.Fatal("nil live set must not be doomed")
		}
	})
}

// TestRecoverStrandedSentinels_SkippedStaleEpochNotRepointed is the 2a
// guard for the stale-epoch sub-class: a PACED empty-peer sentinel that
// also matches the stale-epoch signature (monitoring a live-but-different
// pod provably behind the config-epoch frontier) must NOT be
// re-classified into StaleEpochRepointed and wiped behind the skip's
// back. beingWiped is seeded from the FULL empty-peer class (wiped OR
// skipped), so selectRepointTargets excludes it — otherwise a wedged
// sentinel would be REMOVE + MONITOR'd every base window behind its
// per-address pace, re-opening the read-back livelock the pacing exists
// to close. Mirrors the DeadMaster-half pin
// (TestRecoverStrandedSentinels_SkippedCorpseMonitorNotRepointed).
func TestRecoverStrandedSentinels_SkippedStaleEpochNotRepointed(t *testing.T) {
	target := newFakeSentinel(t)
	defer target.Stop()
	agree := newFakeSentinel(t)
	defer agree.Stop()
	fm := newFakeSentinel(t) // fake master; auto-answers PING
	defer fm.Stop()
	masterHost, masterPort := splitHostPort(t, fm.Addr())

	// The target is empty-peer stranded (paced); agree keeps an intact
	// peer-list (own-host peer) and sits on masterIP at the frontier.
	aHost, _ := splitHostPort(t, agree.Addr())
	queueSentinelsReply(target /* empty — stranded, paced */)
	queueSentinelsReply(agree, PeerInfo{Name: "p", IP: aHost, Port: 26379, RunID: "rid-a"})

	agree.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply(masterHost, masterPort))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean)) // probeOne
	// Queue the gate-2 + destination re-check replies so a REGRESSION
	// (re-point behind the skip) proceeds to the wire assertion instead
	// of blocking on an unqueued reply.
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean))
	agree.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(5, seFlagsClean))

	// The target monitors a live-but-different pod strictly behind the
	// frontier — the stale-epoch signature.
	target.QueueReply("SENTINEL GET-MASTER-ADDR-BY-NAME", getMasterAddrReply("10.0.0.2", 6379))
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean)) // probeOne
	// Regression-path replies (pre-surgery re-check + surgery).
	target.QueueReply("SENTINEL MASTER", masterEpochFlagsReply(3, seFlagsClean))
	target.QueueReply("SENTINEL REMOVE", "+OK\r\n")
	target.QueueReply("SENTINEL MONITOR", "+OK\r\n")
	target.QueueReply("SENTINEL SET", "+OK\r\n")

	m := NewManager(k8sevents.NewFakeRecorder(16), Options{})
	out := m.RecoverStrandedSentinels(context.Background(), InitialResetTarget{
		CR:         types.NamespacedName{Namespace: "ns", Name: "vk0"},
		MasterName: "vk0",
		MasterIP:   masterHost,
		Port:       masterPort,
		Quorum:     2,
		Endpoints: []Endpoint{
			{Name: seTargetName, Addr: target.Addr()},
			{Name: "vk0-sentinel-agree", Addr: agree.Addr()},
		},
		LiveValkeyIPs:          map[string]struct{}{masterHost: {}, "10.0.0.2": {}},
		AllowStaleEpochRepoint: true,
		// Pace this same empty-peer sentinel.
		SkipStrandedAddrs: map[string]struct{}{target.Addr(): {}},
	}, false)

	if len(out.SkippedStranded) != 1 || out.SkippedStranded[0] != seTargetName {
		t.Fatalf("the paced stale-epoch sentinel must be in SkippedStranded, got %v", out.SkippedStranded)
	}
	if len(out.StaleEpochRepointed) != 0 {
		t.Errorf("a paced empty-peer sentinel must NEVER be re-classified into StaleEpochRepointed, got %v", out.StaleEpochRepointed)
	}
	if len(out.Stranded) != 0 || len(out.EmptyPeerStranded) != 0 {
		t.Errorf("nothing wiped: Stranded=%v EmptyPeerStranded=%v", out.Stranded, out.EmptyPeerStranded)
	}
	for _, line := range target.Sent() {
		if strings.HasPrefix(line, "SENTINEL REMOVE") || strings.HasPrefix(line, "SENTINEL MONITOR") {
			t.Errorf("a paced sentinel must never be REMOVE/MONITOR'd behind the skip's back; sent: %q", line)
		}
	}
}
