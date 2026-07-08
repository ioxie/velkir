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
	"strings"
	"testing"
	"time"
)

const triplecheckStaleness = 30 * time.Second

func tcNow() time.Time {
	// Fixed wall-clock for the test suite so staleness windows are
	// deterministic across runs. Date chosen as the project's
	// current-month epoch.
	return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
}

// freshODown returns a map with `n` distinct sentinel pod names, all
// with a last-seen +odown 1 second before tcNow() (well within the
// 30s staleness cutoff).
func freshODown(n int) map[string]time.Time {
	out := make(map[string]time.Time, n)
	t := tcNow().Add(-time.Second)
	for i := range n {
		out["sentinel-"+string(rune('0'+i))] = t
	}
	return out
}

func TestEvaluateTripleCheck_AllSignalsAgree_Allows(t *testing.T) {
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK: true,
				ODown:    freshODown(3),
			},
		},
		PodReady:       false, // kubelet agrees the primary is down
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if !got.Allow {
		t.Fatalf("all three signals agree; want Allow=true, got %+v", got)
	}
	if got.Reason != TripleCheckReasonOK {
		t.Errorf("Allow=true should carry TripleCheckReasonOK; got %q", got.Reason)
	}
	if got.Detail != "" {
		t.Errorf("Allow=true should carry empty Detail; got %q", got.Detail)
	}
}

func TestEvaluateTripleCheck_NoSnapshot_Defers(t *testing.T) {
	// Boot race: observer hasn't published yet. Operator must wait,
	// not fail over on stale-or-missing data.
	in := TripleCheckInputs{
		Snapshot:       Snapshot{Present: false},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if got.Allow {
		t.Fatal("no observer snapshot — must NOT allow failover")
	}
	if got.Reason != TripleCheckReasonNoSnapshot {
		t.Errorf("want TripleCheckReasonNoSnapshot, got %q", got.Reason)
	}
	if !strings.Contains(got.Detail, "observer") {
		t.Errorf("Detail should name the observer as the missing source; got %q", got.Detail)
	}
}

func TestEvaluateTripleCheck_NoQuorum_BlocksAndPointsAtQuorumLost(t *testing.T) {
	// CKQUORUM=NOQUORUM. Even with +odown consensus and Pod.Ready=False,
	// the operator must enter Degraded+QuorumLost rather than failing
	// over.
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK: false, // CKQUORUM not OK
				ODown:    freshODown(3),
			},
		},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if got.Allow {
		t.Fatal("CKQUORUM=NOQUORUM — must NOT allow failover")
	}
	if got.Reason != TripleCheckReasonNoQuorum {
		t.Errorf("want TripleCheckReasonNoQuorum, got %q", got.Reason)
	}
	if !strings.Contains(got.Detail, "Degraded+QuorumLost") {
		t.Errorf("Detail should reference the Degraded+QuorumLost FSM branch; got %q", got.Detail)
	}
}

func TestEvaluateTripleCheck_NoODownConsensus_Blocks(t *testing.T) {
	// CKQUORUM is OK, but only 2 of the required 3 sentinels report
	// +odown. The operator must NOT fail over — the primary may be
	// slow or node-drained, but sentinel hasn't agreed it's dead.
	// Let sentinel run its course.
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK: true,
				ODown:    freshODown(2),
			},
		},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if got.Allow {
		t.Fatal("only 2 of 3 sentinels report +odown — must NOT allow failover")
	}
	if got.Reason != TripleCheckReasonNoODownConsensus {
		t.Errorf("want TripleCheckReasonNoODownConsensus, got %q", got.Reason)
	}
	if !strings.Contains(got.Detail, "2") || !strings.Contains(got.Detail, "quorum=3") {
		t.Errorf("Detail should name both observed (2) and required (3) counts; got %q", got.Detail)
	}
}

func TestEvaluateTripleCheck_PodReadyMismatch_Suppresses(t *testing.T) {
	// CKQUORUM OK + sentinel +odown consensus, but kubelet says
	// Pod.Ready=True. This state is almost always a sentinel-network
	// or sentinel-probe issue, not a real primary failure — the
	// disagreement is in the sentinel layer, not the data plane.
	// Suppress the failover.
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK: true,
				ODown:    freshODown(3),
			},
		},
		PodReady:       true, // <-- the mismatch
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if got.Allow {
		t.Fatal("kubelet says Pod.Ready=True — must suppress failover")
	}
	if got.Reason != TripleCheckReasonPodReady {
		t.Errorf("want TripleCheckReasonPodReady, got %q", got.Reason)
	}
	if !strings.Contains(got.Detail, "kubelet") {
		t.Errorf("Detail should name kubelet as the source of the mismatch; got %q", got.Detail)
	}
}

func TestEvaluateTripleCheck_NoQuorumChecksFirst(t *testing.T) {
	// Ordering invariant: CKQUORUM=NOQUORUM is the most severe
	// signal (the FSM enters Degraded+QuorumLost which suppresses
	// SENTINEL MONITOR/RESET/SET issuance). Even if the +odown map
	// is empty AND PodReady=True, the reason returned must be
	// NoQuorum, not NoODownConsensus or PodReady — operators key
	// status conditions and event logs on the most severe cause.
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK: false,
				ODown:    nil, // would also fail consensus
			},
		},
		PodReady:       true, // would also fail PodReady
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if got.Reason != TripleCheckReasonNoQuorum {
		t.Errorf("NoQuorum is most severe — must be reported first; got %q", got.Reason)
	}
}

func TestEvaluateTripleCheck_StaleSnapshot_Defers(t *testing.T) {
	// All three signals agree and quorum is OK, but the snapshot's
	// last live pull is older than MaxSnapshotAge: the poll tick is
	// wedged while pub/sub re-publishes a stale QuorumOK. The gate
	// must refuse the failover and defer, not act on quorum no live
	// pull confirmed.
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK:  true,
				ODown:     freshODown(3),
				UpdatedAt: tcNow().Add(-31 * time.Second), // 1s past the 30s max
			},
		},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		MaxSnapshotAge: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if got.Allow {
		t.Fatal("snapshot older than MaxSnapshotAge — must NOT allow failover")
	}
	if got.Reason != TripleCheckReasonStaleSnapshot {
		t.Errorf("want TripleCheckReasonStaleSnapshot, got %q", got.Reason)
	}
	if !strings.Contains(got.Detail, "old") {
		t.Errorf("Detail should name the snapshot age; got %q", got.Detail)
	}
}

func TestEvaluateTripleCheck_FreshSnapshot_PassesGate(t *testing.T) {
	// Same as the all-agree case but with the freshness gate ENABLED
	// and a recent UpdatedAt: the gate must let it through to Allow.
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK:  true,
				ODown:     freshODown(3),
				UpdatedAt: tcNow().Add(-time.Second), // well within the cutoff
			},
		},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		MaxSnapshotAge: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if !got.Allow {
		t.Fatalf("fresh snapshot with all signals agreeing; want Allow=true, got %+v", got)
	}
}

func TestEvaluateTripleCheck_StaleBoundaryIsInclusive(t *testing.T) {
	// A snapshot exactly MaxSnapshotAge old is still fresh (inclusive
	// boundary, matching ConsensusODown): a 30s pull tick + 30s max
	// age must accept the previous tick's snapshot on the next tick
	// boundary rather than race-drop it.
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK:  true,
				ODown:     freshODown(3),
				UpdatedAt: tcNow().Add(-triplecheckStaleness), // exactly at the cutoff
			},
		},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		MaxSnapshotAge: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if !got.Allow {
		t.Fatalf("snapshot exactly MaxSnapshotAge old must be inclusive-fresh; got %+v", got)
	}
}

func TestEvaluateTripleCheck_StaleChecksBeforeQuorum(t *testing.T) {
	// Ordering invariant: a stale snapshot whose QuorumOK is also
	// false must report StaleSnapshot, NOT NoQuorum. A stale
	// snapshot's QuorumOK is itself untrustworthy, so the correct
	// action is "defer until a fresh pull lands" (NoSnapshot-class),
	// not "enter Degraded+QuorumLost" (which would act on the quorum
	// signal as if it were current).
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK:  false,
				ODown:     freshODown(3),
				UpdatedAt: tcNow().Add(-time.Hour), // very stale
			},
		},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		MaxSnapshotAge: triplecheckStaleness,
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if got.Reason != TripleCheckReasonStaleSnapshot {
		t.Errorf("stale snapshot must be reported before NoQuorum; got %q", got.Reason)
	}
}

func TestEvaluateTripleCheck_MaxSnapshotAgeZeroDisablesGate(t *testing.T) {
	// MaxSnapshotAge<=0 is the documented test escape: the freshness
	// gate is skipped entirely, so even an ancient UpdatedAt falls
	// through to the normal three-signal evaluation (here: Allow).
	in := TripleCheckInputs{
		Snapshot: Snapshot{
			Present: true,
			Primary: ObservedPrimary{
				QuorumOK:  true,
				ODown:     freshODown(3),
				UpdatedAt: tcNow().Add(-365 * 24 * time.Hour), // ancient
			},
		},
		PodReady:       false,
		Quorum:         3,
		ODownStaleness: triplecheckStaleness,
		MaxSnapshotAge: 0, // gate disabled
		Now:            tcNow(),
	}
	got := EvaluateTripleCheck(in)
	if !got.Allow {
		t.Fatalf("MaxSnapshotAge=0 disables the freshness gate; want Allow=true, got %+v", got)
	}
}

func TestConsensusODown_StaleEntriesDropped(t *testing.T) {
	// 3 sentinels in the map, but one's +odown is older than the
	// staleness cutoff (30s). Consensus should count 2, not 3.
	now := tcNow()
	odown := map[string]time.Time{
		"sentinel-0": now.Add(-1 * time.Second),  // fresh
		"sentinel-1": now.Add(-29 * time.Second), // fresh (just inside the 30s cutoff)
		"sentinel-2": now.Add(-31 * time.Second), // STALE
	}
	got := ConsensusODown(odown, now, 30*time.Second)
	if got != 2 {
		t.Errorf("stale entry should be excluded; want 2, got %d", got)
	}
}

func TestConsensusODown_BoundaryCutoffIsInclusive(t *testing.T) {
	// A sentinel whose +odown landed exactly at the cutoff is
	// considered fresh. This matters: a 30s pull tick + 30s
	// staleness should accept the previous tick's data on the next
	// tick boundary, not race-drop it.
	now := tcNow()
	odown := map[string]time.Time{
		"sentinel-0": now.Add(-30 * time.Second), // boundary
	}
	got := ConsensusODown(odown, now, 30*time.Second)
	if got != 1 {
		t.Errorf("boundary-cutoff entry should be inclusive; want 1, got %d", got)
	}
}

func TestConsensusODown_ZeroValueEntriesIgnored(t *testing.T) {
	// A zero-value timestamp means "we don't actually have a +odown
	// from this sentinel" (defensive — observer should never
	// publish such an entry, but the helper must not count it).
	now := tcNow()
	odown := map[string]time.Time{
		"sentinel-0": now.Add(-1 * time.Second),
		"sentinel-1": {}, // zero value
	}
	got := ConsensusODown(odown, now, 30*time.Second)
	if got != 1 {
		t.Errorf("zero-value timestamp should be ignored; want 1, got %d", got)
	}
}

func TestConsensusODown_StalenessZeroIsTestEscape(t *testing.T) {
	// Documented test-only shape: staleness=0 means "ignore the time
	// check, count any non-zero entry". Production callers always
	// pass a positive staleness, but tests use 0 to focus on the
	// triple-check logic without time-based fixtures.
	now := tcNow()
	odown := map[string]time.Time{
		"sentinel-0": now.Add(-1 * time.Hour),        // would be stale at any positive staleness
		"sentinel-1": now.Add(-365 * 24 * time.Hour), // very stale
		"sentinel-2": {},                             // zero — still excluded
	}
	got := ConsensusODown(odown, now, 0)
	if got != 2 {
		t.Errorf("staleness=0 should count any non-zero entry; want 2, got %d", got)
	}
}

// FuzzConsensusODown_StalenessBoundary fuzzes the inclusive-cutoff
// invariant against randomized per-sentinel offsets. The fixed
// boundary tests above pin the t.Equal(cutoff) case at exactly -30s;
// this fuzz exercises the same predicate at every nanosecond either
// side of the cutoff and across multiple stalenesses.
//
// Invariant: for offset nanoseconds before `now` and staleness `s`,
// the entry is counted iff (now-offset) >= now-s, i.e. offset <= s.
// A zero-value time is never counted, regardless of offset.
func FuzzConsensusODown_StalenessBoundary(f *testing.F) {
	// Seed the corpus with the cases the existing fixed tests pin:
	// just-inside, exact-cutoff, just-past-cutoff, far-stale, and a
	// future entry (operator's wall-clock skew vs sentinel's).
	f.Add(int64(time.Second), int64(30*time.Second))      // fresh
	f.Add(int64(29*time.Second), int64(30*time.Second))   // fresh, near boundary
	f.Add(int64(30*time.Second), int64(30*time.Second))   // exact boundary (inclusive)
	f.Add(int64(30*time.Second+1), int64(30*time.Second)) // 1ns past — stale
	f.Add(int64(31*time.Second), int64(30*time.Second))   // stale
	f.Add(int64(time.Hour), int64(30*time.Second))        // very stale
	f.Add(int64(-time.Second), int64(30*time.Second))     // future timestamp (negative offset)
	f.Add(int64(time.Second), int64(60*time.Second))      // wider staleness
	f.Add(int64(60*time.Second), int64(60*time.Second))   // boundary at wider staleness
	f.Fuzz(func(t *testing.T, offsetNs int64, stalenessNs int64) {
		// Bound staleness to positive — staleness <= 0 is the
		// documented test-escape (covered by its own fixed test) and
		// fuzzing it here would just re-exercise the
		// IgnoreTimeCheck branch.
		if stalenessNs <= 0 {
			t.Skip()
		}
		now := tcNow()
		staleness := time.Duration(stalenessNs)
		// Build a two-entry map: one timestamped, one zero. The zero
		// entry must NEVER be counted, regardless of offset.
		odown := map[string]time.Time{
			"sentinel-fresh": now.Add(-time.Duration(offsetNs)),
			"sentinel-zero":  {},
		}
		got := ConsensusODown(odown, now, staleness)
		// Inclusive boundary: counted iff offset <= staleness.
		// Negative offsets (future timestamps) are always counted —
		// they're "fresher than now".
		want := 0
		if offsetNs <= int64(staleness) {
			want = 1
		}
		if got != want {
			t.Errorf("offset=%dns staleness=%dns: got count=%d, want %d (zero entry must be excluded)",
				offsetNs, int64(staleness), got, want)
		}
	})
}
