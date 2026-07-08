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
	"testing"
	"time"
)

const (
	stuckTestAddr  = "10.0.0.1:26379"
	stuckTestAddr2 = "10.0.0.2:26379"
)

// TestStrandedAddrBackoffLevel_Derivation pins the pure per-address level
// derivation: level 0 below the stuck threshold, 1 at the threshold,
// +1/step, capped at strandedSurgeryMaxBackoffLevel. This reproduces the
// old per-CR single-sentinel pacing (count 3→1, 4→2, 5→3, capped).
func TestStrandedAddrBackoffLevel_Derivation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		noProgress int
		want       int
	}{
		{0, 0},
		{strandedSurgeryStuckThreshold - 1, 0}, // below threshold = base
		{strandedSurgeryStuckThreshold, 1},     // threshold = level 1
		{strandedSurgeryStuckThreshold + 1, 2},
		{strandedSurgeryStuckThreshold + 2, 3},
		{strandedSurgeryStuckThreshold + 3, strandedSurgeryMaxBackoffLevel},  // capped
		{strandedSurgeryStuckThreshold + 10, strandedSurgeryMaxBackoffLevel}, // still capped
	}
	for _, tc := range cases {
		if got := strandedAddrBackoffLevel(tc.noProgress); got != tc.want {
			t.Errorf("strandedAddrBackoffLevel(%d) = %d, want %d", tc.noProgress, got, tc.want)
		}
	}
}

// TestStrandedSkipSet_StuckWithinWindowSkippedFreshNotSkipped is the
// ticket headline: a stuck addr (count >= threshold, recent lastWiped) is
// in the skip-set, while a below-threshold addr (level 0) and an absent
// (fresh, untracked) addr are NOT — so a permanently-wedged sentinel is
// paced while a different fresh strand fires at base cadence.
func TestStrandedSkipSet_StuckWithinWindowSkippedFreshNotSkipped(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &crQuorumState{
		strandedNoProgress: map[string]strandedAddrState{
			// Stuck at level 1 (base<<1 = 60s window), wiped 5s ago → within.
			stuckTestAddr: {noProgress: strandedSurgeryStuckThreshold, lastWiped: now.Add(-5 * time.Second)},
			// Below threshold: derived level 0 → never skipped.
			stuckTestAddr2: {noProgress: strandedSurgeryStuckThreshold - 1, lastWiped: now},
		},
	}
	skip := s.strandedSkipSet(now)
	if _, ok := skip[stuckTestAddr]; !ok {
		t.Errorf("a stuck addr within its re-wipe window must be skipped: %v", skip)
	}
	if _, ok := skip[stuckTestAddr2]; ok {
		t.Errorf("a below-threshold addr (level 0) must never be skipped: %v", skip)
	}
	if _, ok := skip["10.0.0.7:26379"]; ok {
		t.Errorf("a fresh (untracked) addr must never be skipped: %v", skip)
	}
	if len(skip) != 1 {
		t.Errorf("only the stuck-within-window addr should be skipped, got %v", skip)
	}
}

// TestStrandedSkipSet_StuckPastWindowNotSkipped pins that backoff PACES,
// never LATCHES: a stuck addr whose base<<level window has elapsed is no
// longer skipped, so the next probe re-wipes it.
func TestStrandedSkipSet_StuckPastWindowNotSkipped(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Level-1 window is base<<1 = 60s; wiped 61s ago → due for re-wipe.
	s := &crQuorumState{
		strandedNoProgress: map[string]strandedAddrState{
			stuckTestAddr: {
				noProgress: strandedSurgeryStuckThreshold,
				lastWiped:  now.Add(-(strandedRecoveryCooldown<<1 + time.Second)),
			},
		},
	}
	if skip := s.strandedSkipSet(now); len(skip) != 0 {
		t.Errorf("a stuck addr past its re-wipe window must NOT be skipped: %v", skip)
	}
}

// TestStrandedProbeCoolingDown_BaseCadenceIgnoresBackoff pins that the
// coarse probe gate is BASE cadence only — it ignores any address's
// backoff depth, so a fresh strand is probed at base even during a
// deep-backoff episode (also covers 2c: a fresh strand mid-cooldown is
// picked up on the next base boundary, never at a deep-backoff interval).
func TestStrandedProbeCoolingDown_BaseCadenceIgnoresBackoff(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &crQuorumState{
		// A maxed-out stuck addr is present but must not extend the gate.
		strandedRecoveryLastFired: now.Add(-(strandedRecoveryCooldown + time.Second)),
		strandedNoProgress: map[string]strandedAddrState{
			stuckTestAddr: {
				noProgress: strandedSurgeryStuckThreshold + strandedSurgeryMaxBackoffLevel,
				lastWiped:  now,
			},
		},
	}
	if s.strandedProbeCoolingDown(now) {
		t.Errorf("a probe older than base must NOT be cooling, even with a maxed-out stuck addr present")
	}
	// Within base: still cooling (a fresh strand waits for the next base
	// boundary, never a deep-backoff interval).
	s.strandedRecoveryLastFired = now.Add(-(strandedRecoveryCooldown / 2))
	if !s.strandedProbeCoolingDown(now) {
		t.Errorf("a probe within base must still be cooling")
	}
}

// TestDetectStrandedPeerLinkupStuck_WipedAdvancesStampsAndBacksOff pins
// the wiped-address state machine: consecutive re-strandings of one
// address advance its count AND re-stamp its last-wipe clock; the stuck
// verdict + derived level engage only at the threshold; the event edge
// fires once per stuck episode; and recovery resets everything.
func TestDetectStrandedPeerLinkupStuck_WipedAdvancesStampsAndBacksOff(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &crQuorumState{}
	a := stuckTestAddr

	// Below-threshold surgeries: no stuck, no backoff; the count advances
	// and lastWiped re-stamps to each wipe's clock.
	for i := 1; i < strandedSurgeryStuckThreshold; i++ {
		wipeAt := now.Add(time.Duration(i) * time.Second)
		stuck, fire, backoff := s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, wipeAt)
		if len(stuck) != 0 || fire || backoff != 0 {
			t.Fatalf("surgery %d: stuck=%v fire=%v backoff=%d; want none", i, stuck, fire, backoff)
		}
		rec := s.strandedNoProgress[a]
		if rec.noProgress != i {
			t.Errorf("surgery %d: count = %d, want %d", i, rec.noProgress, i)
		}
		if !rec.lastWiped.Equal(wipeAt) {
			t.Errorf("surgery %d: lastWiped = %v, want %v (each wipe re-stamps)", i, rec.lastWiped, wipeAt)
		}
		if lvl := strandedAddrBackoffLevel(rec.noProgress); lvl != 0 {
			t.Errorf("surgery %d: derived level = %d, want 0 below threshold", i, lvl)
		}
	}

	// Threshold surgery: stuck, event fires, derived level 1.
	thresholdAt := now.Add(time.Duration(strandedSurgeryStuckThreshold) * time.Second)
	stuck, fire, backoff := s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, thresholdAt)
	if len(stuck) != 1 || stuck[0] != a || !fire || backoff != 1 {
		t.Fatalf("threshold: stuck=%v fire=%v backoff=%d; want [%s] true 1", stuck, fire, backoff, a)
	}
	if !s.strandedNoProgress[a].lastWiped.Equal(thresholdAt) {
		t.Errorf("threshold wipe must stamp lastWiped=%v, got %v", thresholdAt, s.strandedNoProgress[a].lastWiped)
	}

	// Still stuck, same signature: no re-fire, derived level climbs.
	_, fire, backoff = s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, now)
	if fire {
		t.Errorf("same stuck episode must not re-fire the event")
	}
	if backoff != 2 {
		t.Errorf("backoff = %d, want 2", backoff)
	}

	// Backoff caps at the max level.
	for range 5 {
		_, _, backoff = s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, now)
	}
	if backoff != strandedSurgeryMaxBackoffLevel {
		t.Errorf("backoff = %d, want cap %d", backoff, strandedSurgeryMaxBackoffLevel)
	}

	// Recovery: the address is neither wiped nor skipped → everything resets.
	stuck, fire, backoff = s.detectStrandedPeerLinkupStuck(nil, nil, nil, now)
	if len(stuck) != 0 || fire || backoff != 0 {
		t.Fatalf("recovery: stuck=%v fire=%v backoff=%d; want none", stuck, fire, backoff)
	}
	if s.strandedLinkupStuck {
		t.Errorf("stuck flag must clear on recovery")
	}
	if len(s.strandedNoProgress) != 0 {
		t.Errorf("recovery must drop the addr from the tracker: %v", s.strandedNoProgress)
	}
}

// TestDetectStrandedPeerLinkupStuck_SkippedCarriesForwardRefreshesFreshness
// pins the skip-carry-forward pace: a skipped stuck addr keeps its
// noProgress count and lastWiped clock UNCHANGED (no advance, no
// re-stamp), stays in the stuck set, and refreshes strandedLinkupStuckAt
// so the Degraded condition stays fresh while genuinely paced.
func TestDetectStrandedPeerLinkupStuck_SkippedCarriesForwardRefreshesFreshness(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &crQuorumState{}
	a := stuckTestAddr

	// Drive a to stuck by wiping it up to the threshold.
	for range strandedSurgeryStuckThreshold {
		s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, t0)
	}
	if !s.strandedLinkupStuck {
		t.Fatalf("precondition: a must be stuck")
	}
	before := s.strandedNoProgress[a]

	// A later pass SKIPS a (paced): the record carries forward unchanged,
	// a stays in the stuck set, and freshness refreshes.
	t1 := t0.Add(time.Minute)
	stuck, _, backoff := s.detectStrandedPeerLinkupStuck(nil, []string{a}, nil, t1)
	if len(stuck) != 1 || stuck[0] != a {
		t.Fatalf("a skipped stuck addr must remain in the stuck set: %v", stuck)
	}
	if backoff < 1 {
		t.Errorf("a skipped stuck addr must keep a >=1 backoff level, got %d", backoff)
	}
	got := s.strandedNoProgress[a]
	if got.noProgress != before.noProgress {
		t.Errorf("skip must NOT advance the no-progress count: got %d want %d", got.noProgress, before.noProgress)
	}
	if !got.lastWiped.Equal(before.lastWiped) {
		t.Errorf("skip must NOT re-stamp lastWiped: got %v want %v", got.lastWiped, before.lastWiped)
	}
	if !s.strandedLinkupStuckAt.Equal(t1) {
		t.Errorf("a skip-carry-forward must refresh strandedLinkupStuckAt to %v, got %v", t1, s.strandedLinkupStuckAt)
	}
}

// TestDetectStrandedPeerLinkupStuck_AuthFailureIsImmediate pins that a
// wiped sentinel whose auth re-propagation failed is stuck on the first
// detection — it can never gossip, so waiting out the threshold is
// pointless.
func TestDetectStrandedPeerLinkupStuck_AuthFailureIsImmediate(t *testing.T) {
	t.Parallel()
	s := &crQuorumState{}
	now := time.Now()
	a := stuckTestAddr2

	stuck, fire, backoff := s.detectStrandedPeerLinkupStuck([]string{a}, nil, []string{a}, now)
	if len(stuck) != 1 || stuck[0] != a || !fire || backoff != 1 {
		t.Fatalf("auth-failed first surgery: stuck=%v fire=%v backoff=%d; want immediate stuck", stuck, fire, backoff)
	}
	// An auth failure on an address NOT wiped this pass is ignored.
	s2 := &crQuorumState{}
	stuck, _, _ = s2.detectStrandedPeerLinkupStuck([]string{a}, nil, []string{"10.9.9.9:26379"}, now)
	if len(stuck) != 0 {
		t.Errorf("auth failure on a non-wiped addr must not stick: %v", stuck)
	}
}

// TestDetectStrandedPeerLinkupStuck_PerAddressIsolation pins that a
// recreated pod (new IP, fresh strand) starts a fresh count instead of
// inheriting the dead pod's stuck count — the map is keyed by address
// and rebuilt each pass.
func TestDetectStrandedPeerLinkupStuck_PerAddressIsolation(t *testing.T) {
	t.Parallel()
	s := &crQuorumState{}
	now := time.Now()
	oldAddr := stuckTestAddr

	// Drive oldAddr to stuck.
	for range strandedSurgeryStuckThreshold {
		s.detectStrandedPeerLinkupStuck([]string{oldAddr}, nil, nil, now)
	}
	if !s.strandedLinkupStuck {
		t.Fatalf("precondition: oldAddr should be stuck")
	}

	// Pod recreated at a new IP: the old addr drops, the new addr is a
	// fresh strand (count 1) — not immediately stuck.
	newAddr := "10.0.0.9:26379"
	stuck, _, backoff := s.detectStrandedPeerLinkupStuck([]string{newAddr}, nil, nil, now)
	if len(stuck) != 0 {
		t.Errorf("recreated pod (new IP) must start fresh, not inherit stuck: %v", stuck)
	}
	if backoff != 0 {
		t.Errorf("backoff must reset when the stuck addr is replaced: %d", backoff)
	}
}

// TestStrandedLinkupStuckActive_FreshnessExpiry pins the Degraded read:
// a fresh stuck flag reads active; a stale one (no surgery pass
// refreshed it within the window) expires and re-arms the event edge.
func TestStrandedLinkupStuckActive_FreshnessExpiry(t *testing.T) {
	t.Parallel()
	s := &crQuorumState{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Drive to stuck at `now`.
	a := stuckTestAddr
	for range strandedSurgeryStuckThreshold {
		s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, now)
	}
	if !s.strandedLinkupStuckActiveOrExpire(now.Add(strandedLinkupStuckFreshnessWindow)) {
		t.Errorf("stuck flag within the window must read active")
	}
	// Past the window with no refresh → expires + re-arms edge.
	if s.strandedLinkupStuckActiveOrExpire(now.Add(strandedLinkupStuckFreshnessWindow + time.Second)) {
		t.Errorf("stale stuck flag must expire")
	}
	if s.strandedLinkupStuck {
		t.Errorf("expiry must clear the flag")
	}
	if s.strandedLinkupStuckEdge != "" {
		t.Errorf("expiry must re-arm the event edge")
	}
}

// TestClearStrandedLinkupStuck pins the healthy-classification reset.
func TestClearStrandedLinkupStuck(t *testing.T) {
	t.Parallel()
	s := &crQuorumState{}
	now := time.Now()
	a := stuckTestAddr
	for range strandedSurgeryStuckThreshold {
		s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, now)
	}
	s.clearStrandedLinkupStuck()
	if s.strandedLinkupStuck || s.strandedNoProgress != nil || s.strandedLinkupStuckEdge != "" {
		t.Errorf("clear must reset all no-progress state: %+v", s)
	}
}

// TestSentinelNamesToAddrs pins the name→addr mapping used to key the
// no-progress tracker by IP (drops names absent from the endpoint set).
func TestSentinelNamesToAddrs(t *testing.T) {
	t.Parallel()
	byName := map[string]string{"vk0-sentinel-0": stuckTestAddr, "vk0-sentinel-1": stuckTestAddr2}
	got := sentinelNamesToAddrs([]string{"vk0-sentinel-1", "gone", "vk0-sentinel-0"}, byName)
	want := []string{stuckTestAddr2, stuckTestAddr}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v (order preserved, unknown dropped)", got, want)
	}
}

// TestDetectStrandedPeerLinkupStuck_MultiAddressStuckSet pins the
// multi-address path: two sentinels wedged in the same pass yield a
// deterministically sorted stuck set, the once-per-episode event fires
// exactly once, an identical set does not re-fire (so Go's randomized
// map iteration can't flap the SentinelPeerLinkupStuck alert), and a
// set change (a third address joining) re-fires exactly once.
func TestDetectStrandedPeerLinkupStuck_MultiAddressStuckSet(t *testing.T) {
	t.Parallel()
	s := &crQuorumState{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Pass the addresses unsorted so a dropped sort.Strings would show:
	// stuckTestAddr2 (10.0.0.2) before stuckTestAddr (10.0.0.1).
	set := []string{stuckTestAddr2, stuckTestAddr}

	var stuck []string
	var fire bool
	fires := 0
	for range strandedSurgeryStuckThreshold {
		stuck, fire, _ = s.detectStrandedPeerLinkupStuck(set, nil, nil, now)
		if fire {
			fires++
		}
	}
	if len(stuck) != 2 || stuck[0] != stuckTestAddr || stuck[1] != stuckTestAddr2 {
		t.Fatalf("stuck set not deterministically sorted: %v", stuck)
	}
	if fires != 1 {
		t.Fatalf("two-address stuck episode must fire exactly once, fired %d", fires)
	}

	// An identical-set pass must not re-fire (signature unchanged).
	if _, fire, _ = s.detectStrandedPeerLinkupStuck(set, nil, nil, now); fire {
		t.Errorf("identical stuck set must not re-fire the event")
	}

	// A third address joining the stuck set changes the signature and
	// re-fires exactly once (it must first reach the threshold itself).
	third := "10.0.0.3:26379"
	extended := []string{third, stuckTestAddr, stuckTestAddr2}
	fires = 0
	for range strandedSurgeryStuckThreshold {
		stuck, fire, _ = s.detectStrandedPeerLinkupStuck(extended, nil, nil, now)
		if fire {
			fires++
		}
	}
	if len(stuck) != 3 {
		t.Fatalf("third address must join the stuck set: %v", stuck)
	}
	if fires != 1 {
		t.Errorf("a changed stuck set (A,B → A,B,C) must re-fire exactly once, fired %d", fires)
	}
}

// TestDetectStrandedPeerLinkupStuck_StaleRecordRestartsCount pins the
// consecutiveness bound on the no-progress count: a record whose last
// wipe is older than the freshness window survived a fold-free stretch
// (unresolvable master, empty endpoints, sustained defers), so a new
// wipe against that address — e.g. a replaced pod stranding at a REUSED
// IP — restarts the count at 1 instead of inheriting the dead episode's
// count and being declared linkup-stuck (and deeply paced) after a
// single wipe.
func TestDetectStrandedPeerLinkupStuck_StaleRecordRestartsCount(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := stuckTestAddr

	// Ancient record (past the freshness window): the count restarts.
	s := &crQuorumState{strandedNoProgress: map[string]strandedAddrState{
		a: {noProgress: 5, lastWiped: now.Add(-(strandedLinkupStuckFreshnessWindow + time.Second))},
	}}
	stuck, fire, backoff := s.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, now)
	if len(stuck) != 0 || fire || backoff != 0 {
		t.Fatalf("stale record must restart the count: stuck=%v fire=%v backoff=%d; want none", stuck, fire, backoff)
	}
	if got := s.strandedNoProgress[a].noProgress; got != 1 {
		t.Errorf("stale record must restart the count at 1, got %d", got)
	}

	// Boundary companion: a record exactly AT the window is still
	// consecutive evidence and advances normally.
	s2 := &crQuorumState{strandedNoProgress: map[string]strandedAddrState{
		a: {noProgress: 5, lastWiped: now.Add(-strandedLinkupStuckFreshnessWindow)},
	}}
	stuck, _, _ = s2.detectStrandedPeerLinkupStuck([]string{a}, nil, nil, now)
	if got := s2.strandedNoProgress[a].noProgress; got != 6 {
		t.Errorf("an at-the-window record must advance the count to 6, got %d", got)
	}
	if len(stuck) != 1 || stuck[0] != a {
		t.Errorf("an advanced above-threshold count must stay stuck: %v", stuck)
	}
}
