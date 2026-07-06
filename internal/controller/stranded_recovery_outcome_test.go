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
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// outcomeHarness wires a reconciler + CR + endpoints for driving
// applyStrandedRecoveryOutcome with synthetic RecoverStrandedSentinels
// results — the dispatcher's outcome wiring without a live manager.
func outcomeHarness(t *testing.T) (*ValkeyReconciler, *valkeyv1beta1.Valkey, types.NamespacedName, []sentinel.Endpoint, *k8sevents.FakeRecorder, *crQuorumState) {
	t.Helper()
	rec := k8sevents.NewFakeRecorder(64)
	r := &ValkeyReconciler{Recorder: rec}
	v := newCR(valkeyv1beta1.ModeSentinel)
	v.Spec.Sentinel = &valkeyv1beta1.SentinelPodSpec{MasterName: "vk0", Replicas: 3, Quorum: 2}
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	eps := []sentinel.Endpoint{
		{Name: "vk0-sentinel-0", Addr: stuckTestAddr},
		{Name: "vk0-sentinel-1", Addr: stuckTestAddr2},
		{Name: "vk0-sentinel-2", Addr: "10.0.0.3:26379"},
	}
	return r, v, cr, eps, rec, r.stateFor(cr).quorumTracker()
}

func countEvent(rec *k8sevents.FakeRecorder, reason string) int {
	n := 0
	for _, e := range drainAllEvents(rec.Events) {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}

// TestApplyStrandedRecoveryOutcome_StuckThenClear drives the dispatcher
// wiring end to end on synthetic results: repeated empty-peer stranding
// of one sentinel reaches the stuck threshold (derived level climbs, one
// event fires, the addr enters the skip-set), then a Healthy
// classification clears it.
func TestApplyStrandedRecoveryOutcome_StuckThenClear(t *testing.T) {
	r, v, cr, eps, rec, state := outcomeHarness(t)
	ctx := context.Background()
	strandedOut := sentinel.StrandedRecoveryResult{
		Stranded:          []string{"vk0-sentinel-0"},
		EmptyPeerStranded: []string{"vk0-sentinel-0"},
		ResetResults:      []sentinel.ResetResult{{Name: "vk0-sentinel-0"}},
		Probed:            true,
	}

	for range strandedSurgeryStuckThreshold {
		r.applyStrandedRecoveryOutcome(ctx, v, cr, state, strandedOut, eps, "10.0.0.9")
	}
	if !state.strandedLinkupStuck {
		t.Fatalf("expected stuck after %d dispatches", strandedSurgeryStuckThreshold)
	}
	if got := strandedAddrBackoffLevel(state.strandedNoProgress[stuckTestAddr].noProgress); got != 1 {
		t.Errorf("derived level = %d, want 1 on first stuck", got)
	}
	if _, ok := state.strandedSkipSet(time.Now())[stuckTestAddr]; !ok {
		t.Errorf("a freshly-stuck addr must enter the skip-set (paced)")
	}
	if got := countEvent(rec, "SentinelPeerLinkupStuck"); got != 1 {
		t.Errorf("SentinelPeerLinkupStuck fired %d times, want exactly 1 per episode", got)
	}

	// Healthy classification clears everything.
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{Healthy: true, Probed: true}, eps, "10.0.0.9")
	if state.strandedLinkupStuck || len(state.strandedNoProgress) != 0 {
		t.Errorf("Healthy must clear stuck+tracker; got stuck=%v map=%v", state.strandedLinkupStuck, state.strandedNoProgress)
	}
}

// TestApplyStrandedRecoveryOutcome_FreshWipedWhileOtherSkipped is the
// ticket headline through the dispatcher wiring: a single pass wipes a
// FRESH sentinel while pacing (skipping) a wedged one — the fresh addr
// advances to count 1 at base level 0, and the wedged addr is carried
// forward at its stuck count with its clock preserved and freshness
// refreshed.
func TestApplyStrandedRecoveryOutcome_FreshWipedWhileOtherSkipped(t *testing.T) {
	r, v, cr, eps, _, state := outcomeHarness(t)
	ctx := context.Background()
	seededAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Pre-seed a wedged sentinel (sentinel-1 / stuckTestAddr2) at a stuck count.
	state.strandedNoProgress = map[string]strandedAddrState{
		stuckTestAddr2: {noProgress: strandedSurgeryStuckThreshold + 1, lastWiped: seededAt},
	}
	foldAt := seededAt.Add(time.Hour)
	r.nowFunc = func() time.Time { return foldAt }

	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		Stranded:          []string{"vk0-sentinel-0"},
		EmptyPeerStranded: []string{"vk0-sentinel-0"},
		SkippedStranded:   []string{"vk0-sentinel-1"},
		Probed:            true,
	}, eps, "10.0.0.9")

	// Fresh advanced to count 1 (level 0 = base cadence), clock stamped.
	fresh := state.strandedNoProgress[stuckTestAddr]
	if fresh.noProgress != 1 {
		t.Errorf("fresh wiped addr must advance to count 1, got %d", fresh.noProgress)
	}
	if !fresh.lastWiped.Equal(foldAt) {
		t.Errorf("fresh wiped addr must stamp lastWiped=%v, got %v", foldAt, fresh.lastWiped)
	}
	if strandedAddrBackoffLevel(fresh.noProgress) != 0 {
		t.Errorf("fresh wiped addr must be at base level 0")
	}
	// Wedged carried forward UNCHANGED (count + clock), still stuck.
	wedged := state.strandedNoProgress[stuckTestAddr2]
	if wedged.noProgress != strandedSurgeryStuckThreshold+1 {
		t.Errorf("wedged skipped addr must keep its stuck count, got %d", wedged.noProgress)
	}
	if !wedged.lastWiped.Equal(seededAt) {
		t.Errorf("wedged skipped addr must NOT re-stamp lastWiped, got %v", wedged.lastWiped)
	}
	if !state.strandedLinkupStuckAt.Equal(foldAt) {
		t.Errorf("the pass must refresh strandedLinkupStuckAt to %v, got %v", foldAt, state.strandedLinkupStuckAt)
	}
	if !state.strandedLinkupStuck {
		t.Errorf("the wedged addr keeps the stuck flag set")
	}
}

// TestApplyStrandedRecoveryOutcome_SkipOnlyRefreshesFreshness pins that a
// skip-only pass (SkippedStranded set, nothing wiped, Probed=true) is NOT
// treated as a gate-defer: it carries the wedged record forward, keeps +
// refreshes the stuck flag, and stamps the probe-cadence clock.
func TestApplyStrandedRecoveryOutcome_SkipOnlyRefreshesFreshness(t *testing.T) {
	r, v, cr, eps, _, state := outcomeHarness(t)
	ctx := context.Background()
	seededAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	state.strandedNoProgress = map[string]strandedAddrState{
		stuckTestAddr2: {noProgress: strandedSurgeryStuckThreshold + 1, lastWiped: seededAt},
	}
	state.strandedLinkupStuck = true
	state.strandedLinkupStuckAt = seededAt

	foldAt := seededAt.Add(time.Hour)
	r.nowFunc = func() time.Time { return foldAt }

	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		SkippedStranded: []string{"vk0-sentinel-1"},
		Probed:          true,
	}, eps, "10.0.0.9")

	wedged := state.strandedNoProgress[stuckTestAddr2]
	if wedged.noProgress != strandedSurgeryStuckThreshold+1 || !wedged.lastWiped.Equal(seededAt) {
		t.Errorf("skip-only pass must carry the wedged record forward unchanged, got %+v", wedged)
	}
	if !state.strandedLinkupStuck {
		t.Errorf("skip-only pass must keep the stuck flag (the wedge is re-confirmed)")
	}
	if !state.strandedLinkupStuckAt.Equal(foldAt) {
		t.Errorf("skip-only pass must refresh strandedLinkupStuckAt to %v, got %v", foldAt, state.strandedLinkupStuckAt)
	}
	if !state.strandedRecoveryLastFired.Equal(foldAt) {
		t.Errorf("skip-only pass must stamp the probe-cadence clock (Probed=true), got %v", state.strandedRecoveryLastFired)
	}
}

// TestApplyStrandedRecoveryOutcome_GhostNotCounted pins that a
// ghost-reap target folded into out.Stranded (but NOT in
// EmptyPeerStranded) never advances the no-progress tracker — a healthy
// gossiping survivor must not be declared linkup-stuck.
func TestApplyStrandedRecoveryOutcome_GhostNotCounted(t *testing.T) {
	r, v, cr, eps, _, state := outcomeHarness(t)
	ctx := context.Background()
	// Every pass wipes a ghost-reap survivor: out.Stranded carries it,
	// but EmptyPeerStranded is empty (intact peer-list).
	ghostOut := sentinel.StrandedRecoveryResult{
		Stranded:          []string{"vk0-sentinel-2"}, // ghost, for the event
		EmptyPeerStranded: nil,                        // NOT an empty-peer strand
		ResetResults:      []sentinel.ResetResult{{Name: "vk0-sentinel-2"}},
		Probed:            true,
	}
	for range strandedSurgeryStuckThreshold + 2 {
		r.applyStrandedRecoveryOutcome(ctx, v, cr, state, ghostOut, eps, "10.0.0.9")
	}
	if state.strandedLinkupStuck {
		t.Errorf("a ghost-reap-only dispatch must never declare linkup-stuck")
	}
	if len(state.strandedNoProgress) != 0 {
		t.Errorf("ghost target must not enter the no-progress map: %v", state.strandedNoProgress)
	}
}

// TestApplyStrandedRecoveryOutcome_RepointOnlyResetsStranded pins that a
// repoint-only dispatch (out.Repointed set, EmptyPeerStranded empty)
// does not advance the stranded no-progress count and resets a prior
// stranded streak.
func TestApplyStrandedRecoveryOutcome_RepointOnlyResetsStranded(t *testing.T) {
	r, v, cr, eps, _, state := outcomeHarness(t)
	ctx := context.Background()
	// Build a partial stranded streak first.
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		Stranded: []string{"vk0-sentinel-0"}, EmptyPeerStranded: []string{"vk0-sentinel-0"}, Probed: true,
	}, eps, "10.0.0.9")
	if state.strandedNoProgress[stuckTestAddr].noProgress != 1 {
		t.Fatalf("precondition: expected count 1, got %v", state.strandedNoProgress)
	}
	// A repoint-only pass (no empty-peer strand) resets the tracker.
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		Repointed: []string{"vk0-sentinel-1"}, EmptyPeerStranded: nil, Probed: true,
	}, eps, "10.0.0.9")
	if len(state.strandedNoProgress) != 0 {
		t.Errorf("repoint-only pass must reset the stranded no-progress map: %v", state.strandedNoProgress)
	}
}

// TestApplyStrandedRecoveryOutcome_GateDeferLeavesState pins that a
// gate-deferred result leaves the no-progress state (count + freshness
// stamp) untouched, while the Probed flag governs the probe-cadence
// stamp: a Probed=true post-classification defer DOES stamp
// strandedRecoveryLastFired (a classification probe ran); a Probed=false
// pre-classification early-return stamps nothing and folds nothing.
func TestApplyStrandedRecoveryOutcome_GateDeferLeavesState(t *testing.T) {
	r, v, cr, eps, _, state := outcomeHarness(t)
	ctx := context.Background()
	// Drive to stuck.
	for range strandedSurgeryStuckThreshold {
		r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
			Stranded: []string{"vk0-sentinel-0"}, EmptyPeerStranded: []string{"vk0-sentinel-0"}, Probed: true,
		}, eps, "10.0.0.9")
	}
	beforeCount := state.strandedNoProgress[stuckTestAddr].noProgress
	beforeStamp := state.strandedLinkupStuckAt

	// Probed=true gate-defer at a later clock: no-progress state is left
	// alone, but the probe-cadence clock IS stamped (a probe ran).
	deferAt := beforeStamp.Add(time.Minute)
	r.nowFunc = func() time.Time { return deferAt }
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{Probed: true}, eps, "10.0.0.9")

	if state.strandedNoProgress[stuckTestAddr].noProgress != beforeCount {
		t.Errorf("gate-defer must not change the no-progress count")
	}
	if !state.strandedLinkupStuck {
		t.Errorf("gate-defer must not clear the stuck flag")
	}
	if !state.strandedLinkupStuckAt.Equal(beforeStamp) {
		t.Errorf("gate-defer must NOT refresh the freshness stamp (a persistent defer must age out)")
	}
	if !state.strandedRecoveryLastFired.Equal(deferAt) {
		t.Errorf("a Probed gate-defer must stamp the probe-cadence clock, got %v want %v", state.strandedRecoveryLastFired, deferAt)
	}

	// A Probed=false defer (pre-classification early-return) stamps
	// nothing and folds nothing.
	laterAt := deferAt.Add(time.Minute)
	r.nowFunc = func() time.Time { return laterAt }
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{}, eps, "10.0.0.9")
	if !state.strandedRecoveryLastFired.Equal(deferAt) {
		t.Errorf("a Probed=false defer must NOT stamp the probe-cadence clock; got %v want %v", state.strandedRecoveryLastFired, deferAt)
	}
	if state.strandedNoProgress[stuckTestAddr].noProgress != beforeCount {
		t.Errorf("a Probed=false defer must not change the no-progress count")
	}
}

// TestApplyStrandedRecoveryOutcome_AuthFailureStucksImmediately pins the
// auth-wedge glue: a synthetic result whose EmptyPeerStranded sentinel
// also appears in AuthFailures is declared linkup-stuck on the FIRST
// dispatch (a sentinel that can't AUTH can never gossip), not after the
// threshold.
func TestApplyStrandedRecoveryOutcome_AuthFailureStucksImmediately(t *testing.T) {
	r, v, cr, eps, rec, state := outcomeHarness(t)
	ctx := context.Background()

	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		Stranded:          []string{"vk0-sentinel-0"},
		EmptyPeerStranded: []string{"vk0-sentinel-0"},
		AuthFailures:      []string{"vk0-sentinel-0"},
		Probed:            true,
	}, eps, "10.0.0.9")

	if !state.strandedLinkupStuck {
		t.Fatalf("an auth-failed wipe must declare linkup-stuck on the first dispatch")
	}
	if got := strandedAddrBackoffLevel(state.strandedNoProgress[stuckTestAddr].noProgress); got != 1 {
		t.Errorf("derived level = %d, want 1", got)
	}
	if got := countEvent(rec, "SentinelPeerLinkupStuck"); got != 1 {
		t.Errorf("SentinelPeerLinkupStuck fired %d times, want 1 on the first auth-failed dispatch", got)
	}
}

// TestApplyStrandedRecoveryOutcome_StaleEpochRepoint pins the stale-epoch
// event + audit wiring AND the gate-defer inclusion: a result carrying
// only StaleEpochRepointed fires a SentinelStaleEpochRepoint Warning and
// is NOT read as a gate-defer (the probe-cadence clock is stamped).
func TestApplyStrandedRecoveryOutcome_StaleEpochRepoint(t *testing.T) {
	r, v, cr, eps, rec, state := outcomeHarness(t)
	ctx := context.Background()
	foldAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.nowFunc = func() time.Time { return foldAt }

	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		StaleEpochRepointed: []string{"vk0-sentinel-1"},
		Probed:              true,
	}, eps, "10.0.0.9")

	if got := countEvent(rec, "SentinelStaleEpochRepoint"); got != 1 {
		t.Errorf("SentinelStaleEpochRepoint fired %d times, want 1", got)
	}
	if !state.strandedRecoveryLastFired.Equal(foldAt) {
		t.Errorf("a stale-epoch-only pass must NOT be a gate-defer; probe clock must stamp %v, got %v",
			foldAt, state.strandedRecoveryLastFired)
	}
}

// TestApplyStrandedRecoveryOutcome_StaleEpochOnlyResetsStranded mirrors
// the repoint-only reset: a stale-epoch-only pass resets the empty-peer
// no-progress tracker.
func TestApplyStrandedRecoveryOutcome_StaleEpochOnlyResetsStranded(t *testing.T) {
	r, v, cr, eps, _, state := outcomeHarness(t)
	ctx := context.Background()
	// Build a partial stranded streak first.
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		Stranded: []string{"vk0-sentinel-0"}, EmptyPeerStranded: []string{"vk0-sentinel-0"}, Probed: true,
	}, eps, "10.0.0.9")
	if state.strandedNoProgress[stuckTestAddr].noProgress != 1 {
		t.Fatalf("precondition: expected count 1, got %v", state.strandedNoProgress)
	}
	// A stale-epoch-only pass (no empty-peer strand) resets the tracker.
	r.applyStrandedRecoveryOutcome(ctx, v, cr, state, sentinel.StrandedRecoveryResult{
		StaleEpochRepointed: []string{"vk0-sentinel-1"}, EmptyPeerStranded: nil, Probed: true,
	}, eps, "10.0.0.9")
	if len(state.strandedNoProgress) != 0 {
		t.Errorf("stale-epoch-only pass must reset the stranded no-progress map: %v", state.strandedNoProgress)
	}
}
