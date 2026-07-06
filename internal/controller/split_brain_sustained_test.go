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

	"github.com/ioxie/velkir/internal/sentinel"
)

// TestUpdateSplitBrainSustained_ResetOnAgreement: agreement
// (QuorumStatusOK) resets splitBrainSince to nil and returns 0. The
// chart-shipped ValkeySplitBrainDetected alert keys off the gauge
// value, so agreement must clear the gauge promptly to avoid
// stale-alert firing. Each pass carries a fresh poll stamp (polledAt
// tracking now), the live-observer shape.
func TestUpdateSplitBrainSustained_ResetOnAgreement(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	// First observation: disagreement (Lost) starts.
	got, _ := state.updateSplitBrainSustained(true /* present */, sentinel.QuorumStatusLost, t0, t0)
	if got != 0 {
		t.Errorf("first disagreement observation must return 0, got %f", got)
	}
	if state.splitBrainSince == nil {
		t.Fatalf("first disagreement observation must stamp splitBrainSince")
	}

	// Continuing disagreement returns elapsed seconds.
	t1 := t0.Add(45 * time.Second)
	got, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t1, t1)
	if got != 45 {
		t.Errorf("continuing disagreement should report elapsed=45s, got %f", got)
	}

	// Agreement (OK): reset to 0 + clear timestamp.
	t2 := t1.Add(10 * time.Second)
	got, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusOK, t2, t2)
	if got != 0 {
		t.Errorf("agreement must reset gauge to 0, got %f", got)
	}
	if state.splitBrainSince != nil {
		t.Errorf("agreement must clear splitBrainSince, got %v", state.splitBrainSince)
	}

	// Re-entering disagreement starts a NEW episode (gauge=0, not
	// the previous episode's duration).
	t3 := t2.Add(5 * time.Second)
	got, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t3, t3)
	if got != 0 {
		t.Errorf("re-entering disagreement must start a fresh episode (gauge=0), got %f", got)
	}
}

// TestUpdateSplitBrainSustained_NotPresentResetsToZero pins the
// behavior the chart-shipped alert depends on: when the observer
// hasn't yet published a snapshot (pre-Ensure / pre-first-poll),
// the gauge must read 0. Without this, a re-created CR with the
// same name could inherit the previous episode's elapsed duration
// and false-fire the alert immediately on bootstrap.
func TestUpdateSplitBrainSustained_NotPresentResetsToZero(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	// Pre-seed an in-flight disagreement episode.
	_, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t0, t0)
	if state.splitBrainSince == nil {
		t.Fatalf("pre-seed disagreement must stamp splitBrainSince")
	}

	// !Present arrives (e.g., observer's first poll hasn't run yet).
	// Must reset.
	t1 := t0.Add(30 * time.Second)
	got, _ := state.updateSplitBrainSustained(false /* present */, sentinel.QuorumStatusLost, time.Time{}, t1)
	if got != 0 {
		t.Errorf("!Present must reset gauge to 0, got %f", got)
	}
	if state.splitBrainSince != nil {
		t.Errorf("!Present must clear splitBrainSince, got %v", state.splitBrainSince)
	}

	// !Present with q==Unknown (the absent snapshot's zero-value
	// Quorum) must ALSO reset: the !present check short-circuits
	// before the Unknown no-op branch, so an absent observer never
	// preserves a stale episode regardless of the quorum field.
	_, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t1, t1)
	if state.splitBrainSince == nil {
		t.Fatalf("re-seed disagreement must stamp splitBrainSince")
	}
	if got, _ := state.updateSplitBrainSustained(false, sentinel.QuorumStatusUnknown, time.Time{}, t1.Add(30*time.Second)); got != 0 {
		t.Errorf("!Present with Unknown must reset gauge to 0, got %f", got)
	}
	if state.splitBrainSince != nil {
		t.Errorf("!Present with Unknown must clear splitBrainSince, got %v", state.splitBrainSince)
	}
}

// TestUpdateSplitBrainSustained_ContiguousDuration: the gauge
// reports elapsed seconds since the FIRST observation in the current
// episode, not since the last call, as long as every pass carries a
// fresh poll stamp.
func TestUpdateSplitBrainSustained_ContiguousDuration(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	// Episode starts at t0.
	_, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t0, t0)

	// Multiple calls during the same episode return cumulative
	// elapsed, not delta-since-last-call.
	for _, sec := range []int{10, 40, 90} {
		at := t0.Add(time.Duration(sec) * time.Second)
		if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, at, at); got != float64(sec) {
			t.Errorf("t0+%ds should report %d, got %f", sec, sec, got)
		}
	}
}

// TestUpdateSplitBrainSustained_FrozenPollFreezesReading pins the
// freshness cap inside the staleness window: a wedged observer whose
// final Lost snapshot is replayed with a FROZEN LastPolledAt (pub/sub
// republishes carry it forward unchanged) must not grow the gauge on
// reconcile churn alone. The reading freezes at the last
// live-poll-confirmed elapsed and resumes once a strictly-newer poll
// stamp arrives — so the chart's sustained-disagreement alert can only
// be driven by data the observer actually re-measured. (Past the
// staleness window the frozen episode EXPIRES instead — see
// _FrozenPollExpiresEpisode.)
func TestUpdateSplitBrainSustained_FrozenPollFreezesReading(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	_, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t0, t0)
	pollAt := t0.Add(10 * time.Second)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, pollAt, pollAt); got != 10 {
		t.Fatalf("fresh poll at t0+10s should report 10, got %f", got)
	}

	// Observer wedges: reconciles keep arriving but the poll stamp is
	// frozen at t0+10s. The reading must hold at 10 while the wall-clock
	// advances inside the staleness window (last confirmation t0+10s).
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, pollAt, t0.Add(40*time.Second)); got != 10 {
		t.Errorf("frozen poll at t0+40s must hold the reading at 10, got %f", got)
	}
	// A replayed Unknown with the same frozen stamp must hold too.
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusUnknown, pollAt, t0.Add(55*time.Second)); got != 10 {
		t.Errorf("frozen-poll Unknown replay must hold the reading at 10, got %f", got)
	}
	// Exactly AT the staleness bound (inclusive): still held, not expired.
	atBound := pollAt.Add(splitBrainConfirmationStaleness)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, pollAt, atBound); got != 10 {
		t.Errorf("frozen poll at the staleness bound must still hold the reading at 10, got %f", got)
	}

	// The observer recovers inside the window: a strictly-newer poll
	// re-confirms the SAME episode and the reading catches up to the
	// confirming reconcile (elapsed since the original onset).
	t1 := atBound.Add(time.Second)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t1, t1); got != t1.Sub(t0).Seconds() {
		t.Errorf("a fresh poll must resume the reading (%v), got %f", t1.Sub(t0).Seconds(), got)
	}
}

// TestUpdateSplitBrainSustained_FrozenPollExpiresEpisode pins the
// staleness expiry: once no fresh poll has re-confirmed the episode
// within splitBrainConfirmationStaleness, the frozen reading drops to 0
// (episode dropped) instead of holding its last value — and the
// critical sustained-disagreement alert with it — until operator
// restart. A fresh Lost after the observer recovers starts a NEW
// episode with episodeStarted=true (the SplitBrainDetected re-fire is
// intended: continuity across the wedge could not be verified).
func TestUpdateSplitBrainSustained_FrozenPollExpiresEpisode(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	if _, started := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t0, t0); !started {
		t.Fatalf("first Lost must start the episode")
	}
	pollAt := t0.Add(10 * time.Second)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, pollAt, pollAt); got != 10 {
		t.Fatalf("fresh poll at t0+10s should report 10, got %f", got)
	}

	// One tick past the staleness bound with the poll stamp still
	// frozen: the episode expires and the gauge reads 0.
	expiredAt := pollAt.Add(splitBrainConfirmationStaleness + time.Second)
	got, started := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, pollAt, expiredAt)
	if got != 0 || started {
		t.Fatalf("a frozen poll past the staleness bound must expire the episode (0, false); got %f %v", got, started)
	}
	if state.splitBrainSince != nil {
		t.Fatalf("expiry must drop splitBrainSince")
	}

	// Observer recovered: the next fresh Lost is a NEW episode — gauge
	// restarts from 0 and the once-per-episode edge fires again.
	t1 := expiredAt.Add(30 * time.Second)
	got, started = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t1, t1)
	if got != 0 || !started {
		t.Fatalf("post-expiry fresh Lost must start a new episode (0, true); got %f %v", got, started)
	}
	t2 := t1.Add(50 * time.Second)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t2, t2); got != 50 {
		t.Errorf("the new episode must accrue from its own onset (50), got %f", got)
	}
}

// TestUpdateSplitBrainSustained_PersistentWedgeDoesNotRefire pins the
// no-churn contract: once an episode has expired for staleness, a
// permanently-wedged observer that keeps replaying the SAME frozen poll
// must NOT re-arm the episode — otherwise the once-per-episode edge
// would re-fire SplitBrainDetected (and re-Inc its counter) every
// staleness window against data the expiry already deemed unverifiable.
// Only a strictly-newer poll (a genuine re-measurement) re-arms.
func TestUpdateSplitBrainSustained_PersistentWedgeDoesNotRefire(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	frozen := t0 // the observer's last live poll; never advances again

	if _, started := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, frozen, t0); !started {
		t.Fatalf("first Lost must start the episode")
	}
	// Expire it: same frozen poll, reconcile past the staleness bound.
	expiredAt := frozen.Add(splitBrainConfirmationStaleness + time.Second)
	if got, started := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, frozen, expiredAt); got != 0 || started {
		t.Fatalf("frozen poll past the bound must expire (0, false); got %f %v", got, started)
	}

	// Observer stays wedged: the SAME frozen poll replays across several
	// later reconciles. Each must stay quiet — no re-armed episode.
	for i, dt := range []time.Duration{40 * time.Second, 90 * time.Second, 5 * time.Minute} {
		now := expiredAt.Add(dt)
		got, started := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, frozen, now)
		if got != 0 || started {
			t.Fatalf("wedge replay #%d (frozen poll, now=%v) must not re-fire; got %f %v", i, now, got, started)
		}
		if state.splitBrainSince != nil {
			t.Fatalf("wedge replay #%d must not re-stamp splitBrainSince", i)
		}
	}

	// Observer recovers: a strictly-newer poll is a genuine re-detection
	// and DOES start a new episode (the intended once-per-recovery fire).
	fresh := frozen.Add(10 * time.Minute)
	if got, started := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, fresh, fresh); got != 0 || !started {
		t.Fatalf("a strictly-newer poll must re-arm the episode (0, true); got %f %v", got, started)
	}
}

// TestUpdateSplitBrainSustained_UnknownPlaceholderDoesNotAdvance is
// the restart-placeholder regression guard. The observer publishes Present &&
// QuorumStatusUnknown as the restart placeholder (Source=SourceNone,
// LastPolledAt=zero) when it can't yet reach a quorum of peers — e.g.
// right after an operator restart of a HEALTHY cluster. That
// placeholder must NOT advance the sustained-seconds gauge, or the
// CRITICAL ValkeySplitBrainDetected alert false-pages on restart even
// though the event/Degraded gate (gated on Quorum==Lost) correctly
// stays quiet.
func TestUpdateSplitBrainSustained_UnknownPlaceholderDoesNotAdvance(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	// Restart placeholder: Present + Unknown with a zero LastPolledAt,
	// no prior episode. Gauge must stay 0 and no episode may be stamped.
	got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusUnknown, time.Time{}, t0)
	if got != 0 {
		t.Errorf("Unknown placeholder must read 0, got %f", got)
	}
	if state.splitBrainSince != nil {
		t.Fatalf("Unknown placeholder must NOT stamp splitBrainSince")
	}

	// Repeated Unknown over a window longer than the CRITICAL alert
	// threshold (60s) must still read 0 — the bug was that this
	// crossed the threshold and false-paged.
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusUnknown, time.Time{}, t0.Add(120*time.Second)); got != 0 {
		t.Errorf("sustained Unknown must stay 0 (no false-page), got %f", got)
	}
	if state.splitBrainSince != nil {
		t.Fatalf("sustained Unknown must NOT stamp splitBrainSince")
	}

	// A genuine Lost after the Unknown window still starts a real
	// episode and advances — Unknown is a no-op, not a permanent mute.
	t1 := t0.Add(130 * time.Second)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t1, t1); got != 0 {
		t.Errorf("first genuine Lost must start a fresh episode (gauge=0), got %f", got)
	}
	t2 := t1.Add(50 * time.Second)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t2, t2); got != 50 {
		t.Errorf("genuine Lost must advance the gauge, got %f", got)
	}
}

// TestUpdateSplitBrainSustained_EpisodeStartedEdge pins the
// episodeStarted return that gates the SplitBrainDetected event +
// counter to exactly once per disagreement episode: true only
// on the nil→set edge of splitBrainSince, never on continuation,
// Unknown, agreement, or absent snapshots — and true again only
// after an OK/!Present reset starts a genuinely new episode.
func TestUpdateSplitBrainSustained_EpisodeStartedEdge(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	steps := []struct {
		name        string
		present     bool
		q           sentinel.QuorumStatus
		wantStarted bool
	}{
		{"unknown placeholder pre-episode", true, sentinel.QuorumStatusUnknown, false},
		{"first Lost starts episode", true, sentinel.QuorumStatusLost, true},
		{"continuing Lost", true, sentinel.QuorumStatusLost, false},
		{"unknown mid-episode preserves", true, sentinel.QuorumStatusUnknown, false},
		{"Lost after Unknown — same episode", true, sentinel.QuorumStatusLost, false},
		{"OK resets", true, sentinel.QuorumStatusOK, false},
		{"second episode starts", true, sentinel.QuorumStatusLost, true},
		{"absent snapshot resets", false, sentinel.QuorumStatusUnknown, false},
		{"third episode starts", true, sentinel.QuorumStatusLost, true},
	}
	for i, s := range steps {
		at := t0.Add(time.Duration(i) * 10 * time.Second)
		_, started := state.updateSplitBrainSustained(s.present, s.q, at, at)
		if started != s.wantStarted {
			t.Errorf("step %d (%s): episodeStarted=%v, want %v", i, s.name, started, s.wantStarted)
		}
	}
}

// TestUpdateSplitBrainSustained_UnknownPreservesActiveEpisode pins
// the no-op semantics for the OTHER Unknown case: a transient
// observer-can't-decide window in the MIDDLE of a genuine Lost episode
// must neither reset the episode (which would mute a real alert) nor
// be treated as a fresh start. The onset stamp is preserved and the
// sustained value keeps tracking from the original onset as long as
// the polls stay fresh — consistent with updateQuorumSuppression
// preserving quorumLostSince across Unknown.
func TestUpdateSplitBrainSustained_UnknownPreservesActiveEpisode(t *testing.T) {
	state := &crQuorumState{}
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	// Genuine Lost episode starts at t0.
	_, _ = state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t0, t0)
	if state.splitBrainSince == nil {
		t.Fatalf("Lost must stamp splitBrainSince")
	}
	onsetVal := *state.splitBrainSince

	// Unknown flickers mid-episode with a fresh poll stamp: episode must
	// be preserved and the gauge reports elapsed-since-onset (not reset,
	// not re-stamped).
	t1 := t0.Add(20 * time.Second)
	got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusUnknown, t1, t1)
	if got != 20 {
		t.Errorf("Unknown mid-episode must report elapsed-since-onset=20s, got %f", got)
	}
	if state.splitBrainSince == nil || !state.splitBrainSince.Equal(onsetVal) {
		t.Fatalf("Unknown must NOT reset or re-stamp the active episode onset")
	}

	// Back to Lost: still the same episode, elapsed continues from
	// the original onset (not re-zeroed by the Unknown window).
	t2 := t0.Add(35 * time.Second)
	if got, _ := state.updateSplitBrainSustained(true, sentinel.QuorumStatusLost, t2, t2); got != 35 {
		t.Errorf("Lost after Unknown must continue the same episode (35s), got %f", got)
	}
}
