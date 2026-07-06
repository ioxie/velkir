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

// TestQuorumSuppressionTunables_DefaultsFromZero verifies that a zero-
// valued tunables struct returns the const safety floors. This is the
// production contract: an operator that does not opt into
// `--allow-test-overrides` reads the const defaults unconditionally.
func TestQuorumSuppressionTunables_DefaultsFromZero(t *testing.T) {
	t.Parallel()
	var tt QuorumSuppressionTunables
	if got := tt.lossThreshold(); got != quorumLossSuppressionThreshold {
		t.Errorf("lossThreshold default: got %s, want %s", got, quorumLossSuppressionThreshold)
	}
	if got := tt.recoveryPolls(); got != quorumRecoveryHysteresisPolls {
		t.Errorf("recoveryPolls default: got %d, want %d", got, quorumRecoveryHysteresisPolls)
	}
	if got := tt.phase11Timeout(); got != phase11DefaultTimeout {
		t.Errorf("phase11Timeout default: got %s, want %s", got, phase11DefaultTimeout)
	}
}

// TestQuorumSuppressionTunables_NonZeroOverrides verifies each override
// field is honoured when non-zero. Each field is independent — setting
// one does not change the defaulting of the others.
func TestQuorumSuppressionTunables_NonZeroOverrides(t *testing.T) {
	t.Parallel()
	tt := QuorumSuppressionTunables{
		LossThreshold:  5 * time.Second,
		RecoveryPolls:  1,
		Phase11Timeout: 10 * time.Second,
	}
	if got := tt.lossThreshold(); got != 5*time.Second {
		t.Errorf("lossThreshold override: got %s", got)
	}
	if got := tt.recoveryPolls(); got != 1 {
		t.Errorf("recoveryPolls override: got %d", got)
	}
	if got := tt.phase11Timeout(); got != 10*time.Second {
		t.Errorf("phase11Timeout override: got %s", got)
	}
}

// TestUpdateQuorumSuppression_OverridenLossThreshold drives the gate
// with a 5s loss threshold (vs the 60s default) and asserts that the
// gate fires at the tightened cadence.
func TestUpdateQuorumSuppression_OverridenLossThreshold(t *testing.T) {
	t.Parallel()
	state := &crQuorumState{}
	tt := QuorumSuppressionTunables{LossThreshold: 5 * time.Second}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Step 1: Lost at t0 — opens the loss episode, no entry.
	enter, exit := state.updateQuorumSuppression(sentinel.QuorumStatusLost, t0, t0, tt)
	if enter || exit {
		t.Fatalf("step 1: enter=%v exit=%v want both false", enter, exit)
	}
	if state.suppressionActive {
		t.Fatalf("step 1: suppressionActive=true want false")
	}
	// Step 2: at t0+4s Lost — still below 5s threshold.
	enter, _ = state.updateQuorumSuppression(sentinel.QuorumStatusLost, t0.Add(4*time.Second), t0.Add(4*time.Second), tt)
	if enter {
		t.Fatalf("step 2: enter=true before threshold")
	}
	// Step 3: at t0+5s Lost — exactly at threshold, fires.
	enter, _ = state.updateQuorumSuppression(sentinel.QuorumStatusLost, t0.Add(5*time.Second), t0.Add(5*time.Second), tt)
	if !enter {
		t.Fatalf("step 3: enter=false at threshold (5s)")
	}
	if !state.suppressionActive {
		t.Fatalf("step 3: suppressionActive=false after entry")
	}
}

// TestUpdateQuorumSuppression_OverridenRecoveryPolls drives the gate
// recovery path with RecoveryPolls=1 (vs the 2-poll default) and
// asserts the gate clears on the first fresh OK poll, not the second.
func TestUpdateQuorumSuppression_OverridenRecoveryPolls(t *testing.T) {
	t.Parallel()
	state := &crQuorumState{suppressionActive: true}
	tt := QuorumSuppressionTunables{RecoveryPolls: 1}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// First fresh OK poll under recoveryPolls=1 → exits immediately.
	enter, exit := state.updateQuorumSuppression(sentinel.QuorumStatusOK, t0, t0, tt)
	if enter {
		t.Fatalf("expected no entry on OK poll")
	}
	if !exit {
		t.Fatalf("expected exit on first OK poll under RecoveryPolls=1")
	}
	if state.suppressionActive {
		t.Fatalf("suppressionActive=true after exit")
	}
}

// TestUpdateQuorumSuppression_StampJumpCreditsOne pins the credit-1
// rule: when the poll stamp advanced MORE than one poll interval
// between two observations (intermediate polls unobserved — their
// quorum values are unknowable and one could have been Lost), the
// observation still counts as ONE distinct OK poll, not one per
// elapsed poll interval. Driven with RecoveryPolls=3 so the
// distinction is observable.
func TestUpdateQuorumSuppression_StampJumpCreditsOne(t *testing.T) {
	t.Parallel()
	state := &crQuorumState{suppressionActive: true}
	tt := QuorumSuppressionTunables{RecoveryPolls: 3}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Observation 1: OK at poll t0.
	if _, exit := state.updateQuorumSuppression(sentinel.QuorumStatusOK, t0, t0, tt); exit {
		t.Fatalf("exit after 1 of 3 polls")
	}
	// Observation 2: OK with the stamp jumped 3 poll intervals ahead
	// (30s at the 10s default cadence) — still one credit.
	jump := t0.Add(30 * time.Second)
	if _, exit := state.updateQuorumSuppression(sentinel.QuorumStatusOK, jump, jump, tt); exit {
		t.Fatalf("exit after 2 of 3 credited polls — stamp jump over-credited")
	}
	if state.quorumOKConsecutivePolls != 2 {
		t.Fatalf("counter=%d after two fresh-stamp observations, want 2", state.quorumOKConsecutivePolls)
	}
	// Observation 3: third fresh stamp → exit.
	third := jump.Add(10 * time.Second)
	if _, exit := state.updateQuorumSuppression(sentinel.QuorumStatusOK, third, third, tt); !exit {
		t.Fatalf("no exit on third distinct OK poll")
	}
}

// TestUpdateQuorumSuppression_WedgedObserverEntryFreshness is the
// load-bearing entry-side mutant-killer: it freezes the poll stamp at
// the episode-open anchor (a real non-zero value) while wall-clock
// marches past the loss floor, so it is the one test that distinguishes
// the correct `.After(anchor)` fresh-poll clause from both a
// `!polledAt.IsZero()` and a `!polledAt.Before(anchor)` mis-impl. A
// wedged observer re-reading one Lost poll must never arm the gate off
// reconcile churn; only a strictly-newer live poll crosses.
func TestUpdateQuorumSuppression_WedgedObserverEntryFreshness(t *testing.T) {
	t.Parallel()
	state := &crQuorumState{}
	tt := QuorumSuppressionTunables{LossThreshold: 5 * time.Second}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Episode opens on a real poll stamp; the wedged observer never
	// advances LastPolledAt past this anchor.
	anchor := t0
	if enter, exit := state.updateQuorumSuppression(sentinel.QuorumStatusLost, anchor, t0, tt); enter || exit {
		t.Fatalf("open: enter=%v exit=%v want both false", enter, exit)
	}

	// Wall-clock marches well past the 5s floor with the stamp frozen
	// at the anchor: aging alone, unbacked by a live re-confirmation,
	// must not cross.
	for _, dt := range []time.Duration{6 * time.Second, 10 * time.Second, 20 * time.Second, 30 * time.Second} {
		now := t0.Add(dt)
		if enter, _ := state.updateQuorumSuppression(sentinel.QuorumStatusLost, anchor, now, tt); enter {
			t.Fatalf("dt=%s: entered suppression off a frozen poll stamp", dt)
		}
		if state.suppressionActive {
			t.Fatalf("dt=%s: suppressionActive=true off a frozen poll stamp", dt)
		}
	}

	// First live poll strictly newer than the anchor, still Lost, past
	// the wall-clock floor → entry fires now.
	fresh := t0.Add(31 * time.Second)
	if enter, _ := state.updateQuorumSuppression(sentinel.QuorumStatusLost, fresh, fresh, tt); !enter {
		t.Fatalf("fresh poll after threshold: enter=false, want true")
	}
	if !state.suppressionActive {
		t.Fatalf("fresh poll after threshold: suppressionActive=false, want true")
	}
}
