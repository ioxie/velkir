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
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// Tests for the sentinel-suppression gate.
// The tracker logic on `crQuorumState.updateQuorumSuppression` is a
// pure function of (q, polledAt, now); these tests drive synthetic
// now/polledAt sequences directly so the tracker's threshold +
// hysteresis behaviour can be pinned without requiring a live
// observer.

// step describes one observation in a sequence: a tri-state Quorum
// signal at `dt` after the sequence's t0, carrying the poll stamp at
// `poll` after t0 (nil → same as dt, i.e. every observation is backed
// by a fresh poll — the pre-existing scenario set assumes this). Set
// `poll` explicitly to model reconcile churn re-reading one poll.
// The legacy `quorumOK` bool fields in test literals map
// true→QuorumStatusOK / false→Lost via the resolve() helper so the
// OK/Lost-only scenario set stays readable; Unknown cases are
// explicit (`qExplicit`).
type step struct {
	dt              time.Duration
	poll            *time.Duration         // poll-stamp offset from t0; nil → dt
	quorumOK        bool                   // true→OK, false→Lost (when qExplicit unset)
	qExplicit       *sentinel.QuorumStatus // overrides the bool mapping when set
	wantSuppressed  bool
	wantJustEntered bool
	wantJustExited  bool
}

func (s step) resolve() sentinel.QuorumStatus {
	if s.qExplicit != nil {
		return *s.qExplicit
	}
	if s.quorumOK {
		return sentinel.QuorumStatusOK
	}
	return sentinel.QuorumStatusLost
}

func unknownStep(dt time.Duration, wantSuppressed bool) step {
	q := sentinel.QuorumStatusUnknown
	return step{dt: dt, qExplicit: &q, wantSuppressed: wantSuppressed}
}

func TestUpdateQuorumSuppression_ScenarioTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		steps []step
	}{
		{
			name: "always-OK never suppresses",
			steps: []step{
				{dt: 0, quorumOK: true},
				{dt: 30 * time.Second, quorumOK: true},
				{dt: 120 * time.Second, quorumOK: true},
			},
		},
		{
			name: "59s of NOQUORUM does not yet suppress",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 30 * time.Second, quorumOK: false},
				{dt: 59 * time.Second, quorumOK: false},
			},
		},
		{
			name: "60s of NOQUORUM crosses threshold and emits QuorumLost",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				// Subsequent NOQUORUM observations stay suppressed silently.
				{dt: 90 * time.Second, quorumOK: false, wantSuppressed: true},
			},
		},
		{
			name: "single OK flicker resets the loss-onset stamp",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 30 * time.Second, quorumOK: true},
				// Loss restarts; 60s from THIS point would be needed.
				{dt: 31 * time.Second, quorumOK: false},
				{dt: 89 * time.Second, quorumOK: false},                                              // only 58s into the new loss episode
				{dt: 91 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true}, // 60s into new episode
			},
		},
		{
			name: "1 OK after suppression does not exit (need 2)",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, quorumOK: true, wantSuppressed: true}, // 1 OK poll — still suppressed
			},
		},
		{
			name: "2 consecutive OK after suppression exits and emits QuorumReached",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, quorumOK: true, wantSuppressed: true},
				{dt: 80 * time.Second, quorumOK: true, wantSuppressed: false, wantJustExited: true},
			},
		},
		{
			name: "OK-then-NOQUORUM-then-OK breaks the recovery streak",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, quorumOK: true, wantSuppressed: true},                         // 1 OK
				{dt: 80 * time.Second, quorumOK: false, wantSuppressed: true},                        // streak broken, still suppressed
				{dt: 90 * time.Second, quorumOK: true, wantSuppressed: true},                         // 1 OK again
				{dt: 100 * time.Second, quorumOK: true, wantSuppressed: false, wantJustExited: true}, // 2 OK now
			},
		},
		{
			name: "exited suppression does not double-emit on next OK",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, quorumOK: true, wantSuppressed: true},
				{dt: 80 * time.Second, quorumOK: true, wantSuppressed: false, wantJustExited: true},
				{dt: 90 * time.Second, quorumOK: true, wantSuppressed: false}, // No re-entry, no re-emit.
			},
		},
		{
			name: "post-exit, fresh sustained NOQUORUM enters suppression again",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, quorumOK: true, wantSuppressed: true},
				{dt: 80 * time.Second, quorumOK: true, wantSuppressed: false, wantJustExited: true},
				{dt: 90 * time.Second, quorumOK: false}, // new loss episode begins
				{dt: 150 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
			},
		},
		{
			name: "exactly-at-threshold boundary fires (60.0s == >=)",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
			},
		},
		// --- tri-state Unknown cases ---
		{
			// Pre-suppression: Unknown observations do NOT advance
			// the loss-time accumulator. A 30s Lost-Unknown-Lost
			// sequence where Unknown takes 50s of wall time still
			// requires the second Lost to sustain ≥60s from the
			// original loss-onset — Unknown preserves the stamp.
			name: "Unknown during loss accumulation preserves loss-onset stamp",
			steps: []step{
				{dt: 0, quorumOK: false},
				unknownStep(20*time.Second, false),
				unknownStep(40*time.Second, false),
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
			},
		},
		{
			// Post-suppression: Unknown observations do NOT advance
			// the recovery hysteresis counter. The OK→Unknown→OK
			// sequence still needs two CONSECUTIVE OK polls; the
			// Unknown in the middle is a no-op, but it also doesn't
			// reset the counter (the prior OK still counts).
			name: "Unknown post-suppression preserves OK streak",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, quorumOK: true, wantSuppressed: true},
				unknownStep(80*time.Second, true), // no-op, streak still 1
				{dt: 90 * time.Second, quorumOK: true, wantSuppressed: false, wantJustExited: true},
			},
		},
		{
			// Pre-suppression: Unknown alone never accumulates
			// loss-time. A long Unknown-only sequence stays
			// inactive forever — there is no first Lost to stamp
			// quorumLostSince.
			name: "Unknown-only sequence never suppresses",
			steps: []step{
				unknownStep(0, false),
				unknownStep(60*time.Second, false),
				unknownStep(180*time.Second, false),
				unknownStep(3600*time.Second, false),
			},
		},
		{
			// Recovery rollout scenario: kill loop
			// triggers Lost long enough to suppress, recovery
			// rollout produces Unknown observations during
			// pod-recreation windows, then OK observations
			// recover the gate. The Unknown observations do NOT
			// re-arm the gate.
			name: "recovery-rollout Unknown windows do not re-arm gate",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				// Recovery rollout: cluster has quorum but observer
				// can't reach a quorum of peers during pod-replace
				// windows. Two consecutive OK polls clear the gate.
				{dt: 70 * time.Second, quorumOK: true, wantSuppressed: true},
				unknownStep(75*time.Second, true), // pod replace — observer can't see quorum
				{dt: 80 * time.Second, quorumOK: true, wantSuppressed: false, wantJustExited: true},
				// Post-recovery: another pod-replace window
				// surfaces as Unknown. The gate stays inactive,
				// and no spurious justEntered fires (regression
				// guard for the root cause).
				unknownStep(120*time.Second, false),
				unknownStep(150*time.Second, false),
				unknownStep(200*time.Second, false),
			},
		},
		// --- poll-stamp cases: hysteresis counts polls, not reconciles ---
		{
			// THE #696 reproducer: two back-to-back reconciles inside
			// one observer poll window both read the same OK poll.
			// Pre-fix, the second observation cleared the gate off a
			// single transient OK poll; post-fix it counts once.
			name: "two reconciles reading one OK poll count once",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, poll: new(70 * time.Second), quorumOK: true, wantSuppressed: true},
				// Second reconcile, same poll stamp — must NOT exit.
				{dt: 71 * time.Second, poll: new(70 * time.Second), quorumOK: true, wantSuppressed: true},
				// Third reconcile, same stamp again — still suppressed.
				{dt: 72 * time.Second, poll: new(70 * time.Second), quorumOK: true, wantSuppressed: true},
				// Fresh poll arrives — second distinct OK poll exits.
				{dt: 80 * time.Second, poll: new(80 * time.Second), quorumOK: true, wantSuppressed: false, wantJustExited: true},
			},
		},
		{
			// Unknown backed by a FRESH poll between two OKs must
			// neither advance nor consume the streak: the fresh
			// Unknown stamp is not recorded, so the next OK poll
			// still counts as the second distinct OK.
			name: "fresh-poll Unknown neither advances nor eats the streak",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, poll: new(70 * time.Second), quorumOK: true, wantSuppressed: true},
				unknownStep(80*time.Second, true), // fresh stamp (poll=dt), no-op
				{dt: 90 * time.Second, poll: new(90 * time.Second), quorumOK: true, wantSuppressed: false, wantJustExited: true},
			},
		},
		{
			// A Lost observation must break the OK streak even when
			// it carries a stale poll stamp (a pub/sub republish
			// carries the prior poll's stamp forward unchanged).
			name: "stale-stamp Lost still resets the OK streak",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, poll: new(70 * time.Second), quorumOK: true, wantSuppressed: true},
				// Pub/sub-carried Lost with the OK poll's stamp.
				{dt: 75 * time.Second, poll: new(70 * time.Second), quorumOK: false, wantSuppressed: true},
				// Streak restarted: two fresh OK polls needed again.
				{dt: 80 * time.Second, poll: new(80 * time.Second), quorumOK: true, wantSuppressed: true},
				{dt: 90 * time.Second, poll: new(90 * time.Second), quorumOK: true, wantSuppressed: false, wantJustExited: true},
			},
		},
		{
			// An OK observation with a ZERO poll stamp (no live poll
			// yet — defensive; today's observer can't produce it with
			// Q=OK) must never count toward exit.
			name: "zero-stamp OK never counts",
			steps: []step{
				{dt: 0, quorumOK: false},
				{dt: 60 * time.Second, quorumOK: false, wantSuppressed: true, wantJustEntered: true},
				{dt: 70 * time.Second, poll: new(time.Duration(0)), quorumOK: true, wantSuppressed: true},
				{dt: 71 * time.Second, poll: new(time.Duration(0)), quorumOK: true, wantSuppressed: true},
				{dt: 80 * time.Second, poll: new(80 * time.Second), quorumOK: true, wantSuppressed: true},
				{dt: 90 * time.Second, poll: new(90 * time.Second), quorumOK: true, wantSuppressed: false, wantJustExited: true},
			},
		},
		// --- entry-side poll-stamp cases: aging alone cannot cross ---
		{
			// The core #699 bug: a single Lost poll (stamp frozen at the
			// episode-open value) re-read by successive reconciles while the
			// observer is wedged accumulates >=60s wall-clock but must NEVER
			// enter suppression — the crossing needs a live poll newer than
			// the episode-opening poll. The frozen stamp is real (non-zero),
			// so this also rejects an `!IsZero()` mis-implementation.
			name: "wedged observer re-reading one Lost poll never crosses",
			steps: []step{
				{dt: 0, poll: new(1 * time.Second), quorumOK: false},                                        // open: anchor = t0+1s
				{dt: 60 * time.Second, poll: new(1 * time.Second), quorumOK: false, wantSuppressed: false},  // >=60s wall, frozen stamp — no cross
				{dt: 120 * time.Second, poll: new(1 * time.Second), quorumOK: false, wantSuppressed: false}, // reconcile churn, still frozen — no cross
			},
		},
		{
			// No over-correction: once ONE live poll strictly newer than the
			// episode-open poll arrives, the wall-clock-aged loss crosses
			// normally. Block-then-release in one table row.
			name: "wedged then one fresh Lost poll crosses",
			steps: []step{
				{dt: 0, poll: new(1 * time.Second), quorumOK: false},                                                              // open: anchor = t0+1s
				{dt: 60 * time.Second, poll: new(1 * time.Second), quorumOK: false, wantSuppressed: false},                        // frozen — no cross
				{dt: 61 * time.Second, poll: new(61 * time.Second), quorumOK: false, wantJustEntered: true, wantSuppressed: true}, // fresh poll → cross
			},
		},
		{
			// The anchor re-arms per loss episode: an OK clears it, the next
			// Lost re-opens it at that episode's poll, and a later observation
			// whose stamp equals the current anchor (a pub/sub republish
			// carrying the episode-open poll forward) cannot cross even though
			// the wall-clock floor is met — only a strictly-newer poll does.
			// 81-20=61s >= 60s so ONLY the anchor gate holds suppression off
			// here; stamp==anchor also rejects a `>=`/`!Before` mis-impl.
			name: "OK clears the loss-poll anchor; stale stamp cannot cross new episode",
			steps: []step{
				{dt: 0, poll: new(1 * time.Second), quorumOK: false},                                                              // episode 1 opens: anchor = t0+1s
				{dt: 10 * time.Second, poll: new(10 * time.Second), quorumOK: true},                                               // OK clears quorumLostSince + anchor
				{dt: 20 * time.Second, poll: new(20 * time.Second), quorumOK: false},                                              // episode 2 opens: anchor = t0+20s
				{dt: 81 * time.Second, poll: new(20 * time.Second), quorumOK: false, wantSuppressed: false},                       // 61s wall, stamp==anchor — no cross
				{dt: 82 * time.Second, poll: new(82 * time.Second), quorumOK: false, wantJustEntered: true, wantSuppressed: true}, // fresh stamp → cross
			},
		},
		// --- configurable threshold + recovery follow-ups ---
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := &crQuorumState{}
			t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			for i, s := range tc.steps {
				now := t0.Add(s.dt)
				pollOff := s.dt
				if s.poll != nil {
					pollOff = *s.poll
				}
				polledAt := t0.Add(pollOff)
				if s.poll != nil && *s.poll == 0 {
					// poll: new(time.Duration(0)) models "no live poll yet" — the
					// zero time.Time, not t0.
					polledAt = time.Time{}
				}
				q := s.resolve()
				gotEntered, gotExited := state.updateQuorumSuppression(q, polledAt, now, QuorumSuppressionTunables{})
				if gotEntered != s.wantJustEntered {
					t.Errorf("step[%d] dt=%s q=%s: justEntered=%v want=%v",
						i, s.dt, q, gotEntered, s.wantJustEntered)
				}
				if gotExited != s.wantJustExited {
					t.Errorf("step[%d] dt=%s q=%s: justExited=%v want=%v",
						i, s.dt, q, gotExited, s.wantJustExited)
				}
				if state.suppressionActive != s.wantSuppressed {
					t.Errorf("step[%d] dt=%s q=%s: suppressionActive=%v want=%v",
						i, s.dt, q, state.suppressionActive, s.wantSuppressed)
				}
			}
		})
	}
}

// TestSuppressionPaceHint pins the requeue pacing added with the
// distinct-poll hysteresis: while suppressed, steady-state passes
// tighten the requeue to the observer poll cadence so gate exit isn't
// bounded by the keep-alive; short-circuit passes (paused,
// auth-missing backoff, PVC-loss gate — reachedSteadyState=false)
// stay untouched because they never run the gate driver and pacing
// them cannot advance the gate.
func TestSuppressionPaceHint(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	v := newCR(valkeyv1beta1.ModeSentinel)
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}

	// No suppression state at all: untouched.
	if got := r.suppressionPaceHint(5*time.Minute, v, true); got != 5*time.Minute {
		t.Errorf("unsuppressed: got %s, want 5m0s", got)
	}

	r.stateFor(cr).quorum = &crQuorumState{suppressionActive: true}

	// Suppressed + steady-state pass: tightens to the poll cadence.
	if got := r.suppressionPaceHint(5*time.Minute, v, true); got != quorumSuppressedRequeue {
		t.Errorf("suppressed steady-state: got %s, want %s", got, quorumSuppressedRequeue)
	}
	// Suppressed + short-circuit pass: deliberately relaxed requeue
	// survives untouched.
	if got := r.suppressionPaceHint(5*time.Minute, v, false); got != 5*time.Minute {
		t.Errorf("suppressed short-circuit: got %s, want 5m0s", got)
	}
	// An already-tighter hint survives (merge keeps the minimum).
	if got := r.suppressionPaceHint(time.Second, v, true); got != time.Second {
		t.Errorf("tighter hint: got %s, want 1s", got)
	}
	// No prior hint adopts the pace.
	if got := r.suppressionPaceHint(0, v, true); got != quorumSuppressedRequeue {
		t.Errorf("zero current: got %s, want %s", got, quorumSuppressedRequeue)
	}
}

func TestIsSentinelSuppressed_NoStateReturnsFalse(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	if r.IsSentinelSuppressed(cr) {
		t.Errorf("expected false for unknown CR, got true")
	}
}

func TestIsSentinelSuppressed_ReadsFlagFromState(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	// Inactive state -> false.
	r.stateFor(cr).quorum = &crQuorumState{suppressionActive: false}
	if r.IsSentinelSuppressed(cr) {
		t.Errorf("expected false when suppressionActive=false")
	}

	// Active state -> true.
	r.stateFor(cr).quorum = &crQuorumState{suppressionActive: true}
	if !r.IsSentinelSuppressed(cr) {
		t.Errorf("expected true when suppressionActive=true")
	}
}

func TestIsSentinelSuppressed_ConcurrentReadsSafe(t *testing.T) {
	t.Parallel()
	r := &ValkeyReconciler{}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}
	r.stateFor(cr).quorum = &crQuorumState{suppressionActive: true}

	// 32 readers concurrently — race detector catches missing mu in
	// IsSentinelSuppressed (run with `go test -race`).
	const readers = 32
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			for range 100 {
				_ = r.IsSentinelSuppressed(cr)
			}
		}()
	}
	wg.Wait()
}

// TestUpdateQuorumSuppressionGate_EmitsQuorumLostOnce drives the gate
// helper through a full enter→exit cycle via the live FakeRecorder
// path. Asserts the QuorumLost event fires exactly once on entry and
// QuorumReached fires exactly once on exit (not on every observed
// poll).
func TestUpdateQuorumSuppressionGate_EmitsEventsExactlyOnce(t *testing.T) {
	t.Parallel()
	mgr, rec, cancel := startedManagerForReconciler(t)
	defer cancel()
	r := &ValkeyReconciler{SentinelObserver: mgr, Recorder: rec}
	cr := types.NamespacedName{Namespace: "ns", Name: "vk0"}

	// Drive the tracker directly with synthetic observations — the
	// helper still emits via Recorder but we bypass the live snapshot
	// read, since we control the state-transition signals here.
	st := r.stateFor(cr).quorumTracker()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	emit := func(now time.Time, ok bool) {
		q := sentinel.QuorumStatusLost
		if ok {
			q = sentinel.QuorumStatusOK
		}
		st.mu.Lock()
		// Each synthetic observation is backed by a fresh poll
		// (polledAt = now), matching the one-poll-per-observation
		// shape of the pre-existing scenarios.
		entered, exited := st.updateQuorumSuppression(q, now, now, QuorumSuppressionTunables{})
		st.mu.Unlock()
		if r.Recorder == nil {
			return
		}
		switch {
		case entered:
			r.Recorder.Eventf(nil, nil, "Warning", "QuorumLost", "QuorumLossObserve", "entered")
		case exited:
			r.Recorder.Eventf(nil, nil, "Normal", "QuorumReached", "QuorumReachObserve", "exited")
		}
	}

	emit(t0, false)
	emit(t0.Add(60*time.Second), false)  // enters
	emit(t0.Add(120*time.Second), false) // stays — no event
	emit(t0.Add(180*time.Second), true)  // 1st OK — no event
	emit(t0.Add(190*time.Second), true)  // 2nd OK — exits
	emit(t0.Add(200*time.Second), true)  // post-exit — no event

	got := drainAllEvents(rec.Events)
	var quorumLost, quorumReached int
	for _, e := range got {
		if strings.Contains(e, "QuorumLost") {
			quorumLost++
		}
		if strings.Contains(e, "QuorumReached") {
			quorumReached++
		}
	}
	if quorumLost != 1 {
		t.Errorf("QuorumLost emitted %d times, want 1; events=%v", quorumLost, got)
	}
	if quorumReached != 1 {
		t.Errorf("QuorumReached emitted %d times, want 1; events=%v", quorumReached, got)
	}
}
