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

package orchestration

import (
	"testing"
	"time"

	"github.com/ioxie/velkir/internal/events"
)

// untaggedTag is the placeholder used in failure messages when a
// transition's Tag field is empty. Centralized so the goconst linter
// stops complaining and edits stay consistent.
const untaggedTag = "(untagged)"

// TestApply_TransitionTable walks every transition row with a
// happy-path case (and a few critical negative cases), asserting
// on the returned (state, side-effect, ok) triple. Each row's
// name carries the transition's tag so a failure reports
// immediately which transition broke.
func TestApply_TransitionTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		from        State
		event       Event
		ctx         GuardCtx
		wantTo      State
		wantReason  events.Reason
		wantRequeue time.Duration
		wantOK      bool
	}{
		{
			name:        "T1 Bootstrap stays on CRCreated when quorum not reached",
			from:        StateBootstrap,
			event:       EventCRCreated,
			ctx:         GuardCtx{QuorumPrimaryAgreed: false},
			wantTo:      StateBootstrap,
			wantRequeue: 5 * time.Second,
			wantOK:      true,
		},
		{
			name:       "T2 Bootstrap → Steady when quorum agreed and pod-0 labeled",
			from:       StateBootstrap,
			event:      EventQuorumPrimaryAgreed,
			ctx:        GuardCtx{QuorumPrimaryAgreed: true, Pod0LabeledPrimary: true},
			wantTo:     StateSteady,
			wantReason: events.BootstrapCompleted,
			wantOK:     true,
		},
		{
			name:   "T2 negative: quorum agreed but pod-0 not yet labeled → no transition",
			from:   StateBootstrap,
			event:  EventQuorumPrimaryAgreed,
			ctx:    GuardCtx{QuorumPrimaryAgreed: true, Pod0LabeledPrimary: false},
			wantTo: StateBootstrap,
			wantOK: false,
		},
		{
			name:       "T3 Steady → RolloutPending on trigger when quorum OK",
			from:       StateSteady,
			event:      EventRolloutTrigger,
			ctx:        GuardCtx{QuorumOK: true},
			wantTo:     StateRolloutPending,
			wantReason: events.RolloutStarted,
			wantOK:     true,
		},
		{
			name:        "T4 Steady defers rollout when quorum fragile",
			from:        StateSteady,
			event:       EventRolloutTrigger,
			ctx:         GuardCtx{QuorumOK: false},
			wantTo:      StateSteady,
			wantReason:  events.RolloutDeferred,
			wantRequeue: 10 * time.Second,
			wantOK:      true,
		},
		{
			name:       "T5 Steady → FailoverInFlight on sentinel-initiated +switch-master",
			from:       StateSteady,
			event:      EventSwitchMaster,
			wantTo:     StateFailoverInFlight,
			wantReason: events.UnexpectedFailover,
			wantOK:     true,
		},
		{
			name:       "T6 Steady → Degraded on split-brain",
			from:       StateSteady,
			event:      EventSplitBrainDetected,
			wantTo:     StateDegraded,
			wantReason: events.SplitBrainDetected,
			wantOK:     true,
		},
		{
			name:   "T7 RolloutPending → RolloutReplicas when quorum + candidate ready",
			from:   StateRolloutPending,
			event:  EventReconcileTick,
			ctx:    GuardCtx{QuorumOK: true, QuorumPrimaryAgreed: true, CandidateReplicaReady: true},
			wantTo: StateRolloutReplicas,
			wantOK: true,
		},
		{
			name:       "T8 RolloutPending → Degraded on quorum loss",
			from:       StateRolloutPending,
			event:      EventReconcileTick,
			ctx:        GuardCtx{QuorumOK: false},
			wantTo:     StateDegraded,
			wantReason: events.RolloutAbortedQuorumLost,
			wantOK:     true,
		},
		{
			name:       "T8 RolloutPending → Degraded on split-brain",
			from:       StateRolloutPending,
			event:      EventReconcileTick,
			ctx:        GuardCtx{QuorumOK: true, SplitBrain: true},
			wantTo:     StateDegraded,
			wantReason: events.RolloutAbortedQuorumLost,
			wantOK:     true,
		},
		{
			name:   "T7 wins over T8 when both guards could match (ordering)",
			from:   StateRolloutPending,
			event:  EventReconcileTick,
			ctx:    GuardCtx{QuorumOK: true, QuorumPrimaryAgreed: true, CandidateReplicaReady: true, SplitBrain: false},
			wantTo: StateRolloutReplicas,
			wantOK: true,
		},
		{
			name:   "T9 RolloutReplicas + ReplicaSelected → stay (delete pod side effect handled by reconciler)",
			from:   StateRolloutReplicas,
			event:  EventReplicaSelected,
			wantTo: StateRolloutReplicas,
			wantOK: true,
		},
		{
			name:   "T10 RolloutReplicas + ReplicaRolled (more remain) → stay",
			from:   StateRolloutReplicas,
			event:  EventReplicaRolled,
			ctx:    GuardCtx{HasMoreReplicas: true},
			wantTo: StateRolloutReplicas,
			wantOK: true,
		},
		{
			name:   "T10 negative: ReplicaRolled with no more remaining → no transition (use AllReplicasRolled)",
			from:   StateRolloutReplicas,
			event:  EventReplicaRolled,
			ctx:    GuardCtx{HasMoreReplicas: false},
			wantTo: StateRolloutReplicas,
			wantOK: false,
		},
		{
			name:       "T11 RolloutReplicas → RolloutPrimary on AllReplicasRolled",
			from:       StateRolloutReplicas,
			event:      EventAllReplicasRolled,
			wantTo:     StateRolloutPrimary,
			wantReason: events.ReplicasRolled,
			wantOK:     true,
		},
		{
			name:       "T12 RolloutReplicas → Degraded on QuorumLost",
			from:       StateRolloutReplicas,
			event:      EventQuorumLost,
			wantTo:     StateDegraded,
			wantReason: events.RolloutAbortedQuorumLost,
			wantOK:     true,
		},
		{
			name:       "T13 RolloutReplicas defers spec change",
			from:       StateRolloutReplicas,
			event:      EventSpecChanged,
			wantTo:     StateRolloutReplicas,
			wantReason: events.SpecChangeDeferred,
			wantOK:     true,
		},
		{
			name:       "T14 RolloutPrimary → FailoverInFlight when quorum OK + candidate ready",
			from:       StateRolloutPrimary,
			event:      EventReconcileTick,
			ctx:        GuardCtx{QuorumOK: true, CandidateReplicaReady: true},
			wantTo:     StateFailoverInFlight,
			wantReason: events.FailoverInitiated,
			wantOK:     true,
		},
		{
			name:       "T15 RolloutPrimary → Degraded on quorum loss",
			from:       StateRolloutPrimary,
			event:      EventReconcileTick,
			ctx:        GuardCtx{QuorumOK: false, CandidateReplicaReady: true},
			wantTo:     StateDegraded,
			wantReason: events.PrimaryRolloutBlocked,
			wantOK:     true,
		},
		{
			name:       "T15 RolloutPrimary → Degraded when no candidate ready",
			from:       StateRolloutPrimary,
			event:      EventReconcileTick,
			ctx:        GuardCtx{QuorumOK: true, CandidateReplicaReady: false},
			wantTo:     StateDegraded,
			wantReason: events.PrimaryRolloutBlocked,
			wantOK:     true,
		},
		{
			name:       "T14 wins over T15 when both quorum and candidate are healthy (ordering)",
			from:       StateRolloutPrimary,
			event:      EventReconcileTick,
			ctx:        GuardCtx{QuorumOK: true, CandidateReplicaReady: true},
			wantTo:     StateFailoverInFlight,
			wantReason: events.FailoverInitiated,
			wantOK:     true,
		},
		{
			name:       "T16 FailoverInFlight → RolloutComplete on +failover-end with stable primary",
			from:       StateFailoverInFlight,
			event:      EventFailoverEnd,
			ctx:        GuardCtx{NewPrimaryStable: true, QuorumPrimaryAgreed: true},
			wantTo:     StateRolloutComplete,
			wantReason: events.FailoverSucceeded,
			wantOK:     true,
		},
		{
			name:   "T16 negative: +failover-end without stable primary → no transition (wait for stability)",
			from:   StateFailoverInFlight,
			event:  EventFailoverEnd,
			ctx:    GuardCtx{NewPrimaryStable: false, QuorumPrimaryAgreed: true},
			wantTo: StateFailoverInFlight,
			wantOK: false,
		},
		{
			name:   "T16 negative: +failover-end with split-brain detected → no transition",
			from:   StateFailoverInFlight,
			event:  EventFailoverEnd,
			ctx:    GuardCtx{NewPrimaryStable: true, QuorumPrimaryAgreed: true, SplitBrain: true},
			wantTo: StateFailoverInFlight,
			wantOK: false,
		},
		{
			name:       "T17 FailoverInFlight → RolloutComplete on +failover-end-for-timeout",
			from:       StateFailoverInFlight,
			event:      EventFailoverEndForTimeout,
			wantTo:     StateRolloutComplete,
			wantReason: events.FailoverSucceededWithTimeout,
			wantOK:     true,
		},
		{
			name:       "T18 FailoverInFlight → Degraded on -failover-abort",
			from:       StateFailoverInFlight,
			event:      EventFailoverAbort,
			wantTo:     StateDegraded,
			wantReason: events.FailoverAborted,
			wantOK:     true,
		},
		{
			name:       "T19 FailoverInFlight → Degraded on wall-clock timeout",
			from:       StateFailoverInFlight,
			event:      EventFailoverTimeoutExceeded,
			wantTo:     StateDegraded,
			wantReason: events.FailoverStalled,
			wantOK:     true,
		},
		{
			name:   "T20 RolloutComplete + ReconcileTick + old primary now replica → stay",
			from:   StateRolloutComplete,
			event:  EventReconcileTick,
			ctx:    GuardCtx{OldPrimaryNowReplica: true},
			wantTo: StateRolloutComplete,
			wantOK: true,
		},
		{
			name:       "T21 RolloutComplete → Steady on OldPrimaryReady",
			from:       StateRolloutComplete,
			event:      EventOldPrimaryReady,
			ctx:        GuardCtx{OldPrimaryReplicaReady: true},
			wantTo:     StateSteady,
			wantReason: events.RolloutCompleted,
			wantOK:     true,
		},
		{
			name:       "T22 Degraded → Steady on recovery without pending rollout",
			from:       StateDegraded,
			event:      EventRecovered,
			ctx:        GuardCtx{QuorumOK: true, RolloutSuspendedFromPending: false},
			wantTo:     StateSteady,
			wantReason: events.DegradedResolved,
			wantOK:     true,
		},
		{
			name:       "T23 Degraded → RolloutPending on recovery with pending rollout",
			from:       StateDegraded,
			event:      EventRecovered,
			ctx:        GuardCtx{QuorumOK: true, RolloutSuspendedFromPending: true},
			wantTo:     StateRolloutPending,
			wantReason: events.RolloutResumed,
			wantOK:     true,
		},
		{
			name:   "T22 negative: recovery still split-brain → no transition",
			from:   StateDegraded,
			event:  EventRecovered,
			ctx:    GuardCtx{QuorumOK: true, SplitBrain: true},
			wantTo: StateDegraded,
			wantOK: false,
		},
		{
			name:       "T25 Bootstrap → Degraded+QuorumLost on sustained NOQUORUM",
			from:       StateBootstrap,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T26 Steady → Degraded+QuorumLost on sustained NOQUORUM",
			from:       StateSteady,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T27 RolloutPending → Degraded+QuorumLost on sustained NOQUORUM",
			from:       StateRolloutPending,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T28 RolloutReplicas → Degraded+QuorumLost on sustained NOQUORUM",
			from:       StateRolloutReplicas,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T29 RolloutPrimary → Degraded+QuorumLost on sustained NOQUORUM",
			from:       StateRolloutPrimary,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T30 FailoverInFlight → Degraded+QuorumLost on sustained NOQUORUM",
			from:       StateFailoverInFlight,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T31 RolloutComplete → Degraded+QuorumLost on sustained NOQUORUM",
			from:       StateRolloutComplete,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T32 Degraded → Degraded+QuorumLost on sustained NOQUORUM (escalation)",
			from:       StateDegraded,
			event:      EventQuorumLostSustained,
			ctx:        GuardCtx{},
			wantTo:     StateDegradedQuorumLost,
			wantReason: events.QuorumLost,
			wantOK:     true,
		},
		{
			name:       "T33 Degraded+QuorumLost → Steady on hysteresis-pass without pending rollout",
			from:       StateDegradedQuorumLost,
			event:      EventQuorumReached,
			ctx:        GuardCtx{CKQUORUMOKHysteresisPassed: true, RolloutSuspendedFromPending: false},
			wantTo:     StateSteady,
			wantReason: events.QuorumReached,
			wantOK:     true,
		},
		{
			name:       "T34 Degraded+QuorumLost → RolloutPending on hysteresis-pass with pending rollout",
			from:       StateDegradedQuorumLost,
			event:      EventQuorumReached,
			ctx:        GuardCtx{CKQUORUMOKHysteresisPassed: true, RolloutSuspendedFromPending: true},
			wantTo:     StateRolloutPending,
			wantReason: events.QuorumReached,
			wantOK:     true,
		},
		{
			name:   "T33 negative: QuorumReached without hysteresis-pass → no transition",
			from:   StateDegradedQuorumLost,
			event:  EventQuorumReached,
			ctx:    GuardCtx{CKQUORUMOKHysteresisPassed: false, RolloutSuspendedFromPending: false},
			wantTo: StateDegradedQuorumLost,
			wantOK: false,
		},
		{
			name:   "T34 negative: QuorumReached without hysteresis-pass (with pending) → no transition",
			from:   StateDegradedQuorumLost,
			event:  EventQuorumReached,
			ctx:    GuardCtx{CKQUORUMOKHysteresisPassed: false, RolloutSuspendedFromPending: true},
			wantTo: StateDegradedQuorumLost,
			wantOK: false,
		},
	}

	m := NewMachine()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotTo, gotEffect, gotOK := m.Apply(tc.from, tc.event, tc.ctx)
			if gotOK != tc.wantOK {
				t.Fatalf("ok=%v want=%v (from=%s event=%s)", gotOK, tc.wantOK, tc.from, tc.event)
			}
			if gotTo != tc.wantTo {
				t.Errorf("to=%s want=%s", gotTo, tc.wantTo)
			}
			if gotEffect.EventReason != tc.wantReason {
				t.Errorf("reason=%q want=%q", gotEffect.EventReason, tc.wantReason)
			}
			if gotEffect.RequeueAfter != tc.wantRequeue {
				t.Errorf("requeue=%v want=%v", gotEffect.RequeueAfter, tc.wantRequeue)
			}
		})
	}
}

// TestApply_CRDeletedFromEveryState asserts that every state
// has a CR-delete escape hatch. Without coverage on this, an
// in-rollout state could trap a CR mid-teardown.
func TestApply_CRDeletedFromEveryState(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	for _, s := range AllStates() {
		t.Run(string(s), func(t *testing.T) {
			t.Parallel()
			to, _, ok := m.Apply(s, EventCRDeleted, GuardCtx{})
			if !ok {
				t.Fatalf("CRDeleted from %s did not match a transition", s)
			}
			if to != "" {
				t.Errorf("CRDeleted from %s went to %q, want exit (empty state)", s, to)
			}
		})
	}
}

// TestStateCoverage asserts that every State in AllStates() appears as
// the From of at least one transition. An orphan state would be
// unreachable-from / un-exitable, which is a coverage bug not a
// design choice.
func TestStateCoverage(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	covered := make(map[State]bool, len(AllStates()))
	for _, tr := range m.Transitions() {
		covered[tr.From] = true
	}
	for _, s := range AllStates() {
		if !covered[s] {
			t.Errorf("state %s has no outgoing transitions in the table", s)
		}
	}
}

// TestEventCoverage asserts that every Event in AllEvents() appears in
// at least one transition row. An orphan event is dead code in the
// reconciler — it could fire at runtime and get silently dropped.
func TestEventCoverage(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	covered := make(map[Event]bool, len(AllEvents()))
	for _, tr := range m.Transitions() {
		covered[tr.Event] = true
	}
	for _, e := range AllEvents() {
		if !covered[e] {
			t.Errorf("event %s does not appear in any transition row", e)
		}
	}
}

// TestGuardOrdering asserts the Apply contract: for any (from, event)
// pair with multiple transitions, only the LAST may have a nil guard,
// and all earlier transitions must have a non-nil guard. Violations
// would make later transitions unreachable: Apply walks in declaration
// order, so a nil-guard "catch-all" placed before a guarded transition
// would always match first.
func TestGuardOrdering(t *testing.T) {
	t.Parallel()
	m := NewMachine()

	type key struct {
		From  State
		Event Event
	}
	groups := make(map[key][]Transition)
	order := make([]key, 0)
	for _, tr := range m.Transitions() {
		k := key{tr.From, tr.Event}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], tr)
	}

	for _, k := range order {
		ts := groups[k]
		if len(ts) <= 1 {
			continue
		}
		for i, tr := range ts[:len(ts)-1] {
			if tr.Guard == nil {
				offendingTag := tr.Tag
				if offendingTag == "" {
					offendingTag = untaggedTag
				}
				laterTag := ts[i+1].Tag
				if laterTag == "" {
					laterTag = untaggedTag
				}
				t.Errorf("transition %s (from=%s event=%s) has nil guard but is followed by %s — %s would be unreachable",
					offendingTag, k.From, k.Event, laterTag, laterTag)
			}
		}
	}
}

// TestEventReasonsCatalogued asserts every Reason that appears in the
// transition table is also present in events.AllReasons() — i.e., the
// FSM doesn't reference a Reason that the catalog doesn't declare. The
// events-catalog-membership linter enforces the *call-site* version of
// this invariant; this test enforces the *table* version, which fires
// at unit-test time even before the linter runs.
func TestEventReasonsCatalogued(t *testing.T) {
	t.Parallel()
	catalog := make(map[events.Reason]bool, len(events.AllReasons()))
	for _, r := range events.AllReasons() {
		catalog[r] = true
	}
	m := NewMachine()
	for _, tr := range m.Transitions() {
		r := tr.SideEffect.EventReason
		if r == "" {
			continue
		}
		if !catalog[r] {
			tag := tr.Tag
			if tag == "" {
				tag = untaggedTag
			}
			t.Errorf("transition %s emits Reason %q which is not in events.AllReasons()", tag, r)
		}
	}
}
