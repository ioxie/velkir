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
	"time"

	"github.com/ioxie/velkir/internal/events"
)

// GuardCtx is the read-only observation of cluster state the reconciler
// hands the FSM each tick. Every guard the transition table references
// is a single bool here — narrow surface keeps table-driven tests cheap
// and forces the reconciler integration layer to derive each input
// explicitly (no hidden coupling to live cluster state from inside the
// FSM).
type GuardCtx struct {
	// QuorumOK reflects the latest CKQUORUM result combined with the
	// sentinel-consensus check. False also when split-brain is suspected.
	QuorumOK bool

	SplitBrain          bool
	QuorumPrimaryAgreed bool
	Pod0LabeledPrimary  bool

	// CandidateReplicaReady is true when at least one replica has
	// master_link_status=up and replication-offset delta within the
	// per-CR threshold.
	CandidateReplicaReady bool

	// HasMoreReplicas distinguishes "another replica still carries the
	// old pod-template hash" from "all replicas rolled".
	HasMoreReplicas bool

	// NewPrimaryStable is true when the post-failover primary address
	// has been stable across two consecutive observer snapshots.
	NewPrimaryStable bool

	// OldPrimaryNowReplica is true once the old primary's pod has been
	// observed with role=replica AND its pod-template hash matches the
	// new revision.
	OldPrimaryNowReplica bool

	// OldPrimaryReplicaReady is true when the old primary's replacement
	// pod is Ready=True and ReplicationHealthy=True.
	OldPrimaryReplicaReady bool

	// RolloutSuspendedFromPending records that Degraded was entered
	// from RolloutPending (or any in-rollout state). The reconciler
	// tracks this in status.rollout.suspendedFrom; the FSM uses it to
	// dispatch recovery to Steady vs RolloutPending.
	RolloutSuspendedFromPending bool

	// CKQUORUMOKHysteresisPassed is true after ≥ 2 consecutive
	// CKQUORUM-OK polls in StateDegradedQuorumLost. Single OK is not
	// enough — a transient blip during recovery would otherwise
	// immediately resume work the next NOQUORUM tick would re-suspend,
	// defeating the observation-only state's purpose.
	CKQUORUMOKHysteresisPassed bool
}

// SideEffect describes what the reconciler should record when a
// transition fires. Action verbs that touch the cluster (delete pod,
// SENTINEL FAILOVER) are NOT part of SideEffect — the reconciler
// derives those from the (from, to, event) triple. Keeping SideEffect
// to "what to record" rather than "what to do" preserves the FSM's
// purity: tests assert on emitted reasons + requeue without needing a
// fake k8s client.
type SideEffect struct {
	// EventReason is the events.Reason to emit, or empty if no event
	// is required. Reasons are catalogued in internal/events/catalog.go
	// and enforced by the events-catalog-membership linter.
	EventReason events.Reason

	// RequeueAfter is the explicit requeue duration, or zero if the
	// reconciler should use its default.
	RequeueAfter time.Duration
}

// Transition is one row of the rollout-spec transition table. Tag
// carries the spec row identifier so tests + logs can map back to the
// authoritative table without grep'ing source.
type Transition struct {
	Tag   string
	From  State
	Event Event
	Guard func(GuardCtx) bool
	// NextState is the state Apply reports when this row matches. The
	// operator re-derives the live state from cluster conditions on every
	// reconcile rather than persisting a From→NextState progression, so
	// this column is a per-event lookup result (consumed by Apply for the
	// FSM event message + requeue), not an authoritative state-machine edge.
	NextState  State
	SideEffect SideEffect
}

// Machine is the FSM. Construct via NewMachine; the table is closed at
// construction time so the test suite can iterate it deterministically.
type Machine struct {
	transitions []Transition
}

// NewMachine returns a fresh Machine populated with the canonical
// transition table.
func NewMachine() *Machine {
	return &Machine{transitions: defaultTransitions()}
}

// Apply consumes (state, event, guards) and returns the next state, the
// declared side effect, and ok=true if a transition matched. ok=false
// means no row matched — the reconciler should leave state unchanged
// and treat the event as a no-op (logged at debug, not warned: many
// (from, event) pairs have no row by design, e.g. EventQuorumPrimaryAgreed
// is meaningless outside Bootstrap).
//
// When multiple transitions share (from, event), Apply walks them in
// declaration order and picks the first whose guard returns true. A
// nil guard always matches; only the LAST transition for a given
// (from, event) pair should have a nil guard (otherwise a later
// guarded transition is unreachable, asserted in transitions_test.go).
func (m *Machine) Apply(state State, event Event, ctx GuardCtx) (State, SideEffect, bool) {
	for _, t := range m.transitions {
		if t.From != state || t.Event != event {
			continue
		}
		if t.Guard == nil || t.Guard(ctx) {
			return t.NextState, t.SideEffect, true
		}
	}
	return state, SideEffect{}, false
}

// Transitions exposes the underlying table for tests. Read-only —
// production code should call Apply.
func (m *Machine) Transitions() []Transition {
	out := make([]Transition, len(m.transitions))
	copy(out, m.transitions)
	return out
}

// defaultTransitions encodes the rollout-spec transition table in
// declaration order. Adding a transition: add a row here AND a test
// case in transitions_test.go.
//
// Guard idiom: nil means "no guard, always matches". For (from, event)
// pairs with multiple guarded transitions, list the most specific
// guard first; the LAST entry (catch-all) may be nil.
func defaultTransitions() []Transition {
	return []Transition{
		// Bootstrap with quorum not yet reached → stay, requeue.
		// 5s is tight enough to converge fast on a fresh CR (sentinel
		// pods come up within ~10-20s) without burning the API server
		// on a stuck bootstrap; controller-runtime's cache absorbs the
		// watch-driven path so this requeue only fires when no event
		// landed.
		{
			Tag:       "T1",
			From:      StateBootstrap,
			Event:     EventCRCreated,
			Guard:     func(c GuardCtx) bool { return !c.QuorumPrimaryAgreed },
			NextState: StateBootstrap,
			SideEffect: SideEffect{
				RequeueAfter: 5 * time.Second,
			},
		},
		{
			Tag:   "T2",
			From:  StateBootstrap,
			Event: EventQuorumPrimaryAgreed,
			Guard: func(c GuardCtx) bool {
				return c.QuorumPrimaryAgreed && c.Pod0LabeledPrimary
			},
			NextState: StateSteady,
			SideEffect: SideEffect{
				EventReason: events.BootstrapCompleted,
			},
		},
		{
			Tag:       "T3",
			From:      StateSteady,
			Event:     EventRolloutTrigger,
			Guard:     func(c GuardCtx) bool { return c.QuorumOK },
			NextState: StateRolloutPending,
			SideEffect: SideEffect{
				EventReason: events.RolloutStarted,
			},
		},
		// Steady + rollout trigger + quorum fragile → defer.
		// 10s aligns with the sentinel observer's pull-tick cadence:
		// long enough that a transient quorum dip clears between
		// requeues without spamming RolloutDeferred events, short
		// enough that a recovered quorum starts the rollout within
		// one observer cycle.
		{
			Tag:       "T4",
			From:      StateSteady,
			Event:     EventRolloutTrigger,
			Guard:     func(c GuardCtx) bool { return !c.QuorumOK },
			NextState: StateSteady,
			SideEffect: SideEffect{
				EventReason:  events.RolloutDeferred,
				RequeueAfter: 10 * time.Second,
			},
		},
		{
			Tag:       "T5",
			From:      StateSteady,
			Event:     EventSwitchMaster,
			NextState: StateFailoverInFlight,
			SideEffect: SideEffect{
				EventReason: events.UnexpectedFailover,
			},
		},
		{
			Tag:       "T6",
			From:      StateSteady,
			Event:     EventSplitBrainDetected,
			NextState: StateDegraded,
			SideEffect: SideEffect{
				EventReason: events.SplitBrainDetected,
			},
		},
		{
			Tag:   "T7",
			From:  StateRolloutPending,
			Event: EventReconcileTick,
			Guard: func(c GuardCtx) bool {
				return c.QuorumOK && c.QuorumPrimaryAgreed && c.CandidateReplicaReady
			},
			NextState: StateRolloutReplicas,
		},
		{
			Tag:       "T8",
			From:      StateRolloutPending,
			Event:     EventReconcileTick,
			Guard:     func(c GuardCtx) bool { return !c.QuorumOK || c.SplitBrain },
			NextState: StateDegraded,
			SideEffect: SideEffect{
				EventReason: events.RolloutAbortedQuorumLost,
			},
		},
		{
			Tag:  "T9",
			From: StateRolloutReplicas, Event: EventReplicaSelected,
			NextState: StateRolloutReplicas,
		},
		{
			Tag:       "T10",
			From:      StateRolloutReplicas,
			Event:     EventReplicaRolled,
			Guard:     func(c GuardCtx) bool { return c.HasMoreReplicas },
			NextState: StateRolloutReplicas,
		},
		{
			Tag:       "T11",
			From:      StateRolloutReplicas,
			Event:     EventAllReplicasRolled,
			NextState: StateRolloutPrimary,
			SideEffect: SideEffect{
				EventReason: events.ReplicasRolled,
			},
		},
		{
			Tag:  "T12",
			From: StateRolloutReplicas, Event: EventQuorumLost,
			NextState: StateDegraded,
			SideEffect: SideEffect{
				EventReason: events.RolloutAbortedQuorumLost,
			},
		},
		{
			Tag:  "T13",
			From: StateRolloutReplicas, Event: EventSpecChanged,
			NextState: StateRolloutReplicas,
			SideEffect: SideEffect{
				EventReason: events.SpecChangeDeferred,
			},
		},
		{
			Tag:       "T14",
			From:      StateRolloutPrimary,
			Event:     EventReconcileTick,
			Guard:     func(c GuardCtx) bool { return c.QuorumOK && c.CandidateReplicaReady },
			NextState: StateFailoverInFlight,
			SideEffect: SideEffect{
				EventReason: events.FailoverInitiated,
			},
		},
		{
			Tag:       "T15",
			From:      StateRolloutPrimary,
			Event:     EventReconcileTick,
			Guard:     func(c GuardCtx) bool { return !c.QuorumOK || !c.CandidateReplicaReady },
			NextState: StateDegraded,
			SideEffect: SideEffect{
				EventReason: events.PrimaryRolloutBlocked,
			},
		},
		// FailoverInFlight + +failover-end → RolloutComplete.
		// !SplitBrain is defense-in-depth: every "declare success"
		// transition gates on split-brain locally so the safety
		// property survives future changes to how QuorumPrimaryAgreed
		// is computed.
		{
			Tag:   "T16",
			From:  StateFailoverInFlight,
			Event: EventFailoverEnd,
			Guard: func(c GuardCtx) bool {
				return c.NewPrimaryStable && c.QuorumPrimaryAgreed && !c.SplitBrain
			},
			NextState: StateRolloutComplete,
			SideEffect: SideEffect{
				EventReason: events.FailoverSucceeded,
			},
		},
		{
			Tag:  "T17",
			From: StateFailoverInFlight, Event: EventFailoverEndForTimeout,
			NextState: StateRolloutComplete,
			SideEffect: SideEffect{
				EventReason: events.FailoverSucceededWithTimeout,
			},
		},
		{
			Tag:  "T18",
			From: StateFailoverInFlight, Event: EventFailoverAbort,
			NextState: StateDegraded,
			SideEffect: SideEffect{
				EventReason: events.FailoverAborted,
			},
		},
		{
			Tag:  "T19",
			From: StateFailoverInFlight, Event: EventFailoverTimeoutExceeded,
			NextState: StateDegraded,
			SideEffect: SideEffect{
				EventReason: events.FailoverStalled,
			},
		},
		{
			Tag:       "T20",
			From:      StateRolloutComplete,
			Event:     EventReconcileTick,
			Guard:     func(c GuardCtx) bool { return c.OldPrimaryNowReplica },
			NextState: StateRolloutComplete,
		},
		{
			Tag:  "T21",
			From: StateRolloutComplete, Event: EventOldPrimaryReady,
			Guard:     func(c GuardCtx) bool { return c.OldPrimaryReplicaReady },
			NextState: StateSteady,
			SideEffect: SideEffect{
				EventReason: events.RolloutCompleted,
			},
		},
		{
			Tag:   "T22",
			From:  StateDegraded,
			Event: EventRecovered,
			Guard: func(c GuardCtx) bool {
				return c.QuorumOK && !c.SplitBrain && !c.RolloutSuspendedFromPending
			},
			NextState: StateSteady,
			SideEffect: SideEffect{
				EventReason: events.DegradedResolved,
			},
		},
		{
			Tag:   "T23",
			From:  StateDegraded,
			Event: EventRecovered,
			Guard: func(c GuardCtx) bool {
				return c.QuorumOK && !c.SplitBrain && c.RolloutSuspendedFromPending
			},
			NextState: StateRolloutPending,
			SideEffect: SideEffect{
				EventReason: events.RolloutResumed,
			},
		},
		// Any state → StateDegradedQuorumLost on sustained NOQUORUM.
		// Modeled per-state so iteration in tests sees explicit
		// coverage and so a future state addition surfaces as a
		// missing-row test failure rather than silently failing to
		// absorb the trigger. EventReason QuorumLost emitted on every
		// entry so the audit trail captures the originating state.
		// The reconciler stamps status.rollout.suspendedFrom = <originating
		// state> on the transition; the FSM only carries the boolean
		// RolloutSuspendedFromPending used by the exit dispatch.
		{
			Tag:       "T25-Bootstrap",
			From:      StateBootstrap,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:       "T26-Steady",
			From:      StateSteady,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:       "T27-RolloutPending",
			From:      StateRolloutPending,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:       "T28-RolloutReplicas",
			From:      StateRolloutReplicas,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:       "T29-RolloutPrimary",
			From:      StateRolloutPrimary,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:       "T30-FailoverInFlight",
			From:      StateFailoverInFlight,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:       "T31-RolloutComplete",
			From:      StateRolloutComplete,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:       "T32-Degraded",
			From:      StateDegraded,
			Event:     EventQuorumLostSustained,
			NextState: StateDegradedQuorumLost,
			SideEffect: SideEffect{
				EventReason: events.QuorumLost,
			},
		},
		{
			Tag:   "T33",
			From:  StateDegradedQuorumLost,
			Event: EventQuorumReached,
			Guard: func(c GuardCtx) bool {
				return c.CKQUORUMOKHysteresisPassed && !c.RolloutSuspendedFromPending
			},
			NextState: StateSteady,
			SideEffect: SideEffect{
				EventReason: events.QuorumReached,
			},
		},
		{
			Tag:   "T34",
			From:  StateDegradedQuorumLost,
			Event: EventQuorumReached,
			Guard: func(c GuardCtx) bool {
				return c.CKQUORUMOKHysteresisPassed && c.RolloutSuspendedFromPending
			},
			NextState: StateRolloutPending,
			SideEffect: SideEffect{
				EventReason: events.QuorumReached,
			},
		},
		// Any state → exit on CR delete. Modeled per-state so iteration
		// in tests sees explicit coverage; the empty State is the exit
		// sentinel.
		{Tag: "T24-Bootstrap", From: StateBootstrap, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-Steady", From: StateSteady, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-RolloutPending", From: StateRolloutPending, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-RolloutReplicas", From: StateRolloutReplicas, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-RolloutPrimary", From: StateRolloutPrimary, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-FailoverInFlight", From: StateFailoverInFlight, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-RolloutComplete", From: StateRolloutComplete, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-Degraded", From: StateDegraded, Event: EventCRDeleted, NextState: ""},
		{Tag: "T24-DegradedQuorumLost", From: StateDegradedQuorumLost, Event: EventCRDeleted, NextState: ""},
	}
}
