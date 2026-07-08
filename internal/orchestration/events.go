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

// Event is a transition trigger consumed by Machine.Apply. Closed enum.
type Event string

const (
	EventCRCreated           Event = "CRCreated"
	EventQuorumPrimaryAgreed Event = "QuorumPrimaryAgreed"
	EventReconcileTick       Event = "ReconcileTick"
	EventRolloutTrigger      Event = "RolloutTrigger"
	EventSwitchMaster        Event = "SwitchMaster"
	EventSplitBrainDetected  Event = "SplitBrainDetected"
	EventReplicaSelected     Event = "ReplicaSelected"
	EventReplicaRolled       Event = "ReplicaRolled"
	EventAllReplicasRolled   Event = "AllReplicasRolled"

	// EventQuorumLost is the immediate quorum-drop trigger that aborts
	// an in-flight rollout. EventQuorumLostSustained is the separate
	// 60s-threshold escalation that drives the observation-only
	// Degraded+QuorumLost state.
	EventQuorumLost          Event = "QuorumLost"
	EventQuorumLostSustained Event = "QuorumLostSustained"

	// EventQuorumReached is gated on hysteresis (2 consecutive
	// CKQUORUM-OK polls) so a single transient OK during recovery
	// doesn't immediately resume work that the next NOQUORUM tick
	// would re-suspend.
	EventQuorumReached Event = "QuorumReached"

	EventSpecChanged             Event = "SpecChanged"
	EventFailoverEnd             Event = "FailoverEnd"
	EventFailoverEndForTimeout   Event = "FailoverEndForTimeout"
	EventFailoverAbort           Event = "FailoverAbort"
	EventFailoverTimeoutExceeded Event = "FailoverTimeoutExceeded"
	EventOldPrimaryReady         Event = "OldPrimaryReady"
	EventRecovered               Event = "Recovered"
	EventCRDeleted               Event = "CRDeleted"
)

// AllEvents returns every declared Event in declaration order.
func AllEvents() []Event {
	return []Event{
		EventCRCreated,
		EventQuorumPrimaryAgreed,
		EventReconcileTick,
		EventRolloutTrigger,
		EventSwitchMaster,
		EventSplitBrainDetected,
		EventReplicaSelected,
		EventReplicaRolled,
		EventAllReplicasRolled,
		EventQuorumLost,
		EventQuorumLostSustained,
		EventQuorumReached,
		EventSpecChanged,
		EventFailoverEnd,
		EventFailoverEndForTimeout,
		EventFailoverAbort,
		EventFailoverTimeoutExceeded,
		EventOldPrimaryReady,
		EventRecovered,
		EventCRDeleted,
	}
}
