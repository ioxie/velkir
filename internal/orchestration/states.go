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

// State is the rollout-FSM state. Closed enum.
type State string

const (
	StateBootstrap        State = "Bootstrap"
	StateSteady           State = "Steady"
	StateRolloutPending   State = "RolloutPending"
	StateRolloutReplicas  State = "RolloutReplicas"
	StateRolloutPrimary   State = "RolloutPrimary"
	StateFailoverInFlight State = "FailoverInFlight"
	StateRolloutComplete  State = "RolloutComplete"
	StateDegraded         State = "Degraded"

	// StateDegradedQuorumLost is the explicit-no-action absorption state
	// for sustained CKQUORUM=NOQUORUM (≥ 60s continuous). Distinct from
	// StateDegraded because the operator's behaviour differs in kind:
	// while in this state, the reconciler MUST NOT issue any
	// SENTINEL MONITOR / SENTINEL RESET / SENTINEL SET commands (they
	// could reshape sentinel state in a split-brain-adjacent way) and
	// MUST NOT initiate a failover or touch the STS pod count.
	// Observation-only.
	//
	// Exit requires 2 consecutive CKQUORUM-OK polls (hysteresis): the
	// reconciler tracks the consecutive-OK count and fires
	// EventQuorumReached only on the second OK so a transient blip
	// during recovery doesn't immediately resume work the next NOQUORUM
	// tick would re-suspend.
	StateDegradedQuorumLost State = "Degraded+QuorumLost"
)

// AllStates returns every declared State in declaration order.
func AllStates() []State {
	return []State{
		StateBootstrap,
		StateSteady,
		StateRolloutPending,
		StateRolloutReplicas,
		StateRolloutPrimary,
		StateFailoverInFlight,
		StateRolloutComplete,
		StateDegraded,
		StateDegradedQuorumLost,
	}
}
