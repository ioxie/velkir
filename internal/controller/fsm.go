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
	"time"

	corev1 "k8s.io/api/core/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

// applyFSM wraps orchestration.Machine.Apply and translates the FSM's
// declared SideEffect into actual side effects (recording the named
// event when non-empty, surfacing requeue duration). The FSM itself
// is pure — no recorder, no client, no clock.
//
// matched=false means no transition row matched: the call is a clean
// no-op and callers should leave state and recorder untouched.
func (r *ValkeyReconciler) applyFSM(
	v *valkeyv1beta1.Valkey,
	state orchestration.State,
	event orchestration.Event,
	guards orchestration.GuardCtx,
) (nextState orchestration.State, requeueAfter time.Duration, matched bool) {
	if r.FSM == nil {
		return state, 0, false
	}
	next, side, ok := r.FSM.Apply(state, event, guards)
	if !ok {
		return state, 0, false
	}
	if side.EventReason != "" {
		r.recordEventf(
			v,
			corev1.EventTypeNormal,
			string(side.EventReason),
			"FSM",
			"rollout state machine: %s + %s → %s",
			state,
			event,
			next,
		)
	}
	return next, side.RequeueAfter, true
}

// isInRolloutState reports whether the FSM is in any state that
// represents an active rollout / failover operation — anything that
// is not Steady, not Bootstrap, not Degraded. Used by the STS
// reconciler to gate the ScaleDeferred temporal-deferral branch:
// scale-during-rollout is held back until the FSM returns to
// Steady; Degraded is excluded so a user can scale to recover from
// a stuck state.
func isInRolloutState(s orchestration.State) bool {
	switch s {
	case orchestration.StateRolloutPending,
		orchestration.StateRolloutReplicas,
		orchestration.StateRolloutPrimary,
		orchestration.StateFailoverInFlight,
		orchestration.StateRolloutComplete:
		return true
	default:
		return false
	}
}
