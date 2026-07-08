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
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// rolloutTriggerState carries the per-CR memory needed to fire
// `EventRolloutTrigger` exactly once per spec-change edge AND to
// distinguish a fresh-rollout edge (Steady → Pending) from a
// mid-rollout target swap (Pending → still-Pending with a new
// target revision). The two emit different FSM events: T3
// RolloutStarted for the fresh case, T13 SpecChangeDeferred for
// the mid-rollout case.
//
// "Edge" = transition from "no rollout pending" (STS UpdateRevision
// matches CurrentRevision, or no STS yet) to "rollout pending"
// (UpdateRevision is non-empty AND differs from CurrentRevision).
// Without this gate, RolloutStarted would re-emit every reconcile
// while the rollout was in flight.
//
// Reset: when the rollout completes (UpdateRevision == CurrentRevision
// again), the next divergence fires fresh.
type rolloutTriggerState struct {
	mu             sync.Mutex
	lastWasPending bool
	lastTarget     string
}

// rolloutTriggerSignal reports the two distinct edges the trigger
// detector emits. Both can be false (steady state, no change);
// they're mutually exclusive in any single reconcile because
// midRolloutChange requires lastWasPending and edge requires
// !lastWasPending.
type rolloutTriggerSignal struct {
	// edge fires on the Steady → Pending transition (fresh rollout
	// just started — UpdateRevision diverged from CurrentRevision
	// after a clean prior state). Drives T3 / T4 (RolloutStarted /
	// RolloutDeferred) via EventRolloutTrigger.
	edge bool
	// midRolloutChange fires when the rollout was already pending
	// AND the target revision swapped to a new value (the user
	// edited the CR spec while a previous rollout was still in
	// flight, so the STS controller computed a new UpdateRevision).
	// Drives T13 (SpecChangeDeferred) via EventSpecChanged.
	midRolloutChange bool
}

// rolloutTriggerEdge reports whether this reconcile observed the
// "rollout pending" edge for the CR, and whether a mid-rollout
// target change occurred. Returns the zero signal when the STS is
// not yet present (pre-bootstrap is a legitimate transient state,
// not a reconcile failure).
//
// A real Get failure surfaces as a non-nil error so the caller can
// treat it as a soft non-blocking failure — trigger detection stays
// non-blocking, matching deriveState.
func (r *ValkeyReconciler) rolloutTriggerEdge(ctx context.Context, v *valkeyv1beta1.Valkey) (rolloutTriggerSignal, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return rolloutTriggerSignal{}, nil
		}
		return rolloutTriggerSignal{}, fmt.Errorf("rollout-trigger: %w", err)
	}
	target := sts.Status.UpdateRevision
	current := sts.Status.CurrentRevision
	pendingNow := target != "" && target != current

	key := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	tr := r.stateFor(key).rolloutTriggerTracker()
	tr.mu.Lock()
	defer tr.mu.Unlock()

	sig := rolloutTriggerSignal{
		edge:             pendingNow && !tr.lastWasPending,
		midRolloutChange: pendingNow && tr.lastWasPending && tr.lastTarget != "" && target != tr.lastTarget,
	}
	tr.lastWasPending = pendingNow
	if pendingNow {
		tr.lastTarget = target
	} else {
		// Clearing on rollout-complete keeps the field tidy and
		// avoids a stale `lastTarget` accidentally satisfying the
		// midRolloutChange check after a re-arm on the same value.
		tr.lastTarget = ""
	}
	return sig, nil
}
