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
	"sync"

	"k8s.io/apimachinery/pkg/types"

	"github.com/ioxie/velkir/internal/orchestration"
)

// replicasRolledTracker carries the per-CR memory needed to fire
// `EventAllReplicasRolled` exactly once per "all replicas at target,
// primary stale" transition — without an edge gate every reconcile
// in the hand-off state would re-emit the same audit event.
//
// Re-arm: when the rollout completes (primary catches up too) the
// tracker flips back to false, so the next replica-roll fires fresh.
type replicasRolledTracker struct {
	mu                    sync.Mutex
	lastWasReplicasRolled bool
}

// replicasRolledEdge reports whether this reconcile observed the
// transition into `StateRolloutPrimary` ("all replicas at target,
// primary stale"). Returns false when the previous reconcile also
// observed the same state, or when the state isn't
// `StateRolloutPrimary`.
//
// Takes the already-derived state rather than re-running deriveState
// so the source-of-truth is the same observation Reconcile already
// used elsewhere in this pass — avoids the consistency window where
// two derives could disagree on quorum or revision.
func (r *ValkeyReconciler) replicasRolledEdge(key types.NamespacedName, current orchestration.State) bool {
	pendingNow := current == orchestration.StateRolloutPrimary

	tr := r.stateFor(key).replicasRolledTracker()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	edge := pendingNow && !tr.lastWasReplicasRolled
	tr.lastWasReplicasRolled = pendingNow
	return edge
}
