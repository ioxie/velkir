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
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
)

// SuspendedFrom enum values stamped into Status.Rollout.SuspendedFrom
// on rollout-abort edges and consumed on recovery edges to drive the
// RolloutSuspendedFromPending guard. Kept as string literals (matching
// the API field's Enum constraint) rather than tied to orchestration.State
// because the API package has no FSM import — keep the boundary narrow.
const (
	suspendedFromRolloutPending  = "RolloutPending"
	suspendedFromRolloutReplicas = "RolloutReplicas"
)

// fsmTransitionTracker carries the per-CR "what state did the FSM
// observe last reconcile" memory used by `fsmTransitionEdge` to
// wire abort and recovery transitions.
//
// Rollout-abort and recovery-to-Steady transitions fire on the
// *transition* from one state to another, not on the steady-state
// observation of the destination state. Without this per-CR memory
// the operator cannot distinguish "we just entered Degraded — fire
// the abort event" from "we are still in Degraded — already-emitted,
// no-op." A naive applyFSM call site keyed on the current state
// alone would re-emit the same audit event on every reconcile while
// the CR sat in Degraded.
//
// Re-arm: each reconcile updates `lastState` to the just-observed
// state, so a future transition out of Degraded re-arms the abort
// detector implicitly.
type fsmTransitionTracker struct {
	mu        sync.Mutex
	lastState orchestration.State
}

// fsmTransitionEdge returns the previous-reconcile FSM state for
// the named CR, then stores the current state for the next
// reconcile to read. The first call for a CR returns the empty
// state (`""`), which never matches any valid prior — callers
// switch on (prev, current) and treat empty as "no prior
// observation, no edge to fire."
//
// Uses the just-derived state from the same reconcile pass rather
// than re-running deriveState so the source-of-truth is the same
// observation other FSM call sites already used — avoids the
// consistency window where two derives could disagree on quorum
// or revision.
func (r *ValkeyReconciler) fsmTransitionEdge(key types.NamespacedName, current orchestration.State) orchestration.State {
	tr := r.stateFor(key).fsmTransitionTracker()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	prev := tr.lastState
	tr.lastState = current
	return prev
}

// IsFailoverInFlight is the one canonical failover-in-flight critical
// section. It derives over two existing signals — no new CRD phase
// field — and the operator-driven primary rollout's sentinel-roll gate,
// the dual-master self-heal, and the durable render fallback all read
// this single predicate so they cannot disagree on when the election
// window is open:
//
//   - The durable FailoverDispatch marker, via its in-memory mirror the
//     failover latch. This survives an operator restart (the latch is
//     rehydrated from Status.Rollout.FailoverDispatch by
//     maintainFailoverDispatchMarker), so the section stays closed
//     across a crash mid-election — read pure here (no auto-clear;
//     draining a stale latch is the maintenance path's job). It carries
//     the PreStripEpoch fence: a lower-epoch observer move-off does not
//     exit the section.
//   - The roll-in-progress derivation: the most recent reconcile pass
//     observed the CR in StateFailoverInFlight (the latch already drives
//     this fact in deriveState, but the FSM tracker is process-local and
//     empty immediately after a restart, which is why the durable signal
//     above is the load-bearing half).
//
// Bounded by the marker's Deadline — once it passes the latch reports
// inactive (timeout/escape, never an infinite hold) and the section
// re-derives to degraded/observe.
//
// Wired into sentinel.Manager.SetDeferralPredicate alongside
// IsSentinelSuppressed so the operator defers stranded-sentinel
// REMOVE + MONITOR surgery while a failover is mid-flight — the
// surgery must never race the sentinel's own config-epoch propagation.
//
// Synchronous, no network I/O: the observer snapshot read is an
// in-memory atomic load and the per-CR trackers are read under their
// own mutexes. Returns false on unknown CR (first reconcile, or
// post-CR-delete prune).
func (r *ValkeyReconciler) IsFailoverInFlight(cr types.NamespacedName) bool {
	observedAddr, observedEpoch := r.observedPrimaryAddrEpoch(cr)
	if r.failoverLatchActivePure(cr, observedAddr, observedEpoch) {
		return true
	}
	ps, ok := r.stateForIfPresent(cr)
	if !ok {
		return false
	}
	tr := ps.fsmTransitionIfPresent()
	if tr == nil {
		return false
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.lastState == orchestration.StateFailoverInFlight
}

// observedPrimaryAddrEpoch returns the observer snapshot's current
// primary Addr+config-epoch for cr, or ("", 0) when no observer is
// wired or no snapshot has been published. The read is an in-memory
// atomic load — no network I/O — so the failover predicate stays
// synchronous.
func (r *ValkeyReconciler) observedPrimaryAddrEpoch(cr types.NamespacedName) (string, int64) {
	if r.SentinelObserver == nil {
		return "", 0
	}
	snap := r.SentinelObserver.Snapshot(cr)
	if !snap.Present {
		return "", 0
	}
	return snap.Primary.Addr, snap.Primary.Epoch
}

// valkeyRollActive reports whether a master-aware data-plane rollout is
// in progress for the just-derived FSM state — true while the operator
// is rolling replicas (StateRolloutReplicas) or handing off the primary
// (StateRolloutPrimary). It is the "valkey roll in flight" half of the
// sentinel-roll gate, beside IsFailoverInFlight: the sentinel STS apply
// yields whenever either is true so a sentinel pod is never rolled while
// the data plane is mid-roll. Takes the freshly-derived state (not the
// per-CR tracker) so the gate has no one-reconcile lag — the very pass a
// roll begins must already defer the sentinel apply.
func valkeyRollActive(state orchestration.State) bool {
	return state == orchestration.StateRolloutReplicas ||
		state == orchestration.StateRolloutPrimary
}

// DeferralPredicate is the composed gate consulted by
// sentinel.Manager.RecoverStrandedSentinels before it issues its
// REMOVE + MONITOR surgery. Defers when EITHER sustained-quorum-loss
// suppression is active OR the just-derived FSM state is
// StateFailoverInFlight. Wired by cmd/main.go after the reconciler
// is constructed.
//
// Deferral is retry-next-reconcile: when the predicate holds, the
// stranded-recovery pass returns without touching sentinel state and
// the next reconcile re-evaluates the gate — there is no queue.
func (r *ValkeyReconciler) DeferralPredicate(cr types.NamespacedName) bool {
	return r.IsSentinelSuppressed(cr) || r.IsFailoverInFlight(cr)
}

// readSuspendedFromPending returns true when Status.Rollout.SuspendedFrom
// records a suspended pending-rollout — i.e. the CR was in
// StateRolloutPending when it transitioned into Degraded. Used to
// populate GuardCtx.RolloutSuspendedFromPending for the recovery
// dispatch between DegradedResolved (→ Steady) and RolloutResumed
// (→ RolloutPending). Returns false on nil status, missing field,
// or the RolloutReplicas value (which routes through the standard
// recovery + rollout-trigger re-detection path).
func readSuspendedFromPending(v *valkeyv1beta1.Valkey) bool {
	if v.Status.Rollout == nil || v.Status.Rollout.SuspendedFrom == nil {
		return false
	}
	return *v.Status.Rollout.SuspendedFrom == suspendedFromRolloutPending
}

// stampSuspendedFrom records the abort source on Status.Rollout.SuspendedFrom
// via an inline status patch. Idempotent: a no-op when the field is
// already at the requested value. The patch is best-effort — a conflict
// is swallowed silently (a racing writer; the next reconcile re-stamps
// on the still-firing abort edge), and a non-conflict error is likewise
// left for the next reconcile to retry but logged at V(1) so a
// persistent write failure (RBAC regression, validation reject,
// apiserver outage) is visible rather than silently dropped.
func (r *ValkeyReconciler) stampSuspendedFrom(ctx context.Context, v *valkeyv1beta1.Valkey, value string) {
	if v.Status.Rollout != nil && v.Status.Rollout.SuspendedFrom != nil && *v.Status.Rollout.SuspendedFrom == value {
		return
	}
	orig := v.DeepCopy()
	if v.Status.Rollout == nil {
		v.Status.Rollout = &valkeyv1beta1.RolloutStatus{}
	}
	v.Status.Rollout.SuspendedFrom = new(value)
	if err := r.Status().Patch(ctx, v, client.MergeFrom(orig)); err != nil && !apierrors.IsConflict(err) {
		// A conflict skips this block — it is benign (a racing writer
		// the next reconcile reconciles). A non-conflict error is
		// best-effort here too (the next reconcile re-stamps on the
		// still-firing abort edge) but log it at V(1) so a persistent
		// write failure surfaces instead of being silently swallowed.
		logf.FromContext(ctx).V(1).Info("stampSuspendedFrom: status patch failed (non-conflict)",
			"valkey", v.Name, "namespace", v.Namespace, "suspendedFrom", value, "err", err.Error())
	}
}

// clearSuspendedFrom drops Status.Rollout.SuspendedFrom on a recovery
// edge (Degraded → Steady, or Degraded → RolloutPending). Idempotent:
// no-op when the field is already absent. Same best-effort patch
// semantics as stampSuspendedFrom.
func (r *ValkeyReconciler) clearSuspendedFrom(ctx context.Context, v *valkeyv1beta1.Valkey) {
	if v.Status.Rollout == nil || v.Status.Rollout.SuspendedFrom == nil {
		return
	}
	orig := v.DeepCopy()
	v.Status.Rollout.SuspendedFrom = nil
	if err := r.Status().Patch(ctx, v, client.MergeFrom(orig)); err != nil && !apierrors.IsConflict(err) {
		_ = err
	}
}

// fsmAbortAndRecoveryDispatch processes the per-Reconcile abort +
// recovery transitions on the just-derived state. Tracks the
// previous-reconcile state via fsmTransitionEdge, dispatches abort
// transitions into Degraded with the SuspendedFrom stamp on
// rollout-pending / rollout-replicas aborts, fires the recovery edge
// from Degraded back to Steady or RolloutPending with the
// RolloutSuspendedFromPending guard, and clears any lingering
// SuspendedFrom stamp on Steady.
//
// Keyed on (prev, current) rather than current alone because
// deriveStateFromFacts short-circuits to Degraded on !QuorumOK
// regardless of which rollout state preceded it; the per-CR
// fsmTransitionEdge tracker is what distinguishes "just entered
// Degraded — fire the abort" from "still in Degraded — no-op."
//
// Primary-rollout aborts do NOT stamp SuspendedFrom — they re-emerge
// through the standard rollout-trigger re-detection on the next
// quorum-OK reconcile. The Steady-hygiene clear at the end drops a
// lingering RolloutReplicas-stamp from aborts whose recovery transits
// silently through RolloutReplicas (the most common production path)
// and never hits the Degraded → Steady recovery edge above.
func (r *ValkeyReconciler) fsmAbortAndRecoveryDispatch(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	key types.NamespacedName,
	state orchestration.State,
	quorumOK bool,
) {
	prev := r.fsmTransitionEdge(key, state)
	guards := orchestration.GuardCtx{QuorumOK: quorumOK}
	if state == orchestration.StateDegraded {
		switch prev {
		case orchestration.StateRolloutPending:
			r.stampSuspendedFrom(ctx, v, suspendedFromRolloutPending)
			r.applyFSM(v, orchestration.StateRolloutPending, orchestration.EventReconcileTick, guards)
		case orchestration.StateRolloutReplicas:
			r.stampSuspendedFrom(ctx, v, suspendedFromRolloutReplicas)
			r.applyFSM(v, orchestration.StateRolloutReplicas, orchestration.EventQuorumLost, guards)
		case orchestration.StateRolloutPrimary:
			r.applyFSM(v, orchestration.StateRolloutPrimary, orchestration.EventReconcileTick, guards)
		}
	}
	if prev == orchestration.StateDegraded &&
		(state == orchestration.StateSteady || state == orchestration.StateRolloutPending) {
		guards.RolloutSuspendedFromPending = readSuspendedFromPending(v)
		if _, _, matched := r.applyFSM(v, orchestration.StateDegraded, orchestration.EventRecovered, guards); matched {
			r.clearSuspendedFrom(ctx, v)
		}
	}
	if state == orchestration.StateSteady {
		r.clearSuspendedFrom(ctx, v)
	}
}
