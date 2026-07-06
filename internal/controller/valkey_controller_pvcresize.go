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
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	operatormetrics "github.com/ioxie/velkir/internal/metrics"
	"github.com/ioxie/velkir/internal/pvcresize"
)

// pvcResizeRequeueDefaults are the per-substate requeue intervals fed
// into the transition functions. Tunable in one place for test latency
// vs production responsiveness; 0 means "use controller-runtime
// default backoff", non-zero is an explicit requeue hint.
var pvcResizeRequeueDefaults = pvcresize.Target{
	StsRequeueWait: 2 * time.Second,
	PvcRequeueWait: 5 * time.Second,
	PollInterval:   5 * time.Second,
	StsApplyWait:   1 * time.Second,
}

// reconcilePVCResize is the Phase 4 entry point for the PVC resize
// sub-state-machine. The detector inspects desired vs current PVC
// capacity and stamps `status.rollout.pvcResize` to record the
// operator's verdict; the substate machine then drives the orphan-
// delete-recreate dance through the recorded phases.
//
// Returns:
//   - requeueAfter: non-zero when the substate machine wants the
//     reconciler to come back at a specific cadence (e.g. polling for
//     CSI online expansion). The caller folds this into ctrl.Result.
//   - error: non-nil on terminal-failure detector outcomes (shrink
//     rejected, expansion-not-supported) and on transient transition
//     errors. The deferred status update flips Degraded as needed.
func (r *ValkeyReconciler) reconcilePVCResize(ctx context.Context, v *valkeyv1beta1.Valkey) (time.Duration, error) {
	if v.Spec.Valkey.Persistence == nil {
		// emptyDir or no persistence at all — nothing to detect.
		// Clear any stale substate that may have lingered if the user
		// removed persistence after a prior resize attempt.
		return 0, r.clearPVCResizeStatus(ctx, v)
	}

	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return 0, fmt.Errorf("listing PVCs for resize detect: %w", err)
	}

	sc, err := r.lookupStorageClassForPVCs(ctx, pvcs.Items)
	if err != nil {
		// A transient read failure on the StorageClass is not a hard
		// abort — the next reconcile retries. Surface it so
		// controller-runtime backs off, but leave substate alone.
		return 0, fmt.Errorf("looking up storage class: %w", err)
	}

	result := pvcresize.Detect(pvcresize.Inputs{
		DesiredSize:  v.Spec.Valkey.Persistence.Size,
		PVCs:         pvcs.Items,
		StorageClass: sc,
	})

	switch result.Outcome {
	case pvcresize.OutcomeNoChange:
		// Substate-in-flight guard: when the dispatcher is already
		// walking the substate machine (currentPVCResizePhase non-
		// empty), a fresh detector pass that sees
		// `current == desired` means the CSI driver completed the
		// expansion before this reconcile observed the intermediate
		// phases. Short-circuiting via clearPVCResizeStatus here
		// would bypass FromVerified and the PVCResizeComplete event
		// would never fire. Dispatch the substate machine instead;
		// it walks the remaining phases (FromVerified for the in-
		// flight common case) and emits the terminal event on the
		// all-pods-Ready check.
		if current := currentPVCResizePhase(v); current != "" {
			return r.drivePVCResize(ctx, v, current, v.Spec.Valkey.Persistence.Size)
		}
		return 0, r.clearPVCResizeStatus(ctx, v)

	case pvcresize.OutcomeShrinkRejected:
		msg := fmt.Sprintf("spec.valkey.persistence.size=%s is smaller than current PVC capacity %s; shrinking not supported",
			result.Desired.String(), result.Current.String())
		if err := r.writePVCResizeStatus(ctx, v, valkeyv1beta1.PVCResizePhaseAborted, msg); err != nil {
			return 0, fmt.Errorf("writing shrink-rejected status: %w", err)
		}
		r.recordEventf(v, corev1.EventTypeWarning, string(events.PVCExpansionShrinkRejected), "PVCExpansionShrinkReject", "%s", msg)
		operatormetrics.SetPVCResizeInProgress(v.Namespace, v.Name, false)
		return 0, fmt.Errorf("PVC shrink rejected: %s", msg)

	case pvcresize.OutcomeExpansionNotSupported:
		scName := "<unset>"
		if sc != nil {
			scName = sc.Name
		}
		msg := fmt.Sprintf("StorageClass %q does not allow volume expansion; cannot grow PVCs from %s to %s",
			scName, result.Current.String(), result.Desired.String())
		if err := r.writePVCResizeStatus(ctx, v, valkeyv1beta1.PVCResizePhaseAborted, msg); err != nil {
			return 0, fmt.Errorf("writing expansion-not-supported status: %w", err)
		}
		r.recordEventf(v, corev1.EventTypeWarning, string(events.PVCExpansionNotSupported), "PVCExpansionNotSupportedReject", "%s", msg)
		operatormetrics.SetPVCResizeInProgress(v.Namespace, v.Name, false)
		return 0, fmt.Errorf("PVC expansion not supported: %s", msg)

	case pvcresize.OutcomeResizeNeeded:
		current := currentPVCResizePhase(v)

		// Aborted re-entry gate: when the substate is Aborted and the
		// detector still sees a desired-vs-current mismatch, the
		// retry-with-backoff sequence rate-limits re-entry. The
		// per-attempt backoff (1m / 5m / 15m / 1h cap) is keyed on
		// the persisted Attempt counter — see internal/pvcresize.
		// BackoffForAttempt for the schedule. Drop Aborted (or fix
		// the underlying cause so the detector returns NoChange) to
		// reset the backoff.
		if current == valkeyv1beta1.PVCResizePhaseAborted {
			if wait := pvcResizeAbortedWaitRemaining(v); wait > 0 {
				return wait, nil
			}
			// Backoff exhausted — fall through into the not-in-flight
			// branch below to re-enter Validated. writePVCResizeStatus
			// bumps Attempt on the Aborted → Validated transition.
		}

		if !isPVCResizeInFlight(current) {
			// First detection (or post-Aborted re-entry past the
			// backoff window) — write Validated, emit initiated
			// event, fall through to the dispatch loop below so the
			// same reconcile starts the orphan-delete dance.
			msg := fmt.Sprintf("PVC resize initiated: growing from %s to %s",
				result.Current.String(), result.Desired.String())
			if err := r.writePVCResizeStatus(ctx, v, valkeyv1beta1.PVCResizePhaseValidated, msg); err != nil {
				return 0, fmt.Errorf("writing resize-initiated status: %w", err)
			}
			r.recordEventf(v, corev1.EventTypeNormal, string(events.PVCResizeInitiated), "PVCResizeInitiate", "%s", msg)
			current = valkeyv1beta1.PVCResizePhaseValidated
		}

		// Per-substate stall guard: a non-terminal substate that
		// hasn't advanced in StallThreshold (10m) is presumed stuck.
		// Stuck pauses (preserves substate); only Aborted resets and
		// retries. Emit PVCExpansionStuck with dedup so the events
		// stream doesn't drown under repeated reconciles, then return
		// a longer requeue so we don't hot-loop the apiserver while
		// waiting for an operator to intervene.
		if pvcResizeIsStalled(v) {
			if err := r.maybeEmitPVCStuckEvent(ctx, v); err != nil {
				return 0, fmt.Errorf("recording stuck-event timestamp: %w", err)
			}
			operatormetrics.SetPVCResizeInProgress(v.Namespace, v.Name, true)
			return pvcResizeStallRequeue, nil
		}

		operatormetrics.SetPVCResizeInProgress(v.Namespace, v.Name, true)
		return r.drivePVCResize(ctx, v, current, result.Desired)
	}

	return 0, nil
}

// pvcResizeStallRequeue is the requeue interval the dispatcher
// returns while a substate is stuck (past StallThreshold). Long
// enough to keep apiserver pressure low (a stuck substate doesn't
// auto-recover; an operator must intervene); short enough that an
// operator's fix is observed within a minute.
const pvcResizeStallRequeue = 1 * time.Minute

// pvcResizeIsStalled returns true when the current substate is in
// flight AND the elapsed time since LastTransitionAt is at least
// the per-substate stall threshold. False when the substate is
// Aborted (terminal, distinct retry path) or absent.
func pvcResizeIsStalled(v *valkeyv1beta1.Valkey) bool {
	if v.Status.Rollout == nil || v.Status.Rollout.PVCResize == nil {
		return false
	}
	st := v.Status.Rollout.PVCResize
	if !isPVCResizeInFlight(valkeyv1beta1.PVCResizePhase(st.Phase)) {
		return false
	}
	if st.LastTransitionAt == nil {
		return false
	}
	return pvcresize.IsStalled(st.LastTransitionAt.Time, time.Now())
}

// pvcResizeAbortedWaitRemaining returns how much longer the
// dispatcher must wait before re-entering Detected from Aborted.
// Zero (or negative) means "backoff window has elapsed, re-enter
// immediately on this reconcile". The Attempt counter is sourced
// from the persisted PVCResize substate; absent or zero clamps to
// 1 (defensive — Attempt is stamped to 1 on the first Validated
// transition, so an Aborted state should always carry Attempt >=
// 1, but a malformed status shouldn't crash the dispatcher).
func pvcResizeAbortedWaitRemaining(v *valkeyv1beta1.Valkey) time.Duration {
	if v.Status.Rollout == nil || v.Status.Rollout.PVCResize == nil {
		return 0
	}
	st := v.Status.Rollout.PVCResize
	if st.LastTransitionAt == nil {
		return 0
	}
	return pvcresize.BackoffRemaining(st.LastTransitionAt.Time, st.Attempt, time.Now())
}

// maybeEmitPVCStuckEvent fires a PVCExpansionStuck event the first
// time a stall is detected for a given substate window, then
// suppresses further emissions until the substate moves (which
// refreshes LastTransitionAt and so resets the dedup window).
// Stamps status.rollout.pvcResize.lastStuckEventAt on emission so
// the dedup decision survives operator restart.
func (r *ValkeyReconciler) maybeEmitPVCStuckEvent(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	st := v.Status.Rollout.PVCResize
	// Already emitted for this stall window — LastStuckEventAt is
	// at-or-after LastTransitionAt. Skip.
	if st.LastStuckEventAt != nil && !st.LastStuckEventAt.Before(st.LastTransitionAt) {
		return nil
	}
	now := metav1.Now()
	patched := v.DeepCopy()
	patched.Status.Rollout.PVCResize.LastStuckEventAt = &now
	if err := r.Status().Patch(ctx, patched, client.MergeFrom(v)); err != nil {
		if apierrors.IsConflict(err) {
			// Lost the race; the next reconcile re-evaluates and
			// either emits then or sees the stamped field.
			return nil
		}
		return err
	}
	v.Status = patched.Status
	r.recordEventf(v, corev1.EventTypeWarning, string(events.PVCExpansionStuck), "PVCExpansionStuckObserve",
		"PVC resize substate %s has not advanced for %v (>= %v); operator intervention required",
		st.Phase,
		pvcresize.StallElapsed(st.LastTransitionAt.Time, time.Now()).Round(time.Second),
		pvcresize.StallThreshold)
	return nil
}

// drivePVCResize calls the matching transition function for the
// current substate, applies the result (status write, event, gauge
// drop), and returns a requeue hint for controller-runtime.
func (r *ValkeyReconciler) drivePVCResize(ctx context.Context, v *valkeyv1beta1.Valkey, current valkeyv1beta1.PVCResizePhase, desired resource.Quantity) (time.Duration, error) {
	tgt := pvcResizeRequeueDefaults
	tgt.Namespace = v.Namespace
	tgt.STSName = v.Name
	tgt.PVCLabels = map[string]string{
		CRLabel:        v.Name,
		ComponentLabel: componentValkey,
	}
	tgt.DesiredSize = desired

	var result pvcresize.TransitionResult
	switch current {
	case valkeyv1beta1.PVCResizePhaseValidated:
		result = pvcresize.FromValidated(ctx, r.Client, tgt)
	case valkeyv1beta1.PVCResizePhaseStsOrphaned:
		result = pvcresize.FromStsOrphaned(ctx, r.Client, tgt)
	case valkeyv1beta1.PVCResizePhasePVCsPatched:
		result = pvcresize.FromPVCsPatched(ctx, r.Client, tgt)
	case valkeyv1beta1.PVCResizePhaseStsRecreated:
		result = pvcresize.FromStsRecreated(ctx, r.Client, tgt)
	case valkeyv1beta1.PVCResizePhaseVerified:
		result = pvcresize.FromVerified(ctx, r.Client, tgt)
	default:
		return 0, nil
	}

	if result.EventReason != "" {
		evType := corev1.EventTypeNormal
		if result.EventWarning {
			evType = corev1.EventTypeWarning
		}
		r.recordEventf(v, evType, result.EventReason, result.EventReason, "%s", result.EventMessage)
	}

	switch result.Action {
	case pvcresize.ActionAdvance:
		next := valkeyv1beta1.PVCResizePhase(result.NextPhase)
		if err := r.writePVCResizeStatus(ctx, v, next, result.Message); err != nil {
			return 0, fmt.Errorf("advancing pvcResize substate to %s: %w", next, err)
		}
		return result.RequeueAfter, nil
	case pvcresize.ActionRequeue:
		return result.RequeueAfter, nil
	case pvcresize.ActionAbort:
		if err := r.writePVCResizeStatus(ctx, v, valkeyv1beta1.PVCResizePhaseAborted, result.Message); err != nil {
			return 0, fmt.Errorf("aborting pvcResize substate: %w", err)
		}
		return 0, fmt.Errorf("PVC resize aborted: %s", result.Message)
	case pvcresize.ActionComplete:
		if err := r.clearPVCResizeStatus(ctx, v); err != nil {
			return 0, fmt.Errorf("clearing pvcResize status on complete: %w", err)
		}
		return 0, nil
	}
	return 0, nil
}

// lookupStorageClassForPVCs returns the StorageClass referenced by the
// first PVC in the set. nil result + nil error means there is no class
// to look up (no PVCs, or the first PVC has an empty storageClassName);
// the detector treats that as expansion-not-supported. A non-nil error
// indicates a transient read failure and propagates back so the
// reconcile retries.
func (r *ValkeyReconciler) lookupStorageClassForPVCs(ctx context.Context, pvcs []corev1.PersistentVolumeClaim) (*storagev1.StorageClass, error) {
	if len(pvcs) == 0 {
		return nil, nil
	}
	scName := ""
	if pvcs[0].Spec.StorageClassName != nil {
		scName = *pvcs[0].Spec.StorageClassName
	}
	if scName == "" {
		return nil, nil
	}
	sc := &storagev1.StorageClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: scName}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			// StorageClass deleted but PVCs still reference it — treat
			// as not-found, which the detector maps to
			// ExpansionNotSupported (we cannot prove allowVolumeExpansion).
			return nil, nil
		}
		return nil, err
	}
	return sc, nil
}

// writePVCResizeStatus updates status.rollout.pvcResize with the given
// substate phase + message. The first transition into a non-terminal
// phase stamps StartedAt; every transition refreshes LastTransitionAt.
// Idempotent: re-writing the same phase + message is a no-op patch.
func (r *ValkeyReconciler) writePVCResizeStatus(ctx context.Context, v *valkeyv1beta1.Valkey, phase valkeyv1beta1.PVCResizePhase, message string) error {
	if currentPhase := currentPVCResizePhase(v); currentPhase == phase {
		// Same phase already stamped on this reconcile's view; the
		// detector is steady-state. Skip the patch to avoid hot-loop
		// status churn.
		return nil
	}
	now := metav1.Now()
	patched := v.DeepCopy()
	if patched.Status.Rollout == nil {
		patched.Status.Rollout = &valkeyv1beta1.RolloutStatus{}
	}
	prior := patched.Status.Rollout.PVCResize
	next := &valkeyv1beta1.PVCResizeStatus{
		Phase:            string(phase),
		Message:          message,
		LastTransitionAt: &now,
	}
	if prior == nil || prior.StartedAt == nil {
		next.StartedAt = &now
		next.Attempt = 1
	} else {
		next.StartedAt = prior.StartedAt
		next.Attempt = prior.Attempt
		// Bump attempt whenever we move BACK to Validated from a
		// non-Validated state — that's the operator's "try again"
		// re-entry point.
		if phase == valkeyv1beta1.PVCResizePhaseValidated && prior.Phase != string(phase) {
			next.Attempt = prior.Attempt + 1
		}
	}
	// LastStuckEventAt is intentionally NOT carried forward — a new
	// phase opens a new stall window, and the next stuck-detection
	// (if any) emits a fresh event. Clearing on every transition is
	// the simplest correct dedup-reset.
	patched.Status.Rollout.PVCResize = next
	if err := r.Status().Patch(ctx, patched, client.MergeFrom(v)); err != nil {
		if apierrors.IsConflict(err) {
			// Lost the race to another writer; the next reconcile
			// re-evaluates from a fresh fetch.
			return nil
		}
		return err
	}
	v.Status = patched.Status
	return nil
}

// clearPVCResizeStatus drops status.rollout.pvcResize when the detector
// has nothing to report. Idempotent: a no-op patch when the substate is
// already absent. Also drops the in-progress metric label so the gauge
// stops reporting 1 once the resize completes (or never started).
func (r *ValkeyReconciler) clearPVCResizeStatus(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	operatormetrics.SetPVCResizeInProgress(v.Namespace, v.Name, false)
	if v.Status.Rollout == nil || v.Status.Rollout.PVCResize == nil {
		return nil
	}
	patched := v.DeepCopy()
	patched.Status.Rollout.PVCResize = nil
	if err := r.Status().Patch(ctx, patched, client.MergeFrom(v)); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
	v.Status = patched.Status
	return nil
}

func currentPVCResizePhase(v *valkeyv1beta1.Valkey) valkeyv1beta1.PVCResizePhase {
	if v.Status.Rollout == nil || v.Status.Rollout.PVCResize == nil {
		return ""
	}
	return valkeyv1beta1.PVCResizePhase(v.Status.Rollout.PVCResize.Phase)
}

func isPVCResizeInFlight(phase valkeyv1beta1.PVCResizePhase) bool {
	switch phase {
	case valkeyv1beta1.PVCResizePhaseValidated,
		valkeyv1beta1.PVCResizePhaseStsOrphaned,
		valkeyv1beta1.PVCResizePhasePVCsPatched,
		valkeyv1beta1.PVCResizePhaseStsRecreated,
		valkeyv1beta1.PVCResizePhaseVerified:
		return true
	}
	return false
}

// isPVCResizeGuardingPhase2 reports whether the current substate is one
// where Phase 2 (STS reconcile) must be skipped to avoid racing the
// orphan-delete-recreate dance. The Validated substate is also guarded
// because the FromValidated transition issues the STS delete in the
// SAME reconcile that flips substate to StsOrphaned — running Phase 2
// after FromValidated would immediately recreate the STS we just
// orphan-deleted.
func isPVCResizeGuardingPhase2(phase valkeyv1beta1.PVCResizePhase) bool {
	switch phase {
	case valkeyv1beta1.PVCResizePhaseValidated,
		valkeyv1beta1.PVCResizePhaseStsOrphaned,
		valkeyv1beta1.PVCResizePhasePVCsPatched:
		return true
	}
	return false
}
