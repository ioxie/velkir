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

package pvcresize

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Action is a transition function's verdict — what the controller should
// do next based on the substate transition outcome.
type Action int

const (
	// ActionRequeue means the transition succeeded (or is mid-flight) and
	// the controller should requeue after RequeueAfter to re-enter the
	// substate machine.
	ActionRequeue Action = iota
	// ActionAdvance means the transition completed and the substate
	// should advance to NextPhase. Caller writes status, requeues.
	ActionAdvance
	// ActionAbort means the transition hit a terminal failure; substate
	// should move to Aborted with the message captured in Reason.
	ActionAbort
	// ActionComplete means the substate machine has reached its terminal
	// success state — clear status.rollout.pvcResize, emit
	// PVCResizeComplete, drop the in-progress gauge.
	ActionComplete
)

// TransitionResult is what each substate transition returns.
type TransitionResult struct {
	Action       Action
	NextPhase    string // populated when Action == ActionAdvance
	Reason       string // populated for ActionAbort / aux logs
	Message      string // human-readable summary for status / events
	RequeueAfter time.Duration
	EventReason  string // optional: events.Reason to emit at this transition
	EventMessage string // optional: event message body
	EventWarning bool   // optional: true → corev1.EventTypeWarning
}

// Target identifies the resources this CR's substate machine touches.
// The controller fills these in from the Valkey CR + repo conventions.
type Target struct {
	Namespace      string
	STSName        string            // <cr-name>
	PVCLabels      map[string]string // selector for the data PVCs
	DesiredSize    resource.Quantity // spec.valkey.persistence.size
	StsRequeueWait time.Duration     // requeue after STS delete
	PvcRequeueWait time.Duration     // requeue after PVC patch
	PollInterval   time.Duration     // requeue while polling capacity
	StsApplyWait   time.Duration     // requeue after StsRecreated → wait for Phase 2 to recreate
}

// FromValidated drives Validated → StsOrphaned: deletes the data
// StatefulSet with PropagationPolicy=Orphan so the pods (and PVCs)
// survive while the STS template can be recreated at the new size.
func FromValidated(ctx context.Context, c client.Client, t Target) TransitionResult {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: t.STSName}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// STS already gone; fast-forward to next substate.
			return TransitionResult{
				Action:       ActionAdvance,
				NextPhase:    "StsOrphaned",
				Message:      "STS already absent; fast-forwarding to StsOrphaned",
				RequeueAfter: t.StsRequeueWait,
			}
		}
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("read STS for orphan-delete: %v", err),
			RequeueAfter: t.StsRequeueWait,
		}
	}
	orphan := metav1.DeletePropagationOrphan
	if err := c.Delete(ctx, sts, &client.DeleteOptions{PropagationPolicy: &orphan}); err != nil {
		if apierrors.IsNotFound(err) {
			// Lost the race; treat as success.
			return TransitionResult{
				Action:       ActionAdvance,
				NextPhase:    "StsOrphaned",
				Message:      "STS deleted by a concurrent actor",
				RequeueAfter: t.StsRequeueWait,
			}
		}
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("orphan-delete STS: %v", err),
			RequeueAfter: t.StsRequeueWait,
		}
	}
	return TransitionResult{
		Action:       ActionAdvance,
		NextPhase:    "StsOrphaned",
		Message:      "StatefulSet orphan-deleted; PVCs preserved for resize",
		RequeueAfter: t.StsRequeueWait,
	}
}

// FromStsOrphaned drives StsOrphaned → PVCsPatched: confirms the STS
// is gone from the cache, then patches each PVC's
// spec.resources.requests.storage to the desired size.
func FromStsOrphaned(ctx context.Context, c client.Client, t Target) TransitionResult {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: t.STSName}, sts)
	if err == nil {
		// STS still present (cache lag or controller re-created it);
		// hold the substate so the next reconcile re-checks.
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      "waiting for orphan-deleted STS to clear cache",
			RequeueAfter: t.StsRequeueWait,
		}
	}
	if !apierrors.IsNotFound(err) {
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("read STS while waiting for clearance: %v", err),
			RequeueAfter: t.StsRequeueWait,
		}
	}

	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcs, client.InNamespace(t.Namespace), client.MatchingLabels(t.PVCLabels)); err != nil {
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("listing PVCs for resize-patch: %v", err),
			RequeueAfter: t.PvcRequeueWait,
		}
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		// Skip PVCs already at desired (idempotent re-entry).
		current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if current.Cmp(t.DesiredSize) >= 0 {
			continue
		}
		original := pvc.DeepCopy()
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = t.DesiredSize
		if err := c.Patch(ctx, pvc, client.MergeFrom(original)); err != nil {
			// PVC vanished between list and patch (manual delete during
			// the orphan window, recovery procedure mid-resize). Skip
			// it — a gone PVC has nothing to resize, and the next
			// reconcile re-lists.
			if apierrors.IsNotFound(err) {
				continue
			}
			// Distinguish "expansion not allowed by CSI" (422 / Forbidden)
			// from generic transient errors. The terminal mapping uses
			// PVCExpansionFailed and aborts.
			if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
				return TransitionResult{
					Action:       ActionAbort,
					Reason:       "PVCExpansionFailed",
					Message:      fmt.Sprintf("PVC %q rejected resize patch: %v", pvc.Name, err),
					EventReason:  "PVCExpansionFailed",
					EventMessage: fmt.Sprintf("PVC %q rejected the resize patch (%v); CSI driver may not support online expansion despite the StorageClass advertising it", pvc.Name, err),
					EventWarning: true,
				}
			}
			return TransitionResult{
				Action:       ActionRequeue,
				Message:      fmt.Sprintf("patching PVC %q: %v", pvc.Name, err),
				RequeueAfter: t.PvcRequeueWait,
			}
		}
	}
	return TransitionResult{
		Action:       ActionAdvance,
		NextPhase:    "PVCsPatched",
		Message:      fmt.Sprintf("all PVCs patched to requested size %s", t.DesiredSize.String()),
		RequeueAfter: t.PollInterval,
	}
}

// FromPVCsPatched drives PVCsPatched → StsRecreated: polls each PVC's
// status.capacity until it reflects the desired size (CSI online
// expansion). Holds substate while any PVC is still expanding.
func FromPVCsPatched(ctx context.Context, c client.Client, t Target) TransitionResult {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcs, client.InNamespace(t.Namespace), client.MatchingLabels(t.PVCLabels)); err != nil {
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("listing PVCs while polling capacity: %v", err),
			RequeueAfter: t.PollInterval,
		}
	}
	if len(pvcs.Items) == 0 {
		// PVCs vanished mid-resize (manual delete after orphan-delete,
		// or replicas scaled to 0). The substate machine cannot
		// expand what's not there — abort with PVCMissing so the
		// operator surfaces the stall as a Warning event instead of
		// quietly requeueing forever waiting for Phase C's stall guard.
		return TransitionResult{
			Action:       ActionAbort,
			Reason:       "PVCMissing",
			Message:      "no PVCs found while polling for capacity expansion; resize cannot proceed",
			EventReason:  "PVCMissing",
			EventMessage: "PVCs labelled for this Valkey CR are no longer present; aborting the resize substate machine. Re-create or restore the PVCs to retry.",
			EventWarning: true,
		}
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		got := pvc.Status.Capacity[corev1.ResourceStorage]
		if got.Cmp(t.DesiredSize) < 0 {
			return TransitionResult{
				Action:       ActionRequeue,
				Message:      fmt.Sprintf("PVC %q status.capacity %s < desired %s; CSI expansion in flight", pvc.Name, got.String(), t.DesiredSize.String()),
				RequeueAfter: t.PollInterval,
			}
		}
	}
	return TransitionResult{
		Action:       ActionAdvance,
		NextPhase:    "StsRecreated",
		Message:      fmt.Sprintf("all PVCs report capacity ≥ %s; ready for STS recreate", t.DesiredSize.String()),
		RequeueAfter: t.StsApplyWait,
	}
}

// FromStsRecreated drives StsRecreated → Verified: checks that the
// reconciler's Phase 2 path has recreated the StatefulSet (it runs
// later in the same reconcile because the Phase 2 guard skips only
// during StsOrphaned/PVCsPatched). When the STS exists and its
// volumeClaimTemplates carry the new size, transitions to Verified.
func FromStsRecreated(ctx context.Context, c client.Client, t Target) TransitionResult {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: t.STSName}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Phase 2 hasn't run yet (or hasn't propagated); hold.
			return TransitionResult{
				Action:       ActionRequeue,
				Message:      "awaiting Phase 2 STS recreation",
				RequeueAfter: t.StsApplyWait,
			}
		}
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("read STS for recreate-check: %v", err),
			RequeueAfter: t.StsApplyWait,
		}
	}
	for _, vct := range sts.Spec.VolumeClaimTemplates {
		got := vct.Spec.Resources.Requests[corev1.ResourceStorage]
		if got.Cmp(t.DesiredSize) < 0 {
			// Phase 2 may have recreated with stale size if the spec
			// hadn't propagated yet; hold for the next reconcile.
			return TransitionResult{
				Action:       ActionRequeue,
				Message:      fmt.Sprintf("STS volumeClaimTemplate %q has size %s < desired %s; awaiting Phase 2 re-apply", vct.Name, got.String(), t.DesiredSize.String()),
				RequeueAfter: t.StsApplyWait,
			}
		}
	}
	return TransitionResult{
		Action:       ActionAdvance,
		NextPhase:    "Verified",
		Message:      "StatefulSet recreated at the new size; awaiting pod readiness",
		RequeueAfter: t.PollInterval,
	}
}

// FromVerified drives Verified → terminal-complete: confirms every pod
// in the recreated STS is Ready, then signals the caller to clear
// status.rollout.pvcResize and emit PVCResizeComplete.
func FromVerified(ctx context.Context, c client.Client, t Target) TransitionResult {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: t.STSName}, sts); err != nil {
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("read STS for ready-check: %v", err),
			RequeueAfter: t.PollInterval,
		}
	}
	desired := int32(0)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}
	if sts.Status.ReadyReplicas < desired {
		return TransitionResult{
			Action:       ActionRequeue,
			Message:      fmt.Sprintf("readyReplicas %d < spec.replicas %d", sts.Status.ReadyReplicas, desired),
			RequeueAfter: t.PollInterval,
		}
	}
	return TransitionResult{
		Action:       ActionComplete,
		Message:      "PVC resize complete; all pods Ready at the new size",
		EventReason:  "PVCResizeComplete",
		EventMessage: fmt.Sprintf("PVC resize complete; STS reattached to PVCs at capacity %s", t.DesiredSize.String()),
	}
}
