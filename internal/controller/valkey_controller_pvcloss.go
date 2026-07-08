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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/audit"
)

// AcceptPVCLossRequestorAnnotation is the sibling key the defaulter
// stamps with `userInfo.username` on admission of a CR that carries
// `accept-pvc-loss=true`. The reconciler reads it back here
// at audit-emission time so the compliance trail names a real user
// (or service account) rather than the "operator:reconciler" fallback.
const AcceptPVCLossRequestorAnnotation = AcceptPVCLossAnnotation + "-requestor"

// detectPVCLossAndGate is Phase 0d' — the data-safety gate between
// Phase 0d (auth Secret resolution) and Phase 1 (ConfigMap apply).
// When the operator observes the post-bootstrap STS+PVC absence shape
// (manual STS delete cascading PVC GC, disaster recovery, namespace
// re-create from snapshot, etc.), recovery is refused unless the user
// has explicitly authorized it via `velkir.ioxie.dev/accept-pvc-loss=true`
// on the CR. Without the gate the operator would silently re-mint a
// fresh StatefulSet, which would let K8s create empty PVCs — a
// data-loss event the user did not consent to.
//
// Returns:
//   - proceed=true: continue the reconcile (either the gate doesn't
//     apply or the user has authorized recovery).
//   - proceed=false + statusErr non-nil: refuse recovery; the caller
//     surfaces statusErr (so Reconciled / Degraded flip via the
//     deferred status update) and returns early WITHOUT a controller-
//     runtime error (this is a user-config issue, not a runtime
//     fault — same shape as the auth-secret-missing path in Phase 0d).
//   - proceed=false + statusErr is a transient API error: the caller
//     surfaces it as a runtime error so controller-runtime retries
//     with backoff.
//
// Preconditions for the gate to fire:
//
//  1. `hadFinalizer == true`: the PVC-retention finalizer was already
//     present at the start of this reconcile. Phase 0b' adds the
//     finalizer on the FIRST reconcile of a fresh CR; capturing the
//     pre-Phase-0b' state is what distinguishes initial bootstrap
//     (no finalizer yet → never reconciled → STS+PVC absence is
//     expected) from disaster recovery (finalizer present → was
//     bootstrapped previously → STS+PVC absence is anomalous).
//  2. `spec.pvcRetentionPolicy == Retain`: with `Delete`, PVC loss is
//     a documented consequence of CR deletion; if a Delete-policy CR
//     loses its STS+PVCs mid-life the user can either set the
//     annotation OR re-create the CR. The gate is conservative for
//     Retain (the default) where loss is unambiguously unintended.
//  3. STS for the CR returns NotFound.
//  4. No PVCs match the CR's `velkir.ioxie.dev/cr=<name>` label
//     selector.
//
// Audit emission on the consume path: `event=pvc_loss_accepted` via
// `audit.Log`, with `requestor` from the
// `velkir.ioxie.dev/accept-pvc-loss-requestor` sibling annotation
// Empty sibling → audit.Log defaults requestor to
// "operator:reconciler", which is the right shape for an internal
// or test-context request that bypassed the webhook.
//
// The annotation itself is stripped by the existing single-shot path
// (Phase 0e `clearSingleShotAnnotations`) once the reconcile completes
// successfully — a second STS+PVC loss without re-annotation refuses
// recovery, proving the one-shot semantic.
func (r *ValkeyReconciler) detectPVCLossAndGate(
	ctx context.Context,
	v *valkeyv1beta1.Valkey,
	hadFinalizer bool,
) (proceed bool, statusErr error) {
	// Bootstrap path: never reconciled before.
	if !hadFinalizer {
		return true, nil
	}
	// Delete-policy CRs accept PVC loss as a documented deletion
	// consequence; the gate doesn't apply.
	policy := v.Spec.PVCRetentionPolicy
	if policy == "" {
		policy = valkeyv1beta1.PVCRetentionRetain
	}
	if policy != valkeyv1beta1.PVCRetentionRetain {
		return true, nil
	}

	// STS observation: existence short-circuits the gate. The STS
	// being present means PVCs are either present (steady state) or
	// stuck pending (PVC controller race) — neither is the
	// "STS+PVC absent" disaster shape.
	sts := &appsv1.StatefulSet{}
	stsErr := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name}, sts)
	switch {
	case stsErr == nil:
		return true, nil
	case !apierrors.IsNotFound(stsErr):
		// Transient API error; surface as runtime error so
		// controller-runtime retries with backoff.
		return false, fmt.Errorf("phase 0 pvc-loss gate: get STS: %w", stsErr)
	}

	// PVC observation: any LIVE PVC matching the CR's owner label means
	// recovery would adopt it (orphan-PVC path), not lose data. PVCs in
	// Terminating state (DeletionTimestamp != nil) are treated as
	// already absent — they will finish finalising at any moment, and
	// the user who issued `kubectl delete sts && kubectl delete pvc`
	// has effectively destroyed the data already. Counting them as
	// present would let a delete-race slip past the gate while the
	// API still shows the old PVCs lingering.
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{CRLabel: v.Name},
	); err != nil {
		return false, fmt.Errorf("phase 0 pvc-loss gate: list PVCs: %w", err)
	}
	livePVCs := 0
	for i := range pvcs.Items {
		if pvcs.Items[i].DeletionTimestamp == nil {
			livePVCs++
		}
	}
	if livePVCs > 0 {
		return true, nil
	}

	// STS+PVCs both absent on a previously-bootstrapped CR — the
	// gate fires. Annotation-driven proceed/refuse split:
	if v.Annotations[AcceptPVCLossAnnotation] == "true" {
		audit.Log(ctx, audit.Event{
			Name:      audit.EventPVCLossAccepted,
			CR:        types.NamespacedName{Namespace: v.Namespace, Name: v.Name},
			Requestor: v.Annotations[AcceptPVCLossRequestorAnnotation],
		})
		return true, nil
	}
	// Refuse silent recovery. statusErr surfaces as
	// `Reconciled=False reason=PVCMissing` via the deferred status
	// update. Returning a plain error keeps the user-config-issue
	// shape (no controller-runtime error tick); the caller folds
	// statusErr into the deferred update without setting `err`.
	return false, fmt.Errorf("StatefulSet %q and all matching PVCs are absent; "+
		"set annotation %s=true to authorize re-creating fresh PVCs "+
		"(data-loss event)", v.Name, AcceptPVCLossAnnotation)
}
