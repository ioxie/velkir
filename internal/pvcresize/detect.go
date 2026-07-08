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

// Package pvcresize implements the detector for the PVC resize sub-state-
// machine. The detector is the entry point: it inspects the live PVCs
// against the desired persistence size and decides whether a resize is
// in flight, newly initiated, or rejected. Driving the orphan-delete-
// recreate dance from a detected `Validated` substate lands in a
// follow-up PR.
package pvcresize

import (
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Outcome enumerates the detector's terminal verdicts for a single
// reconcile. The substate machine reads the outcome and decides what to
// write to status, which event to emit, and whether to requeue.
type Outcome int

const (
	// OutcomeNoChange — desired matches current capacity (or there is
	// nothing to compare). Steady state; clear any prior substate.
	OutcomeNoChange Outcome = iota
	// OutcomeShrinkRejected — desired < current. Hard reject; the
	// resize sub-state-machine will not be entered.
	OutcomeShrinkRejected
	// OutcomeExpansionNotSupported — desired > current but the backing
	// StorageClass does not allow volume expansion.
	OutcomeExpansionNotSupported
	// OutcomeResizeNeeded — desired > current and the StorageClass
	// allows expansion. Substate transitions Detected → Validated; the
	// substate machine drives the orphan-delete-recreate flow from here.
	OutcomeResizeNeeded
)

// Result is the detector's full output. Capacities are returned for
// status messages and events.
type Result struct {
	Outcome Outcome
	Current resource.Quantity
	Desired resource.Quantity
}

// Inputs is the detector's read view. Decoupling from the controller
// lets the unit test exercise every branch without an envtest cluster.
type Inputs struct {
	// DesiredSize is taken from spec.valkey.persistence.size. Zero
	// means the CR has no persistence (standalone with emptyDir);
	// the detector returns NoChange.
	DesiredSize resource.Quantity
	// PVCs are the PersistentVolumeClaims labelled for this CR.
	// Empty means no data PVCs exist (bootstrap before STS landed,
	// or standalone with emptyDir). NoChange.
	PVCs []corev1.PersistentVolumeClaim
	// StorageClass is the backing class for the CR's PVCs, looked up
	// from the first PVC's spec.storageClassName. nil means the
	// caller could not resolve the class (deleted, RBAC, etc.); the
	// detector treats this as ExpansionNotSupported because we cannot
	// safely assume `allowVolumeExpansion` without observing it.
	StorageClass *storagev1.StorageClass
}

// Detect inspects the live PVCs against the desired size and returns
// the detector's verdict. Pure function — no side effects, no API calls.
func Detect(in Inputs) Result {
	if in.DesiredSize.IsZero() || len(in.PVCs) == 0 {
		return Result{Outcome: OutcomeNoChange}
	}

	// All PVCs from a single STS are sized identically by
	// volumeClaimTemplates; the smallest current capacity across the
	// set is the conservative read for "do any of them need to grow".
	current := smallestCurrentSize(in.PVCs)
	desired := in.DesiredSize

	cmp := desired.Cmp(current)
	switch {
	case cmp == 0:
		return Result{Outcome: OutcomeNoChange, Current: current, Desired: desired}
	case cmp < 0:
		return Result{Outcome: OutcomeShrinkRejected, Current: current, Desired: desired}
	}

	// desired > current; need StorageClass to allow expansion.
	if in.StorageClass == nil ||
		in.StorageClass.AllowVolumeExpansion == nil ||
		!*in.StorageClass.AllowVolumeExpansion {
		return Result{Outcome: OutcomeExpansionNotSupported, Current: current, Desired: desired}
	}

	return Result{Outcome: OutcomeResizeNeeded, Current: current, Desired: desired}
}

func smallestCurrentSize(pvcs []corev1.PersistentVolumeClaim) resource.Quantity {
	var smallest resource.Quantity
	first := true
	for i := range pvcs {
		q := pvcs[i].Spec.Resources.Requests[corev1.ResourceStorage]
		if first || q.Cmp(smallest) < 0 {
			smallest = q
			first = false
		}
	}
	return smallest
}
