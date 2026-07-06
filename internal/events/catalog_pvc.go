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

package events

// PVCResizeInitiated is emitted when the substate machine transitions
// from Detected to Validated — the resize has cleared shrink-reject and
// allowVolumeExpansion preconditions, and the orphan-delete is about
// to be issued. The event message names the from/to size so a `kubectl
// get events` reader sees the resize intent without consulting status.
//
// Informational; the firing alert on stuck resize keys on PVCResizeStuck.
const PVCResizeInitiated Reason = "PVCResizeInitiated"

// PVCResizeComplete is emitted on the Validated → Verified transition
// once every PVC reports the new capacity, the STS has been recreated
// at the desired size, and pods are Ready. Pairs with the prior
// PVCResizeInitiated for log-level rollup.
//
// Informational.
const PVCResizeComplete Reason = "PVCResizeComplete"

// PVCExpansionShrinkRejected is emitted in Detected when the desired
// storage size is smaller than the current PVC capacity. Kubernetes PVs
// don't support shrink and a partial shrink leaves the cluster in a
// half-state; the substate machine aborts immediately rather than risk
// it. The webhook also rejects shrinks at admission time (CEL storage-
// monotonicity rule); this event covers the in-flight case where a
// resize is mid-rollout and the spec reverts.
//
// Alertable on persistent firing — usually means a misconfigured CR.
const PVCExpansionShrinkRejected Reason = "PVCExpansionShrinkRejected"

// PVCExpansionNotSupported is emitted in Detected when the StorageClass
// referenced by the PVC has `allowVolumeExpansion=false`. CSI drivers
// that don't support online expansion fail the PVC patch with a 422 the
// substate machine can't recover from automatically — admin must move
// the data to a class that does support expansion.
//
// Alertable on persistent firing — wedged on a non-expandable class.
const PVCExpansionNotSupported Reason = "PVCExpansionNotSupported"

// PVCExpansionFailed is emitted in StsOrphaned when a per-PVC patch
// returns 422 (or any other terminal error) from the API server even
// though the StorageClass advertised expansion support. Usually
// indicates a CSI-level issue (driver on the node refuses, quota
// exceeded, etc.) the operator can't reason about further.
//
// Alertable on persistent firing — needs operator-of-the-cluster eyes.
const PVCExpansionFailed Reason = "PVCExpansionFailed"

// PVCExpansionStuck is emitted from PVCsPatched when the per-substate
// 10-minute deadline expires without every PVC reporting the new
// capacity. The substate is preserved (not Aborted) so a manual nudge
// (e.g. force-mount on a stuck node, CSI restart) lets the next
// reconcile pick up where it stopped instead of starting from Detected.
//
// Alertable.
const PVCExpansionStuck Reason = "PVCExpansionStuck"

// PVCResizeAborted is emitted when the substate machine transitions to
// the terminal Aborted phase from any of the recoverable failure paths
// (shrink reject, allowVolumeExpansion=false, CSI 422, repeated stall).
// The next reconcile retries from Detected after a backoff scaling with
// the Attempt counter (capped at 1h). Distinct from PVCExpansionStuck:
// Stuck pauses; Aborted resets-and-retries.
//
// Alertable on persistent firing.
const PVCResizeAborted Reason = "PVCResizeAborted"

// PVCMissing is emitted in any substate when the PVC the operator
// expects to patch (or read FileSystemResizePending status from) is
// not in cache. Usually a transient race during STS recreate where the
// PVC binding hasn't surfaced yet; the substate machine treats it as
// a soft retry rather than abort.
//
// Informational on first emission, alertable on sustained firing.
const PVCMissing Reason = "PVCMissing"
