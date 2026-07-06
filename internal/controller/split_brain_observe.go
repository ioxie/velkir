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
	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// observeSplitBrain reports whether the sentinel observer's current
// snapshot for the CR is a real split-brain signal: Present with
// Quorum==QuorumStatusLost. Surfaced separately so the deferred status
// closure can stamp Degraded=True reason=SplitBrain without taking a
// Phase 7 round-trip.
//
// Gated on QuorumStatusLost, NOT !QuorumOK. QuorumStatusUnknown (the
// observer reached fewer than a quorum of peers — the restart
// placeholder snapshot, or a transient pod-recreation window) means "no
// data yet", not "split-brain"; reporting split-brain on Unknown fires a
// false SplitBrainDetected event + counter + Degraded flap on every
// operator restart of a healthy cluster. Only QuorumStatusLost (≥quorum
// sentinels reachable but agreement not met) is a real disagreement.
// This matches the QuorumStatus contract in the sentinel package and the
// sibling consumers; the Phase 7 relabel guard keeps refusing to relabel
// on !QuorumOK (Unknown OR Lost) separately, in desiredRolesForCR.
//
// Returns false for four short-circuit shapes:
//   - nil CR (defensive; the deferred closure runs after the CR is
//     in hand, but the helper is callable at any time);
//   - non-sentinel modes (standalone / replication) — Phase 7
//     short-circuits to bootstrap roles before consulting the
//     observer, so no split-brain claim is sourced from the
//     observer for these modes;
//   - nil `SentinelObserver` (the standalone build with the manager
//     unwired, or the boot-race window before the manager is
//     constructed);
//   - sentinel-mode but `!snap.Present` (the observer is registered
//     but has not yet published its first poll-tick snapshot).
//
// `Snapshot.Primary` is a value (not pointer) embedded in the `Snapshot`
// struct, so reading `.Quorum` is always safe; the !Present short-circuit
// blocks any reliance on a not-yet-populated value.
func (r *ValkeyReconciler) observeSplitBrain(v *valkeyv1beta1.Valkey) bool {
	if v == nil || v.Spec.Mode != valkeyv1beta1.ModeSentinel || r.SentinelObserver == nil {
		return false
	}
	snap := r.SentinelObserver.Snapshot(types.NamespacedName{Namespace: v.Namespace, Name: v.Name})
	return snapshotReportsSplitBrain(snap)
}

// snapshotReportsSplitBrain reports whether an observer snapshot is a
// real split-brain signal: Present with Quorum==QuorumStatusLost. Drives
// the Degraded signal (observeSplitBrain). QuorumStatusUnknown (the
// restart placeholder / observer-unreachable window) is NOT split-brain
// — see observeSplitBrain for the rationale.
//
// The SplitBrainDetected event + counter are NOT gated here anymore:
// they fire from updateQuorumSuppressionGate on the per-episode edge of
// updateSplitBrainSustained's `Present && Quorum==QuorumStatusLost`
// branch. That branch MUST stay equivalent to this predicate so
// the Degraded condition and the event never disagree — change both
// together if the split-brain definition is ever tightened.
func snapshotReportsSplitBrain(snap sentinel.Snapshot) bool {
	return snap.Present && snap.Primary.Quorum == sentinel.QuorumStatusLost
}
