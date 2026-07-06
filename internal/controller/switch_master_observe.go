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
	"net"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sentinel"
)

// observeExternalSwitchMaster reports whether the sentinel observer's
// snapshot indicates a primary the operator did not promote — an
// external/sentinel-driven failover — that the role labels have not
// yet caught up to. It returns the observer-elected primary Addr (the
// per-episode edge key) and detected=true when ALL of the following
// hold:
//
//   - The snapshot is Present and QuorumOK. An unconfirmed or Lost
//     snapshot is split-brain territory (T6, EventSplitBrainDetected),
//     never read as an unexpected failover here — requiring QuorumOK
//     keeps T5 and T6 from both firing off the same Steady state.
//   - The failover-in-flight latch is NOT active. The caller's
//     state==Steady gate already excludes the operator-initiated
//     window (deriveState folds an active latch into
//     StateFailoverInFlight); the latch parameter makes the predicate
//     correct in isolation too.
//   - The snapshot's primary Addr maps to a live valkey pod by IP. An
//     Addr matching no pod is the NoMasterAgreement wedge — a separate
//     observe path, not an unexpected failover.
//   - That observer-elected pod is NOT the one currently labelled
//     role=primary, AND some other pod currently carries role=primary.
//     The primary moved out from under a stale label. Requiring an
//     existing labelled primary distinguishes a genuine failover
//     (primary X → Y) from the first post-bootstrap label stamp (no
//     primary labelled yet — Phase 7 settling, not a failover).
//
// Pure: no kube reads, no clock, no recorder. The caller gates on
// state==StateSteady and threads in the once-per-reconcile snapshot +
// pod list, so this enumerates deterministically in a table test.
func observeExternalSwitchMaster(snap sentinel.Snapshot, pods []corev1.Pod, latchActive bool) (string, bool) {
	if !snap.Present || !snap.Primary.QuorumOK || latchActive {
		return "", false
	}
	host, _, err := net.SplitHostPort(snap.Primary.Addr)
	if err != nil || host == "" {
		// Malformed Addr — the snapshot is suspect; suppress (the
		// SplitBrain / NoMasterAgreement paths catch real anomalies).
		return "", false
	}
	electedFound := false
	electedIsPrimary := false
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP != "" && p.Status.PodIP == host {
			electedFound = true
			electedIsPrimary = p.Labels[RoleLabel] == roleValuePrimary
			break
		}
	}
	if !electedFound || electedIsPrimary {
		return "", false
	}
	// The elected pod is not labelled primary. Only an existing
	// labelled primary (necessarily a different, now-stale pod) makes
	// this an unexpected failover rather than a first-time stamp.
	if countPrimaryLabeledPods(pods) == 0 {
		return "", false
	}
	return snap.Primary.Addr, true
}

// fsmSwitchMasterDispatch fires the FSM T5 edge (StateSteady →
// StateFailoverInFlight, side-effect UnexpectedFailover) once per
// external-failover episode — a failover the operator did not
// initiate, where sentinel promoted a new primary and the observer
// snapshot reports it before Phase 7 has relabelled the pods.
//
// Gating, in order:
//
//   - state must be StateSteady. Operator-driven rollouts sit in
//     RolloutPrimary/FailoverInFlight (and set the failover latch,
//     which deriveState folds into FailoverInFlight), so they never
//     reach Steady mid-failover; bootstrap reads as Bootstrap. T5's
//     From:StateSteady would no-op outside Steady anyway — the
//     explicit gate keeps the edge tracker from churning on non-Steady
//     reconciles.
//   - observeExternalSwitchMaster must detect the snapshot/label
//     disagreement (latch checked there as defense-in-depth).
//   - the per-CR edge tracker must not have already emitted for this
//     elected-primary Addr. The dispatch runs BEFORE Phase 7's relabel
//     in the same reconcile, so without the tracker the same
//     disagreement would re-emit UnexpectedFailover every reconcile
//     until the relabel lands. The tracker re-arms on any reconcile
//     where the disagreement has cleared, so a later failover to a
//     different primary emits afresh.
//
// Threads the once-per-reconcile snapshot + pod list in rather than
// re-reading, so it observes exactly what the rest of the pass did.
func (r *ValkeyReconciler) fsmSwitchMasterDispatch(
	v *valkeyv1beta1.Valkey,
	key types.NamespacedName,
	state orchestration.State,
	snap sentinel.Snapshot,
	pods []corev1.Pod,
) {
	if state != orchestration.StateSteady {
		r.switchMasterEdgeReset(key)
		return
	}
	latchActive := r.failoverLatchActive(key, snap.Primary.Addr, snap.Primary.Epoch)
	electedAddr, detected := observeExternalSwitchMaster(snap, pods, latchActive)
	if !detected {
		r.switchMasterEdgeReset(key)
		return
	}
	if !r.switchMasterEdgeFire(key, electedAddr) {
		// Already emitted for this elected-primary Addr this episode.
		return
	}
	r.applyFSM(v, orchestration.StateSteady, orchestration.EventSwitchMaster,
		orchestration.GuardCtx{QuorumOK: snap.Primary.QuorumOK})
}

// switchMasterEdgeFire reports whether T5 should fire for the given
// observer-elected primary Addr, recording it so a repeat observation
// of the same Addr this episode is a no-op. Returns false (already
// emitted) when host equals the last-fired value for key.
func (r *ValkeyReconciler) switchMasterEdgeFire(key types.NamespacedName, host string) bool {
	return r.stateFor(key).fireSwitchMasterEdge(host)
}

// switchMasterEdgeReset re-arms the T5 edge for key: the next distinct
// external switch observed from Steady emits a fresh UnexpectedFailover.
// Idempotent.
func (r *ValkeyReconciler) switchMasterEdgeReset(key types.NamespacedName) {
	r.stateFor(key).resetSwitchMasterEdge()
}
