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

package orchestration

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// evalDegraded is True for any state the operator considers a
// degradation it can name. At standalone-only the only canonical
// degradation surface today is "reconcile returned an error";
// sentinel-mode degradations (split-brain, quorum loss) extend
// this evaluator.
//
// Precedence (most specific first):
//
//  1. RolloutWatchdog Active+Expired → RolloutStalled. The watchdog
//     names the exact pod that didn't come back Ready and the
//     deadline that elapsed; that's strictly more actionable than
//     a generic reconcile error and survives even when a
//     concurrent phase failure would otherwise mask it.
//  2. ReconcileError != nil → ReconcileError. Runtime faults trump
//     observation-only signals: the operator made no progress at
//     all this pass, which is strictly worse than observing a
//     specific degradation while otherwise running.
//  3. DualMasterActive → DualMasterDivergence. A dual-master scan
//     observed ≥2 live pods self-reporting role:master — active data
//     divergence: writes are landing on more than one primary and
//     the eventual loser's diverging writes will be discarded by the
//     resolving resync. Outprecedences the quorum signals below
//     because a real split usually co-occurs with quorum loss, and
//     the divergence — not the quorum state — is the fact that must
//     page; slotting it lower would mask it exactly when it matters.
//  4. SentinelPeerLinkupStuckActive → SentinelPeerLinkupStuck. The
//     operator's stranded-sentinel repair is failing to converge (a
//     wiped sentinel still reports an empty peer-list after the
//     configured consecutive surgeries). Above QuorumLost because it
//     names WHY quorum can't recover — the more-actionable, more-
//     specific fact — and the two co-occur.
//  5. QuorumSuppressionActive → QuorumLost. The per-CR suppression
//     gate is active: CKQUORUM has been NOQUORUM past the loss
//     threshold and the operator has suspended all sentinel commands
//     (the FSM's most severe absorption state). Outprecedences the
//     observer-snapshot signals below because while the gate holds
//     the operator acts on neither sentinel agreement nor the
//     reported primary Addr — the suppression is the dominant,
//     most-durable fact. Below the runtime faults above, which mean
//     the operator made no progress this pass at all. Mirrors the
//     QuorumLost condition and the QuorumLost event so all three
//     user-facing surfaces agree.
//  6. NoMasterAgreementActive → NoMasterAgreement. Phase 7
//     observed a sentinel-reported primary Addr that matches no
//     current valkey pod's PodIP — the cascading-wedge state. More
//     specific than SplitBrain (sentinels may agree, just on a
//     dead IP), so it wins on precedence. Cleared when the
//     observer publishes an Addr matching a live pod.
//  7. SplitBrainActive → SplitBrain. The observer reports
//     QuorumOK=false: Phase 7 has suppressed role-label writes,
//     and the Degraded condition surfaces the same observation so
//     `kubectl describe` shows the cause without digging into
//     events. Cleared as soon as the observer publishes a
//     QuorumOK=true snapshot.
//  8. mode=sentinel + replicas<2 → HANotMet. Static config
//     gap: sentinel needs ≥2 valkey replicas to perform a failover,
//     so a sub-HA shape cannot recover from a primary loss without
//     user intervention. The validating webhook accepts this shape
//     with a Warning (lab use); the runtime condition surfaces the
//     same signal so `kubectl describe valkey` shows it after the
//     CR lands. Lower precedence than the above because those are
//     operator-actionable; HANotMet is the user's own choice and
//     persists until they patch replicas up to ≥2.
//  9. Otherwise False/AsExpected.
func evalDegraded(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypeDegraded}
	// 1. RolloutWatchdog Active+Expired → RolloutStalled
	if o.RolloutWatchdog.Active && o.RolloutWatchdog.Expired {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonRolloutStalled
		c.Message = fmt.Sprintf(
			"replica-readiness watchdog expired waiting for pod %q (deadline %s)",
			o.RolloutWatchdog.PodName,
			o.RolloutWatchdog.Deadline.UTC().Format("2006-01-02T15:04:05Z"),
		)
		return c
	}
	// 2. ReconcileError → ReconcileError
	if o.ReconcileError != nil {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonReconcileErr
		c.Message = "reconcile returned an error"
		return c
	}
	// 3. DualMasterActive → DualMasterDivergence. Above the quorum
	//    signals: a split usually co-occurs with quorum loss, and the
	//    active data divergence is the fact that pages.
	if o.DualMasterActive {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonDualMasterDivergence
		c.Message = "two or more valkey pods self-report role:master; writes are landing on more than one primary and the diverging writes will be discarded when the split resolves"
		return c
	}
	// 4. SentinelPeerLinkupStuckActive → SentinelPeerLinkupStuck. Above
	//    QuorumLost: it names WHY quorum can't recover (the operator's
	//    repair surgery is failing to converge), which is strictly more
	//    actionable than the generic quorum-loss signal it co-occurs
	//    with.
	if o.SentinelPeerLinkupStuckActive {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonSentinelPeerLinkupStuck
		c.Message = "a rebuilt sentinel still reports an empty peer-list after repeated REMOVE + MONITOR surgeries; gossip cannot rebuild (auth or NetworkPolicy). Operator surgery is backing off — manual intervention likely required"
		return c
	}
	// 5. QuorumSuppressionActive → QuorumLost. Gated on sentinel mode
	//    so this arm fires under exactly the precondition that drives
	//    the QuorumLost condition True (evalQuorumLost) — the two
	//    surfaces never disagree. The reconciler only ever sets the
	//    flag for sentinel CRs, but the explicit guard keeps the
	//    invariant local rather than relying on that caller contract.
	if o.QuorumSuppressionActive && o.CR != nil && o.CR.Spec.Mode == valkeyv1beta1.ModeSentinel {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonQuorumLost
		c.Message = "sentinel quorum suppression gate active: fewer than `spec.sentinel.quorum` sentinels reachable past the loss threshold; all sentinel commands suspended until quorum recovers"
		return c
	}
	// 6. NoMasterAgreementActive → NoMasterAgreement. Higher
	//    precedence than SplitBrain because a cluster pointing at a
	//    defunct master IP needs operator action even when sentinels
	//    agree (they agree on a dead IP); the surface user-facing
	//    signal must be the more-specific one.
	if o.NoMasterAgreementActive {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonNoMasterAgreement
		c.Message = "sentinel observer reports a primary address that matches no current valkey pod; writes via the `<cr>` Service would land on a non-primary"
		return c
	}
	// 7. SplitBrainActive → SplitBrain
	if o.SplitBrainActive {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonSplitBrain
		c.Message = "sentinel observer reports quorum lost: fewer than `spec.sentinel.quorum` sentinels agree on the primary; role-label writes suppressed until quorum recovers"
		return c
	}
	// 8. mode=sentinel + replicas<2 → HANotMet
	if isSubHASentinel(o.CR) {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonHANotMet
		c.Message = fmt.Sprintf(
			"mode=sentinel with spec.valkey.replicas=%d is sub-HA: at least 2 valkey replicas are required for sentinel to perform a failover",
			o.CR.Spec.Valkey.Replicas,
		)
		return c
	}
	// 9. Otherwise False/AsExpected
	c.Status = metav1.ConditionFalse
	c.Reason = ReasonAsExpected
	c.Message = "no degradation observed"
	return c
}

// isSubHASentinel detects the runtime gap behind the HANotMet
// degradation: a sentinel-mode CR whose data-plane replica count is
// below the 2-replica floor that sentinel needs to perform a
// failover. Centralised so the evaluator and any future caller
// agree on the boundary.
func isSubHASentinel(cr *valkeyv1beta1.Valkey) bool {
	return cr != nil && cr.Spec.Mode == valkeyv1beta1.ModeSentinel && cr.Spec.Valkey.Replicas < 2
}

// DegradedFlippedFalse compares the prior and new conditions and
// reports whether Degraded just transitioned True → False — the
// signal the reconciler uses to emit a `DegradedResolved` event.
func DegradedFlippedFalse(prior, current []metav1.Condition) bool {
	priorDegraded := findCondition(prior, TypeDegraded)
	if priorDegraded == nil || priorDegraded.Status != metav1.ConditionTrue {
		return false
	}
	currentDegraded := findCondition(current, TypeDegraded)
	if currentDegraded == nil {
		return false
	}
	return currentDegraded.Status == metav1.ConditionFalse
}

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}
