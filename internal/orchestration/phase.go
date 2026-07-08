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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// derivePhase reduces the condition tuple to a single cosmetic
// `status.phase` string. The phase is purely
// derived — it never carries information not already present in the
// conditions, and the reconciler never branches on it. Order of
// precedence (highest wins):
//
//  1. Paused        — observation says the CR is paused
//  2. Degraded      — Degraded=True OR Reconciled=False (a reconcile
//     failure is a degradation; surfaced here so it can never read
//     as a healthy phase)
//  3. QuorumLost    — QuorumLost=True (sentinel quorum suppression
//     gate active). Surfaced above Available/Ready so a sustained
//     quorum loss never reads as a healthy cluster.
//  4. Pending       — no STS yet (Available=False, Ready=False, no
//     Progressing reason)
//  5. Progressing   — Progressing=True, replicas catching up
//  6. Available     — at least one replica Ready, more pending
//  7. Ready         — all desired replicas Ready
//
// dashboards / `kubectl get` columns surface this; the operator's
// internal logic always reads the conditions directly.
func derivePhase(conds []metav1.Condition, o Observation) string {
	if o.Paused {
		return PhasePaused
	}
	if statusOf(conds, TypeDegraded) == metav1.ConditionTrue {
		return PhaseDegraded
	}
	// A reconcile failure surfaces as Degraded too. evalDegraded already
	// folds ReconcileError into Degraded=True, so this is belt-and-
	// suspenders today — but deriving the phase from Reconciled directly
	// keeps it correct even if evalDegraded's precedence changes later,
	// so a failing reconcile can never read as a healthy phase.
	if statusOf(conds, TypeReconciled) == metav1.ConditionFalse {
		return PhaseDegraded
	}
	// QuorumLost=True is a degraded surface in its own right: the
	// sentinel quorum suppression gate is active and the operator has
	// suspended all sentinel commands. evalDegraded folds the same gate
	// into Degraded=True, so the check above normally fires first; this
	// guards the cosmetic phase independently so the two surfaces can
	// never disagree about whether the cluster is healthy.
	if statusOf(conds, TypeQuorumLost) == metav1.ConditionTrue {
		return PhaseDegraded
	}
	ready := statusOf(conds, TypeReady)
	available := statusOf(conds, TypeAvailable)
	progressing := statusOf(conds, TypeProgressing)
	switch {
	case o.STS == nil:
		return PhasePending
	case ready == metav1.ConditionTrue:
		return PhaseReady
	case available == metav1.ConditionTrue && progressing == metav1.ConditionTrue:
		return PhaseProgressing
	case available == metav1.ConditionTrue:
		return PhaseAvailable
	default:
		return PhaseProgressing
	}
}

func statusOf(conds []metav1.Condition, t string) metav1.ConditionStatus {
	for i := range conds {
		if conds[i].Type == t {
			return conds[i].Status
		}
	}
	return metav1.ConditionUnknown
}
