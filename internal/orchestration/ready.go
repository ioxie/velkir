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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// evalReady reports True when every desired replica is Ready. For
// standalone (replicas=1) that's just "the one pod has gone Ready".
// The message text is canonical and stable per (type, reason) — see
// TestConditionMessageStability.
func evalReady(o Observation) metav1.Condition {
	c := metav1.Condition{
		Type:   TypeReady,
		Status: metav1.ConditionFalse,
	}
	if o.DualMasterActive {
		// Force Ready=False regardless of STS replica health — with
		// two or more pods accepting writes as master, any write may
		// land on the eventual loser and be discarded by the resync
		// that resolves the split. Highest precedence: the divergence
		// is strictly worse than pointing at a dead or disputed
		// primary.
		c.Reason = ReasonDualMasterDivergence
		c.Message = "two or more valkey pods self-report role:master; writes are split across primaries"
		return c
	}
	if o.NoMasterAgreementActive {
		// Force Ready=False regardless of STS replica health — the
		// pods may be Ready by kubelet's lights but the cluster has
		// no agreed master, so write traffic via the `<cr>` Service
		// would land on a read-only replica.
		c.Reason = ReasonNoMasterAgreement
		c.Message = "sentinel observer reports a primary address that matches no current valkey pod"
		return c
	}
	if o.MasterLostActive {
		// Force Ready=False even with a healthy-looking STS: the pod
		// labelled role=primary has stopped answering INFO, so writes
		// via the `<cr>` Service black-hole until Sentinel promotes a
		// replacement. Lower precedence than NoMasterAgreement (the
		// more-specific quorum-backed dead-IP wedge) but above the
		// generic STS replica checks.
		c.Reason = ReasonMasterLost
		c.Message = "pod labelled role=primary is not responding to INFO; awaiting Sentinel-driven promotion of a replacement primary"
		return c
	}
	if o.STS == nil {
		c.Reason = ReasonNoSTS
		c.Message = "StatefulSet not yet created"
		return c
	}
	desired := int32(0)
	if o.STS.Spec.Replicas != nil {
		desired = *o.STS.Spec.Replicas
	}
	if o.STS.Status.ReadyReplicas >= desired && desired > 0 {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonReady
		c.Message = "all desired replicas are Ready"
		return c
	}
	c.Reason = ReasonReplicaWait
	c.Message = "waiting for replicas to become Ready"
	return c
}
