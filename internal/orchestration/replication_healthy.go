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

// evalReplicationHealthy resolves the ReplicationHealthy condition to a
// definite value for every mode. Standalone is always True/NotApplicable
// (no replication peer). Replication / sentinel modes fold the per-pod
// ReplicationReadyGate aggregate (o.Replication) the reconciler computed
// from the Phase 8 readiness gates:
//
//   - readiness gate disabled (spec.valkey.readinessGate.enabled=false)
//     → True/NotApplicable: the operator doesn't track master_link/lag,
//     so it reports an honest definite value rather than asserting
//     per-replica health.
//   - no replica peers configured (replicas=1) → True/NotApplicable.
//   - every expected replica replication-ready → True/AsExpected.
//   - otherwise → False/WaitingForReplicas.
//
// The condition is NEVER Unknown for these modes: it previously was,
// which hung `kubectl wait --for=condition=ReplicationHealthy`
// indefinitely. Keeping the condition present across modes also
// lets dashboards show a stable column shape.
func evalReplicationHealthy(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypeReplicationHealthy}
	if o.CR != nil && o.CR.Spec.Mode == valkeyv1beta1.ModeStandalone {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonNotApplicable
		c.Message = "standalone mode has no replication peer to track"
		return c
	}
	if !o.Replication.GateEnabled {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonNotApplicable
		c.Message = "replication-lag readiness gate disabled (spec.valkey.readinessGate.enabled=false); replication health not tracked"
		return c
	}
	expected := expectedReplicaPeers(o.CR)
	if expected == 0 {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonNotApplicable
		c.Message = "no replica peers configured (spec.valkey.replicas=1)"
		return c
	}
	if o.Replication.ReadyReplicas >= expected {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonAsExpected
		c.Message = fmt.Sprintf("all %d replica(s) replication-ready (master_link up, lag within budget)", expected)
		return c
	}
	c.Status = metav1.ConditionFalse
	c.Reason = ReasonReplicaWait
	c.Message = fmt.Sprintf("%d/%d replica(s) replication-ready", o.Replication.ReadyReplicas, expected)
	return c
}

// expectedReplicaPeers is the count of replica-role pods a non-standalone
// CR should run: spec.valkey.replicas minus the single primary. Returns
// 0 for a degenerate replicas<=1 spec (no replica to track) or a nil CR.
func expectedReplicaPeers(cr *valkeyv1beta1.Valkey) int {
	if cr == nil {
		return 0
	}
	if n := int(cr.Spec.Valkey.Replicas) - 1; n > 0 {
		return n
	}
	return 0
}
