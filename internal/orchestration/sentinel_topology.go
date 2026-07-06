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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// evalSentinelTopology resolves the decoupled SentinelTopologyReconciled
// hygiene condition. It is pure and reads only the topology fields the
// reconciler derived in updateStatus:
//
//   - non-sentinel mode → True/NotApplicable (mirrors
//     evalReplicationHealthy so `kubectl wait` terminates outside
//     sentinel mode).
//   - sentinel mode, no active mismatch → True/InSync.
//   - sentinel mode, active mismatch → False/SentinelTopologyMismatch
//     with a deterministic message naming the short dimension(s).
//
// This evaluator is intentionally consulted by no other evaluator: it
// never influences Ready, Degraded, or the derived phase. The message is
// built deterministically from the two deficits so the condition stays
// message-stable across repeated evaluations of the same observation.
func evalSentinelTopology(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypeSentinelTopologyReconciled}
	if o.CR == nil || o.CR.Spec.Mode != valkeyv1beta1.ModeSentinel {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonNotApplicable
		c.Message = "sentinel topology reconciliation is not applicable outside sentinel mode"
		return c
	}
	if !o.SentinelTopologyMismatchActive {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonSentinelTopologyInSync
		c.Message = "sentinel-known peer and replica counts match spec"
		return c
	}
	c.Status = metav1.ConditionFalse
	c.Reason = ReasonSentinelTopologyMismatch
	c.Message = "sentinel topology below spec: " +
		SentinelTopologyDetail(o.SentinelTopologySentinelDeficit, o.SentinelTopologyReplicaDeficit)
	return c
}

// SentinelTopologyDetail builds the deterministic dimension-list detail
// shared by the SentinelTopologyReconciled condition message and the
// SentinelTopologyMismatch event note, so the two never diverge. It
// names only the dimensions whose deficit is > 0, in the fixed order
// sentinels then replicas, joined by "; ". Exported so the reconciler's
// event emitter reuses the exact same builder as the condition.
func SentinelTopologyDetail(sentinelDeficit, replicaDeficit int) string {
	parts := make([]string, 0, 2)
	if sentinelDeficit > 0 {
		parts = append(parts, fmt.Sprintf("sentinels short by %d (peer-gossip gap)", sentinelDeficit))
	}
	if replicaDeficit > 0 {
		parts = append(parts, fmt.Sprintf("replicas short by %d", replicaDeficit))
	}
	return strings.Join(parts, "; ")
}
