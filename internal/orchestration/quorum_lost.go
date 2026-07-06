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

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
)

// Reasons for the QuorumLost condition.
const (
	ReasonQuorumOK   = "QuorumReachable"
	ReasonQuorumLost = "QuorumUnreachable"
)

// evalQuorumLost derives the QuorumLost condition from the per-CR
// suppression-gate state, matching the semantics of the QuorumLost /
// QuorumReached events. Only meaningful in sentinel mode.
//
// Both the condition and the events are driven by the same gate:
//
//   - Gate ACTIVATES after the configured loss-threshold (default
//     60s) of sustained Lost observations. The QuorumLost event
//     fires once on entry; this evaluator returns True from that
//     point on.
//   - Gate DEACTIVATES after the configured recovery hysteresis
//     (default 2 consecutive OK polls). The QuorumReached event
//     fires once on exit; this evaluator returns False from that
//     point on.
//
// Routing the condition through the gate (instead of the raw
// aggregator output, as the pre-fix design did) eliminates user-
// visible flap during a recovery rollout: each pod replacement can
// produce a transient NOQUORUM observation, but a single transient
// observation no longer flips the user's `kubectl describe` view.
// Sustained NOQUORUM still re-fires the gate as expected.
//
// Unknown surfacing: the aggregator's tri-state Quorum value
// (Unknown when no fresh records are available) maps directly to
// ConditionUnknown so a fresh CR before its first SentinelQuorum
// write doesn't show a spurious False.
func evalQuorumLost(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypeQuorumLost}
	if o.CR == nil || o.CR.Spec.Mode != valkeyv1beta1.ModeSentinel {
		c.Status = metav1.ConditionFalse
		c.Reason = ReasonNotApplicable
		c.Message = "QuorumLost is not applicable outside sentinel mode"
		return c
	}
	switch {
	case o.QuorumSuppressionActive:
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonQuorumLost
		c.Message = "sentinel quorum suppression gate active (sustained NOQUORUM past the configured threshold, awaiting recovery polls)"
	case o.SentinelQuorum.Quorum == sentinel.QuorumStatusUnknown:
		// No data → Unknown.
		c.Status = metav1.ConditionUnknown
		c.Reason = ReasonNoFreshObservation
		c.Message = "no fresh SentinelQuorum records observed"
	default:
		c.Status = metav1.ConditionFalse
		c.Reason = ReasonQuorumOK
		c.Message = "sentinel quorum suppression gate inactive"
	}
	return c
}
