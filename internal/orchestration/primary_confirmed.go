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

// Reasons for the PrimaryConfirmed condition.
const (
	ReasonPrimaryConfirmed   = "MajorityAgreed"
	ReasonNoPrimaryMajority  = "NoMajority"
	ReasonNoFreshObservation = "NoFreshObservation"
)

// evalPrimaryConfirmed reports whether the per-pod SentinelQuorum
// aggregation reached a strict majority on a non-empty observed
// primary. Only meaningful in sentinel mode; standalone/replication
// CRs see NotApplicable.
func evalPrimaryConfirmed(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypePrimaryConfirmed}
	if o.CR == nil || o.CR.Spec.Mode != valkeyv1beta1.ModeSentinel {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonNotApplicable
		c.Message = "PrimaryConfirmed is not applicable outside sentinel mode"
		return c
	}
	switch {
	case o.SentinelQuorum.PrimaryConfirmed:
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonPrimaryConfirmed
		c.Message = fmt.Sprintf("strict majority of %d fresh sentinel record(s) agree on primary %q",
			o.SentinelQuorum.FreshCount, o.SentinelQuorum.PrimaryPod)
	case o.SentinelQuorum.FreshCount == 0:
		// No data → Unknown rather than False; a sentinel-mode CR
		// before its first SQ write shouldn't show False here.
		c.Status = metav1.ConditionUnknown
		c.Reason = ReasonNoFreshObservation
		c.Message = "no fresh SentinelQuorum records observed"
	default:
		c.Status = metav1.ConditionFalse
		c.Reason = ReasonNoPrimaryMajority
		c.Message = fmt.Sprintf("no strict majority on primary across %d fresh sentinel record(s)",
			o.SentinelQuorum.FreshCount)
	}
	return c
}
