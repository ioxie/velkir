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

// evalProgressing is True while the cluster shape is converging
// (STS missing or replicas catching up). False once steady state is
// reached. Sentinel/replication rollout phases extend this evaluator.
func evalProgressing(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypeProgressing}
	if o.STS == nil {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonNoSTS
		c.Message = "creating StatefulSet"
		return c
	}
	desired := int32(0)
	if o.STS.Spec.Replicas != nil {
		desired = *o.STS.Spec.Replicas
	}
	if o.STS.Status.ReadyReplicas < desired {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonProgressing
		c.Message = "waiting for replicas to become Ready"
		return c
	}
	c.Status = metav1.ConditionFalse
	c.Reason = ReasonAsExpected
	c.Message = "no rollout in progress"
	return c
}
