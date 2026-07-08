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

// evalAvailable is True the moment at least one pod is Ready, even if
// the rest haven't caught up yet. It is a read-path / serving signal:
// "at least one replica can serve". It deliberately does NOT track the
// write-path / primary liveness — Ready owns that. So during a
// MasterLost or NoMasterAgreement window a healthy-STS sentinel CR
// stays Available=True (replicas still serve reads) while Ready flips
// False (writes via the primary Service black-hole). Consumers that
// need write-availability must gate on Ready, not Available.
func evalAvailable(o Observation) metav1.Condition {
	c := metav1.Condition{
		Type:   TypeAvailable,
		Status: metav1.ConditionFalse,
	}
	if o.STS == nil {
		c.Reason = ReasonNoSTS
		c.Message = "StatefulSet not yet created"
		return c
	}
	if o.STS.Status.ReadyReplicas >= 1 {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonAvailable
		c.Message = "at least one replica is Ready"
		return c
	}
	c.Reason = ReasonReplicaWait
	c.Message = "no replicas are Ready yet"
	return c
}
