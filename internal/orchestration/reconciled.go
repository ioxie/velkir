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

// evalReconciled tracks whether the most recent reconcile pass
// completed without error. False with a stable message text when
// the reconciler returned an error; the canonical short reason
// matches the failure-modes catalog from the design.
func evalReconciled(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypeReconciled}
	if o.ReconcileError != nil {
		c.Status = metav1.ConditionFalse
		c.Reason = ReasonReconcileErr
		c.Message = "reconcile returned an error; see the operator log"
		return c
	}
	c.Status = metav1.ConditionTrue
	c.Reason = ReasonReconciled
	c.Message = "reconcile pass completed without error"
	return c
}
