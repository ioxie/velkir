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

// evalBootstrapComplete reports whether the CR has finished its
// initial bootstrap. **Latch semantics**: once the condition reaches
// True with reason `PromotedFromBootstrap`, it stays True for the
// CR's lifetime — even if the cluster later loses quorum or
// degrades. Ongoing health is the job of `SentinelQuorumReached` and
// `Ready`; `BootstrapComplete` answers exactly one question:
// "has the operator ever needed to consult `bootstrapNode` again on
// this CR?". Once the answer is no, it stays no.
//
// Reasons (closed set):
//
//   - BootstrapNotConfigured: spec.bootstrapNode is unset; the CR
//     starts up self-sufficient. Status=True (latched immediately,
//     because there's nothing to bootstrap from).
//   - Replicating: spec.bootstrapNode is set and the operator is
//     still importing data from the external primary. Status=False.
//   - PromotedFromBootstrap: bootstrap succeeded; data was imported
//     and the new primary is now self-hosted. Status=True (latched).
//   - BootstrapFailed: preflight or import error during bootstrap.
//     Status=False; the reconciler is expected to surface the
//     specific failure separately on Reconciled / Degraded.
//   - AlreadyLatched: the prior condition was already True, so the
//     evaluator preserves it without re-deriving. Reason kept stable
//     so message-stability tests don't flap on the latch.
//
// Currently the only deterministic path is the no-bootstrap one:
// spec.bootstrapNode unset → True with BootstrapNotConfigured. The
// other three reasons land alongside the bootstrap controller when
// the import machinery exists.
func evalBootstrapComplete(o Observation) metav1.Condition {
	c := metav1.Condition{Type: TypeBootstrapComplete}

	// Latch: if the prior condition was already True, preserve it
	// without re-deriving. This is what "latched" means — quorum
	// loss / cluster degradation must NOT flip BootstrapComplete
	// back to False.
	if prior := findPriorCondition(o, TypeBootstrapComplete); prior != nil && prior.Status == metav1.ConditionTrue {
		c.Status = metav1.ConditionTrue
		c.Reason = prior.Reason
		c.Message = prior.Message
		return c
	}

	// No bootstrap configured — latched True from the start.
	if o.CR == nil || o.CR.Spec.BootstrapNode == nil {
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonBootstrapNotConfigured
		c.Message = "bootstrap not configured; CR starts self-sufficient"
		return c
	}

	// Until the bootstrap-import controller exists, mark as
	// Replicating so the latch can take over the moment that
	// controller flips it.
	c.Status = metav1.ConditionFalse
	c.Reason = ReasonBootstrapReplicating
	c.Message = "awaiting bootstrap import"
	return c
}
