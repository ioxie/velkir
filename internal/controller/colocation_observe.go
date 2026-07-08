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

package controller

import (
	corev1 "k8s.io/api/core/v1"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
)

// warnOnSameNodeColocation emits a PodsCoLocated Warning Event for each
// node carrying 2+ pods of the given component. The soft (preferred)
// cross-node anti-affinity the defaulter stamps discourages but cannot
// prevent co-location under node scarcity or scheduling pressure, so
// this surfaces the residual single-node-failure risk at runtime.
//
// Same-pod-set only: callers pass a pre-filtered list of one
// component's pods (valkey OR sentinel), so a valkey + sentinel pair
// sharing a node — which is permitted — never triggers a warning.
//
// Best-effort and recorder-dedup-bounded: there is no per-CR edge
// tracker. The EventRecorder collapses repeats of the same
// reason+message within its dedup window, so a co-location that
// persists across reconciles does not spam the event stream.
func (r *ValkeyReconciler) warnOnSameNodeColocation(v *valkeyv1beta1.Valkey, pods []corev1.Pod, component string) {
	if len(pods) < 2 {
		return
	}
	byNode := make(map[string]int, len(pods))
	for i := range pods {
		if node := pods[i].Spec.NodeName; node != "" {
			byNode[node]++
		}
	}
	for node, n := range byNode {
		if n < 2 {
			continue
		}
		r.recordEventf(v, corev1.EventTypeWarning, string(events.PodsCoLocated), "AntiAffinityObserve",
			"%d %s pods scheduled on node %q; the same-pod-set anti-affinity default is soft (preferred), so a single node failure can take down %d of %d %s pods",
			n, component, node, n, len(pods), component)
	}
}
