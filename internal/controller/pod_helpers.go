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
)

// podIPMatchesAny returns true when host equals any pod's
// `Status.PodIP`. Empty PodIPs are skipped (pre-scheduling pods).
// Empty host returns false defensively.
//
// Phase 7's NoMasterAgreement guard uses this to decide whether the
// sentinel-reported primary Addr corresponds to a live pod. A false
// result drives Phase 7 to suppress relabel so the existing label
// set holds while the deferred status closure surfaces
// Degraded=NoMasterAgreement.
func podIPMatchesAny(host string, pods []corev1.Pod) bool {
	if host == "" {
		return false
	}
	for i := range pods {
		if pods[i].Status.PodIP != "" && pods[i].Status.PodIP == host {
			return true
		}
	}
	return false
}

// countPrimaryLabeledPods counts pods carrying
// `velkir.ioxie.dev/role=primary`. Phase 8's stale-replica deletion
// gate keys on the count: zero primaries (with at least one pod
// present) means the cluster is in active recovery — the
// reconciler must NOT delete stale replicas, because they're the
// only failover candidates sentinel can promote.
func countPrimaryLabeledPods(pods []corev1.Pod) int {
	n := 0
	for i := range pods {
		if pods[i].Labels[RoleLabel] == roleValuePrimary {
			n++
		}
	}
	return n
}
