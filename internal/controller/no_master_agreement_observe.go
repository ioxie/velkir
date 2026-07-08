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
	"context"
	"net"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// observeNoMasterAgreement reports whether the sentinel observer's
// current snapshot reports a quorum-OK primary Addr that matches no
// current valkey pod's PodIP. This is the cascading-wedge state the
// startup-reset fix addresses: sentinels agree on a master IP, but
// the IP corresponds to no live pod (typically a pod that was
// recreated with a new IP during operator downtime).
//
// Returns false in four short-circuit shapes (same as
// observeSplitBrain pattern):
//   - nil CR / non-sentinel mode / nil observer / !Present snapshot:
//     no observer signal to interpret as a wedge.
//   - !QuorumOK: the observer-side split-brain guard already
//     suppresses Phase 7 and surfaces Degraded=SplitBrain; the
//     more specific NoMasterAgreement only fires when sentinels
//     agree (QuorumOK=true) AND the agreement is on a dead IP.
//   - List of valkey pods fails: returns false defensively — the
//     condition can be re-evaluated on the next reconcile.
//   - Observer's Primary.Addr maps to ANY live valkey pod's PodIP:
//     the cluster is healthy; no anomaly.
//
// Best-effort: a List error returns false rather than wedging the
// condition into a stuck-True state.
func (r *ValkeyReconciler) observeNoMasterAgreement(ctx context.Context, v *valkeyv1beta1.Valkey) bool {
	if v == nil || v.Spec.Mode != valkeyv1beta1.ModeSentinel || r.SentinelObserver == nil {
		return false
	}
	snap := r.SentinelObserver.Snapshot(types.NamespacedName{Namespace: v.Namespace, Name: v.Name})
	if !snap.Present || !snap.Primary.QuorumOK {
		return false
	}
	host, _, err := net.SplitHostPort(snap.Primary.Addr)
	if err != nil {
		// Malformed Addr — treat as no-anomaly (the snapshot itself
		// is suspect; the SplitBrain path will catch real issues).
		return false
	}
	if host == "" {
		return false
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return false
	}
	for i := range pods.Items {
		if pods.Items[i].Status.PodIP == host {
			return false
		}
	}
	// Observer-reported primary IP matches no current pod — wedge.
	return true
}
