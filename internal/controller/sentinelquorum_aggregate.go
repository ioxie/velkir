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
	"math"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/sqaggregate"
)

// sentinelQuorumFreshnessWindow is the wall-clock window during
// which a SentinelQuorum.Status.LastObservedTime is treated as fresh
// for aggregation. Matches the sustained-loss observation window so
// per-pod reachability staleness and per-CR suppression-gate timing
// stay consistent.
const sentinelQuorumFreshnessWindow = 60 * time.Second

// aggregateSentinelQuorum lists per-pod SentinelQuorum records for
// the CR and computes the per-CR aggregation. Returns the zero
// Result on any list error or when the CR isn't in sentinel mode —
// the status evaluators treat the zero Result as Unknown (not False)
// so a transient apiserver flake or a non-sentinel CR doesn't
// surface as a spurious quorum-lost / no-majority signal.
//
// List errors are logged at V(1). Distinguishing "transient
// apiserver flake" from "no fresh observations" matters when an
// operator is debugging why the QuorumLost condition is Unknown —
// without the log, the silently-swallowed list error has no
// signal trail at all.
func (r *ValkeyReconciler) aggregateSentinelQuorum(ctx context.Context, v *valkeyv1beta1.Valkey) sqaggregate.Result {
	if v == nil || v.Spec.Mode != valkeyv1beta1.ModeSentinel || v.Spec.Sentinel == nil {
		return sqaggregate.Result{}
	}
	sqs := &valkeyv1beta1.SentinelQuorumList{}
	if err := r.List(ctx, sqs,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentSentinel,
		},
	); err != nil {
		logf.FromContext(ctx).V(1).Info(
			"SentinelQuorum list failed; conditions will surface as Unknown until next reconcile",
			"namespace", v.Namespace, "name", v.Name, "error", err.Error())
		return sqaggregate.Result{}
	}
	return sqaggregate.Aggregate(time.Now(), sentinelQuorumFreshnessWindow,
		clampQuorumToPoolMajority(v.Spec.Sentinel.Quorum, v.Spec.Sentinel.Replicas), sqs.Items)
}

// clampQuorumToPoolMajority raises a sub-majority spec.sentinel.quorum
// up to the sentinel pool majority before it gates the operator's own
// aggregated quorum verdict (sqaggregate.Aggregate), which drives the
// QuorumLost status condition and Status.PrimaryPod. A sub-majority
// quorum is admitted on purpose (it tunes Sentinel's own +odown
// threshold via sentinel.conf), but the operator must never aggregate a
// QuorumOK verdict on fewer than a majority of sentinels reporting
// reachable. Reuses sentinel.QuorumThreshold so this floor matches the
// pool-majority the observer's own relabel guard already enforces. A
// no-op once quorum already meets or exceeds the majority.
func clampQuorumToPoolMajority(quorum, replicas int32) int32 {
	floor := sentinel.QuorumThreshold(int(replicas))
	// floor is a majority of replicas, so it is non-negative and fits
	// int32; the explicit bound makes that visible to static analysis.
	if floor < 0 || floor > math.MaxInt32 {
		return quorum
	}
	f := int32(floor)
	if quorum >= f {
		return quorum
	}
	return f
}
