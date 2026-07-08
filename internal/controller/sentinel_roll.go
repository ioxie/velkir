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
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/sentinel"
)

// sentinelRollDeferRequeue is how long Phase 3 waits before re-checking
// the failover / valkey-roll gate after deferring the sentinel STS apply.
// Sized to re-poll a few times across a typical election window without
// hot-looping; the FailoverDispatch deadline escape guarantees the gate
// eventually opens, so a deferred roll can never spin forever.
const sentinelRollDeferRequeue = 10 * time.Second

// sentinelRejoinRequeue is how long Phase 3 waits before re-checking
// whether the just-rolled sentinel pod has re-joined the quorum
// (num-other-sentinels >= quorum-1) before advancing the one-at-a-time
// roll to the next pod.
const sentinelRejoinRequeue = 5 * time.Second

// emitSentinelRollDeferred fires the SentinelRollDeferred Normal event
// when the Phase-3 sentinel STS apply is held because a failover or a
// valkey data-plane roll is in flight. Frequent identical emissions
// aggregate at the recorder, so this is safe to call every deferred pass.
func (r *ValkeyReconciler) emitSentinelRollDeferred(v *valkeyv1beta1.Valkey, cause string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(v, nil, corev1.EventTypeNormal, string(events.SentinelRollDeferred),
		"SentinelRoll", "deferring sentinel StatefulSet apply: %s in flight — yielding to the election window", cause)
}

// peerCounts reads num-other-sentinels per sentinel pod via the
// injectable seam (defaults to sentinel.MasterPeerCountAll). Overridable
// in tests so the re-join gate can be exercised without live sentinels.
func (r *ValkeyReconciler) peerCounts(ctx context.Context, endpoints []sentinel.Endpoint, masterName, password string) []sentinel.MasterPeerCountResult {
	fn := r.sentinelPeerCountFn
	if fn == nil {
		fn = sentinel.MasterPeerCountAll
	}
	return fn(ctx, endpoints, masterName, password)
}

// sentinelReplicas returns the desired sentinel replica count, defaulting
// to 1 for a CR that hasn't been through the defaulter (direct-build unit
// tests). The roll walk treats a single sentinel as un-gateable (there is
// no surviving quorum to protect).
func sentinelReplicas(v *valkeyv1beta1.Valkey) int32 {
	if v.Spec.Sentinel != nil && v.Spec.Sentinel.Replicas > 0 {
		return v.Spec.Sentinel.Replicas
	}
	return 1
}

// sentinelRejoinThreshold is the num-other-sentinels a rebuilt sentinel
// must report before the roll advances: quorum-1 (it has re-found enough
// peers that, with itself, the surviving set can still reach quorum). A
// non-positive threshold (degenerate quorum <= 1) disables the gate.
func sentinelRejoinThreshold(v *valkeyv1beta1.Valkey) int {
	if v.Spec.Sentinel == nil {
		return 0
	}
	return int(v.Spec.Sentinel.Quorum) - 1
}

// sentinelPodOrdinal parses the StatefulSet ordinal off a sentinel pod
// name (`<sts>-<ordinal>`). Returns (ordinal, true) on a well-formed
// name, (0, false) otherwise.
func sentinelPodOrdinal(podName, stsName string) (int32, bool) {
	prefix := stsName + "-"
	if !strings.HasPrefix(podName, prefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(podName, prefix))
	if err != nil || n < 0 {
		return 0, false
	}
	return int32(n), true
}

// computeSentinelRollPartition returns the rollingUpdate.partition the
// sentinel StatefulSet should carry so its roll advances one pod at a
// time, each step gated on the previously-rolled sentinel re-joining the
// quorum (num-other-sentinels >= quorum-1 via MasterPeerCountAll).
//
// Sentinel pods are peers with no ordered bootstrap, but a rolling
// restart that replaces them faster than they re-discover one another
// over gossip can transiently drop the live quorum below the failover
// threshold. Holding the partition until each rebuilt sentinel reports it
// has re-found quorum-1 peers keeps the surviving set able to complete an
// election throughout the roll. The StatefulSet rolls highest-ordinal
// first, so the already-rolled set is always a top suffix of the ordinal
// range and the most-recently-rolled pod is its lowest ordinal.
//
// Returns (partition, requeue, error). requeue > 0 means the roll is
// mid-flight and holding for a re-join; the caller folds it into the
// reconcile result so the next pass re-checks. A missing STS (initial
// creation) or a settled STS (no pending revision) returns partition 0
// (no hold). On a pod-list read error the partition is held at the top
// (no further roll) and the error is returned for the caller to log and
// requeue — never advance on incomplete information.
func (r *ValkeyReconciler) computeSentinelRollPartition(ctx context.Context, v *valkeyv1beta1.Valkey, password string) (int32, time.Duration, error) {
	stsName := v.Name + suffixSentinel
	replicas := sentinelReplicas(v)

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: stsName}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return 0, 0, nil // initial creation — no roll to gate
		}
		return 0, 0, err
	}

	updateRev := sts.Status.UpdateRevision
	if updateRev == "" || updateRev == sts.Status.CurrentRevision {
		// No pending roll (or the controller hasn't computed revisions
		// yet): nothing to gate; let the STS settle at partition 0.
		return 0, 0, nil
	}

	pods, err := listSentinelPodsFor(ctx, r, v)
	if err != nil {
		// Can't read the roll frontier — hold at the top and requeue.
		return replicas, sentinelRejoinRequeue, err
	}

	// Count the rolled pods (at updateRev) and track the lowest-ordinal
	// one — the most recently rebuilt sentinel, the one whose re-join
	// gates the next step.
	var numRolled int32
	lowestRolled := replicas // sentinel value = "none rolled"
	for i := range pods {
		if pods[i].Labels[stsRevisionLabel] != updateRev {
			continue
		}
		numRolled++
		if ord, ok := sentinelPodOrdinal(pods[i].Name, stsName); ok && ord < lowestRolled {
			lowestRolled = ord
		}
	}
	if numRolled >= replicas {
		return 0, 0, nil // fully rolled — release the partition
	}
	if numRolled == 0 {
		// Nothing rolled yet — start the roll at the top ordinal. No
		// prior pod to gate on.
		return replicas - 1, sentinelRejoinRequeue, nil
	}

	// A pod has rolled; advance only once it has re-joined the quorum.
	threshold := sentinelRejoinThreshold(v)
	if threshold <= 0 || r.sentinelPodRejoined(ctx, v, pods, lowestRolled, threshold, password) {
		advance := lowestRolled - 1
		if advance <= 0 {
			return 0, 0, nil // last pod about to roll — release
		}
		return advance, sentinelRejoinRequeue, nil
	}
	// Not re-joined yet — hold the partition at the current frontier.
	return lowestRolled, sentinelRejoinRequeue, nil
}

// sentinelPodRejoined reports whether the sentinel pod at the given
// ordinal reports num-other-sentinels >= threshold — i.e. it has
// re-discovered enough peers over gossip to count as re-joined. A wire
// error, a missing result, or a sub-threshold count all read as
// not-yet-re-joined (hold the roll). password is the sentinel auth
// credential threaded from the reconcile pass.
func (r *ValkeyReconciler) sentinelPodRejoined(ctx context.Context, v *valkeyv1beta1.Valkey, pods []corev1.Pod, ordinal int32, threshold int, password string) bool {
	stsName := v.Name + suffixSentinel
	target := stsName + "-" + strconv.Itoa(int(ordinal))
	// Query ONLY the freshly-rolled target pod. Each per-pod read opens a
	// fresh unpooled dial + AUTH + SENTINEL MASTER round-trip, so fanning
	// out to every sentinel when only the target's count is needed would
	// waste N-1 dials on each re-join poll.
	var targetEP []sentinel.Endpoint
	for _, ep := range sentinelEndpointsFromPodList(pods) {
		if ep.Name == target {
			targetEP = []sentinel.Endpoint{ep}
			break
		}
	}
	if len(targetEP) == 0 {
		// The rebuilt pod has no reachable endpoint yet (no PodIP / mid
		// recreate) — not re-joined.
		return false
	}
	masterName := ""
	if v.Spec.Sentinel != nil {
		masterName = v.Spec.Sentinel.MasterName
	}
	for _, res := range r.peerCounts(ctx, targetEP, masterName, password) {
		if res.Name == target {
			return res.Err == nil && res.Count >= threshold
		}
	}
	return false
}
