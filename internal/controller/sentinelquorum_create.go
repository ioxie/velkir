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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	valkeyv1beta1ac "github.com/ioxie/velkir/api/v1beta1/applyconfiguration/api/v1beta1"
	"github.com/ioxie/velkir/internal/util/ssa"
)

// reconcileSentinelQuorums ensures one SentinelQuorum resource per
// sentinel pod observed for the CR. Each SQ is owner-ref'd to the
// Valkey CR for cascade delete, with spec.valkey + spec.podName
// stamped immutably (type-level CEL enforces; subsequent reconciles
// produce a no-op SSA patch when shape is already correct).
//
// Pull-model architecture: sentinel pods are not K8s API clients —
// they speak only the Valkey protocol. The operator's in-process
// observer (internal/sentinel/observer.go) is the sole intended
// writer of SQ status. The aggregator (internal/sqaggregate) reads
// per-pod records to derive the per-CR quorum view; reads tolerate
// empty/stale status by reporting Unknown, so unwired records do
// not surface as a failure. Per-pod Role + RoleBinding +
// ServiceAccount scaffolding (the push-model alternative) is
// unnecessary because no peer-impersonation path exists when
// sentinel pods cannot reach the apiserver.
func (r *ValkeyReconciler) reconcileSentinelQuorums(ctx context.Context, v *valkeyv1beta1.Valkey, pods []corev1.Pod) error {
	for i := range pods {
		sq := buildSentinelQuorum(v, pods[i].Name)
		if err := ssa.ApplyAC(ctx, r.Client, sq); err != nil {
			return fmt.Errorf("applying SentinelQuorum %q: %w", pods[i].Name, err)
		}
	}
	return nil
}

// primaryPodNameFromAddr maps a sentinel-reported PrimaryAddr (the
// `host:port` answer to SENTINEL GET-MASTER-ADDR-BY-NAME) back to a
// pod name via the operator's pod-IP→name table. Returns "" when:
//
//   - the addr is empty (sentinel hasn't converged on a primary),
//   - net.SplitHostPort rejects the addr shape (malformed reply,
//     missing port, etc.),
//   - the host isn't in the pod table (lagging informer cache, pod
//     just deleted) — better to surface an empty observedPrimary
//     than a fictional pod name.
//
// net.SplitHostPort correctly handles IPv4 (10.0.0.5:6379), IPv6
// ([::1]:6379), and rejects malformed inputs — extracted as a
// helper so the pure logic is unit-testable without standing up
// the full reconciler.
func primaryPodNameFromAddr(addr string, ipToPodName map[string]string) string {
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	return ipToPodName[host]
}

// buildSentinelQuorum constructs the per-pod SentinelQuorum
// resource. The SQ name matches the pod name (1:1 mapping) so a
// `kubectl get sentinelquorums` reader trivially correlates each
// record with its corresponding pod.
func buildSentinelQuorum(v *valkeyv1beta1.Valkey, podName string) *valkeyv1beta1ac.SentinelQuorumApplyConfiguration {
	return valkeyv1beta1ac.SentinelQuorum(podName, v.Namespace).
		WithLabels(ownedLabels(v, componentSentinel)).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(valkeyv1beta1ac.SentinelQuorumSpec().
			WithValkey(v.Name).
			WithPodName(podName))
}

// reconcileSentinelQuorumStatus stamps per-pod observation data onto
// each SQ.Status from the in-process observer's latest pollOnce
// output. Without this writer, `kubectl get sq` shows
// empty PRIMARY / QUORUM columns indefinitely (the observer's
// in-memory snapshot never propagated to the apiserver-visible
// records).
//
// Best-effort:
//   - When the observer hasn't published any observations yet
//     (cold-start, pre-first-poll window), this is a no-op; SQs
//     keep their prior (possibly empty) Status until the next
//     reconcile re-runs.
//   - An EndpointObservation with Reachable=false produces a
//     SQ.Status with QuorumReachable=false, ObservedPrimary="",
//     LastObservedTime=now. This honestly reflects "this sentinel
//     was queried but didn't respond" rather than dropping the
//     record entirely.
//   - Server-side-apply with the operator's field manager — the
//     reconciler is the sole writer of status fields per the type
//     contract; SSA conflicts surface as reconcile errors.
//
// Pod-IP → pod-name resolution: the observer reports addrs in the
// form `<podIP>:<port>` (built by sentinelEndpointsFromPods).
// valkeyPodsByIP gives us the reverse lookup; addrs without a
// matching pod (lagging cache, pod just deleted) leave
// ObservedPrimary empty for that record — better than asserting a
// fictional pod name.
func (r *ValkeyReconciler) reconcileSentinelQuorumStatus(ctx context.Context, v *valkeyv1beta1.Valkey, sentinelPods []corev1.Pod, valkeyPods []corev1.Pod) error {
	if r.SentinelObserver == nil {
		return nil
	}
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	obs := r.SentinelObserver.EndpointObservations(cr)
	if len(obs) == 0 {
		return nil
	}

	ipToPodName := make(map[string]string, len(valkeyPods))
	for i := range valkeyPods {
		if ip := valkeyPods[i].Status.PodIP; ip != "" {
			ipToPodName[ip] = valkeyPods[i].Name
		}
	}

	// Sentinel-pod-name set — only stamp Status on SQs whose
	// corresponding sentinel pod still exists (defensive against
	// observation references to since-deleted pods racing this
	// reconcile pass).
	sentinelExists := make(map[string]bool, len(sentinelPods))
	for i := range sentinelPods {
		sentinelExists[sentinelPods[i].Name] = true
	}

	// Hash-based idempotency. Reconciles run frequently (every pod
	// state change, every CR update, every dependency reconcile);
	// without a skip-when-unchanged guard the status writer fires N
	// SSA-Applies per reconcile per CR regardless of whether the
	// observer has anything new to say. Bound the apiserver / etcd
	// traffic: hash the (Name, PrimaryAddr, QuorumReachable,
	// Reachable, primaryPodName) tuple per pod, skip the writes
	// entirely when this hash matches the last-applied one for this
	// CR. The hash is per-reconcile-pass complete state, so any
	// single-pod observation change forces a rewrite of all SQs in
	// the CR (rare; the cost-payoff is biased toward the steady
	// state). In-memory; lost on operator restart, where the next
	// reconcile resyncs.
	type sqEntry struct {
		name, primaryAddr, primaryPodName string
		quorumReachable, reachable        bool
	}
	entries := make([]sqEntry, 0, len(obs))
	for i := range obs {
		o := obs[i]
		if !sentinelExists[o.Name] {
			continue
		}
		entries = append(entries, sqEntry{
			name:            o.Name,
			primaryAddr:     o.PrimaryAddr,
			primaryPodName:  primaryPodNameFromAddr(o.PrimaryAddr, ipToPodName),
			quorumReachable: o.QuorumReachable && o.Reachable,
			reachable:       o.Reachable,
		})
	}

	// Stable order so the hash is deterministic regardless of
	// observer slice ordering quirks.
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	h := sha256.New()
	for _, e := range entries {
		// sha256.New writer never errors — the error return is part
		// of the io.Writer contract but unused here.
		_, _ = fmt.Fprintf(h, "%s|%s|%s|%t|%t\n",
			e.name, e.primaryAddr, e.primaryPodName, e.quorumReachable, e.reachable)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	crKey := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	// Skip only when the observation is unchanged AND the last write is
	// still recent. The keep-alive (half the freshness window) forces a
	// periodic re-stamp on stable content so a quiet-but-live cluster's
	// LastObservedTime can't freeze and age out of the aggregator's
	// freshness gate — otherwise PrimaryConfirmed latches Unknown and
	// never re-converges. A genuine observation stall is handled above
	// (len(obs) == 0 returns early), so the records correctly age out
	// only when the data plane really stopped flowing.
	if r.stateFor(crKey).sqStatusWriteSkippable(digest, obs[0].At) {
		return nil
	}

	for _, e := range entries {
		now := metav1.NewTime(obs[0].At) // all entries share the poll's wall-clock
		// Re-pin per-entry At from the actual observation (defensive
		// against the unlikely case where pollOnce wrote per-entry
		// timestamps with skew); the entries loop above didn't carry
		// At through. Lookup is O(N) but N is small (3 sentinels).
		for i := range obs {
			if obs[i].Name == e.name {
				now = metav1.NewTime(obs[i].At)
				break
			}
		}
		status := valkeyv1beta1ac.SentinelQuorumStatus().
			WithObservedPrimary(e.primaryPodName).
			WithQuorumReachable(e.quorumReachable).
			WithLastObservedTime(now)
		sq := valkeyv1beta1ac.SentinelQuorum(e.name, v.Namespace).
			WithStatus(status)
		if err := ssa.ApplyACStatus(ctx, r.Client, sq); err != nil {
			return fmt.Errorf("applying SentinelQuorum status %q: %w", e.name, err)
		}
	}
	r.stateFor(crKey).setSQDigest(digest, obs[0].At)
	return nil
}
