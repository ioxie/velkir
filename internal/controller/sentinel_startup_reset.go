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
	"fmt"
	"net"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/events"
	"github.com/ioxie/velkir/internal/sentinel"
	"github.com/ioxie/velkir/internal/valkey"
)

// SentinelStartupReset is a leader-gated one-shot manager.Runnable
// that probes every sentinel-mode Valkey CR on leader-acquire and
// fires a corrective `SENTINEL RESET *` + `SENTINEL MONITOR` only
// when the probe detects an anomaly. A consistent post-restart
// probe is a silent no-op.
//
// Safety net: the per-CR reconciler's pod-UID tracker is in-memory
// only and resets on operator restart, so a sentinel pod replaced
// during downtime would carry a ghost-peer entry for the dead myid
// on every surviving sentinel until the next pod-replacement RESET.
// The probe gate keeps the safety net's recovery purpose while
// preventing the cascading-wedge failure mode an unconditional
// RESET produces — a RESET wipes sentinel's accumulated topology
// (current master IP, slave list, peer-sentinel list), and without
// a follow-up `SENTINEL MONITOR` the sentinels fall back to a
// possibly-stale on-disk sentinel.conf pointer. RunInitialReset
// owns the gate + the MONITOR follow-up.
//
// Bypasses the deferral predicate — leader-acquire is exactly the
// case where missed failover transitions during the leader-loss
// window need to be cleared.
type SentinelStartupReset struct {
	Client client.Client
	// APIReader bypasses the manager's cache for user-supplied auth
	// Secret reads. See ValkeyReconciler.APIReader for rationale. Nil-
	// safe — falls back to Client.
	APIReader        client.Reader
	SentinelObserver *sentinel.Manager

	// ShortAuthPasswordReporter mirrors ValkeyReconciler's reporter so
	// the startup safety-net's auth-Secret reads also surface the
	// AuthSecretShortPassword warning. Nil-safe; the reporter handles
	// dedup so a per-CR-Secret tuple emits at most once even when the
	// reconciler and this safety net both touch the same Secret on a
	// cold leader-acquire.
	ShortAuthPasswordReporter *events.ShortAuthPasswordReporter

	// LagChecker is reused as a wire client that issues `INFO
	// replication` against valkey pods. Used by observedMasterIP's
	// fallback path when no pod carries role=primary — typically the
	// wedge state where Phase 7 has stripped/suppressed labels because
	// the sentinel observer reports a primary IP matching no pod.
	// Nil-safe — defaults to &valkey.DialingLagChecker{} at first use,
	// mirroring the ValkeyReconciler.LagChecker pattern.
	LagChecker valkey.LagChecker
}

// Compile-time enforcement.
var _ manager.Runnable = (*SentinelStartupReset)(nil)
var _ manager.LeaderElectionRunnable = (*SentinelStartupReset)(nil)

// NeedLeaderElection scopes the safety net to the leader. Running
// it from every replica would multiply the sentinel-side load
// without contributing additional recovery (every replica would
// fire the same RESET against the same pods).
func (s *SentinelStartupReset) NeedLeaderElection() bool { return true }

// Start runs once at leader-acquire and returns. Per the
// controller-runtime Runnable contract a returning Start is fine
// for one-shot work; the manager doesn't restart it. ctx
// cancellation aborts the in-flight pass cleanly.
func (s *SentinelStartupReset) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("sentinel-startup-reset")
	logger.Info("starting sentinel startup safety net")

	crs := &valkeyv1beta1.ValkeyList{}
	// Cluster-wide list is intentional: the safety net runs once on
	// leader-acquire and must reach every sentinel-mode CR in every
	// watched namespace. The custom linter forbids unscoped List
	// calls by default; that rule defends against accidental
	// reconciler-loop unboundedness, which doesn't apply here (one
	// list per leader-acquire is bounded by definition).
	//nolint:velkir-lints // intentional cluster-wide scan, fires once per leader-acquire
	if err := s.Client.List(ctx, crs); err != nil {
		return fmt.Errorf("listing Valkey CRs: %w", err)
	}

	targets := make([]sentinel.InitialResetTarget, 0, len(crs.Items))
	for i := range crs.Items {
		v := &crs.Items[i]
		if v.Spec.Mode != valkeyv1beta1.ModeSentinel {
			continue
		}
		if v.Spec.Sentinel == nil || v.Spec.Sentinel.MasterName == "" {
			// Defensive — webhook rejects empty masterName, but
			// guard at consumer site too.
			continue
		}
		endpoints, err := s.sentinelEndpointsForCR(ctx, v)
		if err != nil {
			logger.V(1).Info("listing sentinel pods failed; skipping CR",
				"cr", v.Namespace+"/"+v.Name, "err", err.Error())
			continue
		}
		if len(endpoints) == 0 {
			// Sentinel-mode CR with no STS yet OR every pod missing
			// PodIP — log so the no-op is visible.
			logger.V(1).Info("no sentinel endpoints for CR; skipping",
				"cr", v.Namespace+"/"+v.Name)
			continue
		}
		password, cleanup, err := s.lookupAuthPassword(ctx, v)
		// Deferred Forget runs at Start return — after RunInitialReset
		// has consumed every Password in targets. Cleanup is non-nil on
		// every path, so defer before the err branch is safe.
		defer cleanup()
		if err != nil {
			logger.V(1).Info("auth secret lookup failed; skipping CR",
				"cr", v.Namespace+"/"+v.Name, "err", err.Error())
			continue
		}
		masterIP := s.observedMasterIP(ctx, v, password)
		targets = append(targets, sentinel.InitialResetTarget{
			CR:         types.NamespacedName{Namespace: v.Namespace, Name: v.Name},
			MasterName: v.Spec.Sentinel.MasterName,
			Endpoints:  endpoints,
			Password:   password,
			MasterIP:   masterIP,
			Port:       int(defaultValkeyPort),
			Quorum:     int(v.Spec.Sentinel.Quorum),
			Tuning: sentinel.MasterTuning{
				DownAfterMilliseconds: v.Spec.Sentinel.DownAfterMilliseconds,
				FailoverTimeout:       v.Spec.Sentinel.FailoverTimeout,
				ParallelSyncs:         v.Spec.Sentinel.ParallelSyncs,
			},
		})
	}

	logger.Info("dispatching startup safety-net pass",
		"crs_total", len(crs.Items), "targets", len(targets))
	// Audit only the CRs the safety net actually reset (probe-gated;
	// most post-restart probes are a silent no-op), per the outcome
	// RunInitialReset returns — a bare per-target emit would record a
	// cold_start reset for every healthy sentinel-mode CR on every
	// leader-acquire.
	for _, o := range s.SentinelObserver.RunInitialReset(ctx, targets) {
		auditSentinelReset(ctx, o.CR, o.Targets, "cold_start")
	}
	return nil
}

// observedMasterIP returns the PodIP of the valkey pod the operator
// currently believes to be the master. Resolution order:
//
//  1. Any valkey pod labelled role=primary with a non-empty PodIP.
//     This is what the running reconciler stamps onto the pod
//     reflecting the most recent observer snapshot; sufficient
//     when the operator restarted but cluster state hasn't churned.
//     A multi-primary state (>1 pod labelled primary) is observed
//     but only one IP is returned — Phase 7 enforces mutual
//     exclusivity at steady state, so a multi-primary observation
//     is an early-bootstrap race; the warning surfaces if it
//     persists.
//  2. Fall back to `INFO replication` on every valkey pod and
//     pick the one(s) reporting role=master. Resolves the wedge
//     state where Phase 7 has stripped/suppressed role=primary
//     labels because the sentinel observer reports a master IP
//     matching no pod. Without this fallback, observedMasterIP
//     returns empty and the corrective startup RESET+MONITOR
//     can't fire, leaving the cluster wedged across operator
//     restarts.
//  3. Returns empty string when neither path finds a master.
//     RunInitialReset skips RESET on an empty MasterIP — defense
//     against the cascading-wedge case where firing RESET without
//     a known good MasterIP would wipe sentinel topology.
//
// Best-effort: a List error returns "" so the safety net skips
// the CR cleanly rather than crashing leader-acquire.
func (s *SentinelStartupReset) observedMasterIP(ctx context.Context, v *valkeyv1beta1.Valkey, password string) string {
	logger := log.FromContext(ctx).WithName("sentinel-startup-reset")

	// Step 1 — primary-label lookup. Cache-backed List filtered to
	// (CRLabel + componentValkey + role=primary).
	primaryPods := &corev1.PodList{}
	if err := s.Client.List(ctx, primaryPods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
			RoleLabel:      roleValuePrimary,
		},
	); err != nil {
		logger.V(1).Info("primary-pod list err; skipping master-ip determination",
			"cr", v.Namespace+"/"+v.Name, "err", err.Error())
		return ""
	}
	if len(primaryPods.Items) > 1 {
		// Multi-primary is a labelling race or a Phase 7 bug. Phase 7
		// enforces single-primary; surface the anomaly so the
		// operator-of-the-operator notices, then proceed with the
		// first IP. Non-deterministic but bounded — Phase 7 will
		// re-stamp on the next reconcile.
		names := make([]string, 0, len(primaryPods.Items))
		for i := range primaryPods.Items {
			names = append(names, primaryPods.Items[i].Name)
		}
		logger.Info("multiple pods labelled role=primary; using the first observed IP",
			"cr", v.Namespace+"/"+v.Name, "pods", names)
	}
	for i := range primaryPods.Items {
		if ip := primaryPods.Items[i].Status.PodIP; ip != "" {
			return ip
		}
	}

	// Step 2 — INFO replication fallback. Phase 7 may have
	// stripped/suppressed labels because the observer reports a
	// master IP matching no pod (NoMasterAgreement). In that
	// state, the only source of truth is asking each pod directly.
	// Cluster-wide INFO is expensive (one TCP dial per pod) but
	// the startup-reset path runs at most once per leader-acquire.
	allPods := &corev1.PodList{}
	if err := s.Client.List(ctx, allPods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		logger.V(1).Info("valkey-pod list err during INFO-replication fallback",
			"cr", v.Namespace+"/"+v.Name, "err", err.Error())
		return ""
	}
	checker := s.LagChecker
	if checker == nil {
		checker = &valkey.DialingLagChecker{}
	}
	var masterPods []*corev1.Pod
	for i := range allPods.Items {
		p := &allPods.Items[i]
		if p.Status.PodIP == "" {
			continue
		}
		addr := net.JoinHostPort(p.Status.PodIP, strconv.Itoa(valkey.DefaultPort))
		state, err := checker.CheckLag(ctx, addr, password)
		if err != nil {
			logger.V(1).Info("INFO replication failed; skipping pod for master determination",
				"cr", v.Namespace+"/"+v.Name, "pod", p.Name, "err", err.Error())
			continue
		}
		if state.Role == valkey.RoleMaster {
			masterPods = append(masterPods, p)
		}
	}
	if len(masterPods) == 0 {
		return ""
	}
	if len(masterPods) > 1 {
		// Split-brain on the data-plane: multiple pods report
		// role=master. Sentinel should reconcile this within a few
		// failover-timeout windows. Operator can't pick a winner
		// safely — log and return empty so RESET is skipped.
		names := make([]string, 0, len(masterPods))
		for i := range masterPods {
			names = append(names, masterPods[i].Name)
		}
		logger.Info("multiple valkey pods report role=master via INFO replication; skipping master-ip determination (data-plane split-brain)",
			"cr", v.Namespace+"/"+v.Name, "pods", names)
		return ""
	}
	logger.Info("master IP determined via INFO replication fallback",
		"cr", v.Namespace+"/"+v.Name, "pod", masterPods[0].Name, "ip", masterPods[0].Status.PodIP)
	return masterPods[0].Status.PodIP
}

// listSentinelPodsFor lists the sentinel pods for v using the shared
// {CRLabel, componentSentinel} selector and the "listing sentinel pods"
// error context. Shared by ValkeyReconciler.listSentinelPods and
// SentinelStartupReset.sentinelEndpointsForCR so the reconciler trigger path
// and the startup-reset safety net select the same pods and surface the same
// error wrap. Takes a client.Reader so either receiver's client can drive it.
func listSentinelPodsFor(ctx context.Context, c client.Reader, v *valkeyv1beta1.Valkey) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentSentinel,
		},
	); err != nil {
		return nil, fmt.Errorf("listing sentinel pods: %w", err)
	}
	return pods.Items, nil
}

// sentinelEndpointsFromPodList builds one sentinel.Endpoint per pod with a
// non-empty PodIP (pod name + <podIP>:defaultSentinelPort), skipping
// not-yet-scheduled pods. Shared by ValkeyReconciler.sentinelEndpointsFromPods
// and SentinelStartupReset.sentinelEndpointsForCR so both emit the identical
// Endpoint set. Always returns an initialized (non-nil) slice.
func sentinelEndpointsFromPodList(pods []corev1.Pod) []sentinel.Endpoint {
	out := make([]sentinel.Endpoint, 0, len(pods))
	for i := range pods {
		ip := pods[i].Status.PodIP
		if ip == "" {
			continue
		}
		out = append(out, sentinel.Endpoint{
			Name: pods[i].Name,
			Addr: net.JoinHostPort(ip, strconv.Itoa(int(defaultSentinelPort))),
		})
	}
	return out
}

// sentinelEndpointsForCR builds the sentinel Endpoint set for v via the
// shared listSentinelPodsFor + sentinelEndpointsFromPodList helpers — the
// same lookup + build the reconciler trigger path uses, so the two produce
// identical Endpoint sets. It lives here so cmd/main.go can wire the runnable
// without the reconciler being constructed yet.
func (s *SentinelStartupReset) sentinelEndpointsForCR(ctx context.Context, v *valkeyv1beta1.Valkey) ([]sentinel.Endpoint, error) {
	pods, err := listSentinelPodsFor(ctx, s.Client, v)
	if err != nil {
		return nil, err
	}
	return sentinelEndpointsFromPodList(pods), nil
}

// lookupAuthPassword duplicates the reconciler's password resolver
// path (the reconciler's `lookupAuthPassword` is unexported on
// `*ValkeyReconciler`). Same convention: the auth Secret has a
// `password` data key. Returns empty string when no auth is
// configured.
//
// The returned cleanup func is always non-nil and MUST be invoked
// (typically via defer) once the password is no longer in scope. It
// releases the redaction-registry registration the function makes on
// the success path; on the no-auth, error, and short-password paths
// it is a no-op closure, so callers can `defer cleanup()`
// unconditionally without checking err first.
//
// Reads via APIReader (uncached) so unlabeled user-supplied Secrets
// resolve correctly — the cluster's label-narrowed informer cache
// excludes them.
//
// Side-effect: when the password is non-empty but shorter than
// `logging.MinTokenLen`, fires the AuthSecretShortPassword warning
// event (deduped per CR-Secret tuple). Mirrors the reconciler's
// behaviour so a sentinel-mode CR seen first by the safety net at
// leader-acquire still surfaces the configuration smell.
func (s *SentinelStartupReset) lookupAuthPassword(ctx context.Context, v *valkeyv1beta1.Valkey) (string, func(), error) {
	// Read via APIReader (uncached) when set so unlabeled user-supplied
	// Secrets resolve — the label-narrowed informer cache excludes them.
	reader := client.Reader(s.Client)
	if s.APIReader != nil {
		reader = s.APIReader
	}
	// Delegates to the shared helper so the startup-reset path scrubs the
	// sentinel-auth password too (previously it registered only the master
	// password, leaving spec.auth.sentinelAuthSecretName unredacted here).
	return lookupAuthPasswordWithRedaction(ctx, reader, s.ShortAuthPasswordReporter, v)
}
