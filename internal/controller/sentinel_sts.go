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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
	"github.com/ioxie/velkir/internal/orchestration"
	"github.com/ioxie/velkir/internal/sqaggregate"
	"github.com/ioxie/velkir/internal/util/ssa"
	"github.com/ioxie/velkir/internal/valkeyconf"
)

// sentinelBootstrapRequeue is how long Phase 3 sleeps after deferring the
// sentinel STS because pod-0 of the valkey STS doesn't yet have a PodIP.
// Pods aren't in the controller's watch set, so without an explicit
// requeue the operator sits idle until something else triggers a
// reconcile — without it the operator can wedge at "bootstrap CM has
// empty seedMasterIP" after pod-0 has received an IP. 5 seconds
// matches kubelet's typical pod-status-update latency.
const sentinelBootstrapRequeue = 5 * time.Second

const (
	suffixSentinelConf        = "-sentinel-conf"
	suffixSentinelInitScripts = "-sentinel-init-scripts"
	suffixSentinel            = "-sentinel"
	suffixSentinelHeadless    = "-sentinel-headless"
	mountSentinelTemplate     = "/sentinel-template"
	mountSentinelConf         = "/sentinel"
	mountSentinelInitScripts  = "/sentinel-init-scripts"
	sentinelRenderScriptPath  = "/sentinel-init-scripts/render-sentinel-conf.sh"
	bootstrapSeedMasterIPKey  = "seedMasterIP"
)

// reconcileSentinelInfra creates the sentinel data-plane: the
// sentinel-conf + sentinel-init-scripts ConfigMaps, the
// sentinel-bootstrap ConfigMap, the headless + client Services, and
// the sentinel StatefulSet. No-op for non-sentinel modes.
//
// Bootstrap order: the sentinel STS is held until pod-0 of the
// valkey STS has a PodIP. The sentinel pods' init container reads
// the bootstrap CM at start; creating the STS before pod-0 has an
// IP would CrashLoopBackOff every sentinel pod until the next
// reconcile pass re-creates them.
//
// Returns a requeue hint: non-zero when the STS was deferred (pod-0
// PodIP not yet observed), zero otherwise. Pods aren't in the
// controller's watch set, so the caller must merge this hint into
// the outer reconcile result or the operator sits idle waiting for
// some unrelated event to retrigger reconciliation.
func (r *ValkeyReconciler) reconcileSentinelInfra(ctx context.Context, v *valkeyv1beta1.Valkey, state orchestration.State, password string) (time.Duration, error) {
	if v.Spec.Mode != valkeyv1beta1.ModeSentinel {
		return 0, nil
	}
	if v.Spec.Sentinel == nil || v.Spec.Sentinel.MasterName == "" {
		return 0, nil
	}

	sentinelCMHash, err := r.reconcileSentinelConfigMaps(ctx, v)
	if err != nil {
		return 0, fmt.Errorf("sentinel configmaps: %w", err)
	}
	if err := r.reconcileSentinelServices(ctx, v); err != nil {
		return 0, fmt.Errorf("sentinel services: %w", err)
	}

	seedIP := r.seedMasterIPForCR(ctx, v)
	logf.FromContext(ctx).V(1).Info("phase 3: sentinel infra reconcile",
		"cr", v.Namespace+"/"+v.Name, "seedIP", seedIP)
	if err := r.reconcileSentinelBootstrapCM(ctx, v, seedIP); err != nil {
		return 0, fmt.Errorf("sentinel bootstrap configmap: %w", err)
	}
	if seedIP == "" {
		return sentinelBootstrapRequeue, nil
	}

	// Serialize the sentinel STS apply behind the failover critical
	// section: a sentinel pod must never be rolled while a failover is in
	// flight or the valkey data plane is mid-roll, or the roll can drop
	// the surviving quorum below the threshold the in-flight election
	// needs. Defer (requeue) and re-check once the window clears — the
	// FailoverDispatch deadline escape bounds the wait, so the defer can
	// never hold indefinitely.
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	if r.IsFailoverInFlight(cr) {
		r.emitSentinelRollDeferred(v, "failover")
		return sentinelRollDeferRequeue, nil
	}
	if valkeyRollActive(state) {
		r.emitSentinelRollDeferred(v, "valkey roll")
		return sentinelRollDeferRequeue, nil
	}

	// Window is clear: roll one sentinel pod at a time, advancing only
	// after the previously-rolled pod re-joins the quorum.
	partition, rollRequeue, perr := r.computeSentinelRollPartition(ctx, v, password)
	if perr != nil {
		// Couldn't read the roll frontier; the returned partition already
		// pins the roll at its current point. Log and retry via the
		// requeue rather than advancing on incomplete information.
		logf.FromContext(ctx).V(1).Info("phase 3: sentinel roll partition read failed; holding",
			"cr", v.Namespace+"/"+v.Name, "err", perr.Error())
	}
	if err := r.reconcileSentinelSTS(ctx, v, sentinelCMHash, partition); err != nil {
		return 0, fmt.Errorf("sentinel statefulset: %w", err)
	}
	return rollRequeue, nil
}

// reconcileSentinelConfigMaps applies the sentinel-conf template CM
// (rendered with placeholders for substitution at pod start) and the
// init-scripts CM (the shell script the sentinel init container
// executes). Returns the combined SHA-256 hash so the sentinel STS
// pod-template flips when either input changes.
func (r *ValkeyReconciler) reconcileSentinelConfigMaps(ctx context.Context, v *valkeyv1beta1.Valkey) (string, error) {
	conf := valkeyconf.RenderSentinel(valkeyconf.SentinelInputs{
		MasterName:            v.Spec.Sentinel.MasterName,
		Quorum:                v.Spec.Sentinel.Quorum,
		DownAfterMilliseconds: v.Spec.Sentinel.DownAfterMilliseconds,
		FailoverTimeout:       v.Spec.Sentinel.FailoverTimeout,
		ParallelSyncs:         v.Spec.Sentinel.ParallelSyncs,
	})
	script := renderSentinelInitScript()
	hash := sha256Hex(conf + "\x00" + script)

	confName := v.Name + suffixSentinelConf
	confCM := corev1ac.ConfigMap(confName, v.Namespace).
		WithLabels(ownedLabels(v, componentSentinel)).
		WithAnnotations(map[string]string{ConfigHashAnnotation: hash}).
		WithOwnerReferences(crOwnerRef(v)).
		WithData(map[string]string{"sentinel.conf": conf})
	if err := ssa.ApplyAC(ctx, r.Client, confCM); err != nil {
		return "", fmt.Errorf("applying ConfigMap %q: %w", confName, err)
	}

	scriptName := v.Name + suffixSentinelInitScripts
	scriptCM := corev1ac.ConfigMap(scriptName, v.Namespace).
		WithLabels(ownedLabels(v, componentSentinel)).
		WithAnnotations(map[string]string{ConfigHashAnnotation: hash}).
		WithOwnerReferences(crOwnerRef(v)).
		WithData(map[string]string{"render-sentinel-conf.sh": script})
	if err := ssa.ApplyAC(ctx, r.Client, scriptCM); err != nil {
		return "", fmt.Errorf("applying ConfigMap %q: %w", scriptName, err)
	}
	return hash, nil
}

// reconcileSentinelBootstrapCM writes the per-CR sentinel-bootstrap
// ConfigMap holding the seed primary IP. When seedIP is empty the CM
// is still applied (with an empty value) on first creation, letting
// the data key exist for future updates — but an EXISTING non-empty
// seed is kept rather than overwritten with "" WHEN it still points at
// a live pod of THIS CR: an empty resolution means "undetermined"
// (mid-incident, no labelled primary, no sentinel majority), and the
// last-known primary IP is the safest target for any pod that
// re-renders during that window — a replacement pod booting
// `replicaof <last-known>` against a dead address idles harmlessly
// until sentinels re-elect, whereas booting as an empty master
// manufactures a split brain.
//
// The identity check is the guard against pod-network IP reuse: if the
// stale seed IP no longer belongs to any pod of this CR, the CNI may
// have reassigned it to an UNRELATED valkey pod (another CR in the
// namespace), and a replacement pod-0 would `replicaof` it and flush
// its own dataset on sync. Keeping the seed only while it resolves to
// our own live pod closes that window; otherwise clear it (the worst
// case becomes pod-0 booting as the bootstrap master, exactly the
// dead-address outcome). The existing-CM read uses the uncached
// APIReader so a cache lag at incident onset can't misread the seed as
// absent and wipe a just-written value.
func (r *ValkeyReconciler) reconcileSentinelBootstrapCM(ctx context.Context, v *valkeyv1beta1.Valkey, seedIP string) error {
	name := v.Name + suffixSentinelBootstrap
	if seedIP == "" {
		// Uncached read: a cache lag at incident onset must not misread
		// the seed as absent and wipe a just-written value. APIReader is
		// always wired in production (cmd/main.go); fall back to the
		// cached client in unit/envtest reconcilers that don't set it.
		reader := client.Reader(r.Client)
		if r.APIReader != nil {
			reader = r.APIReader
		}
		existing := &corev1.ConfigMap{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: name}, existing)
		if err == nil {
			if prev := existing.Data[bootstrapSeedMasterIPKey]; prev != "" {
				// Keep the sticky seed when it still names one of our pods,
				// AND on a list error: a transient flake must not wipe a
				// good value (the next reconcile re-evaluates).
				if owned, oerr := r.seedIPOwnedByCR(ctx, v, prev); oerr != nil || owned {
					return nil
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("reading ConfigMap %q: %w", name, err)
		}
	}
	cm := corev1ac.ConfigMap(name, v.Namespace).
		WithLabels(ownedLabels(v, componentSentinel)).
		WithOwnerReferences(crOwnerRef(v)).
		WithData(map[string]string{bootstrapSeedMasterIPKey: seedIP})
	if err := ssa.ApplyAC(ctx, r.Client, cm); err != nil {
		return fmt.Errorf("applying ConfigMap %q: %w", name, err)
	}
	return nil
}

// seedIPOwnedByCR reports whether ip is the PodIP of a live (non-
// terminating) valkey pod belonging to this CR. Used to keep a sticky
// bootstrap seed only while it still names one of our own pods — never
// an IP the pod network may have recycled to a foreign pod.
//
// The List error is surfaced rather than folded into the bool so each
// caller picks its own fail direction: the sticky-CM path keeps the seed
// on error (a transient flake must not wipe a good value), while the
// failover-seed path declines to seed on error (it must not actively
// seed an unverified address).
func (r *ValkeyReconciler) seedIPOwnedByCR(ctx context.Context, v *valkeyv1beta1.Valkey, ip string) (bool, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
		},
	); err != nil {
		return false, fmt.Errorf("listing valkey pods for seed-ownership check: %w", err)
	}
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil && pods.Items[i].Status.PodIP == ip {
			return true, nil
		}
	}
	return false, nil
}

// reconcileSentinelServices applies the two Services backing the
// sentinel pods:
//
//   - <cr>-sentinel-headless — STS-governing headless Service. The
//     sentinel STS's spec.serviceName points here; the valkey
//     preStop hook uses it to issue SENTINEL FAILOVER against any
//     sentinel reachable by DNS.
//   - <cr>-sentinel — ClusterIP Service for general client access.
//     Endpoints include each ready sentinel pod; the e2e suite
//     asserts on `.subsets[*].addresses[*].targetRef.name`.
func (r *ValkeyReconciler) reconcileSentinelServices(ctx context.Context, v *valkeyv1beta1.Valkey) error {
	labels := ownedLabels(v, componentSentinel)
	port := corev1ac.ServicePort().
		WithName("sentinel").
		WithPort(valkeyconf.SentinelPort).
		WithProtocol(corev1.ProtocolTCP)

	headless := corev1ac.Service(v.Name+suffixSentinelHeadless, v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(corev1ac.ServiceSpec().
			WithType(corev1.ServiceTypeClusterIP).
			WithClusterIP(corev1.ClusterIPNone).
			WithSelector(labels).
			WithPublishNotReadyAddresses(true).
			WithPorts(port))
	if err := ssa.ApplyAC(ctx, r.Client, headless); err != nil {
		return fmt.Errorf("applying headless sentinel Service: %w", err)
	}

	clientSvc := corev1ac.Service(v.Name+suffixSentinel, v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(corev1ac.ServiceSpec().
			WithType(corev1.ServiceTypeClusterIP).
			WithSelector(labels).
			WithPorts(port))
	if err := ssa.ApplyAC(ctx, r.Client, clientSvc); err != nil {
		return fmt.Errorf("applying client sentinel Service: %w", err)
	}
	return nil
}

// reconcileSentinelSTS applies the sentinel StatefulSet with the given
// rollingUpdate partition. partition pins the operator-driven
// one-pod-at-a-time roll (computeSentinelRollPartition); 0 means the
// whole set may roll (initial creation or a settled / re-join-cleared
// roll).
func (r *ValkeyReconciler) reconcileSentinelSTS(ctx context.Context, v *valkeyv1beta1.Valkey, cmHash string, partition int32) error {
	sts := buildSentinelSTS(v, cmHash, partition)
	if err := ssa.ApplyAC(ctx, r.Client, sts); err != nil {
		return fmt.Errorf("applying sentinel STS: %w", err)
	}
	return nil
}

// seedMasterIPForCR resolves the IP the sentinel-bootstrap CM should
// seed as the primary. Resolution order:
//
//  1. Any valkey pod labelled role=primary with a non-empty PodIP.
//  2. The SentinelQuorum strict-majority primary, resolved to a live
//     pod with an IP — covers the window where the primary label was
//     lost with its pod (kill, node loss) but a quorum of sentinels
//     still agrees on who the primary is.
//     2a. The durable last-known primary (Status.Rollout.FailoverDispatch's
//     PreStripAddr) when a failover dispatch is in flight, that address
//     still names a live CR-owned pod, and its epoch is not provably
//     superseded — so a pod rebuilt while no quorum majority resolves
//     boots as that primary's replica rather than a second master.
//     Only fires mid-failover (marker set), never at true bootstrap. See
//     seedFromFailoverDispatch.
//  3. Pod-0's PodIP, ONLY at true bootstrap (no SentinelQuorum
//     records exist yet for this CR). Past bootstrap, regressing to
//     "ordinal 0" mid-incident seeds fresh sentinels — and a
//     replacement pod-0 — at a brand-new EMPTY master, manufacturing
//     a second master and deadlocking recovery.
//  4. "" when undetermined — reconcileSentinelBootstrapCM then keeps
//     the CM's last-known-good value, but only while it still names a
//     live CR-owned pod; otherwise it clears the seed (IP-reuse guard).
//
// Why not always pod-0: post-failover the elected primary is some
// other pod; if the bootstrap CM still pointed at pod-0, every
// replica that re-renders its valkey.conf (rolling update, pod
// restart) would set `replicaof <pod-0-ip>` and try to sync from
// the old primary. The sentinel/data-plane never reconverges from
// that state because sentinels can't reconfigure unreachable
// replicas. Tracking the elected primary instead keeps the seed
// fresh across the cluster's lifetime.
func (r *ValkeyReconciler) seedMasterIPForCR(ctx context.Context, v *valkeyv1beta1.Valkey) string {
	log := logf.FromContext(ctx)

	// Prefer the currently-elected primary. Cache-backed List
	// filters at the apiserver level via the role label.
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentValkey,
			RoleLabel:      roleValuePrimary,
		},
	); err != nil {
		log.Info("phase 3: primary-pod list err", "err", err.Error())
		return ""
	}
	for i := range pods.Items {
		if ip := pods.Items[i].Status.PodIP; ip != "" {
			log.V(1).Info("phase 3: seed from primary-labelled pod",
				"cr", v.Namespace+"/"+v.Name, "pod", pods.Items[i].Name, "podIP", ip)
			return ip
		}
	}

	// No labelled primary. Consult the sentinels' own quorum-backed
	// view before considering the bootstrap fallback.
	sqs := &valkeyv1beta1.SentinelQuorumList{}
	if err := r.List(ctx, sqs,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			CRLabel:        v.Name,
			ComponentLabel: componentSentinel,
		},
	); err != nil {
		// Can't tell bootstrap from incident — defer (sticky CM keeps
		// the last seed).
		log.Info("phase 3: SentinelQuorum list err", "err", err.Error())
		return ""
	}
	if len(sqs.Items) > 0 && v.Spec.Sentinel != nil {
		agg := sqaggregate.Aggregate(time.Now(), sentinelQuorumFreshnessWindow,
			clampQuorumToPoolMajority(v.Spec.Sentinel.Quorum, v.Spec.Sentinel.Replicas), sqs.Items)
		if agg.PrimaryConfirmed && agg.PrimaryPod != "" {
			pod := &corev1.Pod{}
			err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: agg.PrimaryPod}, pod)
			if err == nil && pod.DeletionTimestamp == nil && pod.Status.PodIP != "" {
				log.V(1).Info("phase 3: seed from SentinelQuorum majority",
					"cr", v.Namespace+"/"+v.Name, "pod", agg.PrimaryPod, "podIP", pod.Status.PodIP)
				return pod.Status.PodIP
			}
		}
		// No live quorum majority resolves, but a failover dispatch may
		// be in flight: honor the durable last-known primary recorded at
		// strip time before giving up. Seeding it (instead of "") lets a
		// pod rebuilt in the election window boot as that primary's
		// replica rather than as a fresh second master — the data-plane
		// half the quorum path alone can't cover.
		if ip := r.seedFromFailoverDispatch(ctx, v); ip != "" {
			log.V(1).Info("phase 3: seed from durable failover-dispatch marker (mid-failover)",
				"cr", v.Namespace+"/"+v.Name, "podIP", ip)
			return ip
		}
		// Records exist but no resolvable majority — mid-incident.
		// Never regress to the pod-0 fallback here.
		log.V(1).Info("phase 3: seed undetermined (SentinelQuorum records present, no resolvable majority)",
			"cr", v.Namespace+"/"+v.Name, "fresh+stale", len(sqs.Items))
		return ""
	}

	// True bootstrap (no SentinelQuorum records yet) — fall back to
	// pod-0; Phase 7 (reconcileRoleLabels) will stamp it primary
	// once it surfaces.
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Name + "-0"}, pod); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Info("phase 3: pod-0 fallback lookup non-NotFound err",
				"cr", v.Namespace+"/"+v.Name, "err", err.Error())
		}
		return ""
	}
	log.V(1).Info("phase 3: seed from pod-0 fallback (true bootstrap)",
		"cr", v.Namespace+"/"+v.Name, "podIP", pod.Status.PodIP)
	return pod.Status.PodIP
}

// seedFromFailoverDispatch returns the durable last-known primary IP to
// seed while a failover dispatch is in flight, or "" when the marker is
// absent, no longer names a live CR-owned pod, or is provably stale.
// Three gates, all required:
//
//   - the Status.Rollout.FailoverDispatch marker is present. The marker
//     is written only on the operator-driven strip path and never at
//     true bootstrap, so this case can never hijack the pod-0 bootstrap
//     seed (the never-regress-to-pod-0 invariant is preserved).
//   - its PreStripAddr still resolves to a live, non-terminating
//     CR-owned valkey pod (seedIPOwnedByCR) — never an IP the pod
//     network may have recycled to a foreign pod. A list error during
//     that check declines to seed (fail-closed): the address must be
//     positively verified before we boot a pod replicaof it.
//   - the marker's epoch is not provably superseded. The epoch fence
//     matches the in-memory latch's monotonic-token semantics: it bites
//     ONLY when both the marker's PreStripEpoch and the current observed
//     epoch are non-zero AND the marker's is strictly lower (a newer
//     election has advanced the config-epoch past it). A zero PreStripEpoch
//     (config-epoch unparseable at strip — the documented inert fence) or
//     a zero current epoch (the observer has not republished a snapshot
//     yet, e.g. the operator just restarted mid-failover — the exact
//     window this durable marker exists to cover) does NOT fence: the seed
//     still fires off the marker + live-pod gates, so a freshly-restarted
//     operator can honor the durable address before its observer reseeds.
//
// PreStripAddr is the net.JoinHostPort `ip:port` captured at strip time;
// the seed is the host (IP) half.
func (r *ValkeyReconciler) seedFromFailoverDispatch(ctx context.Context, v *valkeyv1beta1.Valkey) string {
	if v.Status.Rollout == nil || v.Status.Rollout.FailoverDispatch == nil {
		return ""
	}
	marker := v.Status.Rollout.FailoverDispatch
	if marker.PreStripAddr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(marker.PreStripAddr)
	if err != nil || host == "" {
		return ""
	}
	owned, err := r.seedIPOwnedByCR(ctx, v, host)
	if err != nil || !owned {
		return ""
	}
	cr := types.NamespacedName{Namespace: v.Namespace, Name: v.Name}
	current := r.currentObservedEpoch(cr)
	if marker.PreStripEpoch > 0 && current > 0 && marker.PreStripEpoch < current {
		return ""
	}
	return host
}

// currentObservedEpoch returns the current observed Sentinel config-epoch
// for cr — the snapshot epoch (observedPrimaryAddrEpoch), or the injected
// stub in tests. Zero when no observer is wired or no snapshot has been
// published.
func (r *ValkeyReconciler) currentObservedEpoch(cr types.NamespacedName) int64 {
	if r.currentEpochFn != nil {
		return r.currentEpochFn(cr)
	}
	_, epoch := r.observedPrimaryAddrEpoch(cr)
	return epoch
}

func buildSentinelSTS(v *valkeyv1beta1.Valkey, cmHash string, partition int32) *appsv1ac.StatefulSetApplyConfiguration {
	labels := ownedLabels(v, componentSentinel)
	podLabels := mergeMaps(labels, v.Spec.Sentinel.PodLabels)
	baseAnnotations := map[string]string{ConfigHashAnnotation: cmHash}
	if gen := v.Annotations[ManualRolloutAnnotation]; gen != "" {
		baseAnnotations[ManualRolloutAnnotation] = gen
	}
	annotations := mergeMaps(baseAnnotations, v.Spec.Sentinel.PodAnnotations)

	// Sentinel pods carry no exporter by default; gate it on the same
	// spec.metrics.enabled toggle the data plane uses (D5). The sidecar
	// produces the redis_sentinel_* series the sentinel alert group and
	// the sentinel PodMonitor consume.
	containers := []*corev1ac.ContainerApplyConfiguration{buildSentinelContainer(v)}
	if v.Spec.Metrics.Enabled != nil && *v.Spec.Metrics.Enabled {
		containers = append(containers, buildSentinelExporterContainer(v))
	}

	podSpec := corev1ac.PodSpec().
		WithServiceAccountName(sentinelServiceAccountName(v)).
		WithAutomountServiceAccountToken(false).
		WithTerminationGracePeriodSeconds(sentinelTerminationGracePeriodVal).
		WithSecurityContext(restrictedPodSecurityContext(v.Spec.Sentinel.SecurityContext)).
		WithInitContainers(buildSentinelInitContainer(v)).
		WithContainers(containers...).
		WithVolumes(buildSentinelVolumes(v)...)
	if aff := acAffinity(v.Spec.Sentinel.Affinity); aff != nil {
		podSpec.WithAffinity(aff)
	}
	if tols := acTolerations(v.Spec.Sentinel.Tolerations); len(tols) > 0 {
		podSpec.WithTolerations(tols...)
	}
	if len(v.Spec.Sentinel.NodeSelector) > 0 {
		podSpec.WithNodeSelector(v.Spec.Sentinel.NodeSelector)
	}
	if tsc := acTopologySpreadConstraints(v.Spec.Sentinel.TopologySpreadConstraints); len(tsc) > 0 {
		podSpec.WithTopologySpreadConstraints(tsc...)
	}
	if v.Spec.Sentinel.PriorityClassName != "" {
		podSpec.WithPriorityClassName(v.Spec.Sentinel.PriorityClassName)
	}
	if dns := acDNSConfig(v.Spec.Sentinel.DNSConfig); dns != nil {
		podSpec.WithDNSConfig(dns)
	}

	template := corev1ac.PodTemplateSpec().
		WithLabels(podLabels).
		WithAnnotations(annotations).
		WithSpec(podSpec)

	// Sentinels are peers (unlike valkey pods, where pod-0 is the
	// canonical bootstrap primary): no master-aware rolling order
	// applies. Parallel pod management lets the STS controller bring
	// all replicas up concurrently so bootstrap takes ~30-60s rather
	// than the OrderedReady chain. RollingUpdate then propagates
	// sentinel.conf changes (quorum, timing) automatically — the
	// existing UID-tracker in reconcileSentinelOrchestration fires
	// SENTINEL RESET on the survivors when the STS controller
	// replaces a pod (D3 path).
	// The operator drives the sentinel roll one pod at a time via the
	// rollingUpdate partition (computeSentinelRollPartition), gating each
	// advance on the previously-rolled sentinel re-joining the quorum —
	// the STS controller's own Ready-gate is too weak (a sentinel is
	// Ready on TCP before it has re-discovered its peers over gossip).
	stsSpec := appsv1ac.StatefulSetSpec().
		WithServiceName(v.Name + suffixSentinelHeadless).
		WithReplicas(v.Spec.Sentinel.Replicas).
		WithPodManagementPolicy(appsv1.ParallelPodManagement).
		WithUpdateStrategy(appsv1ac.StatefulSetUpdateStrategy().
			WithType(appsv1.RollingUpdateStatefulSetStrategyType).
			WithRollingUpdate(appsv1ac.RollingUpdateStatefulSetStrategy().
				WithPartition(partition))).
		WithSelector(metav1ac.LabelSelector().WithMatchLabels(labels)).
		WithTemplate(template)

	return appsv1ac.StatefulSet(v.Name+suffixSentinel, v.Namespace).
		WithLabels(labels).
		WithOwnerReferences(crOwnerRef(v)).
		WithSpec(stsSpec)
}

func buildSentinelInitContainer(v *valkeyv1beta1.Valkey) *corev1ac.ContainerApplyConfiguration {
	env := []*corev1ac.EnvVarApplyConfiguration{
		downwardEnvVar("POD_IP", "status.podIP"),
		downwardEnvVar("POD_NAME", "metadata.name"),
		// MASTER_NAME carries spec.sentinel.masterName so the init
		// script can emit the `sentinel auth-pass <name> <pw>` line
		// when VALKEY_AUTH_PASS is set. Passed as env (not folded into
		// the script body) so the init-scripts ConfigMap stays
		// CR-agnostic and rerenders without churn on master-name
		// edits — sentinel.masterName is immutable today (D12), but
		// the indirection keeps the contract simple.
		corev1ac.EnvVar().WithName("MASTER_NAME").WithValue(v.Spec.Sentinel.MasterName),
	}
	// When spec.auth.secretName is set, sentinel must authenticate to
	// the master with `sentinel auth-pass <master> <pw>`; without it
	// PING/INFO from sentinel → master gets NOAUTH and the quorum
	// never forms (s_down,disconnected indefinitely, num-slaves=0,
	// num-other-sentinels=0 — sentinels discover both via the master's
	// pub/sub channel which they can't subscribe to without auth).
	//
	// Mirror the VALKEY_PASSWORD pattern on the valkey container
	// (valkey_controller.go ~L2900): load the password from the Secret
	// into an env var; the init script appends the directive to the
	// per-pod emptyDir copy of sentinel.conf. The password never lands
	// in the sentinel-conf ConfigMap.
	if v.Spec.Auth != nil && v.Spec.Auth.SecretName != "" {
		key := v.Spec.Auth.SecretKey
		if key == "" {
			key = defaultAuthSecretKey
		}
		env = append(env, corev1ac.EnvVar().
			WithName("VALKEY_AUTH_PASS").
			WithValueFrom(corev1ac.EnvVarSource().
				WithSecretKeyRef(corev1ac.SecretKeySelector().
					WithName(v.Spec.Auth.SecretName).
					WithKey(key))))
		// spec.auth.sentinelAuthSecretName (when set) is the credential
		// the operator emits into the `sentinel auth-pass <master> <pw>`
		// directive — i.e. the password sentinels use to AUTH against
		// the master. Distinct from VALKEY_AUTH_PASS which is the
		// master's own requirepass (and the password the operator's
		// observer presents to the sentinel listener). When the user
		// has set up an ACL user on the master dedicated to sentinel
		// observation, sentinelAuthSecretName lets the sentinel auth
		// with that user's password instead of the default-user one.
		// When unset, the init script falls back to VALKEY_AUTH_PASS
		// — the common single-Secret deployment shape.
		if v.Spec.Auth.SentinelAuthSecretName != "" {
			sentinelKey := v.Spec.Auth.SentinelAuthSecretKey
			if sentinelKey == "" {
				sentinelKey = defaultAuthSecretKey
			}
			env = append(env, corev1ac.EnvVar().
				WithName("SENTINEL_AUTH_PASS").
				WithValueFrom(corev1ac.EnvVarSource().
					WithSecretKeyRef(corev1ac.SecretKeySelector().
						WithName(v.Spec.Auth.SentinelAuthSecretName).
						WithKey(sentinelKey))))
		}
	}
	return corev1ac.Container().
		WithName("render-sentinel-config").
		WithImage(v.Spec.Image.Sentinel.Repository+":"+v.Spec.Image.Sentinel.Tag).
		WithCommand("/bin/sh", sentinelRenderScriptPath).
		WithEnv(env...).
		WithVolumeMounts(
			corev1ac.VolumeMount().WithName("sentinel-template").WithMountPath(mountSentinelTemplate).WithReadOnly(true),
			corev1ac.VolumeMount().WithName("sentinel-init-scripts").WithMountPath(mountSentinelInitScripts).WithReadOnly(true),
			corev1ac.VolumeMount().WithName("sentinel-conf").WithMountPath(mountSentinelConf),
			corev1ac.VolumeMount().WithName("bootstrap").WithMountPath(mountSentinelBootstrap).WithReadOnly(true),
		).
		WithResources(corev1ac.ResourceRequirements().
			WithRequests(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			}).
			WithLimits(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			})).
		WithSecurityContext(restrictedContainerSecurityContext())
}

func buildSentinelContainer(v *valkeyv1beta1.Valkey) *corev1ac.ContainerApplyConfiguration {
	c := corev1ac.Container().
		WithName("sentinel").
		WithImage(v.Spec.Image.Sentinel.Repository+":"+v.Spec.Image.Sentinel.Tag).
		WithPorts(corev1ac.ContainerPort().
			WithName("sentinel").
			WithContainerPort(valkeyconf.SentinelPort).
			WithProtocol(corev1.ProtocolTCP)).
		WithCommand("valkey-sentinel", mountSentinelConf+"/sentinel.conf").
		WithEnv(downwardEnvVar("POD_IP", "status.podIP")).
		WithVolumeMounts(
			corev1ac.VolumeMount().WithName("sentinel-conf").WithMountPath(mountSentinelConf),
			corev1ac.VolumeMount().WithName("tmp").WithMountPath("/tmp"),
		).
		WithResources(sentinelContainerResources(v.Spec.Sentinel.Resources)).
		WithSecurityContext(restrictedContainerSecurityContextReadOnly())
	// Liveness and readiness are distinct probes stamped by the defaulter
	// (stampSentinelProbes): liveness carries a ~60s grace window so a
	// transient stall doesn't flap-restart the sentinel, while readiness is
	// TCP-only and shorter so a wedged sentinel drops out of discovery
	// without a restart. Users override via spec.sentinel.customLivenessProbe
	// / customReadinessProbe; the defaulter always fills these on the
	// admission path, so a nil here only happens in direct-build unit tests.
	if v.Spec.Sentinel.CustomLivenessProbe != nil {
		c.WithLivenessProbe(acProbe(v.Spec.Sentinel.CustomLivenessProbe))
	}
	if v.Spec.Sentinel.CustomReadinessProbe != nil {
		c.WithReadinessProbe(acProbe(v.Spec.Sentinel.CustomReadinessProbe))
	}
	return c
}

func sentinelContainerResources(user corev1.ResourceRequirements) *corev1ac.ResourceRequirementsApplyConfiguration {
	out := corev1ac.ResourceRequirements()
	requests := user.Requests
	if requests == nil {
		requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		}
	}
	if len(requests) > 0 {
		out.WithRequests(requests)
	}
	if len(user.Limits) > 0 {
		out.WithLimits(user.Limits)
	}
	return out
}

func buildSentinelVolumes(v *valkeyv1beta1.Valkey) []*corev1ac.VolumeApplyConfiguration {
	return []*corev1ac.VolumeApplyConfiguration{
		corev1ac.Volume().
			WithName("sentinel-template").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(v.Name + suffixSentinelConf).
				WithDefaultMode(0o444)),
		corev1ac.Volume().
			WithName("sentinel-init-scripts").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(v.Name + suffixSentinelInitScripts).
				WithDefaultMode(0o555)),
		corev1ac.Volume().
			WithName("sentinel-conf").
			WithEmptyDir(corev1ac.EmptyDirVolumeSource()),
		// Writable scratch for the readOnlyRootFilesystem sentinel
		// container (mounted at /tmp).
		corev1ac.Volume().
			WithName("tmp").
			WithEmptyDir(corev1ac.EmptyDirVolumeSource()),
		corev1ac.Volume().
			WithName("bootstrap").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(v.Name + suffixSentinelBootstrap).
				WithDefaultMode(0o444)),
	}
}

// renderSentinelInitScript is the shell stamped into the sentinel
// init-scripts ConfigMap. The script substitutes the two
// placeholders the renderer emitted (_POD_IP_ + _SEED_MASTER_IP_)
// against runtime values (Downward API + bootstrap CM) and writes
// the result to /sentinel/sentinel.conf for the main container.
//
// Sentinel mutates the conf file at runtime (peer discovery, leader
// election state), so it cannot live on a read-only ConfigMap mount
// — D3 keeps it on emptyDir.
func renderSentinelInitScript() string {
	return `#!/bin/sh
# Managed by velkir. Do not edit by hand; this ConfigMap is
# overwritten on every reconcile.
set -eu

if [ ! -r /bootstrap/seedMasterIP ]; then
  echo "bootstrap/seedMasterIP not present; cannot start sentinel" >&2
  exit 1
fi
SEED="$(cat /bootstrap/seedMasterIP 2>/dev/null || true)"
if [ -z "${SEED}" ]; then
  echo "bootstrap/seedMasterIP empty; operator has not yet populated seed" >&2
  exit 1
fi

sed -e "s|_POD_IP_|${POD_IP}|g" \
    -e "s|_SEED_MASTER_IP_|${SEED}|g" \
    "/sentinel-template/sentinel.conf" > "/sentinel/sentinel.conf"

# Append the two auth directives when the operator has injected
# VALKEY_AUTH_PASS (i.e. spec.auth.secretName is set). printf with a
# discrete format string keeps passwords containing sed metacharacters
# (|, &, /, backslash) intact — never go through sed for the password
# value.
#
#   - "sentinel auth-pass <master> <pw>": sentinel → master auth. Without
#     this sentinels can't PING / INFO the master and stay s_down
#     forever (master pub/sub is the discovery channel for replicas and
#     peer sentinels, so num-slaves=0 and num-other-sentinels=0 cascade
#     from the same failure).
#
#   - "requirepass <pw>": sentinel's listener auth, matching the master.
#     The operator's observer and rotation paths (internal/sentinel)
#     send AUTH on every sentinel connection using the same Secret
#     value; without requirepass set on the sentinel side, Valkey
#     returns "ERR Client sent AUTH, but no password is set" (or
#     WRONGPASS under the ACL-default-nopass shape sentinel rewrites
#     on CONFIG REWRITE) and the observer marks every sentinel
#     unreachable — quorum stays Unknown / Lost and the SplitBrain
#     condition latches indefinitely. Pairing requirepass with
#     auth-pass keeps the operator → sentinel and sentinel → master
#     credentials in lock-step on a single Secret.
if [ -n "${VALKEY_AUTH_PASS:-}" ]; then
  # SENTINEL_AUTH_PASS (from spec.auth.sentinelAuthSecretName) is the
  # credential the sentinel uses to AUTH against the master — distinct
  # from the master's own requirepass when the user has set up an ACL
  # user dedicated to sentinel observation. Defaults to VALKEY_AUTH_PASS
  # when the field isn't set (single-Secret deployment).
  AUTHPASS_VALUE="${SENTINEL_AUTH_PASS:-$VALKEY_AUTH_PASS}"
  printf 'sentinel auth-pass %s %s\n' "${MASTER_NAME}" "${AUTHPASS_VALUE}" \
    >> "/sentinel/sentinel.conf"
  printf 'requirepass %s\n' "${VALKEY_AUTH_PASS}" \
    >> "/sentinel/sentinel.conf"
fi
`
}
