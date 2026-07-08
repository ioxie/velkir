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

// Package defaults holds the spec-shaping default stamps shared by the
// mutating admission webhook and the reconciler. The webhook applies
// them at admission time so users see effective values on `kubectl get
// -o yaml`; the reconciler applies them to its in-memory copy before
// computing any rendered output (valkey.conf / sentinel.conf content,
// config hashes, pod templates, derived PDBs), so rendered bytes can
// never depend on admission state. A CR admitted while the defaulting
// webhook was unreachable — or defaulted by an older operator version
// that lacked newer stamps — renders exactly like a fully-defaulted
// one, instead of config-rolling its pods when the defaults later
// arrive.
//
// The reconciler must never write the normalized spec back to the API
// server: normalization is an in-memory view, and persisting it would
// fight the webhook and bump generation on every reconcile.
package defaults

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	valkeyv1beta1 "github.com/ioxie/velkir/api/v1beta1"
)

// Pod-set identity labels, mirroring the controller's ownedLabels
// output (this package cannot import internal/controller). The
// webhook validator references the same values; keep them in one
// place so the stamped selector and the deviation check agree.
const (
	CRLabelKey        = "velkir.ioxie.dev/cr"
	ComponentLabelKey = "velkir.ioxie.dev/component"
	ComponentValkey   = "valkey"
	ComponentSentinel = "sentinel"
	// AntiAffinityTopologyKey is the node-level domain the soft
	// cross-node anti-affinity spreads each pod set across.
	AntiAffinityTopologyKey = "kubernetes.io/hostname"
)

// Default per-component images, applied by stampImageDefaults when a
// component image is unset. Single source of truth for the values that
// previously lived in the CRD schema `+kubebuilder:default` markers.
const (
	DefaultValkeyImageRepo   = "valkey/valkey"
	DefaultValkeyImageTag    = "8.1.6-alpine"
	DefaultExporterImageRepo = "oliver006/redis_exporter"
	DefaultExporterImageTag  = "v1.62.0"
)

// ApplySpecDefaults fills every spec field the operator defaults when a
// user omits it. Same contract as the admission defaulter it backs:
//
//   - Idempotent: applying to an already-defaulted spec is a no-op.
//   - Never overrides user-provided values; only fills nils/zeros.
//   - Pure function on the object — no API calls, no cluster lookups.
//
// Stamp order matters: replica-count stamps run before the PDB and
// anti-affinity derivations that read them.
func ApplySpecDefaults(v *valkeyv1beta1.Valkey) {
	stampImageDefaults(v)
	stampReplicasForMode(v)
	stampValkeyProbes(v)
	stampValkeyResources(v)
	stampReadinessGate(v)
	stampMinReplicas(v)
	stampMetricsDefaults(v)
	stampSentinelDefaults(v)
	stampSentinelProbes(v)
	stampRolloutDefaults(v)
	stampPDB(v)
	stampAntiAffinity(v)
}

// stampImageDefaults fills the per-component image repository/tag when a
// component's image is unset. These defaults previously lived as schema
// `+kubebuilder:default` markers, which bake the values into the published
// CRD OpenAPI; keeping the authoritative default in operator code instead
// means bumping a default tag in a new release no longer mutates the CRD
// schema contract or re-applies to CRs through the apiserver's schema
// defaulting. Matches the prior struct-default semantics: the whole
// {repository, tag} pair is stamped only when the component image is the
// zero value — a partially-set image (e.g. repository set, tag empty) is
// left untouched for the Tag MinLength=1 validation to reject, unchanged.
func stampImageDefaults(v *valkeyv1beta1.Valkey) {
	if v.Spec.Image.Valkey == (valkeyv1beta1.ContainerImage{}) {
		v.Spec.Image.Valkey = valkeyv1beta1.ContainerImage{Repository: DefaultValkeyImageRepo, Tag: DefaultValkeyImageTag}
	}
	if v.Spec.Image.Sentinel == (valkeyv1beta1.ContainerImage{}) {
		v.Spec.Image.Sentinel = valkeyv1beta1.ContainerImage{Repository: DefaultValkeyImageRepo, Tag: DefaultValkeyImageTag}
	}
	if v.Spec.Image.Exporter == (valkeyv1beta1.ContainerImage{}) {
		v.Spec.Image.Exporter = valkeyv1beta1.ContainerImage{Repository: DefaultExporterImageRepo, Tag: DefaultExporterImageTag}
	}
}

// stampReplicasForMode stamps the mode-aware default for
// `spec.valkey.replicas` when the user hasn't set it.
//
//   - standalone → 1 pod (the only legal shape; the schema CEL enforces).
//   - replication → 2 pods (1 primary + 1 replica, the minimum HA topology).
//   - sentinel → 3 pods (the canonical sentinel-watched majority shape;
//     the validator soft-warns sentinel mode below 2 replicas, so 3 is
//     the lab-OK baseline that doesn't trip the warn).
//
// Runs BEFORE stampPDB so the PDB derivation sees the right
// post-default replicas count.
func stampReplicasForMode(v *valkeyv1beta1.Valkey) {
	if v.Spec.Valkey.Replicas != 0 {
		return
	}
	switch v.Spec.Mode {
	case valkeyv1beta1.ModeReplication:
		v.Spec.Valkey.Replicas = 2
	case valkeyv1beta1.ModeSentinel:
		v.Spec.Valkey.Replicas = 3
	default:
		v.Spec.Valkey.Replicas = 1
	}
}

// stampMinReplicas enforces the write-loss floor by stamping
// spec.valkey.minReplicasToWrite=1 and minReplicasMaxLag=10 for
// replication and sentinel modes when the user left them unset. The
// renderer emits these as operator-owned valkey.conf directives, so a
// primary partitioned from all of its replicas stops accepting writes
// that could never replicate (and would be lost on the next failover).
//
// Standalone is exempt: it has no replicas, so `min-replicas-to-write 1`
// would block every write. The fields stay nil there and the renderer
// omits the directives entirely.
//
// Only stamps when nil — a user who sets a stricter floor (e.g.
// minReplicasToWrite=2) keeps it; the validating webhook's Minimum
// markers (1 / 10) reject sub-floor explicit values upstream.
func stampMinReplicas(v *valkeyv1beta1.Valkey) {
	switch v.Spec.Mode {
	case valkeyv1beta1.ModeReplication, valkeyv1beta1.ModeSentinel:
		if v.Spec.Valkey.MinReplicasToWrite == nil {
			v.Spec.Valkey.MinReplicasToWrite = new(int32(1))
		}
		if v.Spec.Valkey.MinReplicasMaxLag == nil {
			v.Spec.Valkey.MinReplicasMaxLag = new(int32(10))
		}
	}
}

// stampValkeyProbes fills the valkey container's liveness/readiness
// probe templates when the user hasn't provided overrides. Timing values
// are chosen to clear the sentinel tilt window (60s liveness grace) and
// to give RDB/AOF loading time on cold start (5s initial readiness delay).
//
// Liveness is exec (not tcpSocket) so the kubelet detects a frozen
// valkey-server process (e.g. SIGSTOP'd, cgroup-freezer'd, in a kernel
// stall). A tcpSocket probe accepts kernel-level connections regardless
// of user-space liveness and would leave a frozen master undetected
// indefinitely. The reconciler injects `-a $(VALKEY_PASSWORD)`
// at pod-template time when auth is configured, mirroring the readiness
// probe path; see injectAuthIntoValkeyCLIProbe.
func stampValkeyProbes(v *valkeyv1beta1.Valkey) {
	if v.Spec.Valkey.CustomLivenessProbe == nil {
		v.Spec.Valkey.CustomLivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"valkey-cli", "-h", "127.0.0.1", "-p", "6379", "ping"},
				},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       10,
			TimeoutSeconds:      3,
			// 6 × 10s = 60s of failed probes before kubelet restarts the
			// container. Tight enough to clear a frozen-process wedge
			// within a usable failover window, loose enough to ride out
			// the slow-INFO-during-RDB-load case.
			FailureThreshold: 6,
			SuccessThreshold: 1,
		}
	}
	if v.Spec.Valkey.CustomReadinessProbe == nil {
		// Loopback-only command: no DNS dependency. The output here is
		// a TEMPLATE — the reconciler MUST rewrite this command at
		// pod-template generation time when `spec.auth.secretName` is
		// configured, injecting `-a $(VALKEY_PASSWORD)` before the
		// `ping` argument. Failure to do so produces
		// `NOAUTH Authentication required` on every probe and pods
		// never go Ready. The defaulter cannot do this rewrite itself
		// because the env-var name is shaped by the auth field which
		// the defaulter is contractually not allowed to inspect for
		// naming (idempotency contract: stamping the secret name into
		// the probe would mean re-running the defaulter after a
		// secret rename produces a different output).
		v.Spec.Valkey.CustomReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"valkey-cli", "-h", "127.0.0.1", "-p", "6379", "ping"},
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			TimeoutSeconds:      3,
			FailureThreshold:    3,
			SuccessThreshold:    1,
		}
	}
}

// stampValkeyResources sets requests/limits on the valkey container when
// the user hasn't. Quantity values are written via `resource.MustParse`
// so the JSON re-serialisation round-trips identically — see
// TestDefaulterQuantityIntegerStable for the pin against 0.5/500m flips.
func stampValkeyResources(v *valkeyv1beta1.Valkey) {
	if v.Spec.Valkey.Resources.Requests == nil {
		v.Spec.Valkey.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		}
	}
	if v.Spec.Valkey.Resources.Limits == nil {
		v.Spec.Valkey.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		}
	}
}

// stampReadinessGate sets the gate enabled/disabled per mode. Standalone
// has no replication peer to lag on, so the gate is meaningless and
// stamped off (the validating webhook also rejects an explicit
// `enabled: true` on standalone). Replication and sentinel get it on.
//
// Only stamps when Enabled is nil, preserving the user's explicit
// `enabled: false` opt-out for replication/sentinel modes.
func stampReadinessGate(v *valkeyv1beta1.Valkey) {
	if v.Spec.Valkey.ReadinessGate.Enabled == nil {
		switch v.Spec.Mode {
		case valkeyv1beta1.ModeStandalone:
			v.Spec.Valkey.ReadinessGate.Enabled = new(false)
		default:
			v.Spec.Valkey.ReadinessGate.Enabled = new(true)
		}
	}
	if v.Spec.Valkey.ReadinessGate.MaxLagBytes == nil {
		v.Spec.Valkey.ReadinessGate.MaxLagBytes = new(int64(1 << 20)) // 1 MiB
	}
}

// stampMetricsDefaults preserves the schema-level `enabled: false`
// default while ensuring the field is materialised on the wire (the
// schema-level default fires on the apiserver, not in the defaulter,
// but the defaulter still has to handle the case where schema defaults
// haven't yet run — e.g. unit-test paths invoking Default directly).
func stampMetricsDefaults(v *valkeyv1beta1.Valkey) {
	if v.Spec.Metrics.Enabled == nil {
		v.Spec.Metrics.Enabled = new(false)
	}
	if v.Spec.Metrics.PodMonitor.Enabled == nil {
		v.Spec.Metrics.PodMonitor.Enabled = new(false)
	}
	if v.Spec.Metrics.PodMonitor.ScrapeInterval == "" {
		v.Spec.Metrics.PodMonitor.ScrapeInterval = "30s"
	}
}

// stampSentinelDefaults fills sentinel-pod fields the user didn't set.
// Skips entirely when spec.sentinel is nil — sentinel-mode CEL elsewhere
// requires it, and silently materialising a SentinelPodSpec under
// non-sentinel mode would surprise users on `kubectl get -o yaml`.
//
// Layered with the apiserver-side kubebuilder:default markers on
// SentinelPodSpec (Replicas=3, DownAfterMilliseconds=3000,
// FailoverTimeout=180000, ParallelSyncs=1): the markers handle the
// apiserver path; this function handles the unit-test path where
// Default() is invoked on a struct the apiserver never saw, AND covers
// fields the markers can't (Quorum derivation depends on Replicas;
// Resources depends on Guaranteed-QoS shape — neither expressible as
// a static default).
//
// Runs BEFORE stampPDB so the PDB derivation sees the post-default
// sentinel.replicas value.
func stampSentinelDefaults(v *valkeyv1beta1.Valkey) {
	if v.Spec.Sentinel == nil {
		return
	}
	s := v.Spec.Sentinel
	if s.Replicas == 0 {
		s.Replicas = 3
	}
	if s.DownAfterMilliseconds == 0 {
		// 3s (formerly 30s → 5s → 3s) supports a sub-10s recovery SLO
		// on stable in-cluster networks. The practical recovery floor
		// is `DownAfterMilliseconds + 6s` (quorum gathering + election
		// + label-flip + EndpointSlice propagation): 3s + 6s = 9s,
		// inside the 10s budget. Users on noisier networks can raise
		// this — `velkir.ioxie.dev/allow-aggressive-timeouts=true` is
		// only needed to go BELOW 1000ms.
		s.DownAfterMilliseconds = 3000
	}
	if s.FailoverTimeout == 0 {
		s.FailoverTimeout = 180000
	}
	if s.ParallelSyncs == 0 {
		s.ParallelSyncs = 1
	}
	if s.Quorum == 0 {
		// quorum = ceil((replicas+1)/2). Integer-math identity:
		// ceil((r+1)/2) = (r+2)/2 (truncating division). Spot-checks:
		// r=3 → 2, r=4 → 3, r=5 → 3, r=7 → 4.
		s.Quorum = (s.Replicas + 2) / 2
	}
	// Guaranteed QoS shape: requests == limits on CPU and memory
	// promotes the pod to the Guaranteed QoS class (kubelet contract;
	// the kubelet checks both sides on every container). Sentinels are
	// tiny — one in-process goroutine per monitored master — so the
	// values are conservative; matching on both sides is what makes
	// them Guaranteed. Don't relax to Burstable later: sentinel
	// reliability is load-bearing for the whole topology.
	switch {
	case s.Resources.Requests == nil && s.Resources.Limits == nil:
		// Neither side set — stamp the conservative Guaranteed baseline.
		s.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		}
		s.Resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		}
	case s.Resources.Limits == nil:
		// Requests-only would fall to Burstable (no limits). Mirror
		// requests into limits so limit == request → Guaranteed. Filling
		// the nil limits side never overrides a user-set value.
		s.Resources.Limits = s.Resources.Requests.DeepCopy()
	case s.Resources.Requests == nil:
		// Limits-only: the kubelet already defaults requests to limits,
		// but materialise it so the effective (Guaranteed) shape is
		// visible on the persisted object.
		s.Resources.Requests = s.Resources.Limits.DeepCopy()
	}
	// Both sides user-set: left untouched (the defaulter never overrides
	// user values). The validating webhook warns if the result isn't
	// Guaranteed-shaped — see validateSentinelQoS.
}

// stampSentinelProbes fills the sentinel container's liveness and
// readiness probe templates when the user leaves them unset. They are two
// distinct probes (not one shared object) tuned to different windows:
//
//   - Liveness: tcpSocket, 6 × 10s = 60s of failed probes before the
//     kubelet restarts the container. Long enough to ride out a transient
//     GC / CPU-throttle stall (every restart churns SENTINEL RESET and
//     opens a ghost-sentinel window); short enough that a frozen sentinel
//     doesn't linger as a non-voting member degrading quorum response.
//   - Readiness: tcpSocket-only, ~15s (3 × 5s). Readiness must not couple
//     to quorum or peer state — a sentinel with degraded quorum is still
//     functional and must keep accepting client-discovery connections — so
//     it only checks the port is listening and drops a wedged sentinel out
//     of discovery quickly without a restart.
//
// Both handlers are tcpSocket on the sentinel port; unlike the valkey
// container there is no exec command and no auth-injection path.
func stampSentinelProbes(v *valkeyv1beta1.Valkey) {
	if v.Spec.Sentinel == nil {
		return
	}
	s := v.Spec.Sentinel
	// 26379 is the sentinel port (valkeyconf.SentinelPort); a literal here
	// keeps this package free of a controller-side import.
	if s.CustomLivenessProbe == nil {
		s.CustomLivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(26379)},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       10,
			TimeoutSeconds:      3,
			FailureThreshold:    6,
			SuccessThreshold:    1,
		}
	}
	if s.CustomReadinessProbe == nil {
		s.CustomReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(26379)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      3,
			FailureThreshold:    3,
			SuccessThreshold:    1,
		}
	}
}

// stampPDB derives `minAvailable = max(1, replicas-1)` for any pod set
// with two or more replicas. Standalone (replicas=1) gets no PDB. The
// derivation is materialised into the spec so `kubectl get valkey -o yaml`
// shows the effective value, not "I'll figure it out later".
//
// Boundary: replicas=2 still gets a PDB with `minAvailable=1` — the
// formula `max(1, replicas-1)` is deliberately never less than 1
// (PDBs with `minAvailable: 0` are pointless). Don't tweak the
// threshold expecting to "skip PDB for tiny clusters" — the floor is
// load-bearing for any HA pair.
func stampPDB(v *valkeyv1beta1.Valkey) {
	if v.Spec.Valkey.Replicas >= 2 && v.Spec.Valkey.PDB == nil {
		min := intstr.FromInt32(max(int32(1), v.Spec.Valkey.Replicas-1))
		v.Spec.Valkey.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &min}
	}
	if v.Spec.Sentinel != nil && v.Spec.Sentinel.Replicas >= 2 && v.Spec.Sentinel.PDB == nil {
		min := intstr.FromInt32(max(int32(1), v.Spec.Sentinel.Replicas-1))
		v.Spec.Sentinel.PDB = &valkeyv1beta1.PDBSpec{MinAvailable: &min}
	}
}

// stampAntiAffinity materializes the cross-node anti-affinity default
// as a SOFT term: a preferredDuringSchedulingIgnoredDuringExecution
// podAntiAffinity (weight 100, topologyKey kubernetes.io/hostname)
// that discourages — but never forbids — co-scheduling two pods of the
// SAME set on one node. Soft (not required) is load-bearing: a hard
// term would leave every replicas≥2 deployment Pending on a single-node
// cluster, which the project must keep supporting.
//
// Stamped separately for the valkey pod set and the sentinel pod set,
// each only when that set has ≥2 replicas and the user left its
// Affinity.PodAntiAffinity unset. No valkey↔sentinel term — the two
// sets are allowed to co-locate; a user may opt into cross-set
// anti-affinity themselves. Materialized into spec (mirror stampPDB) so
// `kubectl get valkey -o yaml` shows the effective value.
//
// Runs AFTER stampReplicasForMode / stampSentinelDefaults so it sees
// post-default replica counts (same ordering constraint as stampPDB).
func stampAntiAffinity(v *valkeyv1beta1.Valkey) {
	if v.Spec.Valkey.Replicas >= 2 {
		ensureSoftPodAntiAffinity(&v.Spec.Valkey.Affinity, ComponentValkey, v.Name)
	}
	if v.Spec.Sentinel != nil && v.Spec.Sentinel.Replicas >= 2 {
		ensureSoftPodAntiAffinity(&v.Spec.Sentinel.Affinity, ComponentSentinel, v.Name)
	}
}

// ensureSoftPodAntiAffinity stamps the soft same-pod-set anti-affinity
// term onto *aff when the user has not already set a PodAntiAffinity.
// Non-overriding: ANY user-provided PodAntiAffinity (soft, hard, or
// cross-set) is left untouched — the validator surfaces a warn if the
// user's choice no longer spreads the set. Idempotent: a second call
// finds the stamped PodAntiAffinity and returns.
func ensureSoftPodAntiAffinity(aff **corev1.Affinity, component, crName string) {
	if *aff == nil {
		*aff = &corev1.Affinity{}
	}
	if (*aff).PodAntiAffinity != nil {
		return
	}
	(*aff).PodAntiAffinity = &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
			{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					TopologyKey: AntiAffinityTopologyKey,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							CRLabelKey:        crName,
							ComponentLabelKey: component,
						},
					},
				},
			},
		},
	}
}

// stampRolloutDefaults fills the master-aware rolling-update knobs when
// the user hasn't set them. The schema-side `+kubebuilder:default` markers
// on RolloutSpec handle the apiserver path (post → defaults applied per
// field), but the defaulter still has to cover unit-test paths where
// Default() is invoked on a struct the apiserver never saw, AND the
// "user explicitly passed 0 means use the default" sentinel for
// FailoverGracePeriodSeconds (which the schema-side default treats as
// "user-provided 0", not "missing").
//
// Defaults from the rolling-update spec:
//   - MaxUnavailable: 1 — single-pod rolls keep CKQUORUM majority intact
//     across the recreate window even on the smallest 3-pod sentinel pool.
//   - FailoverGracePeriodSeconds: 0 — operator treats 0 as "compute as
//     sentinel.failoverTimeout + 30s at reconcile time" (the validator
//     warns when a user-set value is below failoverTimeout).
//   - ReplicaReadyTimeoutSeconds: 300 (5 min) — caps the wait for a
//     replacement replica pod to pass its readiness gate before the
//     state machine declares RolloutStalled.
//   - MaxLagBytes: 10000 — per-replica `master_repl_offset -
//     slave_repl_offset` ceiling tolerated during the rolling-update
//     window. Zero IS treated as "unset" here (mirrors MaxUnavailable):
//     the literal "no lag tolerance" interpretation would block the
//     rolling window on the slightest replica lag, so re-stamping 10000
//     over an explicit 0 is the correct user-facing semantic.
//
// Idempotent: re-running on an already-defaulted spec is a no-op.
func stampRolloutDefaults(v *valkeyv1beta1.Valkey) {
	if v.Spec.Rollout.MaxUnavailable == 0 {
		v.Spec.Rollout.MaxUnavailable = 1
	}
	if v.Spec.Rollout.ReplicaReadyTimeoutSeconds == 0 {
		v.Spec.Rollout.ReplicaReadyTimeoutSeconds = 300
	}
	if v.Spec.Rollout.MaxLagBytes == 0 {
		v.Spec.Rollout.MaxLagBytes = 10000
	}
	// FailoverGracePeriodSeconds intentionally NOT defaulted: 0 IS the
	// well-defined sentinel (compute at reconcile time). Stamping a
	// non-zero value here would shadow that semantic.
}
