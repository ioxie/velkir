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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Mode selects the deployment topology. All three modes
// (standalone, replication, sentinel) are accepted at v1beta1.
// +kubebuilder:validation:Enum=standalone;replication;sentinel
type Mode string

const (
	ModeStandalone  Mode = "standalone"
	ModeReplication Mode = "replication"
	ModeSentinel    Mode = "sentinel"
)

// PVCRetentionPolicy controls whether PVCs survive CR deletion.
// +kubebuilder:validation:Enum=Retain;Delete
type PVCRetentionPolicy string

const (
	PVCRetentionRetain PVCRetentionPolicy = "Retain"
	PVCRetentionDelete PVCRetentionPolicy = "Delete"
)

// ValkeySpec defines the desired state of a Valkey deployment.
//
// Cross-field invariants are expressed as CEL rules on the schema below.
// Rules referring to `sentinel` are inert until `mode` accepts `sentinel`;
// they are written eagerly so the relaxation in later stages is purely a
// `Mode` enum widening.
// Rule 1 (mode immutability) is inert at v1beta1 because the mode enum
// only admits `standalone`; it activates the moment the enum widens to
// admit replication or sentinel.
// +kubebuilder:validation:XValidation:rule="self.mode == oldSelf.mode",message="spec.mode is immutable"
// +kubebuilder:validation:XValidation:rule="self.mode != 'sentinel' || has(self.sentinel)",message="spec.sentinel is required when mode=sentinel"
// +kubebuilder:validation:XValidation:rule="self.mode != 'standalone' || !has(self.sentinel)",message="spec.sentinel must be absent when mode=standalone"
// +kubebuilder:validation:XValidation:rule="self.mode != 'sentinel' || self.sentinel.replicas >= 3",message="sentinel.replicas must be >= 3"
// Cross-field check; defense-in-depth against future relaxation of the
// SentinelPodSpec-level Minimum=2 / Maximum=replicas bounds.
// +kubebuilder:validation:XValidation:rule="self.mode != 'sentinel' || self.sentinel.quorum <= self.sentinel.replicas",message="sentinel.quorum must be <= sentinel.replicas"
// +kubebuilder:validation:XValidation:rule="self.mode != 'standalone' || !has(self.valkey.replicas) || self.valkey.replicas == 1",message="mode=standalone requires valkey.replicas == 1"
// +kubebuilder:validation:XValidation:rule="self.mode != 'replication' || !has(self.valkey.replicas) || self.valkey.replicas >= 2",message="mode=replication requires valkey.replicas >= 2"
// +kubebuilder:validation:XValidation:rule="self.mode != 'replication' || has(self.valkey.persistence)",message="mode=replication requires spec.valkey.persistence (no implicit emptyDir under replication)"
// +kubebuilder:validation:XValidation:rule="self.service.client.type != 'LoadBalancer' || (has(self.service.client.loadBalancerSourceRanges) && size(self.service.client.loadBalancerSourceRanges) > 0)",message="service.client.loadBalancerSourceRanges is required when service.client.type=LoadBalancer"
// +kubebuilder:validation:XValidation:rule="self.service.sentinel.type != 'LoadBalancer' || (has(self.service.sentinel.loadBalancerSourceRanges) && size(self.service.sentinel.loadBalancerSourceRanges) > 0)",message="service.sentinel.loadBalancerSourceRanges is required when service.sentinel.type=LoadBalancer"
// +kubebuilder:validation:XValidation:rule="!has(self.auth) || !has(self.auth.sentinelAuthSecretName) || self.mode == 'sentinel'",message="auth.sentinelAuthSecretName is only permitted when mode=sentinel"
// Trivially-satisfied today (valkey.replicas Minimum=1 catches replicas=0
// first); kept to codify the intent ("bootstrap requires at least one
// valkey pod") so a future Minimum relaxation doesn't silently uncover
// the gap. Not exercised by tests because the precondition is unreachable
// without bypassing field-level validation.
// +kubebuilder:validation:XValidation:rule="!has(self.bootstrapNode) || !has(self.valkey.replicas) || self.valkey.replicas >= 1",message="bootstrapNode requires valkey.replicas >= 1"
// Trivially-satisfied today (image.exporter.repository default is
// non-empty and MinLength=1 catches an explicit empty); kept as a guard
// against future default removal. Not exercised by tests because the
// precondition is unreachable without bypassing field-level validation.
// +kubebuilder:validation:XValidation:rule="!has(self.metrics.enabled) || !self.metrics.enabled || (has(self.image.exporter.repository) && size(self.image.exporter.repository) > 0)",message="metrics.enabled=true requires image.exporter.repository to be set"
// +kubebuilder:validation:XValidation:rule="!has(self.valkey.readinessGate) || !has(self.valkey.readinessGate.enabled) || !self.valkey.readinessGate.enabled || self.mode != 'standalone'",message="valkey.readinessGate.enabled is not meaningful when mode=standalone"
// Storage monotonicity: persistence.size may grow but never
// shrink. Growth is handled by the operator-driven PVC resize sub-
// state-machine; shrinking would require a full data-plane rebuild
// and is hard-rejected. The rule short-circuits on CREATE (oldSelf
// has no persistence to compare against).
// +kubebuilder:validation:XValidation:rule="!has(self.valkey.persistence) || !has(oldSelf.valkey.persistence) || quantity(string(self.valkey.persistence.size)).compareTo(quantity(string(oldSelf.valkey.persistence.size))) >= 0",message="spec.valkey.persistence.size cannot decrease — shrinking is not supported (growing is handled by the operator-driven PVC resize flow)"
//
// Rollout MaxLagBytes ceiling: the field-level Maximum on
// rollout.maxLagBytes already bounds the value at 10 MiB; this CEL rule
// codifies the same intent at spec level so a future field-level
// relaxation (e.g. lifting Maximum to support larger replication
// windows) does not silently uncover the cap. Pure upper-bound check;
// short-circuits when rollout or maxLagBytes is unset.
// +kubebuilder:validation:XValidation:rule="!has(self.rollout) || !has(self.rollout.maxLagBytes) || self.rollout.maxLagBytes <= 10485760",message="spec.rollout.maxLagBytes must be <= 10485760 (10 MiB)"
type ValkeySpec struct {
	// Mode selects the deployment topology. Immutable in v1beta1.
	// +kubebuilder:validation:Required
	Mode Mode `json:"mode"`

	// Image pins the container image for each component.
	// +kubebuilder:default={}
	Image ImageSpec `json:"image,omitempty"`

	// Valkey is the data-plane pod shape.
	// +kubebuilder:default={}
	Valkey ValkeyPodSpec `json:"valkey,omitempty"`

	// Sentinel is the sentinel pod shape. Required iff mode=sentinel.
	// +optional
	Sentinel *SentinelPodSpec `json:"sentinel,omitempty"`

	// Auth references Secrets carrying the Valkey default-user password and,
	// when mode=sentinel, an optional sentinel auth pair.
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// Service shapes the client- and sentinel-facing Services.
	// +kubebuilder:default={}
	Service ServiceSpec `json:"service,omitempty"`

	// Metrics gates the per-pod exporter sidecar and PodMonitor.
	// +kubebuilder:default={}
	Metrics MetricsSpec `json:"metrics,omitempty"`

	// BootstrapNode seeds a new deployment as a replica of an existing
	// external primary, for migrations.
	// +optional
	BootstrapNode *BootstrapNodeSpec `json:"bootstrapNode,omitempty"`

	// Rollout governs rolling-update behaviour. Best-practice defaults
	// applied when fields are omitted; user overrides emit Warning events.
	// +kubebuilder:default={}
	Rollout RolloutSpec `json:"rollout,omitempty"`

	// PVCRetentionPolicy controls PVC lifecycle on CR deletion. Retain is
	// the safe default; Delete cascades via owner-ref injection.
	//
	// Backed by a finalizer (`velkir.ioxie.dev/pvc-retention`): the
	// apiserver blocks the CR's GC until the operator's deletion path
	// has applied the policy and stripped the finalizer. So a CR
	// deletion issued while the operator is offline (pod restart, leader
	// handover, full outage) sits in `Terminating` until the operator
	// wakes up and completes the policy — there is no best-effort
	// window where Retain might incorrectly cascade or Delete might
	// leave PVCs orphaned.
	// +kubebuilder:default=Retain
	PVCRetentionPolicy PVCRetentionPolicy `json:"pvcRetentionPolicy,omitempty"`

	// FeatureGates toggles operator-recognised features on a per-CR
	// basis. Each key's default (when the key is absent from the map)
	// is the safe-by-default behaviour; users opt out by explicitly
	// setting the key to false. Unknown keys are accepted for forward
	// compatibility — the validating webhook emits a Warning per
	// unknown key in `kubectl apply` output without blocking the
	// request, and the operator logs a startup warning for each.
	// Recognised keys (default value in parentheses):
	//   - UpgradePreflight (true) — when false, bypasses the major-
	//     version-downgrade rejection in the runtime version-compat
	//     preflight. Testbed / disaster-recovery only; the operator
	//     emits ValkeyImageTransitionOverridden on the audit trail
	//     each time the bypass fires.
	// +optional
	FeatureGates map[string]bool `json:"featureGates,omitempty"`
}

// ImageSpec pins the container image for each component.
//
// Per-component repository/tag defaults are applied by the defaulting
// webhook (stampImageDefaults), not by schema `+kubebuilder:default`
// markers — keeping the authoritative default in operator code so bumping a
// default tag in a new release does not bake into the published CRD OpenAPI
// or re-apply to existing CRs through the apiserver's schema defaulting. The
// `omitzero` tag option (Go 1.24+) plus `omitempty` keep a zero-valued
// component out of the wire form (the webhook stamps it server-side from the
// deserialized object regardless), so a Go client marshalling
// `Spec{Mode: standalone}` does not emit noisy `image:{valkey:{},…}`.
type ImageSpec struct {
	// Valkey server image. Repository/tag default via the webhook when unset.
	// +optional
	Valkey ContainerImage `json:"valkey,omitempty,omitzero"`

	// Sentinel image — a single Valkey binary; defaults to the Valkey image.
	// +optional
	Sentinel ContainerImage `json:"sentinel,omitempty,omitzero"`

	// Exporter image. Only pulled when spec.metrics.enabled=true.
	// +optional
	Exporter ContainerImage `json:"exporter,omitempty,omitzero"`
}

// ContainerImage is a {repository, tag} pair. Digests may be embedded in
// `repository` as `repo@sha256:<hex>`.
//
// `omitempty` on both fields keeps a zero-valued ContainerImage out of the
// wire form; the defaulting webhook (stampImageDefaults) fills repository and
// tag server-side when the component image is unset.
type ContainerImage struct {
	// +kubebuilder:validation:MinLength=1
	Repository string `json:"repository,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Tag string `json:"tag,omitempty"`
}

// ValkeyPodSpec is the data-plane pod shape.
type ValkeyPodSpec struct {
	// Replicas is the number of Valkey data pods, including the primary.
	// Defaulter stamps a per-mode baseline when unset: 1 for standalone,
	// 2 for replication. No CRD-level default — the mode-aware default has
	// to live in the defaulter so the per-mode value can vary, and a
	// CRD-level default would race the defaulter (the apiserver would
	// stamp 1 before the webhook saw the zero value).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	Replicas int32 `json:"replicas,omitempty"`

	// Resources sets requests and limits on the valkey container. Defaults
	// are stamped by the defaulting webhook to honour QoS class targets.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Persistence describes the PVC. Required for replication/sentinel
	// modes; optional for standalone (emptyDir when unset).
	// +optional
	Persistence *PersistenceSpec `json:"persistence,omitempty"`

	// CustomLivenessProbe overrides the operator's tcpSocket liveness
	// probe. Only the numeric timing fields may be changed; the validating
	// webhook rejects handler changes.
	// +optional
	CustomLivenessProbe *corev1.Probe `json:"customLivenessProbe,omitempty"`

	// CustomReadinessProbe overrides the operator's exec-on-loopback
	// readiness probe. Same handler restriction as liveness.
	// +optional
	CustomReadinessProbe *corev1.Probe `json:"customReadinessProbe,omitempty"`

	// ReadinessGate gates pod readiness on replication-lag observation.
	// Has no effect for standalone (single pod, no replication peer).
	// +optional
	ReadinessGate ReadinessGateSpec `json:"readinessGate,omitempty"`

	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// +optional
	DNSConfig *corev1.PodDNSConfig `json:"dnsConfig,omitempty"`
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`

	// Configuration is a raw valkey.conf snippet appended to the
	// operator-rendered base config. Banned directives are rejected by the
	// validating webhook (operator-managed networking, peer addressing,
	// sentinel monitoring).
	// +optional
	Configuration string `json:"configuration,omitempty"`

	// ConfigurationOverrides is a directive→value map that merges over
	// `Configuration` with key-level replace semantics: the map value wins
	// for any directive that appears in both. Banned directives are
	// rejected here too. Merge logic ships with the config-rendering stage.
	// +optional
	ConfigurationOverrides map[string]string `json:"configurationOverrides,omitempty"`

	// PDB overrides the derived PodDisruptionBudget. Default
	// `minAvailable = replicas - 1` when unset (replicas >= 2).
	// +optional
	PDB *PDBSpec `json:"pdb,omitempty"`

	// MinReplicasToWrite gates writes when fewer than this many
	// replicas are connected to the primary. Maps to valkey.conf's
	// `min-replicas-to-write` directive. Floor 1 — values below
	// (i.e., 0) disable the gate entirely in Valkey, which means a
	// primary disconnected from all replicas continues accepting
	// writes that can never be replicated. The defaulter stamps 1 for
	// replication and sentinel modes when unset (an enforced data-safety
	// floor, not opt-in); standalone has no replicas, so the field stays
	// nil and the operator omits the directive. A user may set a stricter
	// value; the operator renders the field only when non-nil.
	// Availability note: a replicas=1 replication/sentinel cluster refuses
	// writes whenever its single replica is down or lagging beyond
	// minReplicasMaxLag — including during the operator's own rolling
	// restart of that replica. This is the deliberate
	// safety-over-availability trade-off; replicas>=2 preserves write
	// availability across pod updates.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinReplicasToWrite *int32 `json:"minReplicasToWrite,omitempty"`

	// MinReplicasMaxLag is the maximum tolerated replication lag
	// (in seconds) before a replica counts as "not connected" for
	// the MinReplicasToWrite gate. Maps to valkey.conf's
	// `min-replicas-max-lag`. Floor 10s — tighter values would
	// force-degrade on normal replication blips under load.
	// +optional
	// +kubebuilder:validation:Minimum=10
	MinReplicasMaxLag *int32 `json:"minReplicasMaxLag,omitempty"`
}

// PersistenceSpec describes the PVC backing a valkey pod.
//
// Toggling persistence on or off after a CR has bootstrapped is not
// supported at v1beta1: K8s makes StatefulSet.spec.volumeClaimTemplates
// immutable on Update, so flipping the field on an existing CR will
// be rejected by the apiserver during the operator's next SSA apply.
// Either set Persistence at create time, or delete and recreate the
// CR (with pvcRetentionPolicy=Retain on the original if data preservation
// matters across the gap). Resize of an already-persistent CR via
// `size` is the supported in-place mutation, driven by the
// operator's PVC resize sub-state-machine (internal/pvcresize).
type PersistenceSpec struct {
	// +optional
	StorageClass *string `json:"storageClass,omitempty"`
	// +kubebuilder:default="8Gi"
	Size resource.Quantity `json:"size,omitempty"`
	// +kubebuilder:default={"ReadWriteOnce"}
	// +kubebuilder:validation:MinItems=1
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// ReadinessGateSpec gates pod readiness on replication health.
type ReadinessGateSpec struct {
	// Enabled toggles the gate. The defaulting webhook stamps `false` for
	// standalone and `true` for replication/sentinel; `*bool` lets the
	// webhook distinguish "user opted out (`enabled: false`)" from
	// "user left the field unset" (round-trip stable through
	// `kubectl apply`).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
	// MaxLagBytes is the maximum permitted `master_repl_offset -
	// slave_repl_offset` before the replica pod is considered Ready.
	// `*int64` lets the defaulter distinguish "user left the field
	// unset" (stamped to 1 MiB) from "user wants a tighter or looser
	// bound" (preserved verbatim, including `maxLagBytes: 0` — which
	// the validator warns about because zero lag tolerance means a
	// replica would never go Ready).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxLagBytes *int64 `json:"maxLagBytes,omitempty"`
}

// SentinelPodSpec is the sentinel pod shape. Reachable only when mode=sentinel
// (gated by enclosing CEL rules); kept in v1beta1 schema so the relaxation
// in the sentinel stage is enum-only, not a schema additive change.
// +kubebuilder:validation:XValidation:rule="self.replicas >= 3",message="sentinel.replicas must be >= 3"
// +kubebuilder:validation:XValidation:rule="self.replicas <= 7",message="sentinel.replicas must be <= 7"
// +kubebuilder:validation:XValidation:rule="self.quorum >= 2 && self.quorum <= self.replicas",message="sentinel.quorum must be in [2, replicas]"
// Timing floors (downAfterMilliseconds >= 1000 and failoverTimeout >= 180000)
// are enforced by the validating webhook (`validateSentinelTimingFloors` in
// internal/webhook/v1beta1/) instead of CEL — the webhook can read
// `metadata.annotations` to honour the `velkir.ioxie.dev/allow-aggressive-timeouts=true`
// override, which CEL bound to `self` (the SentinelPodSpec) cannot. See #102.
// The downAfterMilliseconds floor was lowered from 30000 to 1000 in #461
// after empirical recovery-SLO measurements showed the 30000 default
// (Sentinel's documented "production safe" value) forced a ~36s recovery
// floor unreachable for sub-15s SLOs; 1s is conservative against
// false-positive failovers on stable in-cluster networks while making
// 6-10s recovery achievable. Values <1000 still require the annotation.
// Defense-in-depth: duplicates the `Minimum=1` field-level marker on
// ParallelSyncs so the constraint survives a future field-level
// relaxation.
// +kubebuilder:validation:XValidation:rule="self.parallelSyncs >= 1",message="parallel-syncs must be >= 1"
// The masterName regex follows Sentinel's own naming rules (the value
// passed to `SENTINEL MONITOR <name> ...`), not RFC-1035: a leading digit
// and trailing hyphen are both legal because Sentinel itself accepts
// them. Valkey never derives a Kubernetes resource name from masterName
// (Service names use the CR name), so the looser shape is safe.
// +kubebuilder:validation:XValidation:rule="self.masterName.matches('^[a-z0-9][a-z0-9-]*$')",message="masterName must match [a-z0-9][a-z0-9-]*"
// +kubebuilder:validation:XValidation:rule="self.masterName == oldSelf.masterName",message="sentinel.masterName is immutable"
type SentinelPodSpec struct {
	// MasterName is passed to sentinel as `SENTINEL MONITOR <masterName> ...`.
	// Required, no default, immutable.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	MasterName string `json:"masterName"`

	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7
	Replicas int32 `json:"replicas,omitempty"`

	// Quorum. The defaulting webhook stamps `ceil((replicas+1)/2)` when zero.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7
	Quorum int32 `json:"quorum,omitempty"`

	// DownAfterMilliseconds is forwarded verbatim to sentinel.conf as
	// `sentinel down-after-milliseconds`. Defines the wait before
	// Sentinel marks the monitored master as +sdown. Practical recovery
	// floor is approximately `DownAfterMilliseconds + 6s` (quorum
	// gathering + election + label-flip + EndpointSlice propagation).
	// Default 3000 (3s) supports a sub-10s recovery SLO on stable
	// in-cluster networks; raise to 30000 (30s) for noisier
	// environments where brief blips would otherwise trigger
	// false-positive failovers. Floor is 1000 (1s); the validating
	// webhook rejects lower values unless the
	// `velkir.ioxie.dev/allow-aggressive-timeouts=true` annotation is set
	// (and emits a warning event when it is).
	// +kubebuilder:default=3000
	DownAfterMilliseconds int32 `json:"downAfterMilliseconds,omitempty"`
	// +kubebuilder:default=180000
	FailoverTimeout int32 `json:"failoverTimeout,omitempty"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	ParallelSyncs int32 `json:"parallelSyncs,omitempty"`

	// PDB overrides the derived PodDisruptionBudget. Default
	// `minAvailable = replicas - 1` when unset.
	// +optional
	PDB *PDBSpec `json:"pdb,omitempty"`

	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// CustomLivenessProbe overrides the operator's default TCP liveness
	// probe for the sentinel container. Intended for tuning the timing
	// fields (period / failureThreshold / timeout) of the restart window;
	// the defaulter stamps a tcpSocket probe with a ~60s anti-flap grace
	// when this is unset.
	// +optional
	CustomLivenessProbe *corev1.Probe `json:"customLivenessProbe,omitempty"`

	// CustomReadinessProbe overrides the operator's default TCP readiness
	// probe. Readiness is deliberately TCP-only — never coupled to quorum
	// state — so a sentinel with degraded quorum still accepts client
	// discovery; the default drops a wedged sentinel out of discovery in
	// ~15s without restarting it.
	// +optional
	CustomReadinessProbe *corev1.Probe `json:"customReadinessProbe,omitempty"`

	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// +optional
	DNSConfig *corev1.PodDNSConfig `json:"dnsConfig,omitempty"`
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`
}

// AuthSpec references Secrets carrying valkey and (optional) sentinel auth.
type AuthSpec struct {
	// SecretName is the Secret containing the Valkey default-user password.
	// +optional
	SecretName string `json:"secretName,omitempty"`
	// +kubebuilder:default="password"
	SecretKey string `json:"secretKey,omitempty"`
	// SentinelAuthSecretName is a separate Secret holding the sentinel
	// auth-user / auth-pass pair. Permitted only when mode=sentinel.
	// +optional
	SentinelAuthSecretName string `json:"sentinelAuthSecretName,omitempty"`
	// +kubebuilder:default="password"
	SentinelAuthSecretKey string `json:"sentinelAuthSecretKey,omitempty"`
}

// ServiceSpec shapes the client- and sentinel-facing Services.
type ServiceSpec struct {
	// +kubebuilder:default={}
	Client ServiceEndpoint `json:"client,omitempty"`
	// +kubebuilder:default={}
	Sentinel ServiceEndpoint `json:"sentinel,omitempty"`
}

// ServiceEndpoint is a per-Service shape (type, annotations, source ranges).
type ServiceEndpoint struct {
	// Type selects the Service shape. The first three are standard Kubernetes
	// Service types; `Headless` is an operator-level abstraction that maps to
	// `type: ClusterIP, clusterIP: None` — exposed as a top-level enum value
	// because direct-pod addressing is a first-class deployment topology for
	// Valkey clients that prefer per-replica DNS over Service load-balancing.
	// +kubebuilder:default="ClusterIP"
	// +kubebuilder:validation:Enum=ClusterIP;LoadBalancer;NodePort;Headless
	Type string `json:"type,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// LoadBalancerSourceRanges restricts ingress when type=LoadBalancer.
	// Required (non-empty) when type=LoadBalancer.
	// +optional
	LoadBalancerSourceRanges []string `json:"loadBalancerSourceRanges,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=Cluster;Local
	ExternalTrafficPolicy string `json:"externalTrafficPolicy,omitempty"`
}

// MetricsSpec gates the exporter sidecar and the PodMonitor.
type MetricsSpec struct {
	// Enabled toggles the per-pod exporter sidecar. `*bool` for the same
	// explicit-vs-unset reason as ReadinessGateSpec.Enabled.
	// +kubebuilder:default=false
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
	// +kubebuilder:default={enabled:false,scrapeInterval:"30s"}
	PodMonitor PodMonitorSpec `json:"podMonitor,omitempty"`

	// Resources overrides the exporter sidecar's requests/limits. When
	// unset the operator stamps `requests: {cpu: 10m, memory: 32Mi}` and
	// `limits: {memory: 64Mi}` — no default CPU limit, because CFS
	// quota bucketing makes a 100m limit trigger CPUThrottlingHighAlert
	// on a sidecar whose long-run average usage is well under 1m.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// PodMonitorSpec is opt-in PodMonitor scraping.
type PodMonitorSpec struct {
	// Enabled toggles PodMonitor emission. `*bool` for the same
	// explicit-vs-unset reason as ReadinessGateSpec.Enabled.
	// +kubebuilder:default=false
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
	// +kubebuilder:default="30s"
	// +kubebuilder:validation:Pattern=`^[0-9]+(s|m)$`
	ScrapeInterval string `json:"scrapeInterval,omitempty"`
}

// BootstrapNodeSpec seeds a new deployment from an existing primary.
type BootstrapNodeSpec struct {
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`
	// +kubebuilder:default=6379
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`
}

// RolloutSpec governs rolling-update behaviour.
type RolloutSpec struct {
	// MaxUnavailable pods during rolling update. The validating webhook
	// emits a Warning when set above 1 (quorum-fragility risk).
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3
	MaxUnavailable int32 `json:"maxUnavailable,omitempty"`

	// FailoverGracePeriodSeconds caps the wait for `+failover-end` during
	// master-aware rolling. Zero means "use default at reconcile time"
	// (sentinel.failoverTimeout + 30s).
	// +kubebuilder:default=0
	FailoverGracePeriodSeconds int32 `json:"failoverGracePeriodSeconds,omitempty"`

	// ReplicaReadyTimeoutSeconds caps how long the rollout waits for a
	// replacement replica pod to become Ready after its old incarnation is
	// deleted. Timeout transitions the state machine to Degraded.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=3600
	ReplicaReadyTimeoutSeconds int32 `json:"replicaReadyTimeoutSeconds,omitempty"`

	// MaxLagBytes is the per-replica `master_repl_offset -
	// slave_repl_offset` ceiling tolerated before the rolling-update
	// advances to the next pod. Distinct from
	// ReadinessGateSpec.MaxLagBytes (steady-state ceiling, 1 MiB
	// default); this knob applies during the rolling-update window only,
	// where slightly larger lag is acceptable in exchange for keeping
	// the rollout moving.
	// +kubebuilder:default=10000
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10485760
	MaxLagBytes int64 `json:"maxLagBytes,omitempty"`
}

// PDBSpec overrides the derived PodDisruptionBudget. Defaults to
// `minAvailable = replicas - 1` when unset. Setting both MinAvailable
// and MaxUnavailable on a single PDB is rejected by the Kubernetes
// PodDisruptionBudget API itself; mirroring that rejection at admission
// time gives a clearer error than waiting for the downstream PDB create
// to fail.
// +kubebuilder:validation:XValidation:rule="!has(self.minAvailable) || !has(self.maxUnavailable)",message="pdb.minAvailable and pdb.maxUnavailable are mutually exclusive"
type PDBSpec struct {
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// MaxUnavailable is mutually exclusive with MinAvailable.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// CR-name validator. Applied at the root because CEL on `spec` cannot read
// metadata.name. The 35-character ceiling leaves headroom for derived
// resource names inside the 63-character DNS-label limit:
// `<cr>-sentinel` adds 9 (= 44), the `-<ord>` PVC suffix adds up to 2
// (= 46), and StatefulSet pod-template-hash labels add ~5 (= 51) — well
// under 63 even with future suffixes.
//
// The `categories={valkey,all}` marker deliberately includes `all` so
// Valkey CRs surface in `kubectl get all` alongside Pods and Services in
// the namespace — a Valkey CR is a first-class workload, not a hidden
// control object. The trade-off is that `kubectl delete all -n <ns>`
// will remove Valkey CRs too — operators relying on `kubectl delete
// all` to scope cleanup must know this.
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$') && size(self.metadata.name) <= 35",message="Valkey name must be an RFC-1035 DNS label (lowercase, start with letter, <= 35 chars)"
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vk,categories={valkey,all}
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +genclient

// Valkey is the Schema for the valkeys API.
type Valkey struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Valkey
	// +required
	Spec ValkeySpec `json:"spec"`

	// status defines the observed state of Valkey
	// +optional
	Status ValkeyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyList contains a list of Valkey
type ValkeyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Valkey `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Valkey{}, &ValkeyList{})
}
