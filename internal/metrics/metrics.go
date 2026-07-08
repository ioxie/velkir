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

// Package metrics declares the operator's custom Prometheus metrics and
// registers them into controller-runtime's shared Registry so they're served
// on the same /metrics endpoint as controller-runtime's built-ins.
//
// Naming conventions:
//   - All metric names start with "valkey_".
//   - Counters: plural subject, "_total" suffix (Prometheus convention).
//   - Histograms: "_seconds" suffix; default buckets unless a measured
//     distribution justifies custom boundaries (failover/rollout durations
//     use exponential buckets out to 600s).
//   - Labels: kind / namespace / name for per-CR identification, plus a
//     bounded discriminator (reason / trigger / event / sentinel_pod / pod)
//     where the metric tracks an enumerated cause or a per-pod axis.
//
// Cardinality discipline: no unbounded labels.
// `name`/`namespace` is bounded by the live CR count. `sentinel_pod`
// is bounded by `sentinel.replicas` (3-5). `pod` (on
// `valkey_exporter_sidecar_up`) is bounded by `valkey.replicas` (1-10).
// We never label on raw pod name in rollout / failover metrics — pods
// churn during a rollout, the cardinality bombs.
//
// Per-CR series eviction: when a CR is deleted, the reconciler's
// NotFound path calls ResetReconcileGauges so series don't linger at
// their last value for the full Prometheus retention window.
//
// `kind` is a constant label today — the only Kind the operator
// reconciles is `Valkey`. Kept on every per-CR metric for forward
// compatibility (a future SentinelQuorum-targeted reconciler could
// share the same metrics namespace), but it's a dead series-multiplier
// cost until that's realised. Documented here rather than dropped — a
// rename is breaking; an unused-but-future-expected label is cheap.
//
// Import convention: the controller-runtime shared registry is aliased
// as `ctrlmetrics` in this package and should be throughout the tree.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ioxie/velkir/internal/logging"
)

// Metric names exposed as consts so tests can assert prefixes / shapes
// against the same strings the declarations use, and so the
// events-catalog-membership linter has a stable identifier set to
// cross-check against catalogs in other packages.
const (
	// Reconciliation lifecycle. Only the two carrying operator-specific
	// signal beyond controller-runtime's per-controller built-ins are
	// kept: failures by classified reason, and per-CR mutex contention.
	// The per-controller total / success / duration views come from the
	// built-in controller_runtime_reconcile_total /
	// controller_runtime_reconcile_time_seconds, served on the same
	// /metrics endpoint.
	NameReconciliationsFailedTotal = "valkey_reconciliations_failed_total"
	NameReconciliationsLockedTotal = "valkey_reconciliations_locked_total"

	// Resource inventory.
	NameResources       = "valkey_resources"
	NameResourceState   = "valkey_resource_state"
	NameResourcesPaused = "valkey_resources_paused"

	// Failover + rollout durations.
	NameFailoversTotal          = "valkey_failovers_total"
	NameFailoverDurationSeconds = "valkey_failover_duration_seconds"
	NameRolloutsTotal           = "valkey_rollouts_total"
	NameRolloutDurationSeconds  = "valkey_rollout_duration_seconds"

	// Sentinel observation surface.
	NameSentinelObserverReconnectsTotal   = "valkey_sentinel_observer_reconnects_total"
	NameSentinelObserverConnected         = "valkey_sentinel_observer_connected"
	NameSentinelPubsubMessagesTotal       = "valkey_sentinel_pubsub_messages_total"
	NameQuorumCheckTotal                  = "valkey_quorum_check_total"
	NameQuorumSuppressionTransitionsTotal = "valkey_quorum_suppression_transitions_total"
	NameSplitBrainDetectionsTotal         = "valkey_split_brain_detections_total"
	NameSplitBrainSustainedSeconds        = "valkey_split_brain_sustained_seconds"
	NameMasterInfoTimeoutSeconds          = "valkey_master_info_replication_timeout_seconds"
	NameDualMasterObserved                = "valkey_dual_master_observed"
	NameSentinelTopologyMismatch          = "valkey_sentinel_topology_mismatch"

	// Webhook surface.
	NameWebhookRequestsTotal          = "valkey_webhook_requests_total"
	NameWebhookRequestDurationSeconds = "valkey_webhook_request_duration_seconds"

	// Cert + secret rotation.
	NameCertRotationsTotal  = "valkey_cert_rotations_total"
	NameCertExpirySeconds   = "valkey_cert_expiry_seconds"
	NameSecretRotationTotal = "valkey_secret_rotation_total"

	// Exporter sidecar reachability — operator-side gauge maintained by
	// the exporter watcher (see internal/metrics/exporter_watcher.go).
	NameExporterSidecarUp = "valkey_exporter_sidecar_up"

	// PVC resize substate-machine progress gauge. Set to 1 while
	// status.rollout.pvcResize is non-nil (any substate other than
	// the absent/cleared state); set to 0 on Verified or on the next
	// reconcile after Aborted clears the substate.
	NamePVCResizeInProgress = "valkey_pvc_resize_in_progress"

	// Redaction registry size — distinct tokens currently registered
	// for log scrubbing in the operator process. No labels (the
	// registry is a process-wide singleton). Steady-state ~1-3 tokens
	// per live CR with auth; sustained > 10 hints at a registry
	// cleanup leak (Register without matching Forget on CR delete or
	// Secret rotation).
	NameRedactionRegistryTokens = "valkey_redaction_registry_tokens"
)

// Histogram bucket sets used across the catalog.
var (
	// failoverRolloutBuckets bounds long-tail durations: most failovers
	// finish under a minute, but the sentinel.failoverTimeout floor is
	// 180s and rollouts of 5+ replicas can stretch past 5 minutes. The
	// 600s ceiling captures hung rollouts that need an alert; bursts
	// past it are tracked via the +Inf overflow bucket.
	failoverRolloutBuckets = []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600}

	// webhookBuckets are tight: admission has a 10s apiserver-side
	// budget per webhook call; everything we care about lives under 1s.
	// Buckets stop at 10s to expose anything brushing the timeout.
	webhookBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
)

// Reconciliation lifecycle metrics. Scoped to the two signals
// controller-runtime's per-controller built-ins don't provide:
// failures broken down by a classified reason, and per-CR reconcile
// mutex contention. Aggregate reconcile total / success / wall-time
// come from controller_runtime_reconcile_total and
// controller_runtime_reconcile_time_seconds on the same endpoint, so
// per-CR duplicates of those were never emitted and are not declared.
var (
	// `reason` is sourced from a bounded operator-internal classification
	// (e.g. AuthSecretNotFound, STSConflict, SentinelUnreachable). The
	// internal/events catalog enumerates the legal values; the lint plugin
	// rejects emit sites that pass an out-of-catalog reason.
	ReconciliationsFailedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameReconciliationsFailedTotal,
			Help: "Reconcile loops that returned an error, labelled by classified reason.",
		},
		[]string{"kind", "namespace", "name", "reason"},
	)

	// Incremented when the per-CR mutex's TryLock fails — a concurrent
	// reconcile is already running for this CR. High counts indicate
	// reconcile contention or a stuck reconciler holding the lock.
	ReconciliationsLockedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameReconciliationsLockedTotal,
			Help: "Reconcile loops skipped because the per-CR mutex was already held.",
		},
		[]string{"kind", "namespace", "name"},
	)
)

// Resource inventory. `valkey_resources` is the cluster-wide CR
// inventory by mode; per-CR metrics live under `valkey_resource_state`
// (one series per CR per condition type per status).
var (
	Resources = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameResources,
			Help: "Count of Valkey CRs in the live cache, partitioned by mode (standalone | replication | sentinel).",
		},
		[]string{"kind", "mode"},
	)

	// Cardinality: per-CR × per-condition-type × per-status. Conditions
	// are a bounded enum (Ready, Degraded, Reachable, QuorumAchieved,
	// PrimaryConfirmed, ...; ~10 types); status is {True, False, Unknown}.
	// Per-CR cardinality ~30 series; cluster-wide bounded by live CR
	// count × 30. The status-writer maintains the gauge after every
	// condition write — emit points: status-write at the end of the
	// reconcile pass; NotFound deletion path on the reconciler.
	ResourceState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameResourceState,
			Help: "1 when the named condition is in the named status; 0 otherwise. One series per (CR, condition_type, status).",
		},
		[]string{"kind", "namespace", "name", "condition_type", "status"},
	)

	// 1 when the CR carries the `velkir.ioxie.dev/paused=true` annotation,
	// 0 otherwise. Refreshed on every requeue while paused so dashboards
	// spot a stuck pause that's older than the operator-restart window.
	ResourcesPaused = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameResourcesPaused,
			Help: "1 when the CR carries velkir.ioxie.dev/paused=true, otherwise 0.",
		},
		[]string{"namespace", "name"},
	)
)

// Failover + rollout durations.
var (
	// `trigger ∈ {sentinel_elected, master_aware_rolling, manual}`. Sentinel-
	// elected fires from the pubsub handler on `+failover-end`; master-aware
	// fires when the operator initiates failover during a rollout (M4 work);
	// manual covers the `velkir.ioxie.dev/failover-now` annotation path.
	FailoversTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameFailoversTotal,
			Help: "Master failovers performed, labelled by trigger.",
		},
		[]string{"namespace", "name", "trigger"},
	)

	FailoverDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    NameFailoverDurationSeconds,
			Help:    "Time from +failover-start to +failover-end (sentinel pubsub).",
			Buckets: failoverRolloutBuckets,
		},
		[]string{"namespace", "name", "trigger"},
	)

	// `reason ∈ {image_bump, config_bump, secret_rotation, user_request}`.
	// Excludes failover-driven primary recreates (those are tracked in
	// FailoversTotal).
	RolloutsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameRolloutsTotal,
			Help: "Rolling-update sequences initiated, labelled by reason.",
		},
		[]string{"namespace", "name", "reason"},
	)

	RolloutDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    NameRolloutDurationSeconds,
			Help:    "End-to-end rollout duration, from first pod-delete to last pod-Ready.",
			Buckets: failoverRolloutBuckets,
		},
		[]string{"namespace", "name", "reason"},
	)
)

// Sentinel observation. The observer goroutine maintains pubsub
// connections to every sentinel pod; these metrics expose connection
// health + message volume + quorum-check outcomes to dashboards.
var (
	SentinelObserverReconnectsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameSentinelObserverReconnectsTotal,
			Help: "Per-CR pubsub connection drops + reconnects to a specific sentinel pod.",
		},
		[]string{"namespace", "name", "sentinel_pod"},
	)

	SentinelObserverConnected = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameSentinelObserverConnected,
			Help: "1 if the observer goroutine currently holds an open subscription to this sentinel pod, 0 otherwise.",
		},
		[]string{"namespace", "name", "sentinel_pod"},
	)

	// `event ∈ {+switch-master, +failover-end, +odown, +tilt, ...}`. The
	// sentinel pubsub event vocabulary is bounded; the operator only
	// labels on events it acts on, anything else collapses to a
	// `other` bucket so the cardinality stays predictable.
	SentinelPubsubMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameSentinelPubsubMessagesTotal,
			Help: "Sentinel pubsub messages received by event type.",
		},
		[]string{"namespace", "name", "event"},
	)

	// `result ∈ {ok, failed}`. Increments on every CKQUORUM RPC the
	// operator issues — typically the triple-check failover rule.
	QuorumCheckTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameQuorumCheckTotal,
			Help: "CKQUORUM RPCs made by the operator, labelled by outcome.",
		},
		[]string{"namespace", "name", "result"},
	)

	// Increments on every detected disagreement between sentinels on
	// `GET-MASTER-ADDR-BY-NAME` (sentinel-protocol invariant 12). A
	// non-zero count is operationally serious — the corresponding alert
	// should page on it.
	SplitBrainDetectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameSplitBrainDetectionsTotal,
			Help: "Times the operator detected sentinel disagreement on the primary identity.",
		},
		[]string{"namespace", "name"},
	)

	// SplitBrainSustainedSeconds tracks the contiguous duration the
	// observer has reported Quorum==Lost for a CR. Resets to 0 on
	// agreement; Unknown (the observer can't reach a quorum of peers,
	// e.g. the restart placeholder) is ignored and does not accumulate.
	// The chart-shipped ValkeySplitBrainDetected alert keys off this
	// gauge (not the counter rate) so transient bootstrap-race
	// disagreements that resolve within ~60s don't page on every fresh
	// deploy / operator restart.
	SplitBrainSustainedSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameSplitBrainSustainedSeconds,
			Help: "Sustained seconds of contiguous Quorum==Lost observation for the CR. Resets to 0 when sentinels reach agreement; Unknown (observer cannot reach a quorum of peers) is ignored.",
		},
		[]string{"namespace", "name"},
	)

	// DualMasterObserved is 1 while the operator's most recent
	// dual-master scan saw two or more live pods self-reporting
	// role:master — active data divergence: writes are landing on more
	// than one primary. Any of four scans set it: the sentinel Phase 7a
	// self-heal scan (inside a failover section) and Phase 11 recovery
	// survey (outside one), and the replication labeled-primary orphan
	// scan and no-labeled-primary observation scan. Set back to 0 by the
	// first scan that sees at most one self-reported master. Alert on
	// sustained non-zero values: the operator surfaces the split but
	// refuses unfenced demotion (outside a failover section in sentinel
	// mode, or with no elected primary to fence against in replication
	// mode), so a lasting 1 means manual resolution is needed.
	DualMasterObserved = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameDualMasterObserved,
			Help: "1 while >=2 valkey pods self-report role:master (active data divergence), 0 otherwise.",
		},
		[]string{"namespace", "name"},
	)

	// SentinelTopologyMismatch is the per-dimension deficit
	// (expected-observed) of the sentinel-known peer / replica counts
	// vs spec while a sustained mismatch is active, 0 otherwise. The
	// `dimension` label splits the sentinels (num-other-sentinels vs
	// spec.sentinel.replicas-1) and replicas (num-slaves vs
	// spec.valkey.replicas-1) surfaces. Single-writer from updateStatus,
	// derived from the same freshness-gated read that drives the
	// SentinelTopologyReconciled condition — so it can't latch after the
	// observation ages out. Emitted only for sentinel-mode CRs.
	SentinelTopologyMismatch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameSentinelTopologyMismatch,
			Help: "Per-dimension deficit (expected-observed) of sentinel-known peer/replica counts vs spec while a sustained mismatch is active, 0 otherwise.",
		},
		[]string{"namespace", "name", "dimension"},
	)

	// MasterInfoTimeoutSeconds tracks the contiguous duration the
	// operator's INFO-replication probe of the labelled primary pod
	// has been timing out or returning malformed replies. Non-zero
	// values indicate "TCP up, valkey-server unresponsive" — the
	// frozen-process signature (SIGSTOP, cgroup-freezer, kernel stall)
	// the chart's exec liveness probe is also designed to catch.
	// Exposed as a passive observability signal (tier-3): alert
	// on sustained values (e.g. > 30s) to detect frozen-process cases
	// that slip past the kubelet liveness restart window. Reset to 0
	// on successful INFO probe.
	MasterInfoTimeoutSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameMasterInfoTimeoutSeconds,
			Help: "Sustained seconds the operator's INFO replication probe of the labelled primary has been timing out / malformed. >0 indicates a frozen-process or unresponsive-master signature.",
		},
		[]string{"namespace", "name"},
	)

	// `from`, `to ∈ {inactive, active}`. Increments on every per-CR
	// transition of the quorum-loss suppression gate — the threshold
	// (≥ lossThreshold of sustained Lost observations) cross fires
	// `inactive → active`, the hysteresis (≥ recoveryPolls
	// consecutive OK observations) cross fires `active → inactive`.
	// Useful for fleet-wide dashboards measuring recovery quality
	// across rollouts without tailing per-CR events.
	QuorumSuppressionTransitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameQuorumSuppressionTransitionsTotal,
			Help: "Per-CR transitions of the quorum-loss suppression gate (inactive↔active), labelled by from/to state.",
		},
		[]string{"namespace", "name", "from", "to"},
	)
)

// Webhook surface. controller-runtime's webhook server doesn't emit
// per-call metrics by default; these are operator-managed wrappers.
var (
	// `webhook ∈ {valkey-defaulter, valkey-validator, sentinelquorum-validator}`,
	// `operation ∈ {CREATE, UPDATE, DELETE, CONNECT}`,
	// `code ∈ {200, 400, 403, 500, ...}` (HTTP-style numeric codes).
	WebhookRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameWebhookRequestsTotal,
			Help: "Per-webhook admission request counts, labelled by webhook + operation + response code.",
		},
		[]string{"webhook", "operation", "code"},
	)

	WebhookRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    NameWebhookRequestDurationSeconds,
			Help:    "Per-webhook admission request latency.",
			Buckets: webhookBuckets,
		},
		[]string{"webhook", "operation", "code"},
	)
)

// Cert + secret rotation surface.
var (
	// `kind ∈ {ca, leaf}`. Increments on every successful rotation
	// performed by the dynauth Authority.
	CertRotationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameCertRotationsTotal,
			Help: "Webhook cert rotations performed, labelled by which cert was rotated.",
		},
		[]string{"kind"},
	)

	// Cardinality bounded at the number of certs the dynauth Authority
	// manages (today: kind=ca, kind=webhook-leaf, kind=metrics-leaf).
	// Maintained by the dynauth Authority's reconcile loop; PrometheusRule
	// `ValkeyCertExpiringSoon` reads this gauge without a kind filter,
	// so adding a new leaf widens the alert automatically.
	CertExpirySeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: NameCertExpirySeconds,
			Help: "Seconds remaining until an operator-managed cert expires (kind=ca|webhook-leaf|metrics-leaf).",
		},
		[]string{"kind"},
	)

	// `result ∈ {success, reverted, failed}`. Operational visibility
	// surface for the secret-rotation reconciler.
	SecretRotationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: NameSecretRotationTotal,
			Help: "Auth-Secret rotations performed, labelled by outcome.",
		},
		[]string{"namespace", "name", "result"},
	)
)

// Exporter sidecar reachability gauge.
//
// Maintained by the operator-side exporter watcher (see
// internal/metrics/exporter_watcher.go), NOT by the exporter itself.
// Per the contract documented on the watcher: 1 when the exporter
// container is present in the pod template AND the pod is `Ready`;
// 0 otherwise (sidecar absent, pod not Ready, or watcher hasn't
// observed the pod yet). Cardinality bounded by `valkey.replicas`
// (1-10) per CR.
//
// PrometheusRule `ValkeyExporterUnreachable` alerts on this gauge
// being 0 for longer than the spec.metrics.scrapeInterval — an
// extended 0 means the dashboards have stopped getting fresh data
// from this pod.
var ExporterSidecarUp = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: NameExporterSidecarUp,
		Help: "1 if the exporter sidecar in this pod is observed Ready by the operator, 0 otherwise.",
	},
	[]string{"namespace", "name", "pod"},
)

// PVC resize sub-state-machine progress gauge.
//
// Set to 1 while a CR's status.rollout.pvcResize substate is in flight
// (any phase other than absent/cleared); flipped to 0 once the substate
// machine reaches Verified, or on the next reconcile after an Aborted
// transition clears the substate (so a stuck Aborted state shows 0
// rather than 1 between attempts).
//
// Cardinality bounded by `live CRs currently mid-resize` — typically 0
// at steady state, ≤ replica-count during planned cluster expansions.
// PrometheusRule that pages on a stuck resize will key on this gauge
// being 1 for longer than the per-substate stall window.
var PVCResizeInProgress = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: NamePVCResizeInProgress,
		Help: "1 while the PVC resize sub-state-machine is mid-flight for this CR, 0 otherwise.",
	},
	[]string{"namespace", "name"},
)

// Redaction registry observability gauge.
//
// Reflects the live size of the process-wide log-redaction registry
// (logging.DefaultRegistry). Implemented as a GaugeFunc so the
// value is read at scrape time — no goroutine, no periodic poll,
// the count cannot drift from the registry between scrapes.
//
// Cardinality: 1 series, no labels. The registry is a process-wide
// singleton; per-CR breakdown would require labelling and a
// substantially different storage shape. The point of this metric is
// to spot operator-process pathologies (cleanup leaks, runaway
// register paths) at a glance, not to attribute tokens back to CRs.
//
// PrometheusRule consumers: alert if this stays above ~10 for an
// extended window (configurable per fleet; the steady-state
// expectation is 1-3 per live CR with auth).
var RedactionRegistryTokens = prometheus.NewGaugeFunc(
	prometheus.GaugeOpts{
		Name: NameRedactionRegistryTokens,
		Help: "Distinct tokens currently registered for log redaction in the operator process. Steady-state 1-3 per live CR with auth; sustained > 10 hints at a registry cleanup leak.",
	},
	func() float64 {
		return float64(logging.DefaultRegistry.Len())
	},
)

// SetPaused stamps the per-CR pause gauge. Safe from any goroutine.
func SetPaused(namespace, name string, paused bool) {
	v := 0.0
	if paused {
		v = 1.0
	}
	ResourcesPaused.WithLabelValues(namespace, name).Set(v)
}

// perCRMetric is the subset of metric-vector behaviour ResetReconcileGauges
// needs: a registered collector whose series can be dropped by a partial
// label match. Every Gauge/Counter/HistogramVec satisfies it.
type perCRMetric interface {
	prometheus.Collector
	DeletePartialMatch(prometheus.Labels) int
}

// ResetReconcileGauges drops every per-CR series for a deleted CR so a
// churned CR — or a churned pod under one — doesn't keep series at their
// last value for the full Prometheus retention window. Called from the
// reconciler's NotFound path after the CR's finalizer has run.
//
// The per-CR vector set is derived at call time from Collectors() rather
// than a hand-maintained list: every registered collector satisfying
// perCRMetric is partial-matched, so a new metric labelled by
// (namespace, name) is reaped automatically with nothing to keep in sync.
// Partial-match by (namespace, name) reaps every series for the CR
// regardless of the vector's other labels (condition_type/status on
// ResourceState, sentinel_pod/pod on the observer + exporter gauges, etc.),
// so even a condition cleared only to terminal Unknown is removed rather
// than left resident. A vector that does not carry both labels matches zero
// series, and a non-vector collector (e.g. the Resources GaugeFunc) fails
// the perCRMetric assertion — so neither widens the reaped set.
func ResetReconcileGauges(namespace, name string) {
	match := prometheus.Labels{"namespace": namespace, "name": name}
	for _, c := range Collectors() {
		if v, ok := c.(perCRMetric); ok {
			v.DeletePartialMatch(match)
		}
	}
}

// SetPVCResizeInProgress stamps the per-CR resize-in-flight gauge.
// Called by the substate machine after every transition: 1 for any
// non-terminal substate, 0 for Verified or for the absent/cleared
// state (after Aborted clears between attempts).
func SetPVCResizeInProgress(namespace, name string, inProgress bool) {
	v := 0.0
	if inProgress {
		v = 1.0
	}
	PVCResizeInProgress.WithLabelValues(namespace, name).Set(v)
}

// SetSentinelTopologyMismatch stamps the per-dimension topology-mismatch
// gauge for a sentinel-mode CR. Single writer (updateStatus), driven by
// the same freshness-gated read as the SentinelTopologyReconciled
// condition: a zero deficit sets the series to 0 (not delete), so the
// gauge tracks the condition and can't latch after the mismatch clears.
func SetSentinelTopologyMismatch(namespace, name string, sentinelDeficit, replicaDeficit int) {
	SentinelTopologyMismatch.WithLabelValues(namespace, name, "sentinels").Set(float64(sentinelDeficit))
	SentinelTopologyMismatch.WithLabelValues(namespace, name, "replicas").Set(float64(replicaDeficit))
}

// DeleteSentinelTopologyMismatch drops both per-CR topology-mismatch
// series (one per dimension) for a CR that no longer exists. This gauge
// carries a third `dimension` label, so a two-value DeleteLabelValues
// would match nothing; a partial match on (namespace, name) reaps every
// dimension series at once. Called from the per-CR teardown path so the
// series clear in lock-step with the sibling single-label gauges instead
// of lingering at their last value.
func DeleteSentinelTopologyMismatch(namespace, name string) {
	SentinelTopologyMismatch.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "name": name})
}

// Collectors returns every metric declared here in registration order.
// Tests use this to scrape or clear state; Register consumes it to
// hand everything to controller-runtime.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		// Reconciliation lifecycle.
		ReconciliationsFailedTotal,
		ReconciliationsLockedTotal,
		// Resource inventory.
		Resources,
		ResourceState,
		ResourcesPaused,
		// Failover + rollout.
		FailoversTotal,
		FailoverDurationSeconds,
		RolloutsTotal,
		RolloutDurationSeconds,
		// Sentinel observation.
		SentinelObserverReconnectsTotal,
		SentinelObserverConnected,
		SentinelPubsubMessagesTotal,
		QuorumCheckTotal,
		QuorumSuppressionTransitionsTotal,
		SplitBrainDetectionsTotal,
		SplitBrainSustainedSeconds,
		DualMasterObserved,
		SentinelTopologyMismatch,
		MasterInfoTimeoutSeconds,
		// Webhook surface.
		WebhookRequestsTotal,
		WebhookRequestDurationSeconds,
		// Cert + secret rotation.
		CertRotationsTotal,
		CertExpirySeconds,
		SecretRotationTotal,
		// Exporter reachability.
		ExporterSidecarUp,
		// Rollout substate gauges.
		PVCResizeInProgress,
		// Operator-process observability.
		RedactionRegistryTokens,
	}
}

var registerOnce sync.Once

// Register adds every operator metric to controller-runtime's shared
// Registry. Safe to call more than once — subsequent calls are no-ops
// via sync.Once. Called from cmd/main.go before mgr.Start; tests that
// want these metrics available can also call it without worrying about
// double-registration.
func Register() {
	registerOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(Collectors()...)
	})
}
