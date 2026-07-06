# Changelog

All notable changes to the operator chart, the operator binary, and
the `Valkey` CRD schema. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this operator uses [Semantic Versioning](https://semver.org/) on a `v0.x` line.
The `Valkey` CRD is served and stored at a single version, `velkir.ioxie.dev/v1beta1`;
the schema evolves additively (no field removals, no type narrowing, no new
required fields), so there is no conversion webhook and existing custom
resources always deserialize unchanged.

## [Unreleased]

### Added

- **Sentinel topology hygiene surface.** Sentinel mode now surfaces a sustained
  deficit between the sentinel-known peer/replica counts and spec — debounced
  and decoupled from the incident ladder — as a `SentinelTopologyReconciled`
  status condition, a per-dimension `valkey_sentinel_topology_mismatch` gauge,
  a once-per-episode `SentinelTopologyMismatch` warning event, and a
  `ValkeySentinelTopologyMismatch` chart alert. Observation-only: it never
  moves Ready/Degraded and triggers no remediation.
- **Stranded-sentinel repair read-back and backoff.** The stranded-sentinel
  surgery now verifies its own convergence: repeated no-progress surgeries on
  the same sentinel back off exponentially per address instead of re-wiping at
  a fixed cadence, and a provably stuck sentinel surfaces once per episode as a
  `SentinelPeerLinkupStuck` warning event plus a Degraded reason of the same
  name (ranked above `QuorumLost` — it names why quorum can't recover).
- **Stale-epoch sentinel re-point.** During an armed re-point pass the operator
  now also force-converges a sentinel monitoring a live-but-superseded primary
  at a stale config-epoch (`SentinelStaleEpochRepoint` warning event).
- **Stale-replica escape.** After a long sustained window with no primary and a
  single dead lineage, the operator deletes the least-fresh stale replica to
  force StatefulSet re-creation and unstick recovery, always preserving the
  promotion candidate (`StaleReplicaEscapeDeleted` warning event, rate-bounded
  to one delete per cooldown).
- **Recovery election under sustained quorum loss.** The zero-master recovery
  election can now arm during sustained quorum-loss suppression via a paced
  forced survey, instead of staying wedged behind the suppression gate.
- **Dual-master detection in replication mode.** The no-labeled-primary
  observation scan now stamps the same `DualMasterObserved`
  event/condition/gauge surface as the sentinel-mode scans.

### Changed

- **`DualMasterObserved` pages once per episode.** The event is edge-gated on
  the accumulated union of de-facto-master pod names, so a persistent split
  pages at episode start and re-pages only when a genuinely new pod joins the
  split — role churn across scans no longer re-fires it.
- **Quorum-suppression entry requires fresh evidence.** The suppression gate's
  entry threshold-cross now requires a fresh sentinel poll stamp, so reconcile
  churn against a wedged observer can no longer trip suppression on stale data.
- **`InitialSentinelReset` narrowed to operator startup.** The redundant
  bootstrap-completion RESET pass was removed; the event now fires only on the
  leader-acquire safety net, and only when its probe detects an anomaly.

### Fixed

- **Stale per-CR alert gauges after a missed delete.** The background state
  pruner now clears the per-CR gauges (`valkey_dual_master_observed`,
  `valkey_split_brain_sustained_seconds`,
  `valkey_master_info_replication_timeout_seconds`,
  `valkey_sentinel_topology_mismatch`) for CRs whose delete event was missed,
  so a critical alert can no longer stay pinned to a workload that no longer
  exists.
- **Dual-master flap on pod recreation.** The replication no-primary scan no
  longer clears an active dual-master episode while a listed pod is Pending
  (IP-less): an incomplete sweep records no verdict, so the condition and gauge
  don't flap off and the warning event doesn't re-fire across a pod restart.
- **Sentinel observer teardown on CR delete.** Deleting a sentinel-mode CR now
  stops its sentinel observer and clears the per-CR ghost-reap debounce state;
  previously the observer kept polling freed pod IPs for the leader's lifetime
  and a recreated same-name CR inherited stale reap timestamps.

## [0.1.0] — 2026-04-25

First public release. The operator manages Valkey on Kubernetes through a
single `Valkey` custom resource (`velkir.ioxie.dev/v1beta1`), mode-switched
across standalone, replication, and Sentinel-backed high-availability
topologies. This entry summarizes the operator's capabilities as of the
first published chart and image (`ghcr.io/ioxie/velkir/...`).

### Added

- **Three deployment modes from one CRD.** `spec.mode: standalone | replication | sentinel`
  drives a mode-aware reconciler. Standalone runs a single instance; replication
  runs a primary plus read replicas with derived read/read-only Services; Sentinel
  mode adds a managed Sentinel quorum for automated failover.
- **Master-aware rolling updates.** Image bumps, config changes, and manual
  rollouts roll one pod at a time, rolling replicas first and the primary last,
  gating each step on the rolled pod rejoining a healthy topology. A finite-state
  machine drives the rollout and aborts cleanly into a Degraded state on quorum
  loss, resuming the rollout on recovery.
- **Sentinel-based automated failover with split-brain guards.** The operator
  observes the Sentinel quorum through a hybrid push (pub/sub) plus pull model and
  never trusts pub/sub alone. Operator-initiated failover requires triple-signal
  agreement (quorum confirmation, an odown consensus, and the kubelet reporting the
  primary not-ready). A durable fencing marker records the in-flight failover so an
  operator crash mid-election cannot re-stamp a stale primary, keeping at most one
  pod labelled `role=primary` across restarts. After a failover the operator
  reconciles role labels add-before-remove (the write Service never empties) and
  evicts pooled write connections off the demoted pod so clients re-route promptly.
- **Sentinel quorum health and recovery.** A tri-state quorum signal (Unknown /
  OK / Lost) with hysteresis surfaces sustained loss as a `QuorumLost` condition
  without flapping on transient blips. During sustained quorum loss the operator
  enters an observation-only mode that suppresses all Sentinel mutation and pod-count
  changes. Stranded or rebuilt Sentinels are detected per reconcile and rejoined to
  the quorum via REMOVE + MONITOR (re-applying the operator's per-master tuning),
  and the Sentinel StatefulSet roll is serialized behind the failover critical
  section so a Sentinel pod is never rolled during an election window.
- **A preStop failover safety net.** A pod-termination hook asks the local server
  for its role; a terminating primary calls `SENTINEL FAILOVER` itself, so a primary
  delete triggers failover immediately even when the operator is unavailable, rather
  than waiting out the Sentinel down-detection timer.
- **Replication-lag readiness gating.** Replicas are held NotReady until their
  replication link is up and their lag is within a configurable budget, so the
  read-only Service only routes to caught-up replicas. The gate is opt-out, and the
  `ReplicationHealthy` condition resolves to a definite value for clean
  `kubectl wait`.
- **Write-loss protection.** Replication and Sentinel modes render
  `min-replicas-to-write` / `min-replicas-max-lag` floors as operator-owned
  directives, so a primary partitioned from all replicas stops accepting writes
  that could never replicate.
- **Persistent storage with retention and in-place resize.** `spec.valkey.persistence`
  provisions PVC-backed StatefulSets. `spec.pvcRetentionPolicy` (`Retain` | `Delete`,
  default `Retain`) is enforced through a finalizer so the policy applies even across
  an operator outage, and an opt-in safety gate refuses silent recovery (and possible
  data loss) when both the StatefulSet and its PVCs have gone missing. Growing
  `spec.valkey.persistence.size` drives an operator-managed orphan-delete / patch /
  recreate resize state machine on expansion-capable StorageClasses, with stall
  detection and backed-off retries; shrink requests are rejected at admission.
- **Authentication and zero-downtime password rotation.** Auth is sourced from a
  Secret. Rotating the Secret's password is hot-applied across replicas and the
  primary with no pod restart, reverting the changed subset on partial failure, with
  a status sub-state, alertable events, and a structured audit trail recording the
  outcome and which pods carry which credential.
- **Metrics, alerts, and dashboards.** An exporter sidecar is gated on
  `spec.metrics.enabled` and runs on both data-plane and Sentinel pods (with
  per-CR resource overrides). The operator emits its own metrics — failover counts
  and duration, sustained split-brain and INFO-replication-timeout gauges,
  quorum-suppression transitions, certificate expiry, and PVC-resize progress — and
  reaps per-CR series on resource deletion to bound cardinality. The chart ships a
  PrometheusRule alert pack (data-plane, Sentinel-plane, and operator groups, with
  promtool unit tests) and opt-in Grafana dashboards (operator, fleet-overview, and
  single-deployment).
- **Validating and defaulting webhooks with safety invariants.** The defaulter fills
  probe templates, resources, PDB derivation, mode-aware replicas, and the readiness
  gate. The validator enforces the load-bearing invariants: IP-only peer addressing
  (no Sentinel hostname resolution), Sentinel timing floors with an annotation-gated
  aggressive-timeout override and a durable warning for aggressive values, immutable
  `spec.sentinel.masterName`, a soft-warn (not reject) for sub-quorum Sentinel
  replicas, an exec-based liveness probe that detects a frozen server process behind
  a live TCP socket, and Valkey-image major-version support and downgrade preflight
  checks (admission-time plus a runtime transition guard).
- **Operator-self TLS and least-privilege footprint.** An in-process certificate
  authority issues and rotates the webhook and metrics leaves (with hot-reload), the
  secure `/metrics` endpoint is fully authenticated and TLS-validated by its own
  ServiceMonitor, and the chart ships a NetworkPolicy with enumerated egress, a
  cluster role plus a namespaced cert-writer role, and PSA-`restricted`-compliant
  security-context defaults.
- **Helm charts.** A CRD-only chart (install first) and an operator chart
  (controller Deployment, RBAC, webhook configuration, NetworkPolicy, ServiceMonitor,
  PrometheusRule, and dashboards), with a published documentation set covering install,
  upgrade, rollback, security, feature gates, supported versions, and adoption of an
  existing Valkey/Redis StatefulSet.

### Security

- **Secret-safe logging.** A refcounted redaction registry scrubs registered secret
  tokens out of every log field shape the operator emits, with a metric to detect
  registry leaks.
