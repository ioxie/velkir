# RBAC audit

The operator's RBAC surface, as of the current release. Audited against the
"least-privilege" goal: every verb the operator holds must be reachable
from a tested code path, and no verb may be wider than the resource the
code path actually mutates.

## Cluster-wide grants

Cluster scope is required because the operator watches `Valkey` CRs
across all namespaces.

| Resource | Verbs | Justification |
|---|---|---|
| `velkir.ioxie.dev/valkeys` | get, list, watch, create, update, patch, delete | CR lifecycle; the `delete` verb is for the future cluster-wide cleanup path (the standard reconciler doesn't issue `Delete`). |
| `velkir.ioxie.dev/valkeys/status` | get, update, patch | Status subresource updates from the reconciler's deferred status block. |
| `velkir.ioxie.dev/valkeys/finalizers` | update | Finalizer add/remove during deletion. |
| `velkir.ioxie.dev/sentinelquorums*` | (same shape) | The helper `SentinelQuorum` CRD; same contract. |
| `apps/statefulsets` | get, list, watch, create, update, patch, delete | Owned data-plane workload. |
| `core/configmaps`, `core/services` | get, list, watch, create, update, patch, delete | Owned data-plane resources. |
| `core/secrets` | **get, list, watch only** | User-supplied auth Secrets live alongside the CR (any namespace). Cluster-wide WRITE on Secrets is the canonical operator privilege-escalation surface — an attacker who can influence Secret content (Secret reflection, configmap-to-secret race, malicious CR) can pivot through any privileged consumer of those Secrets. The operator does NOT have it; WRITE is namespaced (see below). The cluster-wide READ is bounded by an informer cache `Label` selector (see "Defense-in-depth" below) — RBAC permits the cluster, the cache restricts what enters the process. Recent CVE forensics: CVE-2025-55196, CVE-2025-59303. |
| `core/persistentvolumeclaims` | get, list, watch, patch, update | PVC retention policy enforcement, expansion. No `delete` — PV deletion is governed by `pvcRetentionPolicy` and goes through the StatefulSet's volumeClaimTemplate path, not direct PVC deletion. |
| `core/pods` | get, list, watch, patch | Master-aware rollout label flips, role labelling. |
| `core/pods/status` | **patch** | Replication-lag readiness gate. The ONLY subresource verb the operator needs. |
| `policy/poddisruptionbudgets` | get, list, watch, create, update, patch, delete | Owned PDB resources. |
| `core/events` | create, patch | EventRecorder emissions. |
| `admissionregistration.k8s.io/{mutating,validating}webhookconfigurations` | get, list, watch, patch, update | Webhook caBundle injection (dynauth subsystem). Scoped at the controller layer to objects carrying the `velkir.ioxie.dev/inject-ca` label. |

## Namespaced grants (operator's own namespace only)

| Resource | Verbs | Justification |
|---|---|---|
| `core/secrets` | create, update, patch, delete | The dynauth cert injector creates and rotates the operator's own webhook TLS Secret. This grant is namespace-scoped via a `Role` (not a `ClusterRole`), keeping the WRITE blast radius bounded to the operator's namespace even if a code path were exploitable. |
| `coordination.k8s.io/leases` | (kubebuilder leader-election defaults) | Leader election on the operator's own namespace. |

## Deliberate non-grants

The following verbs are **deliberately absent**, even though similar
operators commonly request them:

| Verb / resource | Why omitted |
|---|---|
| `pods/exec` | The operator never `kubectl exec` into managed pods. Failover, replication setup, and CONFIG SET all happen via the in-pod `valkey-cli` invoked over the network from the operator process. Adding `pods/exec` would expose a code-execution path that doesn't need to exist. |
| `pods/portforward` | Same reasoning as `pods/exec` — the operator connects to pods via Service or pod IP directly. |
| `core/secrets` cluster-wide WRITE | See above; the WRITE is namespaced. |
| `nodes/*` | The operator doesn't read or modify Node objects. Topology-aware scheduling is delegated to PDB + topologySpreadConstraints on the StatefulSet. |
| `rbac.authorization.k8s.io/*` | The operator doesn't manage RBAC for its CRs. |
| `apiextensions.k8s.io/customresourcedefinitions` | CRD lifecycle is managed by the chart (or kustomize), not the running operator. |

## How the chart and kustomize stay in sync

Two RBAC sources cooperate:

- **`charts/velkir/templates/clusterrole.yaml` + `role.yaml`**:
  hand-crafted helm chart, expresses the cluster-wide-READ +
  namespaced-WRITE split for Secrets directly.
- **`config/rbac/role.yaml` (regenerated from kubebuilder markers) +
  `config/rbac/cert_writer_role.yaml` (hand-crafted)**: kubebuilder
  generates the cluster-scoped role from `+kubebuilder:rbac` markers
  in `internal/controller/valkey_controller.go`; the cert-writer
  namespaced Role is hand-crafted because kubebuilder markers can't
  express namespaced scope.

The kubebuilder markers were tightened to drop Secret WRITE
from the cluster-wide marker — `make manifests` now regenerates a
role.yaml that mirrors the chart's safe shape.

## Defense-in-depth: informer cache label selector

The cluster-wide Secret READ is necessary (CRs and their auth Secrets live
in arbitrary namespaces) but the cache is narrower than the RBAC grant by
design. The manager wires `cache.ByObject{Label: app.kubernetes.io/managed-by=velkir}`
for every owned type — Secret included — so the list-watch hitting the
apiserver carries that label selector and unlabeled cluster state never
populates the operator's in-memory cache.

The relevant config field is `cacheManagedBySelector` (default
`app.kubernetes.io/managed-by=velkir`); it is rejected at startup
when empty. Source: `cmd/main.go::buildCacheOptions`,
`internal/config/config.go`.

User-supplied auth Secrets (referenced by `spec.auth.secretName`) do NOT
carry the `managed-by=velkir` label and would therefore be
invisible to the cached client. The reconciler reads them through
`mgr.GetAPIReader()` (uncached), so users do not need to label their
Secrets and the cache's blast-radius narrowing for steady-state objects
remains intact — APIReader reads are per-call and do not populate the
informer cache. Source:
`internal/controller/valkey_controller.go::lookupAuthPassword`. Pinned by
`internal/controller/cache_filter_envtest_test.go`.

This is one of three independent layers (cache filter, dedicated
namespace, NetworkPolicy egress) that bound the blast radius of the
cluster-wide Secret READ. See [`deployment-posture.md`](deployment-posture.md)
for the full posture and a checklist for verifying it post-install.

## Regression discipline

The Secret WRITE split is currently maintained by reviewer discipline
on the kubebuilder marker block in `internal/controller/valkey_controller.go`
(the marker comments call out the contract inline). A custom analyzer
that fails CI on any `+kubebuilder:rbac:resources=...secrets...,verbs=...{create,update,patch,delete}...`
marker is a worthwhile follow-up once the lint plugin pattern
is reused for RBAC analysis.
