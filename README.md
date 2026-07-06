# Velkir — a Kubernetes operator for Valkey®

Velkir reconciles Valkey + Sentinel HA clusters on Kubernetes. A single
`Valkey` custom resource (`velkir.ioxie.dev/v1beta1`) declares a deployment
in one of three modes — `standalone`, `replication`, or `sentinel` — and the
operator reconciles the underlying StatefulSets, Services, ConfigMaps,
PodDisruptionBudgets, and (for `sentinel`) a dedicated Sentinel tier.
Distributed as a Helm chart via OCI registry. Kubernetes floor: 1.30.

> Valkey® is a registered trademark of LF Projects, LLC. Velkir is an independent project. It is not affiliated with, endorsed by, or sponsored by the Valkey project, LF Projects, LLC, or the Linux Foundation. Use of the Valkey name is nominative — solely to identify the software that Velkir operates.

## Status

The CRD API and the operator advance on **decoupled SemVer axes**. The CRD is
`velkir.ioxie.dev/v1beta1` and additive-only — fields are added across
releases, never removed or narrowed — while the operator binary and its Helm
charts ship on an independent `v0.x` line. A new operator `v0.x` release does
not imply a CRD version bump, and the `v1beta1` contract holds steady across
operator releases. See [`docs/upgrade.md`](docs/upgrade.md) for the version
contract, [`docs/versions.md`](docs/versions.md) for the compatibility
matrix, and [`docs/FEATURE-GATES.md`](docs/FEATURE-GATES.md) for in-flight
features.

## Modes

| Mode | Replicas | Sentinel | Use case |
|---|---|---|---|
| `standalone` | exactly 1 | n/a | Single-node cache; ephemeral or PVC-backed; smallest blast radius. |
| `replication` | 2+ | none | Read-replica fan-out; manual failover via the operator's master-aware rollout. |
| `sentinel` | 2+ | 3+ (odd preferred) | Production HA. Sentinel tier + operator hybrid push/pull observation; split-brain guarded. |

Mode is set via `spec.mode` at CR creation. Per-mode invariants are
validated by the admission webhook — a `mode: sentinel` CR with
`spec.sentinel.replicas: 1` produces a soft-warn event but is
accepted; sub-1s `down-after-milliseconds` is rejected outright
unless the `velkir.ioxie.dev/allow-aggressive-timeouts=true` annotation
is explicitly set.

## Quickstart

Install the CRDs chart first, then the operator chart. The two ship
as independent OCI artifacts under the same registry path; the operator
chart watches for the CRDs at startup and crash-loops if they're missing.

```sh
helm upgrade --install velkir-crds \
  oci://ghcr.io/ioxie/velkir/charts/velkir-crds \
  --version 0.1.0 \
  --namespace velkir-system --create-namespace

helm upgrade --install velkir \
  oci://ghcr.io/ioxie/velkir/charts/velkir \
  --version 0.1.0 \
  --namespace velkir-system \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.prometheusRule.enabled=true \
  --set dashboards.enabled=true
```

For registry pull-secret setup, Flux `HelmRelease` GitOps deploy, and
Valkey CR samples, see [`docs/install.md`](docs/install.md).

Apply a minimal standalone CR:

```yaml
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: cache
  namespace: default
spec:
  mode: standalone
  valkey:
    replicas: 1
    storage:
      size: 1Gi
```

The operator stamps the StatefulSet, Service, PDB, ConfigMap, and
(if `metrics.enabled: true` on the CR) the redis_exporter sidecar.
Connect via `cache.default.svc.cluster.local:6379`.

## Documentation

- [`docs/install.md`](docs/install.md) — Helm install, Flux
  `HelmRelease`, registry pull-secret, SOPS-encrypted values.
- [`docs/upgrade.md`](docs/upgrade.md) — version compat, what triggers
  a rolling update, what stays in-place.
- [`docs/rollback.md`](docs/rollback.md) — rollback within
  `v1beta1` (additive-only).
- [`docs/migration.md`](docs/migration.md) — adopting an existing
  Valkey/Redis StatefulSet under operator management.
- [`docs/SECURITY.md`](docs/SECURITY.md) — reporting channel,
  supported versions, security model.
- [`docs/CHANGELOG.md`](docs/CHANGELOG.md) — release history.
- [`docs/FEATURE-GATES.md`](docs/FEATURE-GATES.md) — opt-in /
  opt-out feature flags + deprecation timelines.
- [`docs/versions.md`](docs/versions.md) — Valkey image
  compatibility matrix.
- [`docs/security/rbac-audit.md`](docs/security/rbac-audit.md) —
  every verb the operator holds + justification.
- [`docs/runbooks/`](docs/runbooks/) — operational runbooks
  (CRD additive-only lint).
- [`docs/samples/`](docs/samples/) — NetworkPolicy, monitoring,
  and other apply-yourself manifests.

## Discoverability

Velkir's tagline is *a Kubernetes operator for Valkey*. On GitHub it is
surfaced under the topics `valkey`, `kubernetes`, `kubernetes-operator`,
and `sentinel`.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
