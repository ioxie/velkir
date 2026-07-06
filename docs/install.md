# Installation

The operator ships as **two** Helm charts, published as OCI artifacts
to the public GitHub Container Registry (GHCR) and released
independently:

- `velkir-crds` — `Valkey` and `SentinelQuorum` CRDs.
  Install first; lives on its own release lifecycle so CRD-only
  changes do not force a controller redeploy.
- `velkir` — the controller Deployment, webhooks, RBAC,
  Service, optional ServiceMonitor / PrometheusRule / Grafana
  dashboards, and optional NetworkPolicy. Depends on the CRDs
  being present at startup; crash-loops otherwise.

Both charts and the operator image (`ghcr.io/ioxie/velkir/manager`)
are **public**: pulls are anonymous, so no registry login, pull
secret, or `imagePullSecrets` configuration is needed.

This doc covers two install paths: `helm install` directly, and Flux
`HelmRelease` for GitOps-managed clusters.

## Prerequisites

- Kubernetes 1.30+ (the floor — the operator uses 1.30 API shapes).
- A Helm 3.8+ client (OCI registry support is GA from 3.8).
- For metrics: a Prometheus stack reachable via ServiceMonitor /
  PrometheusRule discovery (kube-prometheus-stack is the
  reference shape).
- For dashboards: a Grafana sidecar provisioner watching for
  ConfigMaps with the `grafana_dashboard: "1"` label
  (overridable; see [`charts/velkir/values.yaml`](../charts/velkir/values.yaml)).

## helm install (direct)

Install the CRDs chart first, then the operator chart. Always pass an
explicit `--version` — OCI installs should pin the chart release you
intend to run, and the two charts version independently (per-chart
versions may diverge between coordinated cuts).

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

Verify the operator pod, its admission webhooks, and the cert
authority Secret:

```sh
kubectl -n velkir-system get pods,svc,mutatingwebhookconfigurations,validatingwebhookconfigurations
kubectl -n velkir-system get secret velkir-webhook-cert -o jsonpath='{.metadata.creationTimestamp}'
```

## Verify the chart signature (optional)

Every published chart and image is signed with cosign **keyless**
(Sigstore) — the signing identity is the release workflow's GitHub
Actions OIDC identity, recorded in the Rekor public transparency log:

```sh
cosign verify \
  --certificate-identity-regexp '^https://github.com/ioxie/velkir/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/ioxie/velkir/charts/velkir:0.1.0
```

The same command verifies `velkir-crds` and the
`ghcr.io/ioxie/velkir/manager` image. See
[`docs/SECURITY.md`](SECURITY.md) for the SBOM and build-provenance
attestations.

## Flux HelmRelease

For Flux-managed clusters, the canonical pattern is **two
`HelmRelease` objects sharing one `HelmRepository`**, with the
operator release `dependsOn` the CRDs release. Flux serializes the
reconcile so the operator chart never installs against a cluster
that's still missing CRDs. No `secretRef` is needed — the registry
serves anonymous pulls.

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: velkir
  namespace: flux-system
spec:
  # Both charts are sibling OCI artifacts under the same registry
  # path; one HelmRepository serves both.
  type: oci
  url: oci://ghcr.io/ioxie/velkir/charts
  interval: 30m
---
# CRDs first. Bump version independently of the operator chart when
# CRD-only changes ship; lock-step otherwise. `helm.sh/resource-policy: keep`
# annotations on each CRD template mean `helm uninstall` does NOT
# remove CRDs — delete them explicitly when retiring.
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: velkir-crds
  namespace: velkir-system
spec:
  interval: 15m
  releaseName: velkir-crds
  chart:
    spec:
      chart: velkir-crds
      version: "0.1.0"
      sourceRef:
        kind: HelmRepository
        name: velkir
        namespace: flux-system
  install:
    createNamespace: true
---
# Operator after CRDs. `dependsOn` makes Flux wait until the CRDs
# release is Ready before starting this one. Without `dependsOn`,
# both reconciles race and the operator chart can land on a
# cluster that's still missing CRDs (admission-webhook install
# fails, controller crash-loops).
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: velkir
  namespace: velkir-system
spec:
  interval: 15m
  releaseName: velkir
  dependsOn:
    - name: velkir-crds
  chart:
    spec:
      chart: velkir
      version: "0.1.0"
      sourceRef:
        kind: HelmRepository
        name: velkir
        namespace: flux-system
  install:
    createNamespace: true
  values:
    metrics:
      serviceMonitor:
        enabled: true
      prometheusRule:
        enabled: true
    dashboards:
      enabled: true
```

Environment-specific overrides (resource limits, additionalLabels,
etc.) go in the `values:` block, or in a ConfigMap/Secret referenced
via `valuesFrom` — see the Flux docs for the `valuesFrom` shape.

## Verify

After install, the operator should:

1. Run as a single replica (`replicas: 1` default). See
   [Leader election](#leader-election) for the single- vs multi-replica
   lease-timing trade-off.
2. Serve `/metrics` over HTTPS on the operator pod's port 8443.
3. Stamp the operator-self ServiceMonitor (`metrics.serviceMonitor.enabled=true`),
   PrometheusRule (`metrics.prometheusRule.enabled=true`), Grafana dashboards
   ConfigMaps (`dashboards.enabled=true`), and NetworkPolicy
   (`networkPolicy.enabled=true`, the default) per toggle.
4. Provision its own webhook serving cert via the in-process
   dynauth Authority (default; hand the cert lifecycle to an external
   cert-manager via
   `--set webhook.certManager.enabled=true,webhook.selfSigned.enabled=false`).

Apply the sample CR from
[`docs/samples/valkey-replication.yaml`](samples/valkey-replication.yaml)
to smoke-test the reconcile path.

## Values pointers

The full values surface lives in
[`charts/velkir/values.yaml`](../charts/velkir/values.yaml) (each key
is schema-validated via `values.schema.json`). Commonly tuned:

- `image.repository` / `image.tag` / `image.digest` — defaults to
  `ghcr.io/ioxie/velkir/manager` at the chart's `appVersion`; set
  `image.digest` to pull by digest for image-policy verification.
- `metrics.serviceMonitor.*` / `metrics.prometheusRule.*` — opt-in
  Prometheus integration.
- `dashboards.*` — opt-in Grafana dashboard ConfigMaps (sidecar
  label/value, target namespace, folder annotation).
- `networkPolicy.*` — default-on operator-self NetworkPolicy;
  `apiServerPorts`, `webhookFrom`, `metricsFrom` narrow it.
- `webhook.certManager.enabled` / `webhook.selfSigned.enabled` —
  exactly one must be true (cert source for the admission webhook).
- `watchNamespaces` — confine the operator to listed namespaces;
  empty (default) means cluster-scoped.
- `resources`, `nodeSelector`, `tolerations`, `affinity`,
  `topologySpreadConstraints`, `priorityClassName` — standard
  scheduling/limits knobs.

The CRDs chart exposes only `crds.create` and `crds.keep`
([`charts/velkir-crds/values.yaml`](../charts/velkir-crds/values.yaml)).

## Leader election

The operator runs a single replica by default but still takes a
leader-election lease, so a misconfigured second replica can't
double-reconcile. The lease timing is tunable via `leaderElection.*`
chart values (rendered into the mounted operator config):

| Value | Default | Meaning |
|---|---|---|
| `leaseDuration` | `30s` | How long a lease is held before a standby may claim it. |
| `renewDeadline` | `20s` | How long the leader keeps trying to renew before giving up the lease and exiting. |
| `retryPeriod` | `4s` | Interval between renew/acquire attempts. |
| `releaseOnCancel` | `true` | Release the lease on graceful shutdown so a standby takes over in <1s. |

The defaults are **latency-tolerant**: if the API server is briefly
slow, the leader has a wide renew window before it loses the lease and
restarts. On a single replica that restart is pure reconciliation
downtime — there is no standby to fail over to — so tolerance beats fast
handoff.

Running **multiple replicas**? Tighten `renewDeadline` (and
`leaseDuration`) so a dead leader's standby takes over sooner — the
faster handoff is worth the reduced latency-tolerance once a standby
exists. The invariant `retryPeriod < renewDeadline < leaseDuration` is
enforced at startup; an invalid combination fails the pod fast rather
than being silently ignored.

## Upgrade

Upgrade by re-running `helm upgrade` with the new explicit
`--version`, CRDs chart first (CRD schema additions ship there), then
the operator chart:

```sh
helm upgrade velkir-crds \
  oci://ghcr.io/ioxie/velkir/charts/velkir-crds \
  --version <new-version> --namespace velkir-system

helm upgrade velkir \
  oci://ghcr.io/ioxie/velkir/charts/velkir \
  --version <new-version> --namespace velkir-system --reuse-values
```

The CRD schema is additive-only, so a CRDs-chart upgrade never
invalidates existing CRs. See [`docs/upgrade.md`](upgrade.md) for the
version contract, what triggers a roll on managed data-plane pods, and
the full upgrade walkthrough; [`docs/rollback.md`](rollback.md) covers
rolling back.

## Uninstall

Delete CRs first so the operator's finalizers run (clean up
StatefulSets, Services, PDBs, PVCs per each CR's
`spec.pvcRetentionPolicy`). Then uninstall the two Helm releases in
reverse install order (operator first, CRDs second).

```sh
kubectl delete valkey --all --all-namespaces
helm uninstall velkir      --namespace velkir-system
helm uninstall velkir-crds --namespace velkir-system
kubectl delete namespace velkir-system
```

CRDs survive `helm uninstall velkir-crds` — the CRDs chart
stamps `helm.sh/resource-policy: keep` on each CRD so user data
(Valkey custom resources, even pre-deletion) is not destroyed by a
chart removal. Remove explicitly when fully retiring the operator:

```sh
kubectl delete crd valkeys.velkir.ioxie.dev sentinelquorums.velkir.ioxie.dev
```

`spec.pvcRetentionPolicy` on each `Valkey` CR controls whether PVCs
are retained or deleted on CR deletion (default `Retain`).
