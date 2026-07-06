# Upgrade

How to upgrade the operator chart safely, what triggers a managed
data-plane roll, and the version contract.

## Version contract

The chart version, the operator app version, and the CRD `v1beta1`
schema are decoupled axes that evolve independently:

| Artifact | Version axis | Bumped when |
|---|---|---|
| Chart `version` | semver | Any chart-template / values change. |
| Chart `appVersion` | semver | Operator binary released. |
| CRD storage version | `v1beta1` | Single served + stored version; additive-only, no conversion webhook. |
| Valkey image | upstream | Operator pins a single tested tag per release; see [`docs/versions.md`](versions.md). |

**Additive-only contract on `v1beta1`** — the CRD schema gains
fields over time, but no field is ever removed, narrowed, or
made required after first release. `v1beta1` is the single served
and stored version: there is no conversion webhook, and the CRD
schema versions independently of the operator's `v0.x` semver.

A CI gate (`crd-additive`) diffs every PR's `api/v1beta1/types.go`
against `main` and fails on any field removal, type narrowing, or
new-required-field. See
[`docs/runbooks/crd-additive-lint.md`](runbooks/crd-additive-lint.md)
for the override flow on the rare deliberate-break case.

## What triggers a roll on managed pods

The operator drives a master-aware rolling update on the underlying
StatefulSets when ANY of these change on a `Valkey` CR:

- `spec.valkey.image` or `spec.sentinel.image`
- `spec.valkey.resources` / `spec.sentinel.resources`
- `spec.valkey.configuration` or `spec.valkey.configurationOverrides`
- `spec.valkey.podSecurityContext` or `spec.valkey.securityContext`
- `spec.valkey.podTemplate.metadata.annotations.velkir.ioxie.dev/restart`
  (escape-hatch for forced restart without spec change)
- A change in the rendered ConfigMap content (cascades to a pod
  template hash bump)

Roll order: replicas first (oldest replica → newest), then primary
(after all replicas are healthy AND the readiness gate clears).
The master-aware rollout drives the full state machine.

## What does NOT trigger a roll

- Changing `spec.metrics.enabled` (sidecar add/remove via
  StatefulSet update without pod-template churn).
- Changing `spec.pvcRetentionPolicy` (annotation flip on existing
  PVCs; no pod restart).
- Operator chart upgrades that don't touch the operator image
  (e.g., shipping a new dashboard JSON, tightening a NetworkPolicy
  sample). Managed pods are unaffected.

## Upgrade walkthrough

### 1. Read the changelog

[`docs/CHANGELOG.md`](CHANGELOG.md) names every CRD schema change,
metric addition, alert addition, and behavior change. Note any
fields you're not currently using that became defaultable — the
defaulter will stamp them on next apply.

### 2. Pre-flight check

Run `helm diff upgrade` (with the
[helm-diff plugin](https://github.com/databus23/helm-diff)) to see
the chart-side delta:

```sh
helm diff upgrade velkir \
  oci://ghcr.io/ioxie/velkir/charts/velkir \
  --version <new-version> \
  --namespace velkir-system \
  -f your-values.yaml
```

Look for: ClusterRole verb additions, webhook configuration
changes (especially `failurePolicy` flips), and pod spec
`securityContext` changes (PSA-restricted compatibility).

### 3. Upgrade the operator chart

```sh
helm upgrade velkir \
  oci://ghcr.io/ioxie/velkir/charts/velkir \
  --version <new-version> \
  --namespace velkir-system \
  -f your-values.yaml
```

Watch the operator pod restart. Its readiness probe becomes ready
when the manager has rebuilt its informer cache + reconciled the
existing CRs once.

### 4. Verify managed CRs reconcile cleanly

```sh
kubectl get valkey --all-namespaces
kubectl describe valkey -n <ns> <name>
```

Look for:
- `status.phase` is `Ready` (or `Available`, depending on stage).
- No new `Warning` events under `kubectl describe`.
- Reconcile loop on each CR completes within ~2s
  (`valkey_reconciliation_duration_seconds` p99 stays under the
  alert threshold).

### 5. Roll managed pods (optional, on schedule)

If the upgrade includes new defaulter stamps or a CRD field you
want to opt into, edit each CR explicitly. The defaulter only fills
omitted fields on `apply`, not on existing objects — re-apply
whichever CRs you want to migrate.

For a forced no-spec-change restart:

```sh
kubectl annotate valkey -n <ns> <name> \
  velkir.ioxie.dev/restart="$(date +%s)" --overwrite
```

The operator picks this up and rolls per the master-aware order.

## Skip-version policy

- One-minor-skip is supported (e.g., 0.1.x → 0.3.x) inside the
  alpha train.
- Two-or-more-minor skip lands without a hard CI block but is
  validated in the upgrade-e2e matrix only on the
  current-1 → current path. Skipping further is on the operator;
  read the changelogs in between.
- The CRD is a single `v1beta1` version with no conversion
  webhook, so skip policy is purely an operator/chart-version
  concern — the additive-only contract above keeps the schema
  compatible across the `v0.x` train.

## Rollback

If an upgrade misbehaves:

```sh
helm rollback velkir --namespace velkir-system
```

The operator informer cache rebuilds on restart. The data-plane
StatefulSets are untouched by `helm rollback` itself; if a recent
roll left a managed CR mid-roll, the rolled-back operator picks
up where it left off. See [`docs/rollback.md`](rollback.md) for
the data-plane considerations.
