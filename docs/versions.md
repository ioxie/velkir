# Versions

The operator pins one tested Valkey image tag per release. The
controller-runtime / kubebuilder / Go floor is bumped only when a
new feature requires it.

## Compatibility matrix

| Operator chart | Operator binary (appVersion) | Valkey image tag | Sentinel image tag | Kubernetes floor | controller-runtime |
|---|---|---|---|---|---|
| `0.1.x` | `0.1.x` | `valkey:8.x` (GA-tested) **or** `valkey:9.x` (admitted, best-effort) | (uses Valkey image) | `1.30+` | `v0.23.3` |

The "Sentinel image" column is empty because Valkey ships sentinel in
the same binary; the operator just runs the Valkey image with
`--sentinel` and a sentinel-mode config. Future Valkey major
versions may split this out — the table will gain a column then.

### Supported-major policy

The validating webhook admits any image whose tag parses to a major in
`{8, 9}` (source: `internal/version.SupportedMajors`). Runtime
reconciliation does NOT branch on Valkey major — the operator's state
machines (rollout, failover, sentinel observation, auth rotation)
treat 8.x and 9.x identically; only the wire-protocol shape Valkey
exposes matters, and 8.x and 9.x are wire-compatible at the
operator's call surface (`SENTINEL`, `INFO replication`,
`CONFIG GET/SET`, `AUTH`).

`8.x` carries full e2e + upgrade-matrix coverage. `9.x` is admitted
but the e2e cohort has not added a 9.x row yet — it's best-effort
during the `v0.x` alpha train. Operators-of-the-operator running
9.x in production should monitor closely and report regressions;
a follow-up will add 9.x to the upgrade matrix once the
8.x → 9.x major-upgrade path is hardened.

## Image tag pinning

The chart's `image.tag` value defaults to empty so the chart falls
back on `Chart.appVersion`. Operators-of-the-operator can override
to a specific tag (e.g., for a CVE rebuild).

For the data-plane image, the CR's `spec.valkey.image` defaults to
the operator's tested tag at the operator's appVersion. Setting it
to a custom registry path (private mirror, custom build) is
allowed — the validating webhook checks against an image banlist
but doesn't restrict the registry beyond that.

## Why we pin

- **Reproducibility**: same operator version = same data-plane
  image, regardless of when the chart is installed.
- **Test surface**: every PR's e2e job runs against the pinned
  Valkey image. A floating tag would make CI non-reproducible.
- **CVE response**: when Valkey ships a CVE fix, we bump the
  pinned tag in a dedicated chart minor (e.g., `0.1.0 → 0.1.1`)
  and the operator-of-the-operator gets the patched image with a
  `helm upgrade`.

## Skip-version policy

See [`docs/upgrade.md`](upgrade.md) for the full version contract.
Short form: one-minor-skip is supported; two-or-more-minor skips
are best-effort during alpha and validated in the upgrade
matrix only on the current-1 → current path.

## Future axes

The matrix will gain columns as features land:

- **Cluster variants**: RKE2, k3s, openebs-hostpath StorageClass
  fixture (the cluster-variant matrix).
- **OS variants**: alpine vs debian-slim base images (when we
  add an alpine variant for size-constrained edge clusters).
- **Architecture**: amd64 today; arm64 lands when the build runners
  support it.

## How to read this in CI

`docs/versions.md` is consumed by:

- The quorum-preflight check, which warns when a CR pins a
  Valkey image not in the tested set.
- The upgrade-e2e matrix, which uses the table to
  parameterize current-1 → current image upgrade tests.

Keep the table machine-readable (markdown table, fixed column
order) so future automation can parse it without a brittle
regex.
