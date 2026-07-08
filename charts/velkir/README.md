# velkir

A Kubernetes operator for [Valkey](https://valkey.io), managed through a single
`Valkey` custom resource (`velkir.ioxie.dev/v1beta1`). One CRD, mode-switched
across **standalone**, **replication**, and **Sentinel-backed high-availability**
topologies.

Highlights:

- **Sentinel automated failover with split-brain guards** — hybrid push/pull
  quorum observation, triple-signal failover agreement, and a durable fencing
  marker so an operator crash mid-election can't re-stamp a stale primary.
- **Master-aware rolling updates** — replicas first, primary last, each step
  gated on a healthy rejoin; aborts cleanly to Degraded on quorum loss.
- **Write-loss protection** — `min-replicas-to-write` floors so a partitioned
  primary stops accepting writes it can't replicate.
- **Persistent storage** with retention policy (finalizer-backed) and
  operator-driven in-place PVC resize.
- **Zero-downtime auth rotation**, replication-lag readiness gating, an
  operator metrics/alerts/dashboards pack, and validating/defaulting webhooks
  that enforce the load-bearing HA invariants.

## Install

Install the CRDs chart **first**, then the operator (both are public OCI
artifacts on GHCR):

```sh
helm upgrade --install velkir-crds \
  oci://ghcr.io/ioxie/velkir/charts/velkir-crds --version 0.1.1

helm upgrade --install velkir \
  oci://ghcr.io/ioxie/velkir/charts/velkir --version 0.1.1 \
  --namespace velkir-system --create-namespace
```

Then create a `Valkey` resource — see the
[installation guide](https://github.com/ioxie/velkir/blob/main/docs/install.md).

The operator image and both charts are signed with cosign; see the
[install docs](https://github.com/ioxie/velkir/blob/main/docs/install.md) for
verification.

---

> Velkir is an independent project. It is not affiliated with, endorsed by, or
> sponsored by the Valkey project, LF Projects, LLC, or the Linux Foundation.
> See [TRADEMARKS.md](https://github.com/ioxie/velkir/blob/main/TRADEMARKS.md).
