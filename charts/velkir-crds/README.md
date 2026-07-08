# velkir-crds

The CustomResourceDefinitions for [velkir](https://github.com/ioxie/velkir), the
Kubernetes operator for Valkey. Ships two CRDs:

- `valkeys.velkir.ioxie.dev` — the `Valkey` resource (standalone, replication,
  or sentinel mode).
- `sentinelquorums.velkir.ioxie.dev` — the `SentinelQuorum` helper, populated by
  sentinel pods.

## Install this chart first

The operator chart (`velkir`) crash-loops without the CRDs, so install this one
**before** it:

```sh
helm upgrade --install velkir-crds \
  oci://ghcr.io/ioxie/velkir/charts/velkir-crds --version 0.1.1

helm upgrade --install velkir \
  oci://ghcr.io/ioxie/velkir/charts/velkir --version 0.1.1 \
  --namespace velkir-system --create-namespace
```

The CRDs carry `helm.sh/resource-policy: keep`, so `helm uninstall` leaves them
in place — delete them explicitly if you intend to remove every `Valkey` and
`SentinelQuorum` in the cluster.

---

> Velkir is an independent project. It is not affiliated with, endorsed by, or
> sponsored by the Valkey project, LF Projects, LLC, or the Linux Foundation.
> See [TRADEMARKS.md](https://github.com/ioxie/velkir/blob/main/TRADEMARKS.md).
