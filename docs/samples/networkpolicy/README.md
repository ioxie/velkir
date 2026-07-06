# NetworkPolicy samples

Samples for restricting traffic to and from a `Valkey` CR's pods. Apply
them yourself — the operator does NOT render NetworkPolicy:

- Many clusters use a CNI that doesn't enforce NetworkPolicy (e.g. k3s
  default Flannel). Shipping policies by default would create a false
  sense of security.
- Cluster components that legitimately need access (cluster-mesh
  controllers, debugging tools, custom monitoring) vary per
  installation; the operator can't predict which exemptions you need.

## What's here

- **`valkey-ingress.yaml`** — restricts ingress to the Valkey
  data-plane pods (port 6379) to: pods labelled `velkir.ioxie.dev/client:
  "true"`, sibling sentinel pods, replica peers, the operator, and a
  Prometheus pod scraping the redis_exporter sidecar.
- **`sentinel-ingress.yaml`** — restricts ingress to sentinel pods
  (port 26379) to clients, sibling sentinels, the operator, and
  Prometheus.
- **`egress.yaml`** — restricts egress from Valkey/sentinel pods to
  DNS, sibling pods, and HTTPS to the apiserver. Tighter than ingress
  because unconstrained egress lets a compromised container ship data
  off-cluster.
- **`operator-egress.yaml`** — restricts egress from the
  velkir pod itself: DNS, the apiserver, and managed Valkey
  + sentinel pods. The operator holds cluster-wide Secret READ; this
  policy is what stops an exfiltration path on a compromised operator
  process. Pair with the per-CR samples above. See
  [`docs/security/deployment-posture.md`](../../security/deployment-posture.md)
  for the full deployment posture this fits into.

Each sample names the CR `my-valkey` in the `default` namespace as
the example; replace both before applying.

## Adapting

The selectors use the labels the operator actually stamps on rendered
pods:

| Label | Value |
|---|---|
| `app.kubernetes.io/managed-by` | `velkir` |
| `velkir.ioxie.dev/cr` | the CR name |
| `velkir.ioxie.dev/component` | `valkey` or `sentinel` |

If you scope by namespace (e.g. multiple Valkey CRs in different
namespaces), add a `namespaceSelector` alongside the `podSelector`.

## CNI compatibility

NetworkPolicy is enforced by the CNI, not by Kubernetes itself.
Verify your CNI before relying on these samples:

- **Calico, Cilium, Antrea, Kube-router**: full enforcement.
- **Flannel** (k3s default): no enforcement. Pair with Calico or
  Cilium for policy.
- **Weave Net**: enforcement varies by version.

Apply a sample, exec into a pod that should be denied, and try
connecting — if the connection succeeds, your CNI isn't enforcing
the policy.
