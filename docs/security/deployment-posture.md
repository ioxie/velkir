# Deployment posture

How to deploy the operator so the cluster-wide RBAC surface in
[`rbac-audit.md`](rbac-audit.md) translates into the smallest
possible blast radius. The chart's defaults already cover most of
this; this doc names the few choices an operator-of-the-operator
must make at install time.

The threat the posture defends against: an attacker who gains code
execution inside the operator pod (supply-chain compromise on a
dependency, RCE on the controller manager, etc.) and tries to use
the cluster-wide Secret READ grant to exfiltrate auth material from
unrelated namespaces.

## Defense in depth — three independent layers

The operator's cluster-wide Secret READ is necessary (CRs and their
auth Secrets live in arbitrary namespaces — see the rationale in
[`rbac-audit.md`](rbac-audit.md)). The total blast radius is bounded
by three independent layers, any one of which limits damage even if
the others fail.

### 1. Informer cache label selector

The manager's informer cache is configured with
`cache.ByObject{Label: app.kubernetes.io/managed-by=velkir}`
for every owned type, including `Secret`. The list-watch the cache
sends to the apiserver carries that label selector, so the
*cache populates only with operator-managed objects* — unlabeled
cluster state isn't kept resident in the operator process via this
path.

Source: `cmd/main.go::buildCacheOptions` and the
`cacheManagedBySelector` config field. Default selector value:
`app.kubernetes.io/managed-by=velkir`.

This is the load-bearing scope-narrowing for steady-state cache
contents. Even though the RBAC grant is cluster-wide, the cache's
materialized state is limited to operator-managed objects, which
sharply reduces the data-set available to a process-level
compromise.

**User-supplied auth Secrets are read via APIReader (uncached).**
Secrets referenced by `spec.auth.secretName` are authored by users
and do NOT carry the `managed-by=velkir` label, so the
informer cache's label-narrowed list-watch never picks them up. The
reconciler reads them through `mgr.GetAPIReader()` — a per-call
direct read against the apiserver — instead of the cached client.
The blast-radius argument still holds: APIReader reads don't
populate the in-memory informer cache, so a process compromise sees
only the auth Secrets actively in flight (one per reconcile), not a
resident set of every Secret ever read. Source:
`internal/controller/valkey_controller.go::lookupAuthPassword` and
`SentinelStartupReset.lookupAuthPassword`. Pinned by the envtest in
`internal/controller/cache_filter_envtest_test.go`.

### 2. Dedicated namespace

Run the operator in its own namespace (the chart default
`Release.Namespace` works — most installations land it in
`velkir-system`, not `default`). Two things follow:

- The operator's namespaced Role (TLS Secret writes for the
  webhook serving cert) cannot reach Secrets in any tenant
  namespace.
- The Pod Security Admission level on the operator namespace
  (the chart defaults to PSA-`restricted`-compatible specs) is
  decoupled from tenant-workload PSA levels.

Avoid co-locating the operator with high-value workloads. A
compromise of *any* pod in the namespace is one `kubectl exec`
away from the operator's ServiceAccount token (mounted at
`/var/run/secrets/kubernetes.io/serviceaccount`).

### 3. NetworkPolicy egress isolation

A compromised operator pod with intact RBAC is harmless if it
cannot reach external endpoints. Apply a NetworkPolicy that
restricts the operator's egress to:

- DNS (kube-system, UDP/TCP 53)
- The apiserver (cluster-internal IPs, TCP 443 / 6443)
- Managed Valkey + sentinel pods (TCP 6379, 26379) — required for
  failover commands, lag reads, and config reconciliation

Sample at
[`docs/samples/networkpolicy/operator-egress.yaml`](../samples/networkpolicy/operator-egress.yaml).
Adapt the apiserver block to your cluster (kubeadm, RKE2, k3s, EKS
all expose the apiserver on slightly different paths).

The egress policy is the layer that turns "the operator can read
any Secret in the cluster" into "the operator can read any Secret
in the cluster, but it cannot send the contents anywhere except
back to the apiserver." Without it, an attacker who triggers a
log-exfiltration path (e.g. an error message containing Secret
material — see the redaction notes in [`SECURITY.md`](../SECURITY.md))
can ship that data off-cluster.

## What the chart does and does NOT do

| Layer | Default | Override |
|---|---|---|
| Cache label selector | On — `app.kubernetes.io/managed-by=velkir` | `--config` → `cacheManagedBySelector` (do not unset; rejected at startup) |
| Dedicated namespace | Implied by chart `Release.Namespace` — but YOU choose where to install | `helm install -n <ns> --create-namespace` |
| NetworkPolicy egress | **Not rendered by the chart** — applied separately | Apply [`operator-egress.yaml`](../samples/networkpolicy/operator-egress.yaml) |

The NetworkPolicy is a sample, not a default, for the same reason
the per-CR samples in [`docs/samples/networkpolicy/`](../samples/networkpolicy/)
are samples: cluster CNI variation. Some CNIs don't enforce
NetworkPolicy at all (default Flannel on k3s); others enforce it
fully (Calico, Cilium, Antrea). Shipping a default policy on a
non-enforcing CNI creates a false sense of security; shipping it
on an enforcing CNI without knowing the cluster's apiserver IP
range bricks the operator.

## Verifying the posture in your install

A quick checklist after `helm install`:

```sh
# 1. Confirm the namespace is dedicated
kubectl get pods -n <operator-ns> -L app.kubernetes.io/name
# Only velkir pods should appear.

# 2. Confirm the cache selector is the default
kubectl -n <operator-ns> exec deploy/<operator-name> -- \
  cat /etc/velkir/config.yaml | grep cacheManagedBySelector
# Should print: cacheManagedBySelector: app.kubernetes.io/managed-by=velkir
# (or be unset, which means "use the default").

# 3. Confirm NetworkPolicy egress is applied
kubectl -n <operator-ns> get networkpolicy
# Should list the operator-egress policy if you applied the sample.

# 4. Confirm the CNI enforces it
kubectl -n <operator-ns> exec deploy/<operator-name> -- \
  curl -sS --max-time 3 https://example.com/ ; echo "exit=$?"
# Should fail / time out if egress restriction is enforced.
```
