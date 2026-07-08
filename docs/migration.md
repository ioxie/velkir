# Migration

How to adopt an existing Valkey or Redis StatefulSet (deployed
out-of-operator) under operator management. Vendor-agnostic;
the steps assume a generic StatefulSet + headless Service shape
common to most chart-deployed Redis/Valkey installations.

## When to migrate

You have an existing Valkey or Redis cluster deployed via a
chart (or hand-crafted YAML) and want the operator to take over
management — rolling updates, sentinel-aware failovers,
master-aware ordering, automated PVC retention policy, etc.

Migration is **optional**. The operator and out-of-operator
clusters can coexist in the same cluster; this guide covers the
case where you've decided to adopt.

## Pre-flight

Before touching the running cluster:

1. **Take a backup.** RDB snapshot if you can; verify the
   snapshot is restorable on a fresh pod. The migration is
   designed to be live-data-preserving but the safety net is
   a backup.
2. **Note the existing labels and selectors.** The operator's
   reconciler uses an informer cache filtered on
   `app.kubernetes.io/managed-by=velkir`. The existing
   resources don't carry this label — you'll re-label them as
   step 3 of the migration.
3. **Note the PVC names.** The operator's StatefulSet
   `volumeClaimTemplates` re-uses an existing PVC if the claim
   name matches the new StatefulSet's pod ordinal pattern.
   Verify the existing PVC names match the
   `<sts-name>-<volclaim>-<ordinal>` pattern before you adopt.
4. **Note the auth Secret reference.** The operator references
   auth via `spec.auth.existingSecret`, pointing at an existing
   `Secret` object. If your existing cluster uses a different
   auth-storage shape (e.g., env-var direct), create a Secret
   first and re-reference.

## The migration steps

### Step 1 — Install the operator chart

See [`docs/install.md`](install.md). The operator pod must be
running and reconciling before any adoption work begins.

### Step 2 — Stamp the managed-by label on the existing resources

The operator's informer cache filters on
`app.kubernetes.io/managed-by=velkir`. Without the label,
the operator can't see the resources to manage them.

Apply the label to:

- The existing `StatefulSet`.
- The existing `Service` (the Service that fronts the data
  plane on port 6379).
- The existing `ConfigMap` (if you have one carrying the
  Valkey/Redis config).
- The existing PVCs.

```sh
kubectl -n <ns> label statefulset <existing-sts> \
  app.kubernetes.io/managed-by=velkir \
  velkir.ioxie.dev/cr=<future-cr-name>

kubectl -n <ns> label svc <existing-svc> \
  app.kubernetes.io/managed-by=velkir \
  velkir.ioxie.dev/cr=<future-cr-name>

kubectl -n <ns> label configmap <existing-cm> \
  app.kubernetes.io/managed-by=velkir \
  velkir.ioxie.dev/cr=<future-cr-name>

# PVCs:
kubectl -n <ns> get pvc -l <your-existing-selector> \
  -o name | xargs -I{} kubectl -n <ns> label {} \
  app.kubernetes.io/managed-by=velkir \
  velkir.ioxie.dev/cr=<future-cr-name>
```

### Step 3 — Pause future operator action via annotation

To stage the CR creation safely, pause the operator before
applying the new CR:

```sh
# Apply this annotation on the Valkey CR YOU ARE ABOUT TO CREATE
# in step 4. The CR will be created with the annotation already
# set, so the operator's first reconcile is a no-op.
```

Add to the CR YAML:

```yaml
metadata:
  annotations:
    velkir.ioxie.dev/paused: "true"
```

### Step 4 — Apply the Valkey CR matching the existing shape

Author a `Valkey` CR whose computed StatefulSet name + Service
name + ConfigMap name MATCH the existing labels you applied in
step 2. The operator derives names from `metadata.name`; the
CR name and the existing resource names must align.

Example CR for a 3-replica replication-mode cluster:

```yaml
apiVersion: velkir.ioxie.dev/v1beta1
kind: Valkey
metadata:
  name: <future-cr-name>      # must match labels stamped in step 2
  namespace: <ns>
  annotations:
    velkir.ioxie.dev/paused: "true"   # operator no-op until removed
spec:
  mode: replication
  image:
    valkey:
      repository: valkey             # match existing image to avoid roll
      tag: "8.0"
  valkey:
    replicas: 3                      # must match existing replica count
    persistence:
      size: 10Gi                     # must match existing PVC size
      storageClass: <sc>             # must match existing
    resources:
      requests: { cpu: 200m, memory: 1Gi }
      limits:   { memory: 2Gi }
  pvcRetentionPolicy: Retain         # default; explicit for clarity
  auth:
    secretName: <secret-name>        # if your cluster uses auth
```

Apply with `kubectl apply -f` (or via your GitOps flow).

### Step 5 — Diff the operator's intended state vs. the live state

The operator's reconcile is paused (annotation), so nothing
happens yet. Use `helm template` or a similar tool to render
what the operator WOULD apply, and compare against the live
cluster. The match should be near-byte-identical for image,
replicas, and storage; the operator may want to add labels,
ServiceMonitor, and PDB if those weren't already there — those
are additive and safe.

### Step 6 — Unpause and observe

```sh
kubectl -n <ns> annotate valkey <future-cr-name> \
  velkir.ioxie.dev/paused- --overwrite
```

The operator reconciles. Watch for:

- `kubectl get valkey -n <ns> <name>` — phase should advance to
  `Available` within one or two reconcile passes.
- `kubectl describe valkey -n <ns> <name>` — events should
  show `BootstrapCompleted` (or skip straight to `ReconciledOK`
  on the second pass).
- `kubectl logs -n velkir-system deploy/velkir
  --since=5m` — no errors targeting your CR.

If the operator wants to roll any pod (e.g., your old pod spec
omits the readiness gate), it'll do so master-aware. The data is
preserved on the PVCs.

### Step 7 — Decommission the old install path

Once the operator owns the cluster:

- Stop applying the old Helm chart / hand-crafted YAML — any
  re-apply would conflict with the operator's owner-reference
  reconciliation.
- Delete the old Helm release without `--purge` if it still
  references the same objects: `helm uninstall <old-release> --no-hooks`.
  (Helm's deletion only acts on objects with the `meta.helm.sh/`
  annotations; the operator-managed objects no longer have those.)
- Verify the operator continues reconciling cleanly for at
  least a full reconcile cycle.

## Rollback

If the migration goes wrong:

1. Re-pause the operator (`velkir.ioxie.dev/paused: "true"`).
2. Strip the operator's labels off the resources you re-labeled
   in step 2:
   ```sh
   kubectl -n <ns> label statefulset <name> \
     app.kubernetes.io/managed-by- velkir.ioxie.dev/cr-
   ```
3. Re-apply your old chart / YAML on top.

Because PVCs are preserved (default `pvcRetentionPolicy: Retain`),
data is intact across rollback.

## Common gotchas

- **Existing `app.kubernetes.io/managed-by` set to a different
  value** (most chart-deployed clusters set this to `Helm` or the
  release name). The operator's informer ONLY sees objects with
  the value `velkir`. You're overwriting the existing
  label on purpose; if you have CI / GitOps that re-applies the
  old label, disable that reconciler first.
- **PVC selector mismatch**. If the existing PVCs are named
  `data-<sts>-0`, `data-<sts>-1`, ... and the operator's
  rendered StatefulSet uses `<volclaim>-<sts>-<ordinal>` with
  a different `<volclaim>` name, the new StatefulSet creates new
  PVCs and the data on the old ones is orphaned. Stop, restore
  from backup, and re-author the CR with the matching
  `volumeClaimTemplates[0].metadata.name`.
- **Stale Sentinel state**. If you're migrating a sentinel-mode
  cluster, the operator issues `SENTINEL RESET *` on the first
  reconcile to clean up stale entries from the existing sentinel
  pods. The reset is data-plane-safe but the sentinel quorum is
  briefly inconsistent (~30s); plan a low-traffic window for
  the unpause.
- **Auth secret renamed**. If your old cluster references an auth
  Secret under one name and your CR references it under another,
  the operator emits `AuthSecretNotFound` and the cluster goes
  `Degraded`. Re-create the Secret under the CR-referenced name
  before unpausing.
