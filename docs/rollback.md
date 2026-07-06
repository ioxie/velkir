# Rollback

Rollback semantics inside the `v1beta1` train, both for the
operator chart and for managed `Valkey` CRs.

## Operator chart rollback

```sh
helm rollback velkir --namespace velkir-system
```

The operator pod restarts on the previous image; the informer cache
rebuilds on startup; existing CRs are reconciled once the cache is
warm. RBAC, webhook configurations, ServiceMonitor, PrometheusRule,
and dashboard ConfigMaps roll back to their previous shapes.

**Safe under the additive-only contract** — because every release
adds CRD fields and never removes them, an older operator that
doesn't know about a newly-added field will simply ignore it on
reconcile (the apiserver retains the field on the object, but the
older reconciler doesn't act on it).

## Managed CR rollback

`v1beta1` is alpha (additive-only, not yet stable), so rolling
back a single CR's spec is a normal `kubectl apply` of the older
spec. Three considerations:

### 1. Mid-roll caught in flight

If the operator was rolling pods when the upgrade landed, the
rolled-back operator inherits that mid-roll state via
`status.conditions`. The state machine is idempotent — it picks
up at whatever pod-roll step the new operator left it in, and
finishes the roll under the OLD logic.

If you'd rather pause the in-flight roll, set:

```sh
kubectl annotate valkey -n <ns> <name> \
  velkir.ioxie.dev/paused=true --overwrite
```

The operator stops reconciling immediately (Phase 0 reads the
annotation; the in-flight roll halts at whatever pod boundary
the next reconcile hits). Remove the annotation to resume.

### 2. PVC retention

`spec.pvcRetentionPolicy: Retain` (default) means PVCs survive
CR deletion. A rollback that re-creates a previously-deleted CR
with the SAME name in the SAME namespace will adopt the existing
PVCs (matched by claim selector). Verify with:

```sh
kubectl -n <ns> get pvc -l app.kubernetes.io/managed-by=velkir,velkir.ioxie.dev/cr=<name>
```

If you renamed the CR during rollback, the orphaned PVCs need
manual cleanup or re-import via the
[`docs/migration.md`](migration.md) flow.

### 3. Sentinel state on rollback

Sentinel mode pods run with `sentinel.conf` on emptyDir, NOT PVC
— sentinel state is rebuildable from peers and is intentionally
not persisted. On pod restart, the operator issues `SENTINEL
RESET *` to clean up stale entries before the new sentinel reads
its config. A rolled-back operator does the same. The sentinel
quorum is reconstructed from the live cluster, not from disk —
so rolling back the CR spec doesn't risk a stale-quorum split.

## Forbidden rollback shapes

- **Mode change is one-way.** A CR upgraded from `replication` to
  `sentinel` cannot be rolled back to `replication` via spec edit
  — the validating webhook rejects the mode change. Recreate as a
  new CR if needed.
- **PVC shrink is rejected.** A rollback that lowers
  `spec.valkey.storage.size` is rejected by the validating webhook.
  PVs don't support shrink in Kubernetes; the older operator
  versions also reject this. Recreate the CR with the smaller size
  + `spec.pvcRetentionPolicy: Delete` if you need to shrink.
- **Auth removal mid-cluster.** Removing `spec.auth.existingSecret`
  from a CR that had auth requires a rolling restart of every pod
  to clear `requirepass` from running config. The operator does
  this automatically, but the window between secret-rotation and
  config-reload is when a rollback would corrupt sessions. Roll
  forward to a known-good auth state, then remove auth in a
  separate change.

## Verify after rollback

```sh
kubectl get valkey --all-namespaces
kubectl logs -n velkir-system deploy/velkir \
  --since=10m | grep -i "error\|warn"
```

Operator-self alerts should clear within 10 minutes:

- `ValkeyOperatorReconcileErrors` — the rolled-back operator
  shouldn't be hitting reconcile errors against unchanged CRs.
- `ValkeyPhaseDegraded` — should clear unless a managed CR was
  in `Degraded` before the rollback (in which case investigate the
  CR, not the operator).
- `ValkeyMasterAwareRolloutStuck` — if a roll was in flight, this
  may fire briefly while the new operator picks up the state and
  resumes. Should clear within one full roll cycle.

If anything stays red, file a `bug` issue with:

- `kubectl get valkey <name> -o yaml` (full spec + status).
- Operator logs since the rollback (`--since=...`).
- The `helm history` output for the operator release.

## v1.0 transition note

After the v1.0 cut, the CRD storage version migrates from
`v1beta1` to `v1` (or `v1beta1` if a beta is needed). At that
point a conversion-webhook handles spec round-tripping, and
rollback semantics extend to cross-version. The additive-only
contract becomes a hard guarantee; breaking changes route through
explicit conversion + a major-version bump only.
