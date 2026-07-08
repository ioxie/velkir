# Audit-log integrity

The operator emits structured audit-log lines for privileged
actions (annotation-driven pause / accept-pvc-loss /
allow-aggressive-timeouts admission paths, sentinel failover,
sentinel reset, auth-Secret rotation, etc.). The full schema and
the canonical event catalog live in `internal/audit/log.go`.

This doc is about what the operator's audit log **is** and **is
not** as a compliance artifact, and the deployment assumptions
the threat model relies on.

## Threat model

The audit stream is a **detective control**: it records what the
operator did, so an investigator reconstructing an incident can
correlate operator-side actions with the user-side admission
decisions that triggered them. It is not a non-repudiation
control on its own.

Two threats the audit stream defends against:

1. **"What did the operator do at time T?"** — the standard
   incident-response question. Stream consumers grep on
   `event=<name>` and `cr=<ns>/<name>` to reconstruct the chain.
2. **"Was a privileged annotation honoured silently?"** — every
   annotation-driven escape (pause, accept-pvc-loss,
   allow-aggressive-timeouts, force-scale) emits its own audit
   event with the requesting user's identity (from the admission
   webhook's `userInfo`).

Two threats the audit stream does **not** defend against on its
own:

1. **Audit-trail forgery by a downstream log forwarder.** The
   operator emits to its stdout via the controller-runtime
   `logr.Logger`. Anything with write access to the resulting
   log stream (the node's container runtime spool, a Fluent Bit /
   Vector sidecar, the cluster's log aggregation backend) can in
   principle inject fabricated `event=...` lines indistinguishable
   from genuine emissions. The operator does **not** sign or HMAC
   each line.
2. **Repudiation of a recorded action.** An operator-of-the-
   operator who controls the log pipeline can argue a recorded
   event was injected after the fact. There is no per-line
   integrity proof tying the emission to the running operator
   process.

## The assumption: write-once log shipper + apiserver cross-check

The audit stream's integrity rests on two operational
assumptions, both controlled by the deployer (the
operator-of-the-operator), not the operator code:

### 1. Write-once log shipper

The cluster's log pipeline is configured so that operator log
lines, once shipped off the operator pod, land in a store that
denies retroactive edit and delete. Common shapes:

- **Object storage with object-lock / WORM** — Loki or any
  log shipper backed by S3 with Object Lock (compliance retention
  mode), GCS bucket-level retention, MinIO retention. The lock is
  enforced at the storage layer, not the log shipper, so even a
  compromise of the shipper cannot rewrite frozen objects.
- **Append-only log indexers** — Splunk with frozen indexes
  AND ingest-only role permissions on the indexer (i.e., the
  shipper SA can write but not edit/delete; deletion happens only
  via the index lifecycle, not on demand). The default
  `frozenTimePeriodInSecs` setting is a *retention* control, not
  a WORM lock — it ages data out, but does not prevent in-place
  edit by any role with delete/update capability. WORM behaviour
  requires both the freeze policy AND a separate role-based
  block on edit/delete.
- **An on-prem ELK cluster with index-level write-only API
  permissions** scoped to the log-shipper SA only, plus index
  lifecycle policies that lock written segments.

The common shape is "writes are accepted, edits and deletes are
not — and the constraint is enforced by the storage layer, not
the shipper". Configurations where the shipper itself enforces
retention (with no storage-layer lock behind it) collapse the
moment the shipper is compromised.

What this gets you: an attacker who *later* compromises the
cluster cannot rewrite history, only stop emitting going
forward. The gap between "moment of compromise" and "incident
response notices the gap" is bounded by the cadence of off-
cluster log-volume checks.

What this does **not** get you: protection against an attacker
who is already running attacker-controlled code inside the
operator pod or the log-shipper sidecar at the moment of emission
(supply-chain compromise on the Fluent Bit / Vector image, RCE
inside the operator container). At that point the attacker can
forge audit lines or suppress them before they leave the pod,
and the storage-layer lock only preserves the forgeries. For
that threat, see "When you need stronger guarantees" below.

### 2. Cross-check against apiserver audit

Every annotation-driven privileged action that emits an audit
event is also visible in the apiserver's own audit log: the
`update` / `patch` admission request that set the annotation,
with `userInfo.username` and the request body. A forged
`event=manual_rollout_triggered cr=ns/name requestor=alice`
in the operator stream that has no matching apiserver-audit
`patch` request from `alice` on the same CR is the smoking gun.

Concretely, for a compliance review:

- The operator audit stream is the **detailed "why"** — what the
  operator did and which annotation/event drove it.
- The apiserver audit log is the **authoritative "who"** — who
  made the admission request that the operator reacted to.
- Investigators correlate the two via `cr=<ns>/<name>` plus a
  bounded time window.

This means the apiserver audit log is on the "must be enabled and
shipped" side of the deployment posture, not optional. Without
it, the operator's audit stream stands alone — and the forgery
threat above bites without a cross-check.

`AuditPolicy` configuration is out of scope for this operator
(it's a cluster-installer concern), but the cross-check threat
model has a minimum: **`level: Request` for `velkir.ioxie.dev/*`
resources** so the apiserver-audit row captures the request body
(specifically, the annotation values that drive privileged
events). `level: Metadata` is *not* sufficient — it records the
userInfo and the verb but drops the request body, which means an
investigator cannot tell which annotation value the user actually
sent. `level: RequestResponse` is fine but unnecessary; the
operator does not put Secret material into status responses, but
neither does the operator audit log require the response body for
correlation.

Pair the per-resource `level: Request` rule with a deny-list rule
at higher precedence for `secrets` (so Secret payloads are never
captured at Request level, even when a Valkey CR happens to
reference one in a Watch).

## When you need stronger guarantees

Some compliance regimes (PCI-DSS audit-trail integrity, SOC 2
CC6.1 / CC7.2 in stricter readings, some HIPAA implementations)
require **per-emission integrity** — a tamper-evident proof on
each audit line that a downstream forwarder cannot forge. The
shipped operator does not provide this.

If your regime requires it, the lowest-effort retrofit is a keyed
HMAC on each emission:

- Mount an HMAC key in a Secret (operator namespace, narrow
  Role).
- Wrap `audit.Log` to compute `hmac=<hex>` from the canonical
  rendered fields and append it as the last key in the log line.
- A verifier (separate process, with the same key) re-computes
  the HMAC for each shipped line; mismatches are forgeries.

This is **not currently implemented** because the threat-model
trade-off — adding a key-management dependency the operator
otherwise doesn't need, vs. relying on apiserver-audit cross-
check + write-once log retention — comes out in favour of the
deployment-level controls for the operator's primary audience
(self-hosted clusters with controlled log pipelines).

If you need the HMAC variant, please file an issue. The design
question is non-trivial; specifically the retrofit must address:

- **Versioned keys for rotation.** If the key rotates, lines
  signed with the previous key must remain verifiable. Either
  embed a key-id in each line (`kid=<id> hmac=<hex>`) and ship
  a parallel key-metadata channel to the verifier, or commit to
  a "no rotation; key lifetime equals stream lifetime" model
  with a hard cap on how long a single key can be used.
- **Replay defence.** A bare HMAC over the rendered fields lets
  an attacker copy a valid `event=X hmac=Y` line from time T1
  and re-inject it at time T2; the verifier accepts both.
  Include a sequence number or a high-resolution timestamp in
  the signed payload, and require monotonicity at the verifier.
- **Key-disclosure paths.** The HMAC key is itself a Secret;
  any code path that logs Secret material (existing `fmt.Errorf`
  shapes, future debug emissions) can leak it through the same
  log channel the audit lines flow on. The redaction registry
  in `internal/logging/` covers known passwords; extend it to
  cover the HMAC key on Secret read. Treat key disclosure on
  the audit channel as a full forgery capability and rotate.
- **Verifier availability semantics.** What does the operator
  do when the key Secret is unreachable on startup? "Refuse to
  emit" hides operator actions from compliance review; "emit
  unsigned" silently downgrades to the un-retrofitted model.
  Pick one and document it; do not let the failure mode be
  ambient.

## Verifying the posture in your install

```sh
# 1. Confirm operator audit lines are reaching your log store.
kubectl -n <operator-ns> logs deploy/<operator-name> | \
  grep 'event=' | head
# Should show structured key/value lines with event=, cr=, requestor=.

# 2. Confirm the apiserver audit log captures CR mutations.
# (Cluster-installer-specific. For kubeadm:)
journalctl -u kube-apiserver | grep audit | grep valkeys | head
# Should show `update` / `patch` requests on valkeys.velkir.ioxie.dev,
# with `user.username` populated.

# 3. Confirm the log store enforces retention.
# (Backend-specific. For Loki + S3 with Object Lock:)
aws s3api get-object-lock-configuration --bucket <log-bucket>
# Should print `ObjectLockEnabled: Enabled` and a retention rule.
```

## Related

- `internal/audit/log.go` — the emission API and event catalog.
- [`SECURITY.md`](../SECURITY.md) — the cross-cutting security
  model.
- [`deployment-posture.md`](deployment-posture.md) — companion
  doc on the cluster-wide Secret READ blast radius.
