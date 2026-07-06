# Feature gates

Per-feature opt-in / opt-out flags surfaced as chart values, CR
annotations, or build tags. Each gate has a default state and a
deprecation timeline (if any).

## Chart values gates

These flip features on/off at chart-install time. Operators-of-the-
operator pick them per environment.

| Gate | Default | Stability | Deprecation |
|---|---|---|---|
| `metrics.serviceMonitor.enabled` | `false` | Stable | n/a — opt-in stays opt-in. |
| `metrics.prometheusRule.enabled` | `false` | Stable | n/a |
| `dashboards.enabled` | `false` | Stable | n/a |
| `networkPolicy.enabled` | `false` | Stable | n/a |
| `webhook.certManager.enabled` | `false` | Stable | n/a — alternative to dynauth Authority. |
| `webhook.selfSigned.enabled` | `true` | Stable | Removed if external-CA becomes the only supported path (post-v1.0; tracked separately). |
| `leaderElection.enabled` | `true` | Stable | n/a |

**Stable** means the gate's existence and default are committed
across the v1beta1 train. **Beta** means the default may flip
between minors. **Alpha** means the gate may be renamed or removed.

## Per-CR feature gates (`spec.featureGates`)

These flip behaviour on a per-CR basis. Stamped on the `Valkey`
object's `spec.featureGates` map. Unknown keys are accepted for
forward compatibility (the validating webhook emits a Warning per
unknown key, the operator logs at startup); typo diagnosis lands in
`kubectl apply` output without blocking the request.

| Gate | Default | Effect |
|---|---|---|
| `UpgradePreflight` | `true` | When explicitly set to `false`, bypasses the reconciler's major-version-downgrade rejection in the runtime version-compat preflight. The cluster proceeds with the apply even when the desired image's major version is below the running image's major. The operator records the bypass via `ValkeyImageTransitionOverridden`. Cross-major data-format compatibility is the user's responsibility — testbed / disaster-recovery only. |

## CR annotations gates

These flip behavior on a per-CR basis. Stamped on the `Valkey`
object's `.metadata.annotations`. All annotations live under the
`velkir.ioxie.dev/` prefix.

| Annotation | Effect |
|---|---|
| `velkir.ioxie.dev/paused=true` | The reconciler reads the annotation in Phase 0 and short-circuits — no further reconcile work happens until the annotation is removed. Useful for surgical kubectl operations against a managed cluster. Emits `ResourcePaused` event on first observation. |
| `velkir.ioxie.dev/restart=<unique-string>` | Forces a master-aware roll without a spec change. The unique-string (typically a Unix timestamp) bumps the rendered ConfigMap hash → triggers the rollout. |
| `velkir.ioxie.dev/allow-aggressive-timeouts=true` | Lets the validating webhook accept `down-after-milliseconds < 1s` and `failover-timeout < 180s`. Emits a `WarnAggressiveTimeouts` event; intended for testbed-only use. |
| `velkir.ioxie.dev/accept-pvc-loss=<unique-string>` | Single-shot annotation that lets the operator proceed with the cluster recreate after a PV loss is detected. Self-clears after consumption (the operator strips the annotation post-action). Emits `PVCLossAccepted`. |
| `velkir.ioxie.dev/inject-ca=true` | Marks a `MutatingWebhookConfiguration` / `ValidatingWebhookConfiguration` as a target for the dynauth caBundle injector. Required for the operator's own webhooks; can be applied to other webhooks if you want them to share the same CA. |
| `velkir.ioxie.dev/force-rotate=true` | Forces an immediate webhook cert rotation on the next dynauth reconcile. Self-clears after consumption. |

## Build tags

Compile-time gates. Affect the binary, not chart values.

| Tag | Effect |
|---|---|
| `e2e` | Compiles the e2e test suite under `test/e2e/`. Without this tag, the package is excluded from `go test ./...`. |

## Deprecation policy

The pre-v1.0 contract is **additive-only on CRD fields, no other
hard guarantee**. Annotations and chart values can change between
minors during alpha; we'll publish a deprecation notice in the
changelog one minor before removal and keep the old shape working
under a feature-gate alias until v1.0.

After v1.0, deprecation lifecycle becomes:

- **Beta → stable**: gate's default flipped from `false` to `true`
  (or vice versa, with a clear rationale in the changelog).
  Existing chart values continue to work.
- **Stable removal**: a stable feature is removed only at a major
  version bump (v2.0+). Two minor releases of advance notice with
  the deprecation marker visible in `helm template` output.
- **Annotation removal**: same shape — one minor of advance
  notice with the operator emitting a `FieldDeprecated` event
  per CR per reconcile (deduplicated) when the annotation is
  observed.

The `FieldDeprecated` event mechanism is the canonical
operator-of-the-operator notification path; pair with an
Alertmanager rule that fires on
`rate(kube_event_count{reason="FieldDeprecated"}[1h]) > 0` to get
deprecation visibility into your normal alert flow.

The observer is wired into the reconciler today: each Reconcile pass
walks `controller.ProductionDeprecations` and emits one
`FieldDeprecated` event per `(namespace/name/Path)` tuple that a
predicate matches, deduplicated for the operator's process
lifetime. The next reconcile after a pod restart re-emits the still-
active deprecations once.

**Restart and leader-handover behaviour.** The dedup set lives in
operator memory and resets on operator pod restart or leader handover;
the first reconcile under the new process re-emits each still-active
deprecation exactly once before falling back to the deduplicated
steady state. This is intentional — the event log is the
operator-of-the-operator audit trail, and re-emission on a new leader
is a useful "deprecation still applies" signal, not noise to suppress.
If you alert on the rate query above, the per-restart re-emission
shows up as a single-event spike per active deprecation, not as a
sustained signal.

### Active deprecations

| Field / annotation | Window | Notes |
|---|---|---|
| _(none — `api/v1beta1` is additive-only since v0.1.0)_ | — | The first real deprecation lands by appending a `FieldDeprecation{Path, RemovalWindow, Predicate}` entry to `controller.ProductionDeprecations`; no further wiring is required. |
