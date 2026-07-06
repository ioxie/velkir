# Security

## Reporting

Report security vulnerabilities **privately** through GitHub's
[Private Vulnerability Reporting](https://github.com/ioxie/velkir/security/advisories/new)
(PVR) — do **not** open public issues, pull requests, or discussions
for security reports. PVR opens a **draft GitHub Security Advisory
(GHSA)** visible only to the maintainers and, once invited, the
reporter, so the report and the work that follows stay private until
disclosure.

We acknowledge within 72 hours and aim to ship a fix within
14 days for High/Critical CVEs, 30 days for Medium, best-effort
for Low.

## Coordinated disclosure process

Once a report lands on the draft GHSA:

1. **Triage on the draft advisory.** Severity, affected versions, and
   a CVSS vector are recorded on the draft GHSA. The reporter is
   credited there unless they ask to remain anonymous.
2. **Fix on an isolated branch.** The patch is developed on a branch
   separate from `main` and validated on the maintainer's private
   environment — never on a public branch, whose push would tip off
   the vulnerability ahead of disclosure. If the GHSA's optional
   **temporary private fork** is used to collaborate on the patch,
   note that this fork **runs no CI**: GitHub Actions is disabled in
   advisory temporary forks by design, so staging a pre-disclosure fix
   there never triggers a public workflow run that would leak the
   issue. Validate such a fix locally or on the maintainer's private
   environment instead.
3. **Coordinated publish.** The advisory, the released fix, and the
   CVE are published together. Embargoed reporters get a heads-up
   before the public release when the schedule allows.

Security fixes ship in the latest released minor — see **Supported
versions** below.

## Supported versions

The operator image and the Helm charts version on **independent
SemVer axes** (operator `vX.Y.Z`, charts `chart-vX.Y.Z`); the CRD
API is served and stored at `v1beta1`. While the operator is on the
`v0.x` line, security fixes land on the latest released minor — track
it for patches.

| Component | Version line | Security fixes |
|---|---|---|
| Operator image | `v0.x` | Latest released minor only; no backports to older `v0.x` minors. |
| Helm charts | `chart-v0.x` | Latest released chart minor, versioned independently of the operator. |
| CRD API | `v1beta1` (served + stored) | Schema fixes ship in the operator minor that introduces them. |

## Security model

The operator is designed for **multi-tenant clusters** where the
operator namespace is a privileged blast-radius boundary. Concrete
threat-model decisions:

- **No cluster-wide Secret WRITE.** The operator reads
  user-supplied auth Secrets cluster-wide (CRs can be in any
  namespace) but only WRITES Secrets in its own namespace
  (the dynamically-generated webhook serving cert). Cluster-wide
  Secret WRITE is the canonical operator privilege-escalation
  surface — see
  [`docs/security/rbac-audit.md`](security/rbac-audit.md) for the
  full RBAC surface and the deliberate non-grants. The cluster-wide
  READ blast radius is bounded by three independent layers (informer
  cache `Label` selector, dedicated namespace, NetworkPolicy
  egress); see
  [`docs/security/deployment-posture.md`](security/deployment-posture.md)
  for the full posture and a verification checklist.
- **No `pods/exec`.** All managed-pod interactions happen via
  network protocols (Valkey wire protocol, sentinel pubsub) — the
  operator never `kubectl exec` into pods. This closes a common
  code-execution path that exploits operator service account
  tokens.
- **Webhook serving cert auto-rotated.** The in-process
  dynauth Authority generates a CA + leaf cert pair, rotates them
  on a 90-day cadence, and hot-reloads via fsnotify. Optional
  external CA handover is supported via the chart's
  `webhook.certManager.enabled` toggle.
- **PSA `restricted` compatible by default.** The chart's pod
  spec ships `runAsNonRoot: true`, `seccompProfile.type:
  RuntimeDefault`, `readOnlyRootFilesystem: true`,
  `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`.
  Verified against an enforcing namespace in the helm-unittest
  suite and the e2e enforcement matrix.
- **NetworkPolicy samples (not defaults).** The operator does
  NOT render NetworkPolicy for managed CRs because cluster CNI
  variation makes a one-size-fits-all policy either too tight or
  defeating-the-purpose. Apply the samples in
  [`docs/samples/networkpolicy/`](samples/networkpolicy/) per CR.
- **Admission webhooks fail-closed for validation, fail-open
  for defaulting.** A momentarily-unreachable defaulter doesn't
  block CR CRUD (schema-level OpenAPI defaults still fire); a
  momentarily-unreachable validator does — better to refuse a CR
  than to admit one that violates the contract.
- **Log redaction safety net.** The operator wraps every log
  emission with a token-scrubber that replaces registered Secret
  values with `[REDACTED]`. Reconcilers register tokens at Secret
  read-time. Defense-in-depth against accidental
  `fmt.Errorf("auth failed: %s", secret)` shapes.
- **Supply-chain attestations.** Every release image and chart is
  signed with **cosign keyless** (Sigstore): the signing identity is
  the release workflow's GitHub Actions OIDC identity with a
  Fulcio-issued ephemeral certificate — there is no long-lived
  private signing key — and every signature and certificate is
  recorded in the **Rekor** public transparency log (tlog ON). An
  SBOM (syft) and in-toto SLSA build provenance are attached as
  cosign attestations and likewise transparency-logged. Verify a
  published digest against the workflow identity:

  ```sh
  cosign verify \
    --certificate-identity-regexp '^https://github.com/ioxie/velkir/' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    ghcr.io/ioxie/velkir/manager@sha256:<digest>
  ```

  Use `cosign verify-attestation` with the same identity flags to
  check the attached SBOM and SLSA provenance.

## What this operator does NOT defend against

- **A compromised CR author.** Anyone with `create` on
  `valkeys.velkir.ioxie.dev` in a namespace can stand up a Valkey
  cluster there. Use Kubernetes RBAC to scope CR creation to the
  appropriate principals.
- **A compromised Secret.** If an attacker can read the auth
  Secret a CR references, they can connect to the resulting
  Valkey cluster as that CR's auth principal. The operator
  doesn't add a layer here — it's a Kubernetes secret-management
  problem.
- **Misconfigured network policy.** If you don't apply the
  NetworkPolicy samples (and don't have an equivalent default),
  any pod in the cluster can connect to any Valkey pod. This is
  by design (pluggable CNI assumption); see the samples'
  README.

## Audit trail

Privileged operator actions are logged structurally with
`event=<snake_case>` markers. Events the operator emits include
which user (via admission webhook userInfo) requested
annotation-driven actions (pause, accept-pvc-loss,
allow-aggressive-timeouts), and which sentinel-failover or
sentinel-reset commands were issued.

Configure your audit-log pipeline to filter on
`controller=valkey` for the operator's structured emissions.

The audit stream is a **detective control**, not a non-repudiation
control. Per-line integrity is not signed by the operator: the
shipped operator does NOT HMAC or sign individual audit lines.
The threat model relies on two deployment-level assumptions:

- **Write-once log shipper.** Operator log lines land in a store
  that denies retroactive edit/delete (S3 Object Lock, Splunk
  frozen indexes, Loki with WORM-backed object storage).
- **Cross-check against apiserver audit.** Every annotation-
  driven event the operator records also has a corresponding
  apiserver-audit `update`/`patch` request with `userInfo` —
  investigators correlate the two on `cr=<ns>/<name>`.

See [`security/audit-log-integrity.md`](security/audit-log-integrity.md)
for the full threat model, the operational assumptions, a
verification checklist, and guidance on the HMAC retrofit
for compliance regimes that demand per-emission integrity.

## Known limitations

The current scope (pre-v1.0) does NOT include:

- **At-rest encryption of the data plane.** Valkey itself doesn't
  encrypt RDB/AOF files; rely on Kubernetes CSI driver encryption
  (e.g., LUKS on the node, or CSI-level encryption) for at-rest
  protection.
- **mTLS between Valkey peers.** Valkey 7+ supports TLS for
  client-server and replication; the operator doesn't yet
  configure it. Tracked for post-v1.0.
- **Pre-built admission policies (Kyverno / OPA Gatekeeper).** Not
  shipped; if your cluster requires policy gates above the
  operator's built-in webhook validation, write them yourself.
- **Cryptographic signing on audit-log lines.** Operator audit
  lines are not HMAC'd or otherwise signed; integrity relies on
  write-once log storage plus apiserver-audit cross-check. See
  [`security/audit-log-integrity.md`](security/audit-log-integrity.md)
  for the threat model and the retrofit shape for compliance
  regimes that demand per-emission non-repudiation.
