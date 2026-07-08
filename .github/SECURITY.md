# Security Policy

## Reporting a vulnerability

Report security vulnerabilities **privately** through GitHub's
[Private Vulnerability Reporting](https://github.com/ioxie/velkir/security/advisories/new)
(PVR). Do **not** open public issues, pull requests, or discussions
for security reports.

PVR opens a **draft GitHub Security Advisory (GHSA)** visible only to
the maintainers and, once invited, the reporter. We acknowledge within
72 hours and aim to ship a fix within 14 days for High/Critical CVEs,
30 days for Medium, best-effort for Low.

## How a report is handled

- **Triage on the draft GHSA** — severity, affected versions, and a
  CVSS vector are recorded on the advisory; the reporter is credited
  unless they request anonymity.
- **Fix on an isolated branch** separate from `main`, validated on the
  maintainer's private environment. If the GHSA's temporary private
  fork is used to collaborate, note it **runs no CI** (GitHub Actions
  is disabled in advisory temporary forks), so staging a pre-disclosure
  fix never triggers a public workflow run that would leak the issue.
- **Coordinated publish** — the advisory, the released fix, and the CVE
  are published together.

The full security model, supported-version policy, and supply-chain
attestation details live in [`docs/SECURITY.md`](../docs/SECURITY.md).

## Supported versions

Security fixes land on the latest released minor of the operator
(`v0.x`) and the charts (`chart-v0.x`); the CRD API is served and
stored at `v1beta1`. See [`docs/SECURITY.md`](../docs/SECURITY.md) for
the full supported-versions table.
