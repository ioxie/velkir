# 1. Defer OperatorHub / OLM packaging

- Status: Accepted
- Date: 2026-06-23

## Context

A Kubernetes operator can be distributed in more than one way: as a
container image plus a Helm chart, or as an Operator Lifecycle Manager
(OLM) bundle published to OperatorHub — a `ClusterServiceVersion` plus
bundle manifests and an `annotations.yaml`, built into a bundle image
and submitted through the OperatorHub review process.

Velkir's genesis launch optimises for a small, auditable supply chain
and a single install path the project can fully test. An OLM bundle is a
second packaging format with its own CSV lifecycle, channel and
upgrade-graph semantics, scorecard, and an external submission/review
process — none of which the project is ready to commit to maintaining at
launch.

## Decision

OperatorHub / OLM packaging is **deferred**: it is not shipped in the
genesis launch.

The genesis launch releases exactly two artifact classes:

- the operator container image at `ghcr.io/ioxie/velkir/manager`, and
- the `velkir` and `velkir-crds` Helm charts, published as OCI artifacts
  under `oci://ghcr.io/ioxie/velkir/charts` and mirrored on the
  project's GitHub Pages Helm repository.

No OLM `ClusterServiceVersion`, no OperatorHub bundle (a `bundle/`
manifests directory, `annotations.yaml`, or `bundle.Dockerfile`), and no
`operatorhub.io` metadata ship in the repository today. The absence is
enforced automatically by the `no-olm` pr-gate CI job, which runs
`scripts/ci/no-olm-guard.sh` and fails the build if any such artifact
appears (this directory is excluded, since the ADR names the tokens by
design).

If OLM distribution is revisited later, it will be a deliberate
follow-up that adds the bundle tooling and an OperatorHub submission as
its own change, superseding this record.

## Consequences

- Users install with `helm` or Flux against the OCI or Pages chart
  repository (see `docs/install.md`); there is no OperatorHub "Install"
  entry.
- The supply chain stays limited to one image and two charts, all signed
  and attestable through the existing release pipeline.
- The no-OLM-bundle property is enforced by the `no-olm` pr-gate CI
  guard (`scripts/ci/no-olm-guard.sh`): adding any CSV or bundle artifact
  fails CI until this ADR is superseded.
