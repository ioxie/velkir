# CRD additive-only CI lint

A CI gate runs on every PR that touches `api/v1beta1/`. It snapshots the package's exported API at the PR's base commit, compares against the PR head, and **fails the PR on any backward-incompatible API change** unless the `breaking-change` label is applied.

## Why this lint exists

`v1beta1` is the operator's pre-stable API surface — Kubernetes lets us break it, but the convention has bitten enough projects (silent renames, dropped json tags, MaxLength tightening) that an opt-in gate catches accidents before they ship. Intentional breaks ship with the label; accidents fail loudly.

## How it works

1. **Detection** — the existing `detect-changes` job sets `crd_api=true` when `git diff base..head` shows any change under `api/v1beta1/`.
2. **Snapshot** — the `crd-additive` job worktree-adds the base commit, runs `apidiff -w /tmp/base-api.json /tmp/base-tree/api/v1beta1` to capture the baseline exported API.
3. **Compare** — back on PR head, `apidiff /tmp/base-api.json ./api/v1beta1` reports added / removed / changed exported names. Empty output = no API changes (only generated artifacts moved).
4. **Gate** — if `Incompatible changes:` appears in the apidiff output, the job checks for the `breaking-change` label on the PR. Present → warn-and-pass. Absent → error-and-fail.

## What apidiff reports as Incompatible

- A field/method removed from an exported type
- A field's type narrowed (e.g. `int32` → `int16`)
- A previously-optional field becoming required
- An exported type itself removed

Pure additions (new fields with `+optional`, new types, new methods) are **Compatible**, never trigger the gate.

The lint does NOT see `+kubebuilder:validation:*` markers. A change from `+kubebuilder:validation:MaxLength=253` to `MaxLength=64` tightens the schema but doesn't change the Go type — apidiff misses it. A future enhancement is to also diff the generated CRD YAML's `openAPIV3Schema` block; tracked separately.

## When CI fails this gate

The PR author has three options, in order of likelihood:

1. **Accidental break** — most common. A typo in a `json:"..."` tag, a renamed field, a dropped omitempty. **Fix the source** so the comparison passes; no label needed.
2. **Intentional break** — e.g., a release widens a field's type. Add the `breaking-change` label to the PR; the gate flips from error to warning. The apidiff output remains visible in CI logs as a record of what changed.
3. **Generated-only churn** — `make manifests generate` regenerated something cosmetic. Re-run `make manifests generate` locally and confirm the `git diff` is purely whitespace/ordering; if so, regenerate against the right controller-gen version. Real semantic changes still need option 1 or 2.

## What this lint does NOT do

- It does not enforce the **plan's** stability promise (v1beta1 onward forbids breaks). That's a separate stage when v1beta1 ships.
- It does not run on `push` events to `main` — by then any incompatibility is already merged. The gate is at PR time only.
- It does not snapshot the generated CRD YAML — only the Go-typed exported API. Schema-level tightening (smaller MaxLength, stricter pattern) is invisible to it. CRD YAML diffing is tracked as a follow-up.

## Operating notes

- **First-PR run**: the gate fires on every PR that touches `api/v1beta1/`, not on first-ever runs. There's no special-cased empty-base path.
- **Force-push**: the comparison uses `CI_MERGE_REQUEST_DIFF_BASE_SHA` (resolved at pipeline trigger), so a force-push that rebases the target branch doesn't shift the baseline mid-MR.
- **`apidiff @latest`**: pinned to latest because apidiff's CLI + output format is its public API and changes very rarely. Pin to a specific commit if a regression appears.

## Carved-out follow-ups

The upgrade-matrix stage calls for three more pieces beyond this lint: a Kubernetes upgrade matrix in CI, a Flux HelmRelease integration test, and a cluster-variant matrix (RKE2 + k3s + openebs-hostpath). All three depend on reconciler infrastructure not yet in tree. Tracked as follow-up issues.
