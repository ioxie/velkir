# Running the e2e test suite

The e2e suite under `test/e2e/` exercises the operator end-to-end:
standalone / replication / sentinel mode CR bootstraps, rolling
updates, webhook + cert-rotation scenarios, and the chart's
RBAC + metrics surface (Manager describe block).

There are two run modes — pick the one that fits your environment.

## Mode 1 — kind cluster (default `make test-e2e`)

The kubebuilder-scaffolded path. Spins up a dedicated `kind` cluster,
builds the operator image locally with docker, kind-loads it,
installs cert-manager, deploys via `make install` + `make deploy`,
runs the suite, and tears down the kind cluster at the end.

**Prerequisites:**
- `docker` (or another container engine compatible with kind)
- `kind`
- `kubectl`, `helm`, `go` (versions per `go.mod`)

**Run:**
```bash
make test-e2e
```

**Env overrides:**
- `CERT_MANAGER_INSTALL_SKIP=true` — skip the cert-manager install step
- `KIND` — path to a non-default kind binary
- `KIND_CLUSTER` — kind cluster name (default `velkir-test-e2e`)

## Mode 2 — shared cluster (`make test-e2e-shared`)

Runs against an already-configured Kubernetes cluster (your current
`kubectl` context). Designed for shared clusters that may also run
unrelated production workloads. The harness:

1. Generates a random run-id and derives a unique helm release name
   plus operator + test namespace names from it (so concurrent runs
   never collide).
2. `helm install`s the local chart with
   `webhook.namespaceSelector.matchLabels` scoped to the test
   namespace (the operator's webhooks ONLY fire on the labelled
   test namespace, never on other namespaces in the cluster).
3. Runs the Ginkgo suite with env vars instructing it to skip its
   own deploy/teardown and use the harness-created namespaces.
4. Tears everything down on exit (helm uninstall + namespace
   deletes + sweep of release-labelled cluster-scoped resources).
   No CRDs, no cert-manager state, no resources outside the
   harness-created releases are touched.

**Prerequisites:**
- `kubectl` configured for the target cluster
- `helm` (v3+)
- `go` (versions per `go.mod`)
- A pullable operator image — defaults to whatever the chart's
  `appVersion` field points to. Override with `IMAGE_TAG=...` /
  `IMAGE_REPOSITORY=...` env vars.

**Run:**
```bash
make test-e2e-shared
```

**Env overrides:**
- `IMAGE_TAG` — operator image tag (e.g. `IMAGE_TAG=v0.3.4-rc.1`).
  Defaults to the chart's `appVersion`.
- `IMAGE_REPOSITORY` — image repository (default from chart `values.yaml`).
- `KUBECONFIG` / `KUBE_CONTEXT` — standard kubectl selectors; if
  unset the script targets your current context.
- `E2E_RUN_ID` — pin the run-id (default: timestamp + 5-char random).
- `GINKGO_FOCUS` / `GINKGO_SKIP` — Ginkgo focus / skip regex.
- `KEEP_ON_FAILURE=true` — skip cleanup on test failure so cluster
  state can be triaged. Manual cleanup with
  `tools/e2e-shared-cleanup.sh <release> <op-ns> <test-ns>`.

**What the harness will NOT do:**
- Install or uninstall cert-manager — your cluster's existing
  install (if any) is left alone.
- Install or uninstall the Valkey CRDs cluster-wide — the chart
  ships CRDs as a sub-chart under `charts/velkir-crds`;
  the harness installs them as part of the release, scoped via
  helm release labels. Cleanup removes only what the release
  created.
- Touch any resource not labelled with the harness's release.

**Isolation guarantees:**
- Unique release name per run → all helm-managed resources
  (Deployment, Services, ServiceAccount, ClusterRole/-Binding,
  WebhookConfigurations) carry the release prefix and the
  `app.kubernetes.io/instance=<release>` label.
- WebhookConfigurations' `namespaceSelector` is overridden to
  match ONLY namespaces labelled `velkir.ioxie.dev/e2e-target=true`.
  The test harness applies that label only to the test namespace.
- A parallel install of the operator in the same cluster (e.g.
  production) — whose WebhookConfigurations target their own
  namespaces — is unaffected.

## Why not CI?

This project intentionally does NOT run e2e tests in CI today. The
CI runners do not currently support the kind +
docker workflow the default e2e mode requires. Until
the runner pool supports a workable e2e host, e2e validation is a
manual gate: developers run
`make test-e2e-shared` (or the kind mode locally) before merging
PRs that touch behaviour the e2e suite covers.

CI does run the unit, envtest, lint, and chart-lint jobs on every
PR. That catches the vast majority of regressions; e2e is the
last-mile integration gate.
