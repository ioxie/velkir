#!/usr/bin/env bash
# Shared-cluster e2e runner.
#
# Runs the test/e2e/ Ginkgo suite against an already-configured
# Kubernetes cluster (current kubectl context). Designed for clusters
# that also run unrelated production workloads: the operator is helm-
# installed under a unique release name + namespace per run, the
# webhook namespaceSelector is scoped to a single labelled test
# namespace, and cleanup unwinds everything via helm uninstall + kubectl
# delete ns. No CRDs, cert-manager state, or other cluster-scoped state
# the test did not create is touched.
#
# Usage:
#   tools/e2e-shared.sh
#
# Configurable via env vars (all optional):
#   IMAGE_TAG           — operator image tag (default: chart appVersion)
#   IMAGE_REPOSITORY    — operator image repo (default: chart values.yaml default)
#   KUBECONFIG          — kubectl config path (kubectl's standard)
#   KUBE_CONTEXT        — kubectl context name (default: current-context)
#   E2E_RUN_ID          — unique suffix for release / namespaces (default: timestamp + random)
#   GINKGO_FOCUS        — Ginkgo --focus regex (default: run everything)
#   GINKGO_SKIP         — Ginkgo --skip regex
#   KEEP_ON_FAILURE     — set to "true" to skip cleanup on test failure (default: cleanup)
#   GINKGO_PROCS        — number of parallel Ginkgo test processes (default: 1).
#                         When >1, the harness pre-creates N labelled test
#                         namespaces (<TEST_NS>-p1..pN) so each parallel
#                         process gets its own scratch ns + CNP coverage,
#                         and the suite is invoked via the local Ginkgo CLI
#                         (./bin/ginkgo) instead of `go test`. Webhook-cert
#                         specs are marked Serial so the cluster-scoped
#                         ValidatingWebhookConfiguration rotation never
#                         races between processes.
#
# Exits non-zero on test failure or setup failure. Cleanup runs in
# every exit path via a trap.
#
# Structure: each banner-labelled stage is an unexported function;
# main() at the bottom calls them in the order they execute. The
# computed config (run-id, names, context args, namespace list) is set
# as global state by setup_run_id / resolve_config so the cleanup trap
# and every stage observe the same values.

# `-u` is intentionally NOT set: bash 3.2 (macOS default) errors on
# `"${ARR[@]}"` expansion of an empty array under -u. The script does
# not otherwise rely on unset-variable detection — `${var:-default}`
# guards every read where it matters.
set -eo pipefail

# Shared teardown helpers (e2e_resolve_context_args + e2e_teardown),
# also used by tools/e2e-shared-cleanup.sh.
# shellcheck source=e2e-teardown-lib.sh disable=SC1091
source "$(dirname -- "${BASH_SOURCE[0]}")/e2e-teardown-lib.sh"

# --- run identifier ---------------------------------------------------------
# Random suffix so two concurrent runs (or a re-run after a partial
# failure) never collide on the release name + namespaces. Avoid the
# obvious `tr | head -c` pipeline: head exits after 5 bytes, tr keeps
# reading /dev/urandom, the SIGPIPE on tr trips `set -o pipefail`.
# `openssl rand` produces a fixed-length output in one process.
#
# Sets global state (NOT `local`/`declare -a`, so the cleanup trap and
# the install/run stages all observe the same values; `declare -g` is
# unavailable on the bash 3.2 floor this script targets).
setup_run_id() {
  if [[ -z "${E2E_RUN_ID:-}" ]]; then
    RUN_ID="$(date +%s)-$(openssl rand -hex 3)"
  else
    RUN_ID="${E2E_RUN_ID}"
  fi

  RELEASE_NAME="valkey-e2e-${RUN_ID}"
  CRDS_RELEASE_NAME="valkey-e2e-crds-${RUN_ID}"
  OP_NS="valkey-e2e-op-${RUN_ID}"
  TEST_NS="valkey-e2e-test-${RUN_ID}"

  # Ginkgo parallel-procs plumbing. GINKGO_PROCS=1 (default) preserves the
  # legacy single-process / single-namespace flow. GINKGO_PROCS>1 spawns
  # the per-process namespaces named "${TEST_NS}-p<n>" so each Ginkgo
  # worker writes to its own scratch ns. The chart's namespaceSelector
  # matches on the `velkir.ioxie.dev/e2e-target=true` label, which we stamp
  # on every per-process namespace below.
  GINKGO_PROCS="${GINKGO_PROCS:-1}"
  if ! [[ "${GINKGO_PROCS}" =~ ^[0-9]+$ ]] || [[ "${GINKGO_PROCS}" -lt 1 ]]; then
    echo "[e2e-shared] invalid GINKGO_PROCS='${GINKGO_PROCS}' — must be a positive integer" >&2
    exit 2
  fi

  # When running serially the Ginkgo BeforeSuite resolves e2eNamespace
  # to the bare base "${TEST_NS}"; parallel mode appends -p<N> per worker.
  # Compute the full list so cleanup + pre-create + CNP iterate over all.
  TEST_NAMESPACES=()
  if [[ "${GINKGO_PROCS}" -eq 1 ]]; then
    TEST_NAMESPACES=("${TEST_NS}")
  else
    for i in $(seq 1 "${GINKGO_PROCS}"); do
      TEST_NAMESPACES+=("${TEST_NS}-p${i}")
    done
  fi

  # Track whether THIS run installed the CRDs chart. If the cluster
  # already had Valkey CRDs (e.g. from a production install), we skip
  # the CRDs install AND skip the CRDs uninstall on cleanup — never
  # touch cluster-scoped state we didn't create.
  CRDS_INSTALLED_BY_THIS_RUN="false"
}

# --- context resolution + banner --------------------------------------------
# Resolves the kubectl/helm context args, enforces the KUBE_CONTEXT vs
# current-context safety gate, computes the repo/chart paths, and echoes
# the run banner. Sets global state for the stages and cleanup trap.
resolve_config() {
  e2e_resolve_context_args

  # Load-bearing safety gate. The Go e2e suite (test/e2e) runs bare `kubectl`
  # with NO --context, so it always targets the kubeconfig's current-context —
  # it does not honour KUBE_CONTEXT. If KUBE_CONTEXT is set to a different
  # context than current-context, this harness would install the operator on one
  # cluster (via --context) while the suite silently creates Valkey CRs on
  # whatever current-context happens to be. Refuse to run on that mismatch.
  if [[ -n "${KUBE_CONTEXT:-}" ]]; then
    _current_ctx="$(kubectl config current-context 2>/dev/null || true)"
    if [[ "${KUBE_CONTEXT}" != "${_current_ctx}" ]]; then
      echo "[e2e-shared] FATAL: KUBE_CONTEXT='${KUBE_CONTEXT}' but kubeconfig current-context='${_current_ctx}'." >&2
      echo "[e2e-shared] The Go test suite ignores KUBE_CONTEXT and uses current-context, so it would" >&2
      echo "[e2e-shared] create test resources on '${_current_ctx}' while the operator is installed on" >&2
      echo "[e2e-shared] '${KUBE_CONTEXT}'. Point current-context at '${KUBE_CONTEXT}' (e.g." >&2
      echo "[e2e-shared] 'kubectl config use-context ${KUBE_CONTEXT}', or run under a KUBECONFIG whose" >&2
      echo "[e2e-shared] current-context is '${KUBE_CONTEXT}') and re-run." >&2
      exit 2
    fi
  fi

  REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
  CHART_DIR="${REPO_ROOT}/charts/velkir"
  CRDS_CHART_DIR="${REPO_ROOT}/charts/velkir-crds"

  echo "[e2e-shared] run-id        = ${RUN_ID}"
  echo "[e2e-shared] release       = ${RELEASE_NAME}"
  echo "[e2e-shared] crds release  = ${CRDS_RELEASE_NAME} (only installed if CRDs absent)"
  echo "[e2e-shared] operator ns   = ${OP_NS}"
  echo "[e2e-shared] test ns       = ${TEST_NS}"
  echo "[e2e-shared] ginkgo procs  = ${GINKGO_PROCS}"
  echo "[e2e-shared] test ns set   = ${TEST_NAMESPACES[*]}"
  # Show the EFFECTIVE context. `kubectl config current-context` reports the
  # kubeconfig's current-context field and ignores the --context override, so
  # printing it here would name the wrong cluster on a --context-pinned run.
  echo "[e2e-shared] kube context  = ${KUBE_CONTEXT:-$(kubectl config current-context)}"
  echo "[e2e-shared] chart dir     = ${CHART_DIR}"
}

# --- cleanup trap ----------------------------------------------------------
# Unwinds every cluster-scoped + namespaced resource this run created.
# Idempotent (each step is --ignore-not-found / || true) so a partial
# install state still cleans up. Skipped only if KEEP_ON_FAILURE=true
# AND a test failed — left in place for triage.

cleanup() {
  local exit_code=$?
  if [[ "${exit_code}" -ne 0 && "${KEEP_ON_FAILURE:-false}" == "true" ]]; then
    # Join the namespace array with commas for the cleanup script's
    # CSV argument (single ns or per-process list both work).
    local ns_csv
    ns_csv="$(IFS=,; echo "${TEST_NAMESPACES[*]}")"
    echo "[e2e-shared] KEEP_ON_FAILURE=true and tests failed — leaving cluster state for triage:"
    echo "             release=${RELEASE_NAME} op-ns=${OP_NS} test-ns=${ns_csv}"
    echo "             clean up with: tools/e2e-shared-cleanup.sh '${RELEASE_NAME}' '${OP_NS}' '${ns_csv}'"
    return
  fi

  echo "[e2e-shared] cleanup start"
  e2e_teardown

  # Only uninstall the CRDs chart if THIS run installed it. Existing
  # cluster CRDs (production install, pre-existing dev install) stay
  # untouched.
  #
  # The CRDs chart sets helm.sh/resource-policy: keep on each CRD, so
  # helm uninstall leaves the CRDs behind. After helm uninstall we
  # explicitly kubectl-delete the CRDs we installed. This propagates
  # to any remaining Valkey / SentinelQuorum CRs cluster-wide — which
  # is the expected teardown shape because this branch only runs when
  # the CRDs were absent at install-time, meaning no pre-existing CRs
  # could exist.
  if [[ "${CRDS_INSTALLED_BY_THIS_RUN}" == "true" ]]; then
    echo "[e2e-shared] uninstalling CRDs chart ${CRDS_RELEASE_NAME}"
    helm "${HELM_CONTEXT_ARG[@]}" uninstall "${CRDS_RELEASE_NAME}" -n "${OP_NS}" --ignore-not-found 2>/dev/null || true
    echo "[e2e-shared] removing kept CRDs (this run installed them)"
    kubectl "${KUBECTL_CONTEXT_ARG[@]}" delete crd \
      valkeys.velkir.ioxie.dev sentinelquorums.velkir.ioxie.dev \
      --ignore-not-found --timeout=30s 2>/dev/null || true
  else
    echo "[e2e-shared] leaving pre-existing CRDs in place (not installed by this run)"
  fi

  echo "[e2e-shared] cleanup done (exit=${exit_code})"
}

# --- image override --------------------------------------------------------
configure_image_args() {
  HELM_IMAGE_ARGS=()
  if [[ -n "${IMAGE_TAG:-}" ]]; then
    HELM_IMAGE_ARGS+=(--set "image.tag=${IMAGE_TAG}")
  fi
  if [[ -n "${IMAGE_REPOSITORY:-}" ]]; then
    HELM_IMAGE_ARGS+=(--set "image.repository=${IMAGE_REPOSITORY}")
  fi
}

# --- pre-flight ------------------------------------------------------------
preflight() {
  echo "[e2e-shared] pre-flight: confirming kubectl access"
  kubectl "${KUBECTL_CONTEXT_ARG[@]}" version --output=yaml >/dev/null

  # Refuse to install if any of the names we're about to claim already
  # exist. Safer than racing whatever's there.
  for ns in "${OP_NS}" "${TEST_NAMESPACES[@]}"; do
    if kubectl "${KUBECTL_CONTEXT_ARG[@]}" get ns "${ns}" >/dev/null 2>&1; then
      echo "[e2e-shared] refusing to proceed: namespace '${ns}' already exists" >&2
      echo "[e2e-shared] manual cleanup: kubectl delete ns ${ns}" >&2
      exit 2
    fi
  done

  # Foreign-operator guard: a standing velkir install on this
  # cluster co-reconciles every CR this run creates (cluster-scoped
  # watch), and a pre-rc.21 install additionally force-stamps its own CA
  # into this run's webhook configurations — either poisons the run with
  # failures that look like operator bugs. This run's own operator is
  # confined to its scratch namespaces via watchNamespaces (set at
  # install below), but that cannot stop the STANDING install from
  # touching this run's CRs. Override only when the standing install is
  # rc.21+ AND scoped away from the e2e namespaces.
  if [[ "${E2E_ALLOW_FOREIGN_OPERATOR:-0}" != "1" ]]; then
    FOREIGN_WEBHOOKS="$(kubectl "${KUBECTL_CONTEXT_ARG[@]}" get validatingwebhookconfigurations \
      -l velkir.ioxie.dev/inject-ca=true -o name 2>/dev/null || true)"
    if [[ -n "${FOREIGN_WEBHOOKS}" ]]; then
      echo "[e2e-shared] refusing to proceed: another velkir install exists on this cluster:" >&2
      echo "${FOREIGN_WEBHOOKS}" >&2
      echo "[e2e-shared] it will co-reconcile this run's CRs and (pre-rc.21) fight the CA injector." >&2
      echo "[e2e-shared] uninstall/scope it, or set E2E_ALLOW_FOREIGN_OPERATOR=1 to proceed anyway." >&2
      exit 2
    fi
  fi
}

# --- install ---------------------------------------------------------------
# webhook.namespaceSelector scopes the WebhookConfigurations to ONLY
# the labelled e2e test namespace, so the operator's webhooks never
# fire on production namespaces in this shared cluster. The test suite
# labels TEST_NS with velkir.ioxie.dev/e2e-target=true at BeforeAll time.
# --- CRDs ---
# Cluster-scoped; install the CRDs chart only if this cluster does not
# already have the Valkey CRDs from another install. Cleanup mirrors
# the same logic: only uninstall what we installed.
install_crds() {
  if kubectl "${KUBECTL_CONTEXT_ARG[@]}" get crd valkeys.velkir.ioxie.dev >/dev/null 2>&1 \
     && kubectl "${KUBECTL_CONTEXT_ARG[@]}" get crd sentinelquorums.velkir.ioxie.dev >/dev/null 2>&1; then
    echo "[e2e-shared] Valkey CRDs already present — skipping CRDs chart install"
  else
    echo "[e2e-shared] Valkey CRDs absent — helm install ${CRDS_RELEASE_NAME}"
    helm "${HELM_CONTEXT_ARG[@]}" install "${CRDS_RELEASE_NAME}" "${CRDS_CHART_DIR}" \
      --namespace "${OP_NS}" --create-namespace \
      --wait --timeout=2m
    CRDS_INSTALLED_BY_THIS_RUN="true"
  fi
}

install_operator() {
  echo "[e2e-shared] helm install ${RELEASE_NAME} -> ${OP_NS}"
  # Defaults to the chart's selfSigned (dynauth) mode now that #396
  # landed the chart init container that pre-mints the webhook + metrics
  # leaf Secrets via `--bootstrap-only`. To exercise the cert-manager
  # opt-in path instead, pass --set webhook.certManager.enabled=true
  # --set webhook.selfSigned.enabled=false via the HELM_EXTRA_SET env
  # (or invoke helm install directly).
  #
  # --set-string for the label value: Kubernetes label values are always
  # strings and helm without --set-string can render unquoted `true` →
  # YAML boolean → admission rejection on the WebhookConfiguration.
  # Confine the run's operator to its own namespaces: the operator ns
  # (cert Secrets + leader lease) plus every test namespace. Without
  # this a per-run operator co-reconciles every Valkey CR on the shared
  # cluster — including production ones.
  WATCH_NS_ARGS=(--set "watchNamespaces[0]=${OP_NS}")
  _wi=1
  for ns in "${TEST_NAMESPACES[@]}"; do
    WATCH_NS_ARGS+=(--set "watchNamespaces[${_wi}]=${ns}")
    _wi=$((_wi + 1))
  done

  helm "${HELM_CONTEXT_ARG[@]}" install "${RELEASE_NAME}" "${CHART_DIR}" \
    --namespace "${OP_NS}" --create-namespace \
    --set-string 'webhook.namespaceSelector.matchLabels.velkir\.ioxie\.dev/e2e-target=true' \
    "${WATCH_NS_ARGS[@]}" \
    --set allowTestOverrides=true \
    --set 'extraEnv[0].name=VALKEY_OPERATOR_AUTHORITY_MIN_INTERVAL_SEC' \
    --set-string 'extraEnv[0].value=10' \
    --set 'extraEnv[1].name=VALKEY_OPERATOR_SENTINEL_OBSERVER_POLL_SEC' \
    --set-string 'extraEnv[1].value=5' \
    "${HELM_IMAGE_ARGS[@]}" \
    --wait --timeout=5m

  echo "[e2e-shared] helm install OK; waiting for operator pod Ready"
  kubectl "${KUBECTL_CONTEXT_ARG[@]}" -n "${OP_NS}" wait deployment/"${RELEASE_NAME}-velkir" \
    --for=condition=Available --timeout=3m
}

apply_network_policies() {
  # Cilium-default-deny clusters drop apiserver →
  # webhook ingress unless a CiliumNetworkPolicy explicitly allows the
  # kube-apiserver entity. The chart ships a stock NetworkPolicy that
  # is permissive on the named webhook port (no `from:` clause = "any
  # source"), but Cilium's identity-based default-deny on cluster-wide
  # ingress overrides per-namespace standard NetworkPolicy on apiserver
  # sources. Without this, every PATCH against a Valkey CR fails with
  # "dial tcp <svc>: connect: operation not permitted" and the
  # operator never finishes a reconcile.
  #
  # Apply only when the cluster's API surfaces CiliumNetworkPolicy.
  # `kubectl api-resources` is the cheapest probe; the `|| true` keeps
  # `set -eo pipefail` from killing the harness when no Cilium CRDs are
  # registered. Branch verdict is echoed so non-Cilium and Cilium runs
  # both leave a trace.
  CILIUM_PROBE=$(kubectl "${KUBECTL_CONTEXT_ARG[@]}" api-resources --api-group=cilium.io 2>/dev/null | grep -c ciliumnetworkpolicies || true)
  echo "[e2e-shared] Cilium probe: ${CILIUM_PROBE} ciliumnetworkpolicies API resource(s)"
  if [[ "${CILIUM_PROBE}" -gt 0 ]]; then
    echo "[e2e-shared] Cilium detected — applying CNP to allow apiserver -> webhook ingress"
    kubectl "${KUBECTL_CONTEXT_ARG[@]}" -n "${OP_NS}" apply -f - <<EOF
apiVersion: cilium.io/v2
kind: CiliumNetworkPolicy
metadata:
  name: ${RELEASE_NAME}-allow-apiserver
  namespace: ${OP_NS}
spec:
  endpointSelector:
    matchLabels:
      app.kubernetes.io/instance: ${RELEASE_NAME}
      app.kubernetes.io/name: velkir
  ingress:
    - fromEntities:
        - kube-apiserver
      toPorts:
        - ports:
            - port: "9443"
              protocol: TCP
            - port: "8443"
              protocol: TCP
            - port: "8081"
              protocol: TCP
EOF

    # The operator must reach valkey pods (:6379, replication-lag
    # probing) and sentinel pods (:26379, observer + orchestration)
    # in every test namespace. Cilium ingress default-deny would drop
    # these connections — they show up as "Stale or unroutable IP
    # DROPPED" in hubble against the data-plane endpoint identity.
    # Allow ingress to those ports from the operator namespace.
    #
    # Each TEST_NAMESPACES entry is created here (the test suite
    # re-creates it idempotently — `kubectl create ns` AlreadyExists is
    # discarded) so the CNP attaches to an existing namespace rather
    # than racing the spec's ns-create. Parallel mode (GINKGO_PROCS>1)
    # provisions N namespaces; each gets its own CNP scoped to the
    # release.
    for ns in "${TEST_NAMESPACES[@]}"; do
      kubectl "${KUBECTL_CONTEXT_ARG[@]}" create ns "${ns}" 2>/dev/null || true
      kubectl "${KUBECTL_CONTEXT_ARG[@]}" label --overwrite ns "${ns}" velkir.ioxie.dev/e2e-target=true >/dev/null
      echo "[e2e-shared] Cilium detected — applying CNP to allow operator -> data-plane ingress in ${ns}"
      kubectl "${KUBECTL_CONTEXT_ARG[@]}" -n "${ns}" apply -f - <<EOF
apiVersion: cilium.io/v2
kind: CiliumNetworkPolicy
metadata:
  name: ${RELEASE_NAME}-allow-operator
  namespace: ${ns}
spec:
  endpointSelector:
    matchLabels:
      app.kubernetes.io/managed-by: velkir
  ingress:
    - fromEndpoints:
        - matchLabels:
            k8s:io.kubernetes.pod.namespace: ${OP_NS}
            app.kubernetes.io/instance: ${RELEASE_NAME}
      toPorts:
        - ports:
            - port: "6379"
              protocol: TCP
            - port: "26379"
              protocol: TCP
EOF
      # Intra-namespace data-plane traffic between operator-managed
      # pods: replicas connecting to the primary on :6379 for
      # replication stream + initial RDB transfer, sentinels probing
      # valkey pods on :6379 with `INFO replication`, sentinels
      # gossiping among each other on :26379. The operator-ingress
      # policy above intentionally scopes ingress to the operator
      # namespace only; without this companion policy, Cilium's
      # default-deny drops replica→primary connections and the
      # test scenario sees an empty replica with no replication state.
      echo "[e2e-shared] Cilium detected — applying CNP to allow data-plane peer traffic in ${ns}"
      kubectl "${KUBECTL_CONTEXT_ARG[@]}" -n "${ns}" apply -f - <<EOF
apiVersion: cilium.io/v2
kind: CiliumNetworkPolicy
metadata:
  name: ${RELEASE_NAME}-allow-data-plane-peers
  namespace: ${ns}
spec:
  endpointSelector:
    matchLabels:
      app.kubernetes.io/managed-by: velkir
  ingress:
    - fromEndpoints:
        - matchLabels:
            k8s:io.kubernetes.pod.namespace: ${ns}
            app.kubernetes.io/managed-by: velkir
      toPorts:
        - ports:
            - port: "6379"
              protocol: TCP
            - port: "26379"
              protocol: TCP
EOF
    done
  else
    # Non-Cilium clusters: still pre-create + label every test ns so
    # the chart's namespaceSelector matches at admission time. Without
    # this, parallel mode's per-process namespaces show up unlabelled
    # the first time the suite hits them.
    for ns in "${TEST_NAMESPACES[@]}"; do
      kubectl "${KUBECTL_CONTEXT_ARG[@]}" create ns "${ns}" 2>/dev/null || true
      kubectl "${KUBECTL_CONTEXT_ARG[@]}" label --overwrite ns "${ns}" velkir.ioxie.dev/e2e-target=true >/dev/null
    done
  fi
}

# --- run -------------------------------------------------------------------
# Tell the suite to skip its own deploy/teardown (we own that), point
# it at our namespaces, and tell BeforeSuite to skip kind setup +
# cert-manager install.
run_suite() {
  export E2E_SHARED_CLUSTER=true
  export E2E_OPERATOR_NAMESPACE="${OP_NS}"
  export E2E_TEST_NAMESPACE="${TEST_NS}"
  # Chart pods carry the helm-instance label, not the kustomize control-
  # plane label; chart WebhookConfigurations are release-prefixed too.
  # Suite reads these to find the right pod + webhook configs.
  export E2E_OPERATOR_LABEL="app.kubernetes.io/instance=${RELEASE_NAME}"
  export E2E_VALIDATING_WEBHOOK_NAME="${RELEASE_NAME}-velkir-validator"
  export E2E_MUTATING_WEBHOOK_NAME="${RELEASE_NAME}-velkir-defaulter"
  # Webhook Service name is also release-prefixed under the chart. The
  # webhook-cert lifecycle suite uses this to watch endpoint drain /
  # repopulate during failurePolicy=Fail scenarios; without it the suite
  # falls back to the kustomize-style name `velkir-webhook-service`
  # which doesn't exist on the chart-deploy path, the drain wait becomes
  # a no-op, and the CR-create that follows races with the still-warm
  # webhook endpoint cache instead of the operator-down state the test
  # is asserting.
  export E2E_WEBHOOK_SERVICE_NAME="${RELEASE_NAME}-velkir-webhook"
  # PVC-resize fixtures default to `rancher.io/local-path` (kind's
  # provisioner). On shared clusters that provisioner isn't installed.
  # The no-expand fixture only needs `allowVolumeExpansion=false`, which
  # is bare-SC-compatible — openebs.io/local works. The expand fixture
  # needs a provisioner that ACTUALLY resizes the underlying volume
  # (status.capacity round-trip is the operator's substate gate); for
  # that we reuse an existing cluster-owned CSI SC by name via
  # E2E_PVC_RESIZE_SC_EXPAND — bypassing the bare-SC creation path
  # because real CSI SCs require non-trivial parameters (clusterID,
  # secrets, pool) that the test fixture doesn't carry.
  export E2E_PVC_RESIZE_PROVISIONER="${E2E_PVC_RESIZE_PROVISIONER:-openebs.io/local}"
  # Unset-only default (`-`, not `:-`): an explicitly EMPTY value means "this
  # cluster has no expansion-capable CSI" — the suite then creates its synthetic
  # hostpath SCs and the resize happy-path spec skips. tools/e2e-minikube.sh
  # relies on this to keep the happy path off minikube.
  export E2E_PVC_RESIZE_SC_EXPAND="${E2E_PVC_RESIZE_SC_EXPAND-ceph-block}"
  export CERT_MANAGER_INSTALL_SKIP=true
  # NOTE: kubectl has no KUBECTL_CONTEXT env var, so the suite cannot be steered
  # that way — it targets the kubeconfig current-context. The KUBE_CONTEXT vs
  # current-context guard near the top of this script enforces that the two
  # agree, so the suite lands on the same cluster the harness installed onto.

  # `go test` defaults to 10m. Sentinel-mode scenarios with master-aware
  # rolling routinely take 8-12 minutes per spec (3 pods × sentinel
  # re-discovery floor 30s + replication catch-up + primary failover);
  # a single spec can blow the default and an unfiltered suite run easily
  # pushes past an hour. Override here with a 2h ceiling; callers can
  # trim it via GO_TEST_TIMEOUT for tight CI windows.
  GO_TEST_TIMEOUT="${GO_TEST_TIMEOUT:-2h}"

  cd "${REPO_ROOT}"

  if [[ "${GINKGO_PROCS}" -gt 1 ]]; then
    # Parallel mode: invoke via the local Ginkgo CLI (./bin/ginkgo). The
    # CLI orchestrates parallel test-process spawn; `go test` runs the
    # binary as a single process and can't drive Ginkgo --procs. The
    # Makefile target `make ginkgo` installs the CLI into ./bin at the
    # same version the test binary's onsi/ginkgo dep pins.
    GINKGO_BIN="${REPO_ROOT}/bin/ginkgo"
    if [[ ! -x "${GINKGO_BIN}" ]]; then
      echo "[e2e-shared] ${GINKGO_BIN} not found; run 'make ginkgo' first" >&2
      exit 2
    fi

    GINKGO_CLI_ARGS=(--procs="${GINKGO_PROCS}" -v --timeout="${GO_TEST_TIMEOUT}")
    if [[ -n "${GINKGO_FOCUS:-}" ]]; then
      GINKGO_CLI_ARGS+=(--focus="${GINKGO_FOCUS}")
    fi
    if [[ -n "${GINKGO_SKIP:-}" ]]; then
      GINKGO_CLI_ARGS+=(--skip="${GINKGO_SKIP}")
    fi

    echo "[e2e-shared] running ginkgo suite via ${GINKGO_BIN} --procs=${GINKGO_PROCS} (timeout ${GO_TEST_TIMEOUT})"
    "${GINKGO_BIN}" "${GINKGO_CLI_ARGS[@]}" --tags=e2e ./test/e2e/
  else
    # Serial mode: classic `go test` invocation. Backwards-compatible
    # with the pre-parallel-mode flow.
    GINKGO_ARGS=(-v)
    if [[ -n "${GINKGO_FOCUS:-}" ]]; then
      GINKGO_ARGS+=(-ginkgo.focus="${GINKGO_FOCUS}")
    fi
    if [[ -n "${GINKGO_SKIP:-}" ]]; then
      GINKGO_ARGS+=(-ginkgo.skip="${GINKGO_SKIP}")
    fi
    # GO_TEST_TIMEOUT is the SUITE budget, so it must drive Ginkgo's own
    # suite timeout (-ginkgo.timeout), not just `go test -timeout`. Those
    # are two different clocks: `go test -timeout` is a hard kill of the
    # whole binary, while -ginkgo.timeout defaults to 1h and, when it is
    # shorter, fires first — an unfiltered serial run (54 specs, sentinel
    # scenarios 8-12m each) then dies at 1h with a bare "Suite Timeout
    # Elapsed" mid-spec no matter how large GO_TEST_TIMEOUT is set. Give
    # Ginkgo the budget and disable `go test`'s own timeout (-timeout=0)
    # so it never preempts Ginkgo's graceful timeout + progress report —
    # Ginkgo is the single suite-timeout authority, matching the parallel
    # path's --timeout above.
    echo "[e2e-shared] running ginkgo suite (suite timeout ${GO_TEST_TIMEOUT})"
    go test -tags=e2e ./test/e2e/ -timeout=0 "${GINKGO_ARGS[@]}" -ginkgo.timeout="${GO_TEST_TIMEOUT}" -ginkgo.v
  fi
}

# Stage order is identical to the pre-refactor top-level flow. The
# cleanup trap is registered after run-id + context resolution (whose
# config-validation exits must NOT run cleanup — nothing is created yet)
# and before the first resource-creating stage, exactly as before.
main() {
  setup_run_id
  resolve_config
  trap cleanup EXIT
  configure_image_args
  preflight
  install_crds
  install_operator
  apply_network_policies
  run_suite
}

main "$@"
