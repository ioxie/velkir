#!/usr/bin/env bash
# Shared teardown helpers for the e2e-shared tooling: the cleanup trap in
# tools/e2e-shared.sh and the standalone tools/e2e-shared-cleanup.sh. Sourced
# by both, never executed directly.
#
# The functions read and write the global state both callers already
# maintain (RELEASE_NAME, OP_NS, TEST_NAMESPACES, KUBECTL_CONTEXT_ARG,
# HELM_CONTEXT_ARG). Those globals are assigned by the sourcing script —
# hence the file-level SC2154 disable below — and the helpers use plain
# assignment (no `local`/`declare`) so they stay global on the bash 3.2
# floor the scripts target (`declare -g` is unavailable there).
#
# shellcheck disable=SC2154  # RELEASE_NAME/OP_NS/TEST_NAMESPACES/*_CONTEXT_ARG are set by the sourcing script

# e2e_resolve_context_args sets the kubectl/helm context-flag arrays from
# KUBE_CONTEXT (empty arrays when it is unset). kubectl uses --context;
# helm uses --kube-context (different flag names for the same selection).
e2e_resolve_context_args() {
  KUBECTL_CONTEXT_ARG=()
  HELM_CONTEXT_ARG=()
  if [[ -n "${KUBE_CONTEXT:-}" ]]; then
    KUBECTL_CONTEXT_ARG=(--context "${KUBE_CONTEXT}")
    HELM_CONTEXT_ARG=(--kube-context "${KUBE_CONTEXT}")
  fi
}

# e2e_teardown emits the teardown sequence shared by both entry points:
# the per-namespace CR force-delete, the operator helm uninstall, the
# namespace deletes, and the release-instance-labelled cluster-scoped
# sweep. Every step is idempotent (--ignore-not-found / || true) so a
# partial-install state still unwinds. The conditional CRD removal differs
# between callers (e2e-shared.sh also helm-uninstalls its CRDs release;
# the manual script does not) and stays in each caller.
e2e_teardown() {
  # Delete test-namespace CRs first so finalizers run while the operator
  # is still up; otherwise namespace delete can stall on finalizers.
  for ns in "${TEST_NAMESPACES[@]}"; do
    kubectl "${KUBECTL_CONTEXT_ARG[@]}" -n "${ns}" delete valkey --all --grace-period=0 --force --ignore-not-found 2>/dev/null || true
    kubectl "${KUBECTL_CONTEXT_ARG[@]}" -n "${ns}" delete sentinelquorum --all --grace-period=0 --force --ignore-not-found 2>/dev/null || true
  done

  helm "${HELM_CONTEXT_ARG[@]}" uninstall "${RELEASE_NAME}" -n "${OP_NS}" --ignore-not-found 2>/dev/null || true

  for ns in "${TEST_NAMESPACES[@]}"; do
    kubectl "${KUBECTL_CONTEXT_ARG[@]}" delete ns "${ns}" --ignore-not-found --wait=false 2>/dev/null || true
  done
  kubectl "${KUBECTL_CONTEXT_ARG[@]}" delete ns "${OP_NS}" --ignore-not-found --wait=false 2>/dev/null || true

  # Any stray release-prefixed cluster-scoped resources (ClusterRole,
  # ClusterRoleBinding, WebhookConfigurations) — helm uninstall already
  # handles these but a sweeper guard catches partial-install leftovers.
  kubectl "${KUBECTL_CONTEXT_ARG[@]}" delete clusterrole,clusterrolebinding,validatingwebhookconfiguration,mutatingwebhookconfiguration \
    -l "app.kubernetes.io/instance=${RELEASE_NAME}" --ignore-not-found 2>/dev/null || true
}
