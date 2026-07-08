#!/usr/bin/env bash
# Manual cleanup for an e2e-shared run left in place via KEEP_ON_FAILURE.
#
# Usage:
#   tools/e2e-shared-cleanup.sh <release-name> <operator-namespace> <test-namespace-csv>
#
# <test-namespace-csv> may be a single namespace (serial-mode runs) or
# a comma-separated list of per-process namespaces (parallel-mode
# runs, e.g. "valkey-e2e-test-<run-id>-p1,...-p4").
#
# Same teardown sequence the e2e-shared.sh trap runs; idempotent. Use
# this when a previous run failed with KEEP_ON_FAILURE=true and you've
# finished triaging the cluster state.

set -eo pipefail

# Shared teardown helpers (e2e_resolve_context_args + e2e_teardown),
# also used by tools/e2e-shared.sh.
# shellcheck source=e2e-teardown-lib.sh disable=SC1091
source "$(dirname -- "${BASH_SOURCE[0]}")/e2e-teardown-lib.sh"

if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "usage: $0 <release-name> <operator-namespace> <test-namespace-csv> [--remove-crds]" >&2
  echo "" >&2
  echo "  <test-namespace-csv>  one or more test namespaces, comma-separated" >&2
  echo "                        (parallel-mode runs use per-process ns names)" >&2
  echo "  --remove-crds  also kubectl-delete the Valkey + SentinelQuorum CRDs" >&2
  echo "                 (only pass this if you're sure they were installed by" >&2
  echo "                 the failed run — never if a separate operator install" >&2
  echo "                 on this cluster relies on them)" >&2
  exit 1
fi

RELEASE_NAME="$1"
OP_NS="$2"
# Split the comma-separated list into an array. macOS bash 3.2 lacks
# `mapfile`, so `read -ra` is the portable form. Trim no whitespace
# inside fields — namespace names are well-formed.
IFS=',' read -ra TEST_NAMESPACES <<<"$3"
REMOVE_CRDS="${4:-}"

e2e_resolve_context_args

echo "[cleanup] release=${RELEASE_NAME} op-ns=${OP_NS} test-ns=${TEST_NAMESPACES[*]}"

e2e_teardown

if [[ "${REMOVE_CRDS}" == "--remove-crds" ]]; then
  echo "[cleanup] --remove-crds passed: deleting Valkey + SentinelQuorum CRDs"
  kubectl "${KUBECTL_CONTEXT_ARG[@]}" delete crd \
    valkeys.velkir.ioxie.dev sentinelquorums.velkir.ioxie.dev \
    --ignore-not-found --timeout=30s 2>/dev/null || true
fi

echo "[cleanup] done"
