#!/usr/bin/env bash
# Fail the PR check when a CRD-types diff lands without a matching
# docs/CHANGELOG.md entry. Wired into .gitlab/ci/mr-gates.yml (changelog-tag job).
#
# A CRD-types change is any modification to api/v1beta1/*types*.go
# excluding generated (zz_generated_*.go) and test (*_test.go) files —
# the additive-only contract documented in docs/upgrade.md needs a
# changelog entry per landing minor.
#
# Bypass labels: type::ci, type::chore (no schema impact). type::docs is
# NOT bypassed because doc-only PRs shouldn't touch api/v1beta1 at
# all; if they do, the changelog entry is still required.
#
# The workflow is responsible for fetching the base ref and producing
# the changed-file list — the runner doesn't ship `gh`. Pass the file
# list either as the first argument (newline-separated path) or via
# the CHANGED_FILES env var (newline-separated).

set -euo pipefail

LABELS_JSON="${LABELS_JSON:-[]}"

if printf '%s' "$LABELS_JSON" | grep -qE '"type::(ci|chore)"'; then
  echo "::notice::skip — type::ci or type::chore PR (no schema impact expected)"
  exit 0
fi

CHANGED="${CHANGED_FILES:-${1:-}}"
if [ -z "$CHANGED" ]; then
  echo "::error::no changed-file list provided (set CHANGED_FILES env or pass as arg)"
  exit 1
fi

TYPES_TOUCHED=$(printf '%s\n' "$CHANGED" \
  | grep -E '^api/v1beta1/.*types.*\.go$' \
  | grep -vE 'zz_generated_.*\.go$' \
  | grep -vE '_test\.go$' \
  || true)

if [ -z "$TYPES_TOUCHED" ]; then
  echo "::notice::no api/v1beta1 types.go changes; skip"
  exit 0
fi

if printf '%s\n' "$CHANGED" | grep -qx 'docs/CHANGELOG.md'; then
  echo "::notice::OK — types.go changed and docs/CHANGELOG.md updated"
  exit 0
fi

echo "::error::api/v1beta1 types.go changed but docs/CHANGELOG.md was not — add an entry to docs/CHANGELOG.md describing the schema change (additive-only contract)"
echo "files touched:"
printf '  %s\n' $TYPES_TOUCHED
exit 1
