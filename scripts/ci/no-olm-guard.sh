#!/usr/bin/env bash
# no-olm-guard.sh -- fail if the tree carries any OperatorHub / OLM artifact.
#
# The OperatorHub/OLM deferral ADR (docs/decisions/0001-operatorhub-olm-deferral.md)
# records that no OLM ClusterServiceVersion / OperatorHub bundle / operatorhub.io
# metadata ships at genesis. This guard enforces that automatically so the
# deferral cannot regress silently. docs/decisions/ is excluded: the ADR
# legitimately names these tokens.
#
# Detected:
#   - content `kind: ClusterServiceVersion` (an OLM CSV)
#   - content `operatorhub.io` (OperatorHub metadata, e.g. bundle annotations)
#   - a `bundle.Dockerfile` (the OperatorHub bundle image build file)
#   - a `bundle/manifests` or `bundle/metadata` directory (the bundle layout)
#
# Usage: no-olm-guard.sh [tree]   (default: the current directory)
# Exit:  0 clean; 1 an OperatorHub/OLM artifact was found; 2 usage/IO error.
set -euo pipefail

tree="${1:-.}"
[ -d "$tree" ] || { printf 'no-olm-guard: not a directory: %s\n' "$tree" >&2; exit 2; }

# Drop from every match list the .git store, the ADR directory (docs/decisions/
# names these tokens by design), and this guard + its harness (they name the
# tokens in order to detect/test them, not as OLM artifacts).
in_scope() {
  grep -vE '(^|/)\.git/|(^|/)docs/decisions/|/no-olm-guard\.sh$|/test_no_olm_guard\.sh$' || true
}

problems=""

# `|| true` on each pipeline so a no-match grep (exit 1 under pipefail) is "clean",
# not a fatal error under `set -e`.
content="$(grep -rIlE 'kind:[[:space:]]*ClusterServiceVersion|operatorhub\.io' "$tree" \
             --exclude-dir=.git 2>/dev/null | in_scope || true)"
[ -n "$content" ] && problems="${problems}"$'\n'"  ClusterServiceVersion / operatorhub.io content:"$'\n'"$content"

paths="$(
  {
    find "$tree" -name .git -prune -o -name bundle.Dockerfile -print 2>/dev/null
    find "$tree" -name .git -prune -o -type d -path '*/bundle/manifests' -print 2>/dev/null
    find "$tree" -name .git -prune -o -type d -path '*/bundle/metadata' -print 2>/dev/null
  } | in_scope || true
)"
[ -n "$paths" ] && problems="${problems}"$'\n'"  OperatorHub bundle artifact path(s):"$'\n'"$paths"

if [ -n "$problems" ]; then
  printf 'no-olm-guard: OperatorHub/OLM artifact(s) found -- the OLM deferral (docs/decisions/0001) forbids these at genesis:%s\n' "$problems" >&2
  exit 1
fi
printf 'no-olm-guard: clean -- no OperatorHub/OLM artifacts under %s\n' "$tree"
