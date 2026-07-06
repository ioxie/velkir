#!/usr/bin/env bash
# Guard test for resolve-image-digest.sh. Exercises the three decision paths
# without a live registry — fresh push, 412 immutable re-push, and a non-412
# build failure — so a regression that lets the release silently lose its
# digest (or recover one it shouldn't) is caught in CI. The registry query is
# stubbed via $DIGEST_RESOLVER; jq is exercised for real on the metadata path.
#
# This is the unit-level guard for the "a re-run of the same v* tag reaches the
# attestation stage" invariant. Full end-to-end validation (an actual
# duplicate-tag pipeline against Harbor) remains an RC-release concern.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
script="${here}/resolve-image-digest.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail=0
check() { # <name> <expected> <actual>
  if [ "$2" = "$3" ]; then
    echo "ok   - $1"
  else
    echo "FAIL - $1: expected '$2', got '$3'"
    fail=1
  fi
}
check_rc() { # <name> <expected-rc> <actual-rc>
  if [ "$2" -eq "$3" ]; then
    echo "ok   - $1"
  else
    echo "FAIL - $1: expected rc $2, got $3"
    fail=1
  fi
}

img="harbor.example/velkir/manager:v9.9.9"
fresh_digest="sha256:1111111111111111111111111111111111111111111111111111111111111111"
existing_digest="sha256:2222222222222222222222222222222222222222222222222222222222222222"

# A resolver stub that prints the existing-tag digest, asserting it was handed
# the image ref. Used by the 412 path.
stub="${work}/stub-resolver.sh"
cat > "$stub" <<EOF
#!/usr/bin/env bash
[ "\$1" = "${img}" ] || { echo "stub: wrong ref \$1" >&2; exit 3; }
echo "${existing_digest}"
EOF
chmod +x "$stub"

# 1. Fresh push: build rc=0, metadata carries the digest, resolver not consulted.
meta="${work}/meta.json"
printf '{"containerimage.digest":"%s"}' "$fresh_digest" > "$meta"
log="${work}/build.log"
echo "pushing manifest sha256:... done" > "$log"
out=$(DIGEST_RESOLVER="${work}/should-not-run.sh" "$script" "$img" 0 "$log" "$meta")
check "fresh push uses BuildKit metadata digest" "$fresh_digest" "$out"

# 2. 412 re-push: build rc!=0, log shows 412, empty metadata → registry recovery.
empty_meta="${work}/empty.json"
: > "$empty_meta"
log412="${work}/build412.log"
echo "error: failed to push image: unexpected status from PUT request: 412 Precondition Failed" > "$log412"
out=$(DIGEST_RESOLVER="$stub" "$script" "$img" 1 "$log412" "$empty_meta")
check "412 re-push recovers the existing tag digest from the registry" "$existing_digest" "$out"

# 3. Non-412 build failure: no recovery attempted, fail closed.
logbad="${work}/buildbad.log"
echo "error: failed to solve: dockerfile parse error on line 3" > "$logbad"
set +e
DIGEST_RESOLVER="$stub" "$script" "$img" 1 "$logbad" "$empty_meta" >/dev/null 2>&1
rc=$?
set -e
check_rc "non-412 build failure does not recover (fails closed)" 1 "$rc"

# 4. 412 but the registry can't return a digest → fail closed, never emit empty.
set +e
DIGEST_RESOLVER="true" "$script" "$img" 1 "$log412" "$empty_meta" >/dev/null 2>&1
rc=$?
set -e
check_rc "412 with unresolvable registry digest fails closed" 1 "$rc"

# 5. A resolver that returns a non-sha256 string is rejected (guards against a
#    stray log line being mistaken for a digest).
junk="${work}/junk-resolver.sh"
printf '#!/usr/bin/env bash\necho "not-a-digest"\n' > "$junk"
chmod +x "$junk"
set +e
DIGEST_RESOLVER="$junk" "$script" "$img" 1 "$log412" "$empty_meta" >/dev/null 2>&1
rc=$?
set -e
check_rc "non-sha256 resolver output is rejected" 1 "$rc"

if [ "$fail" -ne 0 ]; then
  echo "RESULT: FAILURES"
  exit 1
fi
echo "RESULT: all resolve-image-digest guard tests passed"
