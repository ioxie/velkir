#!/usr/bin/env bash
# Resolve the digest of the operator image produced by the release build,
# tolerating a Harbor immutable-tag (HTTP 412) re-push.
#
# A v* release pipeline can be re-run after a partial failure — e.g. the image
# pushed successfully but a later attestation step failed. On the re-run,
# BuildKit attempts to push the same tag again and Harbor rejects it with 412
# (immutable_tags), hard-failing the build job and writing no metadata file —
# so the sign/attest stages have no digest to act on and the release can never
# be completed. The chart path already decouples push from digest resolution
# (helm pull resolves the digest whether the chart was freshly pushed or
# already present); this gives the image path the same property.
#
# Digest source by situation:
#   - fresh push:  BuildKit's --metadata-file `containerimage.digest`.
#   - 412 re-push: a registry query for the EXISTING tag's digest.
# Signing the already-published artifact is the correct behaviour: a Go build is
# not bit-reproducible here (no -trimpath / SOURCE_DATE_EPOCH), so re-deriving
# the digest from a local rebuild would sign a different manifest than the one
# Harbor actually serves for the tag.
#
# Usage:
#   resolve-image-digest.sh <image-ref:tag> <build-rc> <build-log> <metadata-file>
#
# Emits the resolved digest (sha256:...) to stdout; all diagnostics go to
# stderr so the caller can capture the digest with $(...). Exits non-zero when
# no digest can be resolved (fail-closed — never emits an empty digest).
#
# The registry-query command defaults to `crane digest` (which reads
# $HOME/.docker/config.json, wired via .tools:docker-creds). Override it with
# $DIGEST_RESOLVER for testing; it is invoked as `$DIGEST_RESOLVER <image-ref>`
# and must print the digest to stdout.
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <image-ref:tag> <build-rc> <build-log> <metadata-file>" >&2
  exit 2
fi

image_ref="$1"
build_rc="$2"
build_log="$3"
metadata_file="$4"

resolver="${DIGEST_RESOLVER:-crane digest}"

digest=""

# Fresh push: BuildKit recorded the pushed manifest digest in the metadata file.
if [ "$build_rc" -eq 0 ] && [ -s "$metadata_file" ]; then
  digest=$(jq -r '."containerimage.digest" // empty' "$metadata_file")
  if [ -n "$digest" ]; then
    echo "resolved digest from BuildKit metadata (fresh push): ${digest}" >&2
  fi
fi

# Re-push rejected as immutable: recover the existing tag's digest from the
# registry rather than failing the release at the push stage.
if [ -z "$digest" ] && [ "$build_rc" -ne 0 ]; then
  if grep -qiE '412|immutable' "$build_log"; then
    echo "build push hit a Harbor immutable-tag (412) rejection; resolving the existing ${image_ref} digest from the registry" >&2
    digest=$($resolver "$image_ref" || true)
    if [ -n "$digest" ]; then
      echo "resolved digest from registry (existing tag): ${digest}" >&2
    fi
  else
    echo "build failed for a non-412 reason (rc=${build_rc}); not attempting digest recovery" >&2
  fi
fi

if [ -z "$digest" ] || [ "$digest" = "null" ]; then
  echo "ERROR: could not resolve a digest for ${image_ref} (build_rc=${build_rc})" >&2
  exit 1
fi

case "$digest" in
  sha256:*) ;;
  *)
    echo "ERROR: resolved digest '${digest}' is not a sha256:... reference" >&2
    exit 1
    ;;
esac

printf '%s\n' "$digest"
