#!/usr/bin/env bash
# One-command shared e2e against a local minikube cluster.
#
# Wraps tools/e2e-shared.sh with everything a minikube run needs, so there are
# no env vars to remember AND no way to target the wrong cluster:
#
#   - ensures a minikube cluster exists (vfkit driver, sized) and is running
#   - builds an arch-correct operator image: the binary is cross-compiled on
#     the host (Go toolchain already present) and packaged in a minimal runtime
#     image. The published operator image is linux/amd64-only and won't run on
#     an arm64 / Apple-Silicon minikube node, so a local build is required.
#   - PINS THE WHOLE RUN TO MINIKUBE VIA A DEDICATED KUBECONFIG. This is
#     load-bearing: the Go test suite runs bare `kubectl` (no --context) and so
#     targets the kubeconfig's current-context. Setting KUBE_CONTEXT alone does
#     NOT constrain the suite — it only steers tools/e2e-shared.sh's own
#     commands. So we write a minimal, minikube-only kubeconfig (whose
#     current-context IS the minikube profile) and point KUBECONFIG at it for
#     the whole run. The user's real kubeconfig is never mutated (its
#     current-context is restored on exit in case `minikube start` flipped it).
#   - points the PVC-resize fixtures at minikube's hostpath provisioner and
#     keeps the resize happy-path spec skipped (no resize-capable CSI here)
#
# GINKGO_FOCUS / GINKGO_SKIP / KEEP_ON_FAILURE / GINKGO_PROCS and the other
# tools/e2e-shared.sh knobs are read straight from the environment.
#
# Env overrides (all optional):
#   MINIKUBE_PROFILE    minikube profile name            (default: minikube)
#   MINIKUBE_DRIVER     driver for a fresh cluster        (default: vfkit)
#   MINIKUBE_CPUS       cpus for a fresh cluster          (default: 4)
#   MINIKUBE_MEMORY     memory (MB) for a fresh cluster   (default: 8192)
#   E2E_MINIKUBE_IMAGE  operator image built in-cluster   (default: velkir:e2e)
#   IMAGE_REPOSITORY + IMAGE_TAG
#                       if BOTH are set, the in-cluster build is skipped and
#                       that image is used as-is. Otherwise it is built locally.
#
# `-u` is intentionally NOT set (bash 3.2 / empty-array expansion), matching
# tools/e2e-shared.sh.
#
# NOTE: the cluster start guard below recovers a *stopped* profile via
# `minikube start`, but a wedged / paused / half-degraded profile can read as
# running-yet-unrecoverable and slip past it. If a reused profile misbehaves,
# `minikube delete -p "$PROFILE"` it and re-run for a clean cluster.
set -eo pipefail

PROFILE="${MINIKUBE_PROFILE:-minikube}"
DRIVER="${MINIKUBE_DRIVER:-vfkit}"
CPUS="${MINIKUBE_CPUS:-4}"
MEMORY="${MINIKUBE_MEMORY:-8192}"
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

command -v minikube >/dev/null 2>&1 || {
  echo "[e2e-minikube] minikube is not on PATH — install it first" >&2
  exit 2
}
command -v kubectl >/dev/null 2>&1 || {
  echo "[e2e-minikube] kubectl is not on PATH" >&2
  exit 2
}

# --- cleanup ---------------------------------------------------------------
# One trap restores the user's kubeconfig context and removes everything we
# created (temp build dir, pinned kubeconfig). Variables are populated as the
# run progresses; each step is guarded so a partial run still cleans up.
ORIG_KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
ORIG_CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
BUILD_DIR=""
PINNED_KUBECONFIG=""
cleanup() {
  set +e  # never let one failed cleanup step (under set -e) abort the rest
  [[ -n "$BUILD_DIR" ]] && rm -rf "$BUILD_DIR"
  [[ -n "$PINNED_KUBECONFIG" ]] && rm -f "$PINNED_KUBECONFIG"
  # Restore the user's real current-context (minikube start may have flipped it).
  if [[ -n "$ORIG_CONTEXT" ]]; then
    KUBECONFIG="$ORIG_KUBECONFIG" kubectl config use-context "$ORIG_CONTEXT" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# 1. Ensure the cluster is up (create it if the profile isn't running).
if ! minikube -p "$PROFILE" status >/dev/null 2>&1; then
  echo "[e2e-minikube] starting minikube profile '$PROFILE' (driver=$DRIVER cpus=$CPUS memory=${MEMORY}MB)"
  minikube start -p "$PROFILE" --driver="$DRIVER" --cpus="$CPUS" --memory="$MEMORY"
else
  echo "[e2e-minikube] reusing running minikube profile '$PROFILE'"
fi

# 2. Operator image (arch-correct local build unless the caller pinned one).
if [[ -n "${IMAGE_REPOSITORY:-}" && -n "${IMAGE_TAG:-}" ]]; then
  echo "[e2e-minikube] using caller-pinned image ${IMAGE_REPOSITORY}:${IMAGE_TAG} (skipping build)"
  # Fail fast if the caller pinned an image but never loaded it into the node:
  # otherwise the omission surfaces a step later as an ImagePullBackOff at
  # helm-install, a step removed from the cause.
  #
  # Match the whole repo:tag entry, not a substring: a superstring tag
  # (e.g. ...:v1.0.1 loaded while the pinned ...:v1.0 is absent) false-passes
  # a plain `grep -F` and skips the load. Regex-escape the needle (repo paths
  # carry `.` and `/`) and anchor the tag to end-of-entry; the `(^|/)` prefix
  # tolerates a registry/namespace prefix that `minikube image ls` may print.
  needle="$(printf '%s' "${IMAGE_REPOSITORY}:${IMAGE_TAG}" | sed 's/[][\.*^$/]/\\&/g')"
  if ! minikube -p "$PROFILE" image ls | grep -qE "(^|/)${needle}\$"; then
    echo "[e2e-minikube] pinned image ${IMAGE_REPOSITORY}:${IMAGE_TAG} not found in minikube profile '$PROFILE' — load it first (e.g. 'minikube -p $PROFILE image load ${IMAGE_REPOSITORY}:${IMAGE_TAG}')" >&2
    exit 2
  fi
else
  IMG="${E2E_MINIKUBE_IMAGE:-velkir:e2e}"
  node_arch="$(minikube -p "$PROFILE" ssh -- uname -m 2>/dev/null | tr -d '[:space:]')"
  case "$node_arch" in
    aarch64 | arm64) goarch=arm64 ;;
    x86_64 | amd64) goarch=amd64 ;;
    *)
      echo "[e2e-minikube] unrecognised node arch '$node_arch'; defaulting to arm64" >&2
      goarch=arm64
      ;;
  esac
  # Cross-compile on host + package in a minimal runtime image. We avoid
  # `minikube image build` on the repo Dockerfile: its host-side context tar
  # mishandles the allow-list .dockerignore and drops subdir source. The
  # runtime stage mirrors ./Dockerfile.
  BUILD_DIR="$(mktemp -d)"
  echo "[e2e-minikube] cross-compiling operator (linux/$goarch) on host"
  ( cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" \
      go build -ldflags="-X main.version=e2e" -o "$BUILD_DIR/manager" ./cmd/main.go )
  cat > "$BUILD_DIR/Dockerfile" <<'DOCKERFILE'
# Minimal runtime image for local e2e — mirrors the final stage of ./Dockerfile.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
DOCKERFILE
  echo "[e2e-minikube] building runtime image '$IMG' inside minikube (linux/$goarch)"
  minikube -p "$PROFILE" image build -t "$IMG" "$BUILD_DIR"
  rm -rf "$BUILD_DIR"; BUILD_DIR=""
  export IMAGE_REPOSITORY="${IMG%:*}"
  export IMAGE_TAG="${IMG##*:}"
fi

# 3. Pin the WHOLE run to minikube via a dedicated kubeconfig (see header).
#    `--minify --context` keeps only the minikube context, so this temp file
#    carries the local minikube creds, not the user's other (e.g. production)
#    cluster credentials.
PINNED_KUBECONFIG="$(mktemp -t e2e-minikube-kubeconfig.XXXXXX)"
kubectl config view --raw --minify --flatten --context "$PROFILE" > "$PINNED_KUBECONFIG"
export KUBECONFIG="$PINNED_KUBECONFIG"
export KUBE_CONTEXT="$PROFILE"

# Hard safety gate: refuse to run unless the effective current-context really
# is the minikube profile. Last line of defence against the suite (or
# tools/e2e-shared.sh) creating resources on the wrong cluster.
pinned_ctx="$(kubectl config current-context 2>/dev/null || true)"
if [[ "$pinned_ctx" != "$PROFILE" ]]; then
  echo "[e2e-minikube] FATAL: pinned current-context is '$pinned_ctx', expected '$PROFILE' — aborting" >&2
  exit 2
fi

# 4. PVC-resize fixtures: provisioner for the suite's synthetic StorageClasses
#    (the no-expand spec needs it; minikube's default provisioner differs from
#    the kind default the suite assumes). The resize HAPPY-PATH spec waits for
#    a real CSI status.capacity round-trip; minikube's hostpath provisioner has
#    no resize controller, so that spec can never pass here. Export
#    E2E_PVC_RESIZE_SC_EXPAND as explicitly EMPTY so tools/e2e-shared.sh does
#    not default it to its shared-cluster CSI SC (ceph-block): the suite then
#    creates its own synthetic SCs and the happy-path spec skips. The real-CSI
#    happy-path run stays on the e2e-shared/external path. A caller may still
#    set the var to an existing expansion-capable SC to opt the happy path in.
export E2E_PVC_RESIZE_PROVISIONER="${E2E_PVC_RESIZE_PROVISIONER:-k8s.io/minikube-hostpath}"
export E2E_PVC_RESIZE_SC_EXPAND="${E2E_PVC_RESIZE_SC_EXPAND:-}"

echo "[e2e-minikube] pinned context=$KUBE_CONTEXT (KUBECONFIG=$PINNED_KUBECONFIG)"
echo "[e2e-minikube] image=${IMAGE_REPOSITORY}:${IMAGE_TAG} pvc-provisioner=$E2E_PVC_RESIZE_PROVISIONER expand-sc=${E2E_PVC_RESIZE_SC_EXPAND:-<empty: happy-path skips>}"

# Run as a child (NOT exec) so the EXIT trap fires for cleanup; set -e
# propagates the suite's exit code.
"$REPO_ROOT/tools/e2e-shared.sh"
