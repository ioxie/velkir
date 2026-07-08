# Build the manager (operator) binary.
FROM golang:1.26 AS builder
# The golang image ships GOTOOLCHAIN=local; go.mod's `go 1.26.5` floor is the
# authoritative minimum (clears the stdlib vuln advisories), so allow the Go
# runtime to fetch a matching toolchain if the base image's bundled patch lags.
ENV GOTOOLCHAIN=auto
ARG TARGETOS
ARG TARGETARCH
# VERSION is the release tag the build is being run for (passed via
# --build-arg VERSION=$tag from release.yaml). Default "dev" applies
# to local `docker build` without the arg and keeps the in-binary
# stamp meaningful for dev builds.
ARG VERSION=dev

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the Go source (relies on .dockerignore to filter)
COPY . .

# Build
# the GOARCH has no default value to allow the binary to be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -a -ldflags="-X main.version=${VERSION}" -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary.
# Refer to https://github.com/GoogleContainerTools/distroless for more details.
#
# Pinned by digest so a re-tag of `:nonroot` upstream cannot silently
# change what we package on the next build. Refresh on every chart
# release pre-release pass: re-resolve via
#   crane digest gcr.io/distroless/static:nonroot
# (or the GCR token API), update the @sha256 below, and rebuild.
# `crane digest` returns the manifest-list digest, which is what we
# want for multi-arch — do not substitute single-platform variants
# like `regctl image digest --request-format manifest`, which would
# resolve a per-platform manifest and silently degrade the multi-arch
# build (only one architecture would resolve at pull time).
FROM gcr.io/distroless/static:nonroot@sha256:e3f945647ffb95b5839c07038d64f9811adf17308b9121d8a2b87b6a22a80a39
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
