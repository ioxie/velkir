# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases
# Mirror the canonical CRDs into the chart-CRDs install path so the
# chart can never drift from controller-gen's output.
	$(MAKE) sync-chart-crds

.PHONY: sync-chart-crds
sync-chart-crds: ## Mirror canonical CRDs from config/crd/bases/ to the chart's files/ directory.
# Keeps charts/velkir-crds/files/ in lockstep with the canonical
# controller-gen output. Wired into `manifests` above so a CRD-additive
# PR cannot drift the two copies — the chart-CRDs install path produces
# byte-identical manifests to a Flux Kustomization of config/crd/bases.
	cp config/crd/bases/velkir.ioxie.dev_valkeys.yaml \
	   charts/velkir-crds/files/valkeys.velkir.ioxie.dev.yaml
	cp config/crd/bases/velkir.ioxie.dev_sentinelquorums.yaml \
	   charts/velkir-crds/files/sentinelquorums.velkir.ioxie.dev.yaml

.PHONY: generate
generate: controller-gen applyconfiguration-gen ## Generate code containing DeepCopy/DeepCopyInto methods and apply-configuration builders for the project's CRDs.
# applyconfiguration-gen runs first so the controller package's imports of
# the apply-config builders resolve before controller-gen walks ./... and
# tries to typecheck them. controller-gen only writes DeepCopy methods on
# the CRD types under api/, so the order doesn't matter for cold builds —
# but if `api/v1beta1/applyconfiguration/` is deleted (e.g. for a clean
# regen) the controller-gen typecheck fails first without this ordering.
	"$(APPLYCONFIG_GEN)" \
		--output-dir api/v1beta1/applyconfiguration \
		--output-pkg github.com/ioxie/velkir/api/v1beta1/applyconfiguration \
		--go-header-file tools/boilerplate.go.txt \
		github.com/ioxie/velkir/api/v1beta1
	"$(CONTROLLER_GEN)" object:headerFile="tools/boilerplate.go.txt" paths="./..."
# Regenerate the events-catalog membership list (AllReasons) from the
# package's Reason consts so it can't drift from the declarations. Pure
# stdlib generator, no installed tool needed.
	go run ./tools/gen-reasons

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

# codegen-verify runs the shared regenerate-and-verify chain (manifests,
# generate, fmt, vet) consumed by the test/build/run targets, so the sequence
# lives in one place. Internal aggregate — no ## help text, so it stays out of
# `make help`.
.PHONY: codegen-verify
codegen-verify: manifests generate fmt vet

.PHONY: test
test: codegen-verify setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= velkir-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e codegen-verify ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: test-e2e-shared
test-e2e-shared: codegen-verify ## Run e2e against the current kubectl context (no kind, no docker build). See tools/e2e-shared.sh for env vars.
	@./tools/e2e-shared.sh

.PHONY: test-e2e-minikube
test-e2e-minikube: codegen-verify ## Run the shared e2e suite on a local minikube cluster (auto-starts vfkit, builds the operator image for the node arch). No env vars to remember; see tools/e2e-minikube.sh.
	@./tools/e2e-minikube.sh

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: codegen-verify ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: codegen-verify ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name velkir-builder
	$(CONTAINER_TOOL) buildx use velkir-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm velkir-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
APPLYCONFIG_GEN ?= $(LOCALBIN)/applyconfiguration-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
GINKGO ?= $(LOCALBIN)/ginkgo

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1
# Ginkgo CLI version is pinned to the same release as the test
# binary's onsi/ginkgo dependency (go.mod). The CLI orchestrates
# Ginkgo parallel mode (`--procs=N`); without it, `go test` runs the
# test binary as a single process. Used by tools/e2e-shared.sh when
# GINKGO_PROCS > 1.
GINKGO_VERSION ?= $(shell v='$(call gomodver,github.com/onsi/ginkgo/v2)'; \
  [ -n "$$v" ] || { echo "Set GINKGO_VERSION manually (ginkgo/v2 not in go.mod)" >&2; exit 1; }; \
  printf '%s\n' "$$v")
# CODEGEN_VERSION pins applyconfiguration-gen to the same client-go release
# the project depends on, so the generated apply-configs match the upstream
# typed-builder shapes we already use elsewhere (corev1ac, appsv1ac, etc.).
CODEGEN_VERSION ?= v0.35.0

# ENVTEST_VERSION pins the standalone setup-envtest sub-module.
# Upstream extracted tools/setup-envtest from controller-runtime's main
# module starting in v0.23, so the parent-module branch refs
# (release-0.X) no longer resolve to a package that exists. The sub-
# module has its own tag stream under tools/setup-envtest/<version>;
# Go's `@v0.24.0` resolves correctly via that prefix.
ENVTEST_VERSION ?= v0.24.0

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.8.0
PROMTOOL_VERSION ?= 3.1.0
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: applyconfiguration-gen
applyconfiguration-gen: $(APPLYCONFIG_GEN) ## Download applyconfiguration-gen locally if necessary.
$(APPLYCONFIG_GEN): $(LOCALBIN)
	$(call go-install-tool,$(APPLYCONFIG_GEN),k8s.io/code-generator/cmd/applyconfiguration-gen,$(CODEGEN_VERSION))

.PHONY: ginkgo
ginkgo: $(GINKGO) ## Download the ginkgo CLI locally if necessary.
$(GINKGO): $(LOCALBIN)
	$(call go-install-tool,$(GINKGO),github.com/onsi/ginkgo/v2/ginkgo,$(GINKGO_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: promtool-test
promtool-test: ## Run promtool unit tests against the PrometheusRule pack.
# promtool isn't auto-downloaded — prometheus' go.mod has replace
# directives that block `go install`, and fetching the prebuilt release
# tarball needs explicit operator approval. Install promtool yourself
# (https://prometheus.io/download/ or `brew install prometheus`) and
# ensure it's on PATH before invoking this target.
	@command -v promtool >/dev/null 2>&1 || { \
		echo "promtool not found on PATH. Install from https://prometheus.io/download/ (target version: $(PROMTOOL_VERSION))." >&2; \
		exit 1; \
	}
	cd tests/promtool && promtool test rules alerts.test.yaml

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef

##@ Project-specific

.PHONY: chart-lint
chart-lint: ## Lint both Helm charts (requires helm + helm-unittest plugin).
	helm lint charts/velkir charts/velkir-crds
	# Specs live in <chart>/unittests/, not helm-unittest's default
	# tests/ glob — without -f the runner matches 0 files and exits 0.
	helm unittest -f 'unittests/**/*_test.yaml' charts/velkir
	helm unittest -f 'unittests/**/*_test.yaml' charts/velkir-crds

.PHONY: check
check: lint test ## Run lint + unit + envtest fast gate locally.

##@ Publishing

.PHONY: replay
replay: ## Replay an accepted public GitHub PR onto a private contrib/<N> branch (maintainer contribution funnel). Usage: make replay PR=<N>; REPLAY_DRYRUN=1 runs hermetically against a throwaway fixture.
	@test -n "$(PR)" || { echo "make replay: PR is required — usage: make replay PR=<N>" >&2; exit 2; }
	@test -f tools/publish/replay-entrypoint.sh || { echo "make replay: maintainer-only — runs on the private development repo (tools/publish/ is not part of the public mirror)." >&2; exit 2; }
	@REPLAY_DRYRUN='$(REPLAY_DRYRUN)' tools/publish/replay-entrypoint.sh '$(PR)'
