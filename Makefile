# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.31.0
# The version of ETCD to run e2e tests against
E2E_ETCD_VERSION ?= $(shell go list -m -f {{.Version}} go.etcd.io/etcd/api/v3)

# Adapted from k/k, simplified to use the go.mod and the Makefile:
# https://github.com/kubernetes/kubernetes/blob/17854f0e0a153b06f9d0db096e2cd8ab2fa89c11/hack/lib/golang.sh#L510-L520
# Set the GOTOOLCHAIN to force the toolchain defined in go.mod
GOTOOLCHAIN ?= auto
ifeq (auto,$(GOTOOLCHAIN)) # User didn't specify the GOTOOLCHAIN, or is set to auto.
ifeq (,$(FORCE_HOST_GO)) # User didn't provide FORCE_HOST_GO, use go.mod's toolchain
	export GOTOOLCHAIN=$(shell grep '^toolchain go' go.mod | cut -d' ' -f2)
else # User provided FORCE_HOST_GO, use the local version
	export GOTOOLCHAIN=local
endif
endif

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
ARCHIVE_NAME ?= etcdOperator.tar

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
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet envtest helm-tool ## Run tests.
	ENVTEST_K8S_VERSION=$(ENVTEST_K8S_VERSION) \
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
	PATH="$(LOCALBIN):$(PATH)" \
	go test -v $$(go list ./... | grep -v /e2e) -coverprofile cover.out
	@echo "==> Validating Helm Chart generation..."
	$(MAKE) helm
	@echo "==> Running Helm lint..."
	$(HELM) lint helm/
	@echo "==> Testing Helm template rendering..."
	$(HELM) template test helm/ > /dev/null
	@echo "✅ All tests passed including Helm validation!"

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# Prometheus and CertManager are installed by default; skip with:
# - PROMETHEUS_INSTALL_SKIP=true
# - CERT_MANAGER_INSTALL_SKIP=true
# 
# DEPLOY_METHOD controls the deployment method for E2E tests:
# - all (default): Test both deployment methods sequentially
# - kustomize: Deploy using Kustomize only
# - helm: Deploy using Helm Chart only
DEPLOY_METHOD ?= all

.PHONY: test-e2e
test-e2e: generate fmt vet kind helm-tool ## Run the e2e tests. Expected an isolated environment using Kind.
	@if [ "$(DEPLOY_METHOD)" = "all" ]; then \
		echo "==> Testing all deployment methods..."; \
		echo ""; \
		echo "==> [1/2] Testing Kustomize deployment"; \
		DEPLOY_METHOD=kustomize ETCD_VERSION="$(E2E_ETCD_VERSION)" PATH="$(LOCALBIN):$(PATH)" go test ./test/e2e/ -v || exit 1; \
		echo ""; \
		echo "==> [2/2] Testing Helm deployment"; \
		$(MAKE) helm; \
		DEPLOY_METHOD=helm ETCD_VERSION="$(E2E_ETCD_VERSION)" PATH="$(LOCALBIN):$(PATH)" go test ./test/e2e/ -v || exit 1; \
		echo ""; \
		echo "✅ All deployment methods tested successfully!"; \
	else \
		echo "==> Testing with DEPLOY_METHOD=$(DEPLOY_METHOD)"; \
		ETCD_VERSION="$(E2E_ETCD_VERSION)" PATH="$(LOCALBIN):$(PATH)" go test ./test/e2e/ -v; \
	fi

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: verify-mod-tidy
verify-mod-tidy:  ## Ensure that running go mod tidy has a clean exit. 
	@echo "Verifying go.mod tidy"
	@out=$$(go mod tidy --diff); \
	test -z "$$out" || (echo "go.mod is not tidy. Please run 'go mod tidy'"; echo "$$out"; exit 1)

	@echo "Verifying tools/mod/go.mod tidy"
	@out=$$(cd tools/mod && go mod tidy --diff); \
	test -z "$$out" || (echo "tools/mod/go.mod is not tidy. Please run 'cd tools/mod && go mod tidy'"; echo "$$out"; exit 1)

.PHONY: verify
verify: verify-mod-tidy lint ## Run static checks against the code.

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
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

.PHONY: docker-save
docker-save: ## Save image as archive
	$(CONTAINER_TOOL) save -o ${ARCHIVE_NAME} ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name etcd-operator-builder
	$(CONTAINER_TOOL) buildx use etcd-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm etcd-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install$(VERSION_SUFFIX).yaml

##@ Helm

.PHONY: helm
helm: manifests kustomize helmify yq helm-tool ## Generate Helm Chart from Kustomize manifests.
	@echo "==> Generating Helm Chart..."
	@mkdir -p helm
	$(KUSTOMIZE) build config/default | $(HELMIFY) -crd-dir -image-pull-secrets helm
	@echo "==> Updating Chart metadata..."
	$(YQ) -i '.name = "etcd-operator"' helm/Chart.yaml
	$(YQ) -i '.description = "Official Kubernetes operator for etcd"' helm/Chart.yaml
	$(YQ) -i '.home = "https://github.com/etcd-io/etcd-operator"' helm/Chart.yaml
	@echo "==> Validating generated Chart..."
	@$(HELM) lint helm/ || (echo "❌ Helm Chart validation failed!" && exit 1)
	@echo "✅ Helm Chart generated and validated at helm/"

.PHONY: helm-lint
helm-lint: helm ## Lint the generated Helm Chart.
	@echo "==> Linting Helm Chart..."
	@$(HELM) lint helm/
	@echo "✅ Helm Chart validation passed!"

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Documentation

.PHONY: api-docs
api-docs: crd-ref-docs ## Generate api references docs.
	$(CRD_REF_DOCS) \
		--source-path=./api \
		--renderer=markdown \
		--output-path=./docs/api-references/docs.md \
		--templates-dir=./docs/api-references/template/ \
		--config=./docs/api-references/config.yaml

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
CRD_REF_DOCS ?= $(LOCALBIN)/crd-ref-docs
KIND ?= $(LOCALBIN)/kind
HELMIFY ?= $(LOCALBIN)/helmify
YQ ?= $(LOCALBIN)/yq
HELM ?= $(LOCALBIN)/helm

## Tool Versions
KUSTOMIZE_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} sigs.k8s.io/kustomize/kustomize/v5)
CONTROLLER_TOOLS_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} sigs.k8s.io/controller-tools)
ENVTEST_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} sigs.k8s.io/controller-runtime/tools/setup-envtest)
GOLANGCI_LINT_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} github.com/golangci/golangci-lint/v2)
CRD_REF_DOCS_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} github.com/elastic/crd-ref-docs)
KIND_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} sigs.k8s.io/kind)
HELMIFY_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} github.com/arttor/helmify)
YQ_VERSION = $(shell cd tools/mod && go list -m -f {{.Version}} github.com/mikefarah/yq/v4)
# Helm is not a Go module, must specify version directly
HELM_VERSION ?= v3.17.0

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: crd-ref-docs
crd-ref-docs: $(CRD_REF_DOCS) ## Install crd-ref-docs tool, ref: https://github.com/elastic/crd-ref-docs.
$(CRD_REF_DOCS): $(LOCALBIN)
	$(call go-install-tool,$(CRD_REF_DOCS),github.com/elastic/crd-ref-docs,$(CRD_REF_DOCS_VERSION))

.PHONY: kind
kind: $(KIND) ## Download kind locally if necessary.
$(KIND): $(LOCALBIN)
	$(call go-install-tool,$(KIND),sigs.k8s.io/kind,$(KIND_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(LOCALBIN) $(GOLANGCI_LINT_VERSION)

.PHONY: helmify
helmify: $(HELMIFY) ## Download helmify locally if necessary.
$(HELMIFY): $(LOCALBIN)
	$(call go-install-tool,$(HELMIFY),github.com/arttor/helmify/cmd/helmify,$(HELMIFY_VERSION))

.PHONY: yq
yq: $(YQ) ## Download yq locally if necessary.
$(YQ): $(LOCALBIN)
	$(call go-install-tool,$(YQ),github.com/mikefarah/yq/v4,$(YQ_VERSION))

.PHONY: helm-tool
helm-tool: $(HELM) ## Download helm locally if necessary.
$(HELM): $(LOCALBIN)
	@[ -f "$(HELM)-$(HELM_VERSION)" ] || { \
	set -e; \
	echo "Downloading Helm $(HELM_VERSION)"; \
	rm -f $(HELM) || true; \
	OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
	ARCH=$$(uname -m); \
	case $$ARCH in \
		x86_64) ARCH=amd64 ;; \
		aarch64) ARCH=arm64 ;; \
	esac; \
	curl -sSL https://get.helm.sh/helm-$(HELM_VERSION)-$${OS}-$${ARCH}.tar.gz | \
		tar xz -C $(LOCALBIN) --strip-components=1 $${OS}-$${ARCH}/helm; \
	mv $(HELM) $(HELM)-$(HELM_VERSION); \
	}
	@ln -sf $(HELM)-$(HELM_VERSION) $(HELM)

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef
