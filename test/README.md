# Running E2E Tests Locally

The etcd-operator includes end-to-end (e2e) tests that validate the operator's functionality in a Kubernetes cluster. These tests use the [e2e-framework](https://github.com/kubernetes-sigs/e2e-framework) and can be run locally using Kind (Kubernetes in Docker).

## Prerequisites

- go version v1.24+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

## Quick Start

The simplest way to run e2e tests is using the Makefile target from the project root:

```sh
make test-e2e
```

This command will:
1. Generate necessary code and manifests
2. Install Kind (if not already present)
3. Enable gofail for failure injection testing
4. Create a Kind cluster
5. Build and load the operator Docker image
6. Install Prometheus Operator (for metrics testing)
7. Deploy the etcd-operator
8. Run all e2e tests
9. Clean up resources

## Configuration Options

### Using an Existing Kind Cluster

To reuse an existing Kind cluster instead of creating a new one:

```sh
export TEST_KIND_CLUSTER=my-existing-cluster
make test-e2e
```

When using an existing cluster, the tests will:
- Skip cluster creation/deletion
- Reuse the operator image if already loaded
- Skip Prometheus installation if already present
- Preserve the cluster after tests complete

### Skipping Teardown

To keep the test environment running after tests complete (useful for debugging):

```sh
export ETCD_E2E_SKIP_TEARDOWN=true
make test-e2e
```

This will preserve:
- The Kind cluster
- Deployed operator and CRDs
- Test namespaces and resources
- Prometheus and cert-manager installations

### Running Individual Test Suites

To run specific e2e test files:

```sh
# Run only auto provider tests
go test ./test/e2e/ -v -run TestAutoProvider

# Run only cert-manager tests
go test ./test/e2e/ -v -run TestCertManager

# Run data persistence tests
go test ./test/e2e/ -v -run TestDataPersistence

# Run etcd options tests
go test ./test/e2e/ -v -run TestEtcdOptions
```

## Manual Setup for Development

For iterative development and debugging, you can set up the environment manually:

1. Create a Kind Cluster

```sh
kind create cluster --name etcd-cluster --image kindest/node:v1.32.0
```

2. Build and Load the Operator Image

```sh
make docker-build IMG=etcd-operator:v0.1
kind load docker-image etcd-operator:v0.1 --name etcd-cluster
```

3. Install CRDs

```sh
make install
```

4. Deploy the Operator

```sh
make deploy DEPLOY_MODE=e2e IMG=etcd-operator:v0.1
```

5. Run Tests with Existing Cluster

```sh
export TEST_KIND_CLUSTER=etcd-cluster
export ETCD_E2E_SKIP_TEARDOWN=true
go test ./test/e2e/ -v
```

6. Clean Up When Done

```sh
make undeploy DEPLOY_MODE=e2e
make uninstall
kind delete cluster --name etcd-cluster
```

### Test Coverage

The e2e test suite includes:

- **Auto Provider Tests** (`auto_provider_test.go`): Self-signed certificate generation and management
- **Cert-Manager Tests** (`cert_manager_test.go`): Integration with cert-manager for certificate provisioning
- **Data Persistence Tests** (`datapersistence_test.go`): Verification of etcd data persistence across pod restarts
- **etcd Options Tests** (`etcd_options_test.go`): Custom etcd configuration options
- **Basic Cluster Operations** (`e2e_test.go`): Creation, scaling, and deletion of etcd clusters
