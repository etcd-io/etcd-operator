# E2E Testing

- [E2E Testing](#e2e-testing)
  - [Overview](#overview)
  - [System Requirements](#system-requirements)
  - [Test Options](#test-options)
  - [Writing Tests](#writing-tests)
    - [Test Suite Flow](#test-suite-flow)
    - [When to write e2e tests](#when-to-write-e2e-tests)
    - [Test Coverage](#test-coverage)


## Overview

E2E validate end to end deployment of the etcd operator and validate different etcd cluster configuration options.

The e2e test suite is build on the the Kubernetes [e2e-framework](https://github.com/kubernetes-sigs/e2e-framework) project.

The test suite uses [Kind](https://kind.sigs.k8s.io/) to create a disposable Kubernetes cluster to deploy the operator and execute various test scenarios.

The e2e test suite is executed by running the command `make test-e2e`. Refer to [Test Options](#test-options) to view test configuration options.

## System Requirements
To run e2e tests your system will need a container runtime such as Docker or Podman and Kind installed.

Kind can be installed using the command `make kind`.

## Test Options
There are a number of options available when executing the test suite. These are provided as arguments for the `make test-e2e` command. 

| Argument | Description | Default Value |
| -------- | ----------- | ------------- |
| IMG | Specify a custom image registry and name | controller:latest |
| CONTAINER_TOOL | Specify the container tool being used | docker |
| PROMETHEUS_INSTALL_SKIP | Skip prometheus operator install by using `PROMETHEUS_INSTALL_SKIP=true` | false |
| CERT_MANAGER_INSTALL_SKIP | Skip certificate manager operator install by using `CERT_MANAGER_INSTALL_SKIP=true` | false |

## Writing Tests
The Kubernetes e2e test framework is built to use the native Go testing API and as such the tests are written in a very similar structure.

### Test Suite Flow
Below is a high level overview of the process flow when running the test suit.

1. Create Kind cluster
2. Build operator container image and create manifests
   1. If using podman a temporary image archive is created
3. Upload image / image archive to Kind cluster
4. Deploy Prometheus Operator if not skipped
5. Deploy Cert Manager Operator if not skipped 
6. Deploy etcd operator 
7. Wait for etcd operator to be ready
8. Run tests
9. Delete Kind cluster

### When to write e2e tests
You should create new e2e tests if you have added a feature to the operator.

If you have updated an existing feature you should either update existing e2e tests for that feature or create a new test to cover the updated functionality. Some features may have multiple options which cannot be covered in a single test suite, therefore multiple tests may be required.

### Test Coverage
To validate e2e functionality it is important to consider the scope of your test suite.

The basic level your tests should check the following:
- Does the CR get created?
- Does the statefulSet get created?
- Does the statefulSet resource contain the expected settings?
- Do the correct amount of pods get created?
- Do the pod(s) become ready? 
- Do the pods contain the correct settings?

Depending on the functionality being tested your test may need to perform additional tasks that validate configuration within the container or etcd itself.