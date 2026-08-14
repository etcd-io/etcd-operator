# E2E tests

The e2e suite uses [`sigs.k8s.io/e2e-framework`](https://github.com/kubernetes-sigs/e2e-framework)'s
`TestMain` (see [e2e_suite_test.go](e2e_suite_test.go)) to provision a KinD cluster and the
operator before any `Test*` function runs, and to tear it all down afterwards. `TestMain` always
runs on `go test`, even with `-run TestXXX` — there is no way to make `go test` skip it — so this
doc covers how to keep an already-prepared environment around and point individual test runs at it
instead of re-provisioning on every invocation.

## Fixed names

These are hardcoded in `e2e_suite_test.go`, not overridable via flags/env:

| What | Value |
|---|---|
| KinD cluster name | `etcd-cluster` |
| KinD node image | `kindest/node:v1.32.0` |
| Operator image | `etcd-operator:v0.1` |
| Namespace | `etcd-operator-system` |

`CONTAINER_TOOL` (env var, default `docker`) selects how the image is loaded into KinD; set it to
`podman` if that's your container runtime.

## Normal usage: one-shot run

```sh
make test-e2e
```

This builds the image, spins up KinD, installs Prometheus, deploys the operator, runs every test,
then tears everything down. Fine for CI or a full local run, but slow to iterate against.

## Iterating on a single test against a live environment

Two env vars gate the setup/teardown steps in `TestMain`:

- `ETCD_E2E_SKIP_TEARDOWN=true` — skip the `Finish` steps (undeploy, uninstall CRDs, delete
  namespace, remove Prometheus/cert-manager, destroy the KinD cluster). Leaves the environment
  running after the test binary exits.
- `ETCD_E2E_SKIP_SETUP=true` — skip the mutating `Setup` steps (image build/load, Prometheus
  install, namespace creation, CRD install, operator deploy). Assumes all of that already exists.
  The KinD-cluster-create step still runs even with this set — it's idempotent (KinD detects the
  existing cluster and just re-fetches its kubeconfig) and it's what wires the test's k8s client
  to the right cluster, so it can't be skipped outright.

Typical flow:

```sh
# 1. First run: provision everything, but leave it standing afterwards.
ETCD_E2E_SKIP_TEARDOWN=true go test ./test/e2e/... -run TestSomething -v

# 2. Iterate: point individual runs at the environment left up by step 1.
ETCD_E2E_SKIP_SETUP=true ETCD_E2E_SKIP_TEARDOWN=true \
  go test ./test/e2e/... -run TestSomethingElse -v

# ... repeat step 2 as many times as you like, editing test code between runs ...

# 3. When done, tear it down for real (drop ETCD_E2E_SKIP_TEARDOWN).
ETCD_E2E_SKIP_SETUP=true go test ./test/e2e/... -run TestNothing -v
```

(Step 3's `-run TestNothing` is a cheap way to match no tests while still running `TestMain`'s
setup/teardown; any run without `ETCD_E2E_SKIP_TEARDOWN=true` tears the environment down.)

## Preparing the environment fully by hand

If you'd rather not go through `go test` at all for setup, you can reproduce what `TestMain` does
manually with `kind`/`kubectl`/`make`, then point `go test` at it with
`ETCD_E2E_SKIP_SETUP=true`:

```sh
# Create the KinD cluster
kind create cluster --name etcd-cluster --image kindest/node:v1.32.0

# Build and load the operator image
make docker-build IMG=etcd-operator:v0.1
kind load docker-image etcd-operator:v0.1 --name etcd-cluster
# (podman users: load via an image archive instead of `kind load docker-image`)

# Install the Prometheus operator
kubectl create -f https://github.com/prometheus-operator/prometheus-operator/releases/download/v0.77.1/bundle.yaml

# Namespace, CRDs, operator deployment
kubectl create namespace etcd-operator-system
make install
make deploy DEPLOY_MODE=e2e IMG=etcd-operator:v0.1
kubectl wait deployment/etcd-operator-controller-manager \
  -n etcd-operator-system --for=condition=Available --timeout=3m
```

Then run tests against it:

```sh
ETCD_E2E_SKIP_SETUP=true ETCD_E2E_SKIP_TEARDOWN=true \
  go test ./test/e2e/... -run TestSomething -v
```

Some tests (e.g. `TestCertManagerProvider`) additionally install cert-manager themselves on
demand; no manual step is needed for that.

To clean up when you're done with the hand-prepared environment:

```sh
make undeploy DEPLOY_MODE=e2e ignore-not-found=true
make uninstall ignore-not-found=true
kubectl delete namespace etcd-operator-system
kubectl delete -f https://github.com/prometheus-operator/prometheus-operator/releases/download/v0.77.1/bundle.yaml
kind delete cluster --name etcd-cluster
```
