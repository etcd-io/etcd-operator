# Helm Chart for etcd-operator

This directory contains the Helm Chart for deploying the etcd-operator on Kubernetes.

## Overview

The Helm Chart is **automatically generated** from the Kustomize manifests (located in `config/default/`) using [helmify](https://github.com/arttor/helmify). This ensures that the Helm Chart and Kustomize deployment remain in sync.

**Single Source of Truth**: All configuration changes should be made in the Kustomize manifests. The Helm Chart is regenerated automatically during build and test processes.

## Quick Start

### Prerequisites

- Kubernetes cluster 1.30+
- Helm 3.x
- cert-manager v1.16+ installed ([Installation Guide](https://cert-manager.io/docs/installation/))

### Installation

```bash
# 1. Install cert-manager using Helm (recommended)
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.19.1 \
  --set crds.enabled=true

# Wait for cert-manager to be ready
kubectl wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=2m
kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=2m

# 2. Build and push your operator image
make docker-build docker-push IMG=<your-registry>/etcd-operator:v0.1.0

# 3. Generate the Helm Chart (helmify and yq will be auto-installed if needed)
make helm

# 4. Install the operator
helm install etcd-operator ./helm/ \
  --create-namespace \
  --namespace etcd-operator-system \
  --set controllerManager.manager.image.repository=<your-registry>/etcd-operator \
  --set controllerManager.manager.image.tag=v0.1.0

# 5. Create an etcd cluster
kubectl apply -f - <<EOF
apiVersion: operator.etcd.io/v1alpha1
kind: EtcdCluster
metadata:
  name: my-etcd-cluster
spec:
  size: 3
  version: "v3.6.5"
EOF

# 6. Verify the cluster is running
kubectl get etcdclusters
kubectl get pods -l app=my-etcd-cluster
```

**Note**: The etcd version `v3.6.5` is tested and validated in our E2E tests. You can use other etcd versions, but ensure they are compatible with your use case.

**For local Kind clusters**:

```bash
# Install cert-manager with Helm
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.19.1 \
  --set crds.enabled=true

# Build and load image to Kind
make docker-build IMG=etcd-operator:v0.1
kind load docker-image etcd-operator:v0.1

# Generate Chart and install (tools auto-installed)
make helm
helm install etcd-operator ./helm/ \
  --create-namespace \
  --namespace etcd-operator-system \
  --set controllerManager.manager.image.repository=etcd-operator \
  --set controllerManager.manager.image.tag=v0.1

# 5. Create an etcd cluster
kubectl apply -f - <<EOF
apiVersion: operator.etcd.io/v1alpha1
kind: EtcdCluster
metadata:
  name: my-etcd-cluster
spec:
  size: 3
  version: "v3.6.5"
EOF

# 6. Verify the cluster is running
kubectl get etcdclusters
kubectl get pods -l app=my-etcd-cluster
```

**Note**: The etcd version `v3.6.5` is tested and validated in our E2E tests. You can use other etcd versions, but ensure they are compatible with your use case.

## Chart Generation

### How It Works

The Helm Chart is generated using the following pipeline:

```
Kustomize manifests (config/default/)
    ↓
kustomize build
    ↓
helmify (converts to Helm templates)
    ↓
yq (updates Chart metadata)
    ↓
helm lint (validates Chart)
    ↓
Generated Chart (helm/)
```

### Generate the Chart

```bash
# Generate and validate
make helm

# The above command will:
# 1. Auto-install required tools if not present in bin/:
#    - helmify: Converts Kustomize to Helm Chart
#    - yq: Updates YAML metadata
#    - helm: Validates the generated Chart
# 2. Build Kustomize manifests
# 3. Convert to Helm Chart using helmify
# 4. Update Chart.yaml metadata with yq
# 5. Run helm lint to validate
```

**Tool Installation**: The `make helm` command automatically downloads and installs `helmify` and `yq` to the local `bin/` directory if they are not already present. No manual installation required!

### Generated Structure

After running `make helm`, the following files are generated:

```
helm/
├── Chart.yaml              # Chart metadata (auto-generated)
├── values.yaml             # Default configuration values (auto-generated)
├── templates/              # Kubernetes resource templates (auto-generated)
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── rbac.yaml
│   └── ...
├── crds/                   # CRD definitions (auto-installed by Helm)
│   └── etcdcluster-crd.yaml
├── examples/               # Example configurations (manually maintained)
│   ├── values-ha.yaml      # High availability config
│   └── values-minimal.yaml # Minimal resource config
└── README.md               # This file (manually maintained)
```

**Note**: All files except `examples/` and `README.md` are automatically generated and should not be manually edited.

## Customizing Values

### Using Custom Values Files

The `examples/` directory contains pre-configured values files for common scenarios:

#### High Availability (Production)

```bash
helm install etcd-operator ./helm/ \
  -f ./helm/examples/values-ha.yaml \
  --set controllerManager.manager.image.repository=<your-registry>/etcd-operator \
  --set controllerManager.manager.image.tag=v0.1.0
```

Features:
- 3 controller replicas for redundancy
- Leader election enabled (only one replica actively reconciles)
- Higher resource limits (1 CPU, 512Mi memory)
- Kubernetes scheduler automatically distributes pods across nodes

**Note**: The operator uses leader election, so only one replica is active at a time. The other replicas are in standby mode and will take over if the leader fails.

#### Minimal Resources (Development/Testing)

```bash
helm install etcd-operator ./helm/ \
  -f ./helm/examples/values-minimal.yaml \
  --set controllerManager.manager.image.repository=<your-registry>/etcd-operator \
  --set controllerManager.manager.image.tag=v0.1.0
```

Features:
- 1 controller replica
- Minimal resource limits (200m CPU, 128Mi memory)
- Suitable for resource-constrained environments

### Custom Values File

Create your own `my-values.yaml`:

```yaml
# Controller configuration
controllerManager:
  replicas: 2
  
  manager:
    image:
      repository: ghcr.io/my-org/etcd-operator
      tag: v0.1.0
    
    resources:
      limits:
        cpu: 500m
        memory: 256Mi
      requests:
        cpu: 100m
        memory: 128Mi

# Kubernetes cluster domain (if different)
kubernetesClusterDomain: cluster.local
```

Install with custom values:

```bash
helm install etcd-operator ./helm/ -f my-values.yaml
```

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `controllerManager.replicas` | Number of controller replicas | `1` |
| `controllerManager.manager.image.repository` | Controller image repository | `controller` |
| `controllerManager.manager.image.tag` | Controller image tag | `latest` |
| `controllerManager.manager.resources.limits.cpu` | CPU limit | `500m` |
| `controllerManager.manager.resources.limits.memory` | Memory limit | `128Mi` |
| `kubernetesClusterDomain` | Kubernetes cluster domain | `cluster.local` |

**Important**: The default `values.yaml` contains placeholder image values. You **must** override them with your actual image using `--set` flags or a custom values file.

## Upgrading

```bash
# Regenerate Chart if Kustomize configs changed
make helm

# Upgrade the release
helm upgrade etcd-operator ./helm/ \
  --namespace etcd-operator-system \
  --reuse-values
```

**Note**: Helm does not automatically update CRDs during upgrades. If CRDs have changed, manually apply them:

```bash
kubectl apply -f helm/crds/
```

## Uninstalling

```bash
# Uninstall the operator
helm uninstall etcd-operator --namespace etcd-operator-system

# Optionally remove CRDs (this will delete all EtcdCluster resources!)
kubectl delete crd etcdclusters.operator.etcd.io

# Optionally remove namespace
kubectl delete namespace etcd-operator-system

# Optionally uninstall cert-manager (if no other apps are using it)
helm uninstall cert-manager --namespace cert-manager
kubectl delete namespace cert-manager
```

## Development Workflow

### Modifying the Chart

To make changes to the Helm Chart:

1. **Modify Kustomize manifests** in `config/default/` (not the Helm Chart directly)
2. **Regenerate the Chart**: `make helm`
3. **Test the changes**: `DEPLOY_METHOD=helm make test-e2e`
4. **Validate**: `helm lint helm/`

### Testing Both Deployment Methods

The project supports testing both Kustomize and Helm deployments:

```bash
# Test Kustomize only
DEPLOY_METHOD=kustomize make test-e2e

# Test Helm only
DEPLOY_METHOD=helm make test-e2e

# Test both (default)
make test-e2e
```

## Troubleshooting

### Chart Generation Fails

```bash
# Check Kustomize configuration
kustomize build config/default/

# Ensure tools are installed
make helmify yq

# Regenerate with verbose output
make helm
```

### Helm Lint Errors

The `make helm` command automatically runs `helm lint`. If it fails:

1. Check the Kustomize manifests for invalid YAML
2. Ensure all required fields are present in `config/manager/manager.yaml`
3. Review the lint output for specific issues

### Image Pull Errors

If you see `ImagePullBackOff`:

1. Ensure the operator image is built and available:
   ```bash
   make docker-build IMG=<your-image>
   ```

2. For Kind clusters, load the operator image:
   ```bash
   kind load docker-image <your-image>
   ```

3. Verify image is specified correctly in Helm values:
   ```bash
   helm get values etcd-operator -n etcd-operator-system
   ```

**Note**: The operator uses `gcr.io/etcd-development/etcd` as the default registry for etcd pods. If you need to use a different registry, specify it in your EtcdCluster CR:

```yaml
spec:
  imageRegistry: "your-registry/etcd"
  version: "v3.6.5"
```

### CRDs Not Found

CRDs in `helm/crds/` are automatically installed during `helm install`. If you see CRD-related errors:

```bash
# Verify CRDs are installed
kubectl get crd etcdclusters.operator.etcd.io

# Manually install if needed
kubectl apply -f helm/crds/
```

## Additional Resources

- [Kustomize Configuration](../config/default/) - Source of truth for all manifests
- [Installation Guide](../docs/install.md) - Detailed installation instructions
- [Contributing Guide](../CONTRIBUTING.md) - How to test deployment methods
- [Helm Documentation](https://helm.sh/docs/) - Official Helm documentation

## Architecture

### Why Auto-Generate?

**Benefits**:
- ✅ Single source of truth (Kustomize)
- ✅ No manual synchronization needed
- ✅ Consistent manifests across deployment methods
- ✅ Automated validation in CI/CD

**Trade-offs**:
- ⚠️ Chart must be regenerated after Kustomize changes
- ⚠️ Custom Chart modifications will be overwritten
- ⚠️ Customizations should be done via `values.yaml` or `examples/`

### Helm vs Kustomize

Both deployment methods are supported and tested:

| Feature | Kustomize | Helm |
|---------|-----------|------|
| **Configuration** | Patches and overlays | Values files |
| **Templating** | Limited | Full Go templates |
| **Package Management** | No | Yes (Helm repos) |
| **Versioning** | Manual | Built-in |
| **Rollback** | Manual | Built-in |
| **Use Case** | GitOps, simple deploys | Package distribution, upgrades |

Choose based on your needs:
- **Kustomize**: Direct, simple, GitOps-friendly
- **Helm**: Package management, templating, easier upgrades

Both methods deploy the same operator with identical functionality.
