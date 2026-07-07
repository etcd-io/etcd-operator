# etcd-operator Helm chart

Installs the etcd-operator: the EtcdCluster CRD, controller Deployment, RBAC,
metrics Service, and optionally the admission webhooks (server and
configurations). Covers the same surface as `kustomize build config/default`.

```sh
helm install etcd-operator ./charts/etcd-operator \
  --namespace etcd-operator-system --create-namespace
```

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `nameOverride` / `fullnameOverride` | `""` | Usual name overrides. |
| `crds.enabled` | `true` | Install the CRDs (`templates/crds/`, synced from `config/crd/bases` by `make helm-crds`). |
| `controller.enabled` | `true` | Render the runtime: Deployment, ServiceAccount, RBAC, Services. |
| `controller.replicas` | `1` | |
| `controller.image.repository` | `gcr.io/etcd-development/etcd-operator` | |
| `controller.image.tag` | `""` | Defaults to the chart `appVersion`. |
| `controller.image.digest` | `""` | Pins `repository@digest`; wins over `tag`. |
| `controller.image.pullPolicy` | `IfNotPresent` | |
| `controller.imagePullSecrets` | `[]` | |
| `controller.operatorImage` | `""` | `OPERATOR_IMAGE` env (image for operator-run utility containers). Defaults to the manager image ref, digest pin included. |
| `controller.leaderElection` | `true` | `--leader-elect`. |
| `controller.watchNamespace` | `""` | Scope the operator to one namespace: namespaced Role/RoleBinding there instead of ClusterRole/ClusterRoleBinding, plus `--watch-namespace` / `WATCH_NAMESPACE`. |
| `controller.restrictClusterIssuer` | `false` | `RESTRICT_CLUSTER_ISSUER=true`: webhook rejects `issuerKind: ClusterIssuer`. |
| `controller.resources` | see `values.yaml` | |
| `controller.serviceAccount.annotations` | `{}` | |
| `metrics.secure` | `true` | HTTPS metrics with authn/authz (installs the metrics-auth ClusterRoles). `false`: plain HTTP, no cluster-scoped metrics RBAC. |
| `metrics.port` | `8443` | Bind address, metrics Service, and NetworkPolicy port. Convention: `8080` when `secure=false`. |
| `metrics.serviceMonitor.enabled` | `false` | ServiceMonitor; scheme/auth follow `metrics.secure`. |
| `metrics.serviceMonitor.additionalLabels` | `{}` | e.g. your Prometheus `release:` selector. |
| `metrics.networkPolicy.enabled` | `false` | Allow metrics ingress only from namespaces labeled `metrics: enabled`. |
| `webhook.enabled` | `false` | Webhook server side: webhook Service, serving-cert mount and port 9443 on the Deployment. Off: `ENABLE_WEBHOOKS=false`. |
| `webhook.certManager.enabled` | `true` | SelfSigned Issuer + serving Certificate; CA-injection annotation on the WebhookConfigurations. |
| `admissionWebhooks.enabled` | `false` | The cluster-scoped Validating/Mutating WebhookConfigurations. |
| `admissionWebhooks.namespaceSelector` | `{}` | Constrain the fail-closed webhooks; empty = all namespaces. |

Webhook toggles default off because the `appVersion` operator does not serve
admission webhooks yet; enable them with an operator build that does
(etcd-io/etcd-operator#384).

## Split install (CRDs + webhook configurations vs runtime)

Cluster-scoped installs (CRDs, WebhookConfigurations) often deploy through a
privileged lane while the runtime deploys namespaced — e.g. two ArgoCD
Applications rendering this chart twice. Both releases must produce the same
resource names: set the same `fullnameOverride` and target namespace.

Application 1 — cluster-scoped only (CRDs + the two WebhookConfigurations):

```yaml
fullnameOverride: etcd-operator
controller:
  enabled: false
admissionWebhooks:
  enabled: true
  namespaceSelector:
    matchLabels:
      kubernetes.io/metadata.name: kv
```

Application 2 — runtime only, in namespace `kv`:

```yaml
fullnameOverride: etcd-operator
crds:
  enabled: false
webhook:
  enabled: true
```

## Namespace-scoped operator

```yaml
controller:
  watchNamespace: kv
  restrictClusterIssuer: true
metrics:
  secure: false
  port: 8080
```

Renders a Role/RoleBinding in `kv` instead of the manager
ClusterRole/ClusterRoleBinding and drops all other cluster-scoped RBAC.

## CRD sync

`templates/crds/` is generated — never edit it by hand:

```sh
make manifests helm-crds
```
