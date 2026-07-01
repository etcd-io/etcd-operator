# Metrics

The etcd-operator exports a set of **per-cluster domain metrics** describing the
state of every `EtcdCluster` it reconciles. These are emitted by the operator
process itself (not by etcd) and are served on the operator's existing
`/metrics` endpoint, alongside the standard controller-runtime and Go runtime
metrics. They are registered against the controller-runtime global Prometheus
registry, so no extra HTTP plumbing is required.

This document lists every custom metric, its labels, the CR knobs that control
emission and scraping, and the shipped dashboard/alert assets.

## Metric reference

All custom metrics share the subsystem prefix `etcd_operator_cluster_` and are
labelled with the cluster's `namespace` and `name`, so a single operator
managing many clusters produces one time series per `(namespace, name)`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `etcd_operator_cluster_member_count_desired` | gauge | `namespace`, `name` | Desired members, i.e. `spec.size`. |
| `etcd_operator_cluster_member_count` | gauge | `namespace`, `name` | Members registered in the etcd member-list API (`status.memberCount`). |
| `etcd_operator_cluster_member_count_ready` | gauge | `namespace`, `name` | Ready etcd pods (StatefulSet `readyReplicas`). |
| `etcd_operator_cluster_member_count_healthy` | gauge | `namespace`, `name` | Members reported healthy in cluster status. |
| `etcd_operator_cluster_learner_count` | gauge | `namespace`, `name` | Members currently acting as learners (non-voting). |
| `etcd_operator_cluster_has_quorum` | gauge | `namespace`, `name` | `1` when a strict majority of **voting** members is healthy, else `0`. |
| `etcd_operator_cluster_has_leader` | gauge | `namespace`, `name` | `1` when the operator knows the current leader, else `0`. |
| `etcd_operator_cluster_available` | gauge | `namespace`, `name` | `1` when the `Available` status condition is `True`, else `0`. |
| `etcd_operator_cluster_tls_enabled` | gauge | `namespace`, `name` | `1` when the cluster is configured for TLS (`spec.tls` set), else `0`. |
| `etcd_operator_cluster_tls_ready` | gauge | `namespace`, `name` | **Only present for TLS-configured clusters.** `1` when the TLS-secured cluster is `Available`, `0` when configured but not yet Available. |
| `etcd_operator_cluster_leader_changes_total` | counter | `namespace`, `name` | Leader changes observed by the operator. The first observation and recovery from a leaderless window are **not** counted. |
| `etcd_operator_cluster_reconcile_duration_seconds` | histogram | `namespace`, `name` | Wall-clock duration of reconcile loops (default Prometheus buckets). |
| `etcd_operator_cluster_reconcile_errors_total` | counter | `namespace`, `name` | Reconcile loops that returned an error. |

### Notes on semantics

- **Quorum is computed over voting members only.** Learners do not participate
  in the raft quorum, so a healthy learner can never make a degraded voting set
  look quorate. `has_quorum` uses `voting = memberCount - learnerCount` and
  requires `healthyVoting >= floor(voting/2) + 1`.
- **`has_leader` reflects the current reconcile.** When no leader is known the
  operator clears `status.leaderID`, so this gauge drops to `0` during a
  leaderless window rather than reporting a stale leader.
- **`tls_ready` exists only when TLS is configured.** Plaintext clusters emit no
  `tls_ready` series, so alerts on "TLS configured but not ready" never fire for
  clusters that do not use TLS. Removing `spec.tls` drops the series.
- **`leader_changes_total` ignores the first/leaderless transitions.** Operator
  restarts and transient leaderless windows do not inflate the counter; only a
  transition between two known, distinct leaders increments it.
- **Cardinality is bounded.** When an `EtcdCluster` is deleted (or has metrics
  disabled), the operator drops all of that cluster's series so stale clusters
  do not accumulate time series.

## Controlling emission: the `spec.metrics` knob

Per-cluster metric emission is configured through the optional `spec.metrics`
block on `EtcdCluster`:

```yaml
apiVersion: operator.etcd.io/v1alpha1
kind: EtcdCluster
metadata:
  name: my-etcd
spec:
  size: 3
  version: v3.5.21
  metrics:
    # Export the operator's per-cluster domain metrics for this cluster.
    # Defaults to true when the whole metrics block (or this field) is omitted.
    enabled: true
    podMonitor:
      # Create a prometheus-operator PodMonitor selecting this cluster's etcd
      # member pods so Prometheus also scrapes etcd's own /metrics endpoint.
      enabled: true
      interval: "30s"   # optional; prometheus-operator default when empty
      port: "client"    # optional; pod port name, defaults to "client"
```

| Field | Default | Effect |
| --- | --- | --- |
| `spec.metrics` | unset | When omitted, the operator still exports per-cluster domain metrics; no PodMonitor is created. |
| `spec.metrics.enabled` | `true` | When `false`, the operator drops this cluster's domain-metric series from `/metrics`. |
| `spec.metrics.podMonitor.enabled` | `false` | When `true` (and metrics enabled), the operator creates/maintains a `PodMonitor` for the etcd member pods. |
| `spec.metrics.podMonitor.interval` | prometheus default | Scrape interval for the etcd member pods. |
| `spec.metrics.podMonitor.port` | `client` | Name of the pod port exposing etcd's `/metrics`. |

`PodMonitorEnabled()` is `true` only when metrics are enabled **and**
`podMonitor.enabled` is `true`, so disabling metrics also disables the
PodMonitor. The PodMonitor is owned by the `EtcdCluster` (garbage-collected on
delete). If the `PodMonitor` CRD (prometheus-operator) is not installed, the
operator logs and skips PodMonitor reconciliation without failing the rest of
the reconcile.

A ready-to-apply example lives at
[`config/samples/sample_metrics_etcdcluster.yaml`](../config/samples/sample_metrics_etcdcluster.yaml).

## Scraping the operator's own metrics

The operator's `/metrics` endpoint (where the `etcd_operator_cluster_*` series
live) is scraped via the `ServiceMonitor` in
[`config/prometheus/monitor.yaml`](../config/prometheus/monitor.yaml), which
selects the controller-manager metrics Service. Enable it by uncommenting the
`../prometheus` line in `config/default/kustomization.yaml` (this also pulls in
the alert rules below). By default the endpoint serves HTTPS on `:8443` behind
authn/authz; see the ServiceMonitor comments for TLS verification options.

The two scrape paths are complementary:

- **ServiceMonitor** (always, via `config/prometheus`) → scrapes the **operator**
  for the domain metrics in this document.
- **PodMonitor** (opt-in, via `spec.metrics.podMonitor`) → scrapes the **etcd
  member pods** for etcd's own built-in metrics.

## Dashboard and alerts

- **Grafana dashboard:**
  [`config/grafana/etcd-operator-dashboard.json`](../config/grafana/etcd-operator-dashboard.json)
  (uid `etcd-operator-domain`). It has rows for cluster health (quorum, leader,
  available, TLS-ready), membership counts/learners, and stability/reconcile
  (leader-change rate, reconcile error rate, reconcile-duration quantiles), with
  `datasource`, `namespace`, and `cluster` template variables.
  `kustomize build config/grafana` ships it as a ConfigMap labelled
  `grafana_dashboard: "1"` for the Grafana dashboard sidecar.

- **PrometheusRule alerts:**
  [`config/prometheus/alerts.yaml`](../config/prometheus/alerts.yaml)
  (`PrometheusRule/etcd-operator-domain-rules`):

  | Alert | Expr (summary) | For | Severity |
  | --- | --- | --- | --- |
  | `EtcdClusterQuorumLost` | `has_quorum == 0` | 1m | critical |
  | `EtcdClusterNoLeader` | `has_leader == 0` | 2m | critical |
  | `EtcdClusterMembersNotReady` | `member_count_ready < member_count_desired` | 10m | warning |
  | `EtcdClusterTLSNotReady` | `tls_ready == 0` | 5m | warning |
  | `EtcdOperatorReconcileErrors` | `increase(reconcile_errors_total[10m]) > 0` | 15m | warning |
  | `EtcdClusterLeaderFlapping` | `increase(leader_changes_total[15m]) > 3` | — | warning |
  | `EtcdClusterNotAvailable` | `available == 0` | 10m | warning |

  Both the ServiceMonitor and the alert rules are included by the
  `config/prometheus` kustomization; uncommenting `../prometheus` in the default
  overlay ships them together. They require the prometheus-operator
  `ServiceMonitor`/`PrometheusRule` CRDs.
