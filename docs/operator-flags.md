# Operator Flags

These flags configure the etcd-operator **manager process** itself (the
controller), as opposed to per-cluster settings expressed on the `EtcdCluster`
custom resource. They are passed as command-line arguments to the operator
binary (see `cmd/main.go`).

## `--max-concurrent-reconciles`

- **Type:** int
- **Default:** `5`

Number of reconcile workers the controller runs in parallel.
controller-runtime's own default is `1`.

Each `EtcdCluster` is reconciled on its own workqueue key — the queue dedups by
namespaced name — so a single cluster is **never** reconciled by two workers at
the same time. Concurrency therefore only ever parallelizes work across
**distinct** clusters; it does not introduce intra-cluster races.

A reconcile in this operator is relatively heavy and long-running: it patches a
StatefulSet, issues member-list and health RPCs against the managed etcd
cluster, and may perform certificate work. With a single worker, one slow
cluster blocks progress on every other cluster. A small pool (default `5`)
meaningfully improves throughput when many clusters need attention at once
(operator restart, mass upgrade, node churn).

The cost of a **larger** pool is more simultaneous load on the apiserver and on
the managed etcd clusters. Wise operators running large fleets should tune this
value for their environment; operators with a handful of clusters can leave it
at the default.

A value `<= 0` falls back to controller-runtime's default of a single worker,
which stays behaviorally safe.

```sh
# Widen to 10 workers for a large fleet.
manager --max-concurrent-reconciles=10
```
