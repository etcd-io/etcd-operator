# PodDisruptionBudget

The operator always creates a PodDisruptionBudget for every `EtcdCluster`,
named after the cluster and selecting all its etcd pods. `minAvailable` is
sized so that voluntary evictions (node drains, cluster-autoscaler
scale-down, managed node-pool upgrades) can never break etcd quorum, and is
re-tightened ahead of scale operations. There is no spec field to disable or
override it; manual edits are reverted on the next reconcile, and a
same-named PDB not created by the operator is left untouched (the operator
then manages none).

## Sizing

`minAvailable = pods - (voters - quorum)`, computed from the live member
list (learners and not-yet-removed pods count as pods but not voters):

| Cluster size | Quorum | minAvailable | Evictions allowed |
| --- | --- | --- | --- |
| 1 | 1 | 1 | 0 |
| 2 | 2 | 2 | 0 |
| 3 | 2 | 2 | 1 |
| 5 | 3 | 3 | 2 |
| 7 | 4 | 4 | 3 |

## Sizes 1 and 2 block drains by design

A 1- or 2-member cluster cannot lose any member without losing quorum, so
its PDB permanently disallows all voluntary evictions. `kubectl drain` and
autoscaler scale-down will wedge on these pods with a generic
"cannot evict pod as it would violate the pod's disruption budget" error.
To move such a pod, either scale the cluster to 3 first, or delete the pod
directly (`kubectl delete pod` bypasses the eviction API; expect downtime
while the StatefulSet recreates it).
