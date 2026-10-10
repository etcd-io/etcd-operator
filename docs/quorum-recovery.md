# Recovering from lost quorum

An etcd cluster needs a majority of its members to elect a leader. When a
majority is gone for good, for example because their nodes and disks were
lost, the cluster stops serving requests and the operator stops changing it.
Its log says:

```
EtcdCluster is not healthy; waiting for it to recover or for a human operator to intervene
```

The operator does not recover a cluster on its own. If the missing members can
come back, bring them back instead: the cluster recovers by itself once a
majority runs again.

## What a recovery does

A recovery restarts one member, the survivor, as a new single-member cluster
with `--force-new-cluster`. The operator then deletes every other member with
its Pod and PVC, and scale-out adds members back until the cluster has
`spec.size` members again. Data that only the deleted members had is lost, so
recover from the member with the most recent data.

A recovery needs persistent storage (`spec.storageSpec`), because the
survivor's Pod is recreated.

## Starting a recovery

Annotate the EtcdCluster with the ordinal of the survivor, for example `1` for
the member `<cluster name>-1`:

```sh
kubectl annotate etcdcluster <cluster name> operator.etcd.io/recover-quorum-from=1
```

Or let the operator pick the reachable voting member with the highest Raft
index:

```sh
kubectl annotate etcdcluster <cluster name> operator.etcd.io/recover-quorum-from=auto
```

The operator removes the annotation as soon as it reads it. It ignores the
request, and logs why, when the cluster is healthy, when the member doesn't
exist or isn't a voting member, when the cluster has no persistent storage, or
when no member answers for `auto`.

## Following a recovery

While the recovery runs, `status.quorumRecovery.survivor` holds the ordinal of
the survivor. When it finishes, `status.quorumRecovery` is cleared and
`status.recoveryCount`, `status.lastRecoveredFrom` and
`status.lastRecoveryTime` are updated. If quorum comes back before the
survivor is restarted, the operator cancels the recovery and nothing is
deleted.

If the node of a member being deleted is gone, Kubernetes keeps its Pod
`Terminating` and the recovery waits for it. Once you are sure the node won't
come back, delete the Pod with `kubectl delete pod <pod> --force
--grace-period=0`.
