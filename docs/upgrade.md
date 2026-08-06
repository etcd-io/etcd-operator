# Upgrading etcd clusters

## Supported upgrade paths

The etcd-operator follows the same rules in terms of [supported upgrade
paths](https://etcd.io/docs/latest/upgrades/) as the main etcd project.

If you are using the official etcd images for your etcd cluster
managed by the etcd-operator, the operator will perform validation
for the upgrade path and follow the same rules mentioned above.

However, if you are using custom etcd image tags that are not
compatible with [Semantic Versioning](https://semver.org/),
no validation will be performed and you have to manually ensure
that the upgrade path is supported.

## Upgrade mechanics

The StatefulSet uses the `OnDelete` update strategy, so a version change
re-renders the pod template without restarting anything. The operator then
replaces one pod per reconcile, and only while all members are healthy, a
leader exists, and every member's revision is within 90% of the leader's.
The leader's pod is replaced last. Persistent storage (`spec.storageSpec`)
is strongly recommended: a deleted pod without a PVC loses its data
directory and cannot rejoin the cluster cleanly.

## Failed upgrades and rollback

If a replaced pod cannot run the new version (nonexistent tag, incompatible
flags), fix the spec — reverting `spec.version` to the version the members
still report is accepted, since upgrade-path validation runs against the
observed cluster version. While the remaining members hold quorum the
operator keeps syncing the template and replaces the broken pod
automatically. If quorum is lost it stops deleting pods; recover manually by
fixing the spec and deleting the affected pods
(`kubectl delete pod <cluster>-<ordinal>`).
