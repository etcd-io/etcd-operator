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
