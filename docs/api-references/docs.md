# ETCD Operator API References

## Packages
- [operator.etcd.io/v1alpha1](#operatoretcdiov1alpha1)


## operator.etcd.io/v1alpha1

Package v1alpha1 contains API Schema definitions for the operator v1alpha1 API group.

### Resource Types
- [EtcdCluster](#etcdcluster)
- [EtcdClusterList](#etcdclusterlist)



#### EtcdCluster



EtcdCluster is the Schema for the etcdclusters API.



_Appears in:_
- [EtcdClusterList](#etcdclusterlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdCluster` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[EtcdClusterSpec](#etcdclusterspec)_ |  |  |  |


#### EtcdClusterList



EtcdClusterList contains a list of EtcdCluster.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdClusterList` | | |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[EtcdCluster](#etcdcluster) array_ |  |  |  |


#### EtcdClusterSpec



EtcdClusterSpec defines the desired state of EtcdCluster.



_Appears in:_
- [EtcdCluster](#etcdcluster)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `size` _integer_ | Size is the expected size of the etcd cluster. |  |  |
| `version` _string_ | Version is the expected version of the etcd container image. |  |  |




