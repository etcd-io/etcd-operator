# Install the etcd operator on Kubernetes

Welcome to version 0.1.0 of the etcd-operator!  At this point it doesn't do a lot, but it can be installed, and you can play with it.

## Prerequisites

* A Kubernetes cluster running version 1.30+.
* You must have `kubectl` installed, or have a tool which lets you run Kubernetes YAML files.
* You must use a service account or user that can create ClusterRoles, ClusterRoleBindings, CustomResourceDefinitions, Deployments, Namespaces, Roles, RoleBindings, Services, and ServiceAccounts.

## Install the Operator

To install the default generated operator, run the following command:

```bash
curl -L https://raw.githubusercontent.com/etcd-io/etcd-operator/refs/heads/main/dist/install-v0.1.0.yaml | kubectl apply -f -
```

If this isn't a good approach for your environment, download the `install-v0.1.0.yaml` installation file from GitHub, and use your chosen Kubernetes management tool to apply it.

You should see the operator objects being created:

```
namespace/etcd-operator-system created
customresourcedefinition.apiextensions.k8s.io/etcdclusters.operator.etcd.io created
serviceaccount/etcd-operator-controller-manager created
role.rbac.authorization.k8s.io/etcd-operator-leader-election-role created
clusterrole.rbac.authorization.k8s.io/etcd-operator-etcdcluster-editor-role created
clusterrole.rbac.authorization.k8s.io/etcd-operator-etcdcluster-viewer-role created
clusterrole.rbac.authorization.k8s.io/etcd-operator-manager-role created
clusterrole.rbac.authorization.k8s.io/etcd-operator-metrics-auth-role created
clusterrole.rbac.authorization.k8s.io/etcd-operator-metrics-reader created
rolebinding.rbac.authorization.k8s.io/etcd-operator-leader-election-rolebinding created
clusterrolebinding.rbac.authorization.k8s.io/etcd-operator-manager-rolebinding created
clusterrolebinding.rbac.authorization.k8s.io/etcd-operator-metrics-auth-rolebinding created
service/etcd-operator-controller-manager-metrics-service created
deployment.apps/etcd-operator-controller-manager created
```

All objects will be created in the etcd-operator-system namespace, which is what we recommend for offering etcd clusters as a utility on your Kubernetes cluster.

## Check the Status of the Operator

You can check the status by running the following command:

```bash
$ kubectl get deployment -n etcd-operator-system
NAME                           	READY   UP-TO-DATE   AVAILABLE   AGE
etcd-operator-controller-manager   1/1 	1        	1       	49s
```

## Create an etcd Cluster

To create a cluster, you need to push an EtcdCluster object.  Do this by creating a YAML file (i.e., `etcdcluster.yaml`), with the following contents:

```yaml
apiVersion: operator.etcd.io/v1alpha1
kind: EtcdCluster
metadata:
  name: test-cluster
spec:
  version: v3.6.0-rc.3
  size: 3
```

Note: Refer to the API documentation for the spec fields from the EtcdCluster (i.e., `kubectl explain EtcdCluster.spec`).

Running `kubectl apply -f etcdcluster.yaml` will create a three node etcd cluster. Now perform the following steps to verify the created cluster.

First, get the pod IP addresses:

```bash
kubectl get pod -o wide -l controller=test-cluster
```

```bash
NAME         	READY   STATUS	RESTARTS   AGE 	IP        	NODE                     	NOMINATED NODE   READINESS GATES
test-cluster-0   1/1 	Running   0      	3m16s   10.244.0.10   kind-cluster-control-plane   <none>       	<none>
test-cluster-1   1/1 	Running   1      	2m47s   10.244.0.11   kind-cluster-control-plane   <none>       	<none>
test-cluster-2   1/1 	Running   0      	2m13s   10.244.0.12   kind-cluster-control-plane   <none>       	<none>
```

To use them with the etcd-client, load the addresses into an ENV var:

```bash
ETCD_CLUSTER_IP_ADDRESSES=$(kubectl get pod -l controller=test-cluster -o jsonpath='{range .items[*]}{.status.podIP}:2379,{end}')
```

Write a key into the cluster by running etcd-client on the server. If you have `etcdcl` installed on your system, you can use that instead.

```bash
kubectl run etcd-client --attach --restart=Never --rm --image gcr.io/etcd-development/etcd:v3.6.0-rc.3 -- etcdctl --endpoints="$ETCD_CLUSTER_IP_ADDRESSES" put foo bar

OK

pod "etcd-client" deleted
```

Read the written key from the cluster:

```bash
kubectl run etcd-client --attach --restart=Never --rm --image gcr.io/etcd-development/etcd:v3.6.0-rc.3 -- etcdctl --endpoints="$ETCD_CLUSTER_IP_ADDRESSES" get foo

foo
bar

pod "etcd-client" deleted
```

### Clean up the EtcdCluster

If you're not actually using this cluster, you can delete it:

Execute: `kubectl delete etcdcluster test-cluster`

## Uninstalling the Operator

To completely remove the operator from the Kubernetes cluster, run the following:

```bash
$ curl -L https://raw.githubusercontent.com/etcd-io/etcd-operator/refs/heads/main/dist/install-v0.1.0.yaml | kubectl delete -f -
```

Or, if you've downloaded the install file:

```bash
kubectl delete -f install-v0.1.0.yaml
```
