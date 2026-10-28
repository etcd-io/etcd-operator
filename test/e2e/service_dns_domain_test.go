/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

const (
	operatorDeploymentName = "etcd-operator-controller-manager"
	operatorContainerName  = "manager"
	serviceDNSDomainFlag   = "service-dns-domain"
)

// TestServiceDNSDomain verifies that member FQDNs work under both forms the
// --service-dns-domain flag allows:
//   - the default (empty) value, which yields the short ".svc" suffix;
//   - "cluster.local", which yields the fully qualified ".svc.cluster.local".
func TestServiceDNSDomain(t *testing.T) {
	feature := features.New("service-dns-domain")
	defaultDomainCluster := "etcd-dns-default"
	customDomainCluster := "etcd-dns-cluster-local"
	clusterSize := 3

	feature.Assess("members use the short .svc form when --service-dns-domain is empty (the default)",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			verifyClusterMemberFQDNs(ctx, t, c, defaultDomainCluster, clusterSize, "svc")

			// The cluster must be fully deleted before the operator switches
			// to another domain: member removal matches live members by the
			// peer URL built from the operator's current domain, so draining
			// members while their advertised URLs use a different suffix
			// would wedge the leave sequence.
			cleanupEtcdCluster(ctx, t, c, defaultDomainCluster)
			waitForClusterDeletion(ctx, t, c, defaultDomainCluster)
			return ctx
		},
	)

	feature.Assess("members use svc.cluster.local when --service-dns-domain=cluster.local",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			setOperatorServiceDNSDomain(ctx, t, c, "cluster.local")
			waitForOperatorRollout(ctx, t, c)

			verifyClusterMemberFQDNs(ctx, t, c, customDomainCluster, clusterSize, "svc.cluster.local")
			// The custom-domain cluster is deliberately left to the teardown,
			// which still runs against the cluster.local operator.
			return ctx
		},
	)

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		// Delete the custom-domain cluster while the operator still runs
		// --service-dns-domain=cluster.local (see the leave-sequence note
		// in the first assess). Both waits tolerate clusters that were
		// never created or already removed by a failed assess.
		cleanupEtcdCluster(ctx, t, c, customDomainCluster)
		waitForClusterDeletion(ctx, t, c, customDomainCluster)
		cleanupEtcdCluster(ctx, t, c, defaultDomainCluster)

		// Restore the default (empty) domain so later tests run against an
		// unmodified operator.
		setOperatorServiceDNSDomain(ctx, t, c, "")
		waitForOperatorRollout(ctx, t, c)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// verifyClusterMemberFQDNs creates a cluster, waits for it to become Ready,
// and requires every live etcd member to advertise the peer URL that ends in
// dnsSuffix. The caller owns the cluster's lifecycle afterwards.
func verifyClusterMemberFQDNs(
	ctx context.Context, t *testing.T, c *envconf.Config, clusterName string, size int, dnsSuffix string,
) {
	t.Helper()
	createEtcdClusterWithPVC(ctx, t, c, clusterName, size)
	if err := waitForAllEtcdMemberReady(t, c, etcdClusterRef(clusterName, size)); err != nil {
		t.Fatalf("etcd pods of cluster %s failed to reach readiness: %v", clusterName, err)
	}

	memberList := getEtcdMemberListPB(t, c, clusterName+"-0")
	if len(memberList.Members) != size {
		t.Fatalf("cluster %s has %d members, want %d", clusterName, len(memberList.Members), size)
	}
	for _, member := range memberList.Members {
		wantPeerURL := fmt.Sprintf("http://%s.%s.%s.%s:2380", member.Name, clusterName, namespace, dnsSuffix)
		if len(member.PeerURLs) != 1 || member.PeerURLs[0] != wantPeerURL {
			t.Fatalf("member %s advertises peer URLs %v, want [%s]", member.Name, member.PeerURLs, wantPeerURL)
		}
	}
}

// setOperatorServiceDNSDomain rewrites the --service-dns-domain flag on the
// operator deployment's manager container. An empty dnsDomain removes the
// flag, restoring the default.
func setOperatorServiceDNSDomain(ctx context.Context, t *testing.T, c *envconf.Config, dnsDomain string) {
	t.Helper()
	res := c.Client().Resources()

	var deployment appsv1.Deployment
	if err := res.Get(ctx, operatorDeploymentName, namespace, &deployment); err != nil {
		t.Fatalf("Failed to get operator deployment %s: %v", operatorDeploymentName, err)
	}

	found := false
	for i := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[i]
		if container.Name != operatorContainerName {
			continue
		}
		found = true
		args := make([]string, 0, len(container.Args)+1)
		for _, arg := range container.Args {
			if strings.HasPrefix(arg, "--"+serviceDNSDomainFlag) {
				continue
			}
			args = append(args, arg)
		}
		if dnsDomain != "" {
			args = append(args, "--"+serviceDNSDomainFlag+"="+dnsDomain)
		}
		container.Args = args
	}
	if !found {
		t.Fatalf("No %s container in operator deployment %s", operatorContainerName, operatorDeploymentName)
	}

	if err := res.Update(ctx, &deployment); err != nil {
		t.Fatalf("Failed to update operator deployment %s: %v", operatorDeploymentName, err)
	}
}

// waitForOperatorRollout blocks until the operator deployment has observed
// its current generation and every replica is updated and available, i.e.
// the previously patched flags are in effect.
func waitForOperatorRollout(ctx context.Context, t *testing.T, c *envconf.Config) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var deployment appsv1.Deployment
		if err := c.Client().Resources().Get(ctx, operatorDeploymentName, namespace, &deployment); err != nil {
			return false, err
		}
		if deployment.Status.ObservedGeneration < deployment.Generation {
			return false, nil
		}
		replicas := int32(1)
		if deployment.Spec.Replicas != nil {
			replicas = *deployment.Spec.Replicas
		}
		return deployment.Status.UpdatedReplicas >= replicas && deployment.Status.AvailableReplicas >= replicas, nil
	}, wait.WithContext(ctx), wait.WithTimeout(5*time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("Timed out waiting for the operator deployment %s rollout: %v", operatorDeploymentName, err)
	}
}

// waitForClusterDeletion blocks until the given EtcdCluster and all its
// EtcdMembers are gone.
func waitForClusterDeletion(ctx context.Context, t *testing.T, c *envconf.Config, clusterName string) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		res := c.Client().Resources()
		if err := res.Get(ctx, clusterName, namespace, &ecv1alpha1.EtcdCluster{}); err == nil {
			return false, nil
		} else if !errors.IsNotFound(err) {
			return false, err
		}

		var members ecv1alpha1.EtcdMemberList
		if err := res.List(ctx, &members); err != nil {
			return false, err
		}
		for i := range members.Items {
			if members.Items[i].Namespace == namespace && members.Items[i].Spec.ClusterName == clusterName {
				return false, nil
			}
		}
		return true, nil
	}, wait.WithContext(ctx), wait.WithTimeout(5*time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("EtcdCluster %s was not fully deleted: %v", clusterName, err)
	}
}
