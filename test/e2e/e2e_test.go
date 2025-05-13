/*
Copyright 2024.

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
	"os"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

var etcdVersion = os.Getenv("ETCD_VERSION")

// TestZeroMemberCluster tests if zero member Etcd Cluster does not create its StatefulSet
// TODO: update this test once https://github.com/etcd-io/etcd-operator/issues/125 is addressed
func TestZeroMemberCluster(t *testing.T) {
	feature := features.New("zero-member-cluster")
	etcdClusterName := "etcd-cluster-zero"
	size := 0

	etcdClusterSpec := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdClusterName,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: etcdVersion,
		},
	}

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

		if err := c.Client().Resources().Create(ctx, etcdClusterSpec); err != nil {
			t.Fatalf("fail to create Etcd cluster: %s", err)
		}

		return ctx
	})

	feature.Assess("statefulSet is not created when etcdCluster.Spec.Size is 0",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

			var etcdCluster ecv1alpha1.EtcdCluster
			if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster); err != nil {
				t.Fatalf("unable to fetch Etcd cluster: %s", err)
			}

			var sts appsv1.StatefulSet
			err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &sts)

			if !errors.IsNotFound(err) {
				t.Fatalf("statefulSet found when Etcd Cluster size is zero: %s", err)
			}

			return ctx
		},
	)

	_ = testEnv.Test(t, feature.Feature())

}

// TestNegativeClusterSize tests negative membership cluster creation
func TestNegativeClusterSize(t *testing.T) {
	feature := features.New("negative-member-cluster")
	etcdClusterName := "etcd-cluster-negative"
	size := -1

	etcdClusterSpec := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdClusterName,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: etcdVersion,
		},
	}

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

		err := c.Client().Resources().Create(ctx, etcdClusterSpec)
		if !errors.IsInvalid(err) {
			t.Fatalf("etcdCluster with negative size failed with unexpected error: %s", err)
		}

		return ctx
	})

	feature.Assess("etcdCluster resource should not be created when etcdCluster.Spec.Size is negative",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

			var etcdCluster ecv1alpha1.EtcdCluster
			err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster)
			if !errors.IsNotFound(err) {
				t.Fatalf("found etcdCluster resource with negative size: %s", err)
			}

			return ctx
		},
	)

	_ = testEnv.Test(t, feature.Feature())
}

func TestClusterHealthy(t *testing.T) {
	feature := features.New("etcd-operator-controller")

	feature.Assess("ensure the etcd-operator pod is running",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()

			// get the etcd controller deployment
			var deployment appsv1.Deployment
			if err := client.Resources().Get(ctx, "etcd-operator-controller-manager", namespace, &deployment); err != nil {
				t.Fatalf("Failed to get deployment: %s", err)
			}

			// check for deployment to become available with minimum replicas
			if err := wait.For(
				conditions.New(client.Resources()).
					DeploymentConditionMatch(&deployment, appsv1.DeploymentAvailable, corev1.ConditionTrue),
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				t.Fatal(err)
			}

			return ctx
		})

	// 'testEnv' is the env.Environment you set up in TestMain
	_ = testEnv.Test(t, feature.Feature())
}
