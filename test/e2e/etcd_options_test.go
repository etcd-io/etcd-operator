/*
Copyright 2025.

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
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/test/utils"
)

func TestEtcdOptions(t *testing.T) {
	feature := features.New("etcd-options")

	const etcdClusterName = "example"
	const namespace = "etcd-test"
	var expectedArgs = []string{
		"--name=$(POD_NAME)",
		"--listen-peer-urls=http://0.0.0.0:2380",
		"--listen-client-urls=http://0.0.0.0:2379",
		"--initial-advertise-peer-urls=http://$(POD_NAME).example.$(POD_NAMESPACE).svc.cluster.local:2380",
		"--advertise-client-urls=http://$(POD_NAME).example.$(POD_NAMESPACE).svc.cluster.local:2379",
		"--experimental-peer-skip-client-san-verification",
	}

	etcdCluster := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdClusterName,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    1,
			Version: os.Getenv("ETCD_VERSION"),
			EtcdOptions: []string{
				"--experimental-peer-skip-client-san-verification",
			},
		},
	}

	objKey := runtimeClient.ObjectKeyFromObject(etcdCluster)

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {

		client := cfg.Client()

		ctx, err := envfuncs.CreateNamespace(etcdCluster.Namespace)(ctx, cfg)
		if err != nil {
			t.Fatalf("failed to create namespace: %s", err)
		}

		// create etcd cluster
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create etcd cluster: %s", err)
		}

		// get etcd cluster object
		var ec ecv1alpha1.EtcdCluster
		ops := []wait.Option{
			wait.WithTimeout(1 * time.Minute),
			wait.WithInterval(5 * time.Second),
		}

		if err := utils.GetKubernetesResource(ctx, client, objKey, &ec, ops...); err != nil {
			t.Fatalf("unable to fetch etcd cluster: %s", err)
		}

		return ctx
	})

	feature.Assess("Check if resource created",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {

			client := cfg.Client()
			var sts appsv1.StatefulSet

			ops := []wait.Option{
				wait.WithTimeout(2 * time.Minute),
				wait.WithInterval(5 * time.Second),
			}

			if err := utils.GetKubernetesResource(ctx, client, objKey, &sts, ops...); err != nil {
				t.Fatal(err)
			}
			return ctx
		},
	)

	feature.Assess("Check etcd options are configured",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {

			client := cfg.Client()
			var sts appsv1.StatefulSet

			if err := utils.GetKubernetesResource(ctx, client, objKey, &sts); err != nil {
				t.Fatal(err)
			}

			etcdContainer, err := utils.GetContainerByName(sts.Spec.Template.Spec.Containers, "etcd")
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(expectedArgs, etcdContainer.Args) {
				t.Fatalf("Expecting args to be %v got %v", expectedArgs, etcdContainer.Args)
			}
			return ctx
		},
	)

	feature.Assess("Check if sts has desired replicas",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {

			client := cfg.Client()
			var sts appsv1.StatefulSet

			if err := utils.GetKubernetesResource(ctx, client, objKey, &sts); err != nil {
				t.Fatal(err)
			}

			desiredReplicas := int32(etcdCluster.Spec.Size)
			if err := wait.For(
				conditions.New(client.Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
					return sts.Status.ReadyReplicas
				}, desiredReplicas),
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				t.Fatalf("unable to create sts with size %d: %s", desiredReplicas, err)
			}
			return ctx
		},
	)

	_ = testEnv.Test(t, feature.Feature())
}
