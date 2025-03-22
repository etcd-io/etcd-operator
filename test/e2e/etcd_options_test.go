/*
Copyright 2021 The Kubernetes Authors.

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
	"reflect"
	"testing"
	"time"

	"log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/pkg/features"
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
			Version: "v3.5.17",
			StorageSpec: &ecv1alpha1.StorageSpec{
				AccessModes:       corev1.ReadWriteOnce,
				VolumeSizeRequest: resource.MustParse("4Mi"),
				VolumeSizeLimit:   resource.MustParse("4Mi"),
			},
			EtcdOptions: []string{
				"--experimental-peer-skip-client-san-verification",
			},
		},
	}

	stsObj := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdClusterName,
			Namespace: namespace,
		},
	}

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {

		client := cfg.Client()
		_ = corev1.AddToScheme(client.Resources().GetScheme())
		_ = metav1.AddMetaToScheme(client.Resources().GetScheme())
		_ = ecv1alpha1.AddToScheme(client.Resources().GetScheme())

		ctx, err := envfuncs.CreateNamespace(namespace)(ctx, cfg)
		if err != nil {
			log.Printf("failed to create namespace: %s", err)
			return ctx
		}

		// create etcd cluster
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create etcd cluster: %s", err)
		}

		// get etcd cluster object
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to fetch etcd cluster: %s", err)
		}

		return ctx

	})

	feature.Assess("Check if resource created",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()

			var sts appsv1.StatefulSet

			if err := wait.For(
				conditions.New(client.Resources()).ResourceMatch(stsObj, func(object k8s.Object) bool {
					err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts)
					return err == nil
				}),
				wait.WithTimeout(2*time.Minute),
				wait.WithInterval(5*time.Second),
			); err != nil {
				t.Fatalf("unable to get sts: %s", err)
			}

			return ctx
		})

	feature.Assess("Check etcd options are configured", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {

		client := cfg.Client()
		var sts appsv1.StatefulSet
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
			t.Fatal(err)
		}

		var etcdContainer corev1.Container
		for c := range sts.Spec.Template.Spec.Containers {
			if sts.Spec.Template.Spec.Containers[c].Name == "etcd" {
				etcdContainer = sts.Spec.Template.Spec.Containers[c]
				break
			}
		}
		if !reflect.DeepEqual(expectedArgs, etcdContainer.Args) {
			t.Fatalf("Expecting args to be %v got %v", expectedArgs, etcdContainer.Args)
		}

		return ctx

	})

	_ = testEnv.Test(t, feature.Feature())
}
