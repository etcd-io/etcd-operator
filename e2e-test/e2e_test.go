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
	"log"

	// "os"
	"testing"
	"time"

	// "sigs.k8s.io/e2e-framework/klient"
	apiextensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	// corev1 "k8s.io/api/core/v1"
	appsv1 "k8s.io/api/apps/v1"

	// "k8s.io/klog/v2"
	// "sigs.k8s.io/e2e-framework/klient/decoder"
	// "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	// "sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	// "k8s.io/apimachinery/pkg/api/errors"


	// "sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	// "sigs.k8s.io/e2e-framework/pkg/envfuncs"

	// "sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/pkg/features"
	// "sigs.k8s.io/e2e-framework/support/kind"

	// "go.etcd.io/etcd-operator/test/utils"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

var (
	etcdClusterName = "etcdcluster-sample"
	size = 1
	etcdCluster = &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: etcdClusterName,
			Namespace: namespace, 
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: "v3.5.0",
		},
	}
)


func TestCreateAndDeleteOneMemberEtcdCluster(t *testing.T) {
	feature := features.New("etcd-operator controller")
	size = 1

	// feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
	// 	r, err := resources.New(c.Client().RESTConfig())
	// 	if err != nil {
	// 		t.Fail()
	// 	}
	// 	ecv1alpha1.AddToScheme(r.GetScheme())
	// 	r.WithNamespace(namespace)
	// 	err = decoder.DecodeEachFile(
	// 		ctx, os.DirFS("./testdata/etcd_cluster"), "*",
	// 		decoder.CreateHandler(r),
	// 		decoder.MutateNamespace(namespace),
	// 	)
	// 	if err != nil {
	// 		t.Fatalf("Failed due to error: %s", err)
	// 	}
	// 	return ctx
	// })

	feature.Assess("Check if CRD created", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		apiextensionsV1.AddToScheme(client.Resources().GetScheme())
		name := "etcdclusters.operator.etcd.io"
		var crd apiextensionsV1.CustomResourceDefinition
		if err := client.Resources().Get(ctx, name, "", &crd); err != nil {
			t.Fatalf("Failed due to error: %s", err)
		}

		if crd.Spec.Group != "operator.etcd.io" {
			t.Fatalf("Expected crd group to be operator.etcd.io, got %s", crd.Spec.Group)
		}

		// klog.InfoS("CRD Details", "cr", crd)
		return ctx
	})

	feature.Assess("Check if etcd cluster resource created with size=1", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		ecv1alpha1.AddToScheme(client.Resources(namespace).GetScheme())
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("Failed to create an etcd cluster resource: %s", err)
		}
		// wait for resource to be created
		if err := wait.For(
			conditions.New(client.Resources()).ResourceMatch(etcdCluster, func(object k8s.Object) bool {
				return true
			}),
			wait.WithTimeout(3*time.Minute),
			wait.WithInterval(30*time.Second),
		); err != nil {
			t.Fatal(err)
		}

		var ec ecv1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			if ec.Spec.Size != 1 {
				log.Fatalf("etcd cluster size is not equal to 1: %s", err)
			}
		}

		return ctx
	})

	feature.Assess("Check if the sts exists and is of size 1", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context { 
		client := c.Client()
		appsv1.AddToScheme(client.Resources(namespace).GetScheme())
		var sts appsv1.StatefulSet
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
			t.Fatalf("sts does not exist: %s", err)
		}

		if *sts.Spec.Replicas != int32(etcdCluster.Spec.Size) {
			t.Fatalf("sts does not have the same number of replicas as the required etcd cluster size")
		}
		return ctx
	})

	// feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
	// 	client := c.Client()
	// 	var ec ecv1alpha1.EtcdCluster
	// 	if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
	// 		log.Println("no resource to teardown...")
	// 		log.Println(err)
	// 	}
	// 	log.Println("deleting etcd cluster resource")
	// 	if err := client.Resources().Delete(ctx, &ec); err != nil {
	// 		t.Fatalf("unable to delete etcd cluster: %s", err)
	// 	}

	// 	// sts must not exist
	// 	var sts appsv1.StatefulSet
	// 	if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
	// 		if !errors.IsNotFound(err) {
	// 			t.Fatalf("there was a problem with the statefulset: %s", err)
	// 		}
	// 	}
	// 	return ctx
	// })

	_ = testenv.Test(t, feature.Feature())

}

func TestScaleOutExistingEtcdCluster(t *testing.T) {
	feature := features.New("etcd-operator controller")
	feature.Assess("Check if CRD created", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		apiextensionsV1.AddToScheme(client.Resources().GetScheme())
		name := "etcdclusters.operator.etcd.io"
		var crd apiextensionsV1.CustomResourceDefinition
		if err := client.Resources().Get(ctx, name, "", &crd); err != nil {
			t.Fatalf("Failed due to error: %s", err)
		}

		if crd.Spec.Group != "operator.etcd.io" {
			t.Fatalf("Expected crd group to be operator.etcd.io, got %s", crd.Spec.Group)
		}

		// klog.InfoS("CRD Details", "cr", crd)
		return ctx
	})

	feature.Assess("Scale out etcd cluster to size 3", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		ecv1alpha1.AddToScheme(client.Resources(namespace).GetScheme())
		// if err := client.Resources().Create(ctx, etcdCluster); err != nil {
		// 	t.Fatalf("Failed to create an etcd cluster resource: %s", err)
		// }
		// // wait for resource to be created
		// if err := wait.For(
		// 	conditions.New(client.Resources()).ResourceMatch(etcdCluster, func(object k8s.Object) bool {
		// 		return true
		// 	}),
		// 	wait.WithTimeout(3*time.Minute),
		// 	wait.WithInterval(30*time.Second),
		// ); err != nil {
		// 	t.Fatal(err)
		// }

		etcdCluster.Spec.Size = 3
		var ec ecv1alpha1.EtcdCluster
		log.Println("beginning to scale to 3...")
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &ec); err == nil {
			if ec.Spec.Size != 3 {
				if err := c.Client().Resources().Update(ctx, etcdCluster); err != nil {
					t.Fatalf("failed to update etcd cluster size: %v", err)
				}
		
				var sts appsv1.StatefulSet
				c.Client().Resources().Get(ctx, etcdClusterName, namespace, &sts)
				// Wait for StatefulSet to scale to 3 replicas
				if err := wait.For(
					conditions.New(c.Client().Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
							// Fetch the current replica count from the StatefulSet
							if s, ok := object.(*appsv1.StatefulSet); ok {
								return s.Status.Replicas
							}
							return 0
						},3, // Desired replica count
					),
					wait.WithTimeout(3*time.Minute),   // Timeout for the wait
					wait.WithInterval(30*time.Second), // Polling interval
				); err != nil {
					t.Fatalf("failed to scale StatefulSet to 3 replicas: %v", err)
				}
		
				log.Println("etcd cluster successfully scaled to size 3...")
			}
		}

		return ctx
	})

	feature.Assess("Check if the sts exists and is of size 3", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context { 
		client := c.Client()
		appsv1.AddToScheme(client.Resources(namespace).GetScheme())
		var sts appsv1.StatefulSet
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
			t.Fatalf("sts does not exist: %s", err)
		}

		if *sts.Spec.Replicas != int32(etcdCluster.Spec.Size) {
			t.Fatalf("sts does not have the same number of replicas as the required etcd cluster size")
		}
		return ctx
	})

	_ = testenv.Test(t, feature.Feature())

}
