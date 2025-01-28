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
	"testing"
	"time"

	apiextensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"

	appsv1 "k8s.io/api/apps/v1"

	// "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"

	"sigs.k8s.io/e2e-framework/pkg/envconf"

	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	controller "go.etcd.io/etcd-operator/internal/controller"
)


func TestCreateAndDeleteOneMemberEtcdCluster(t *testing.T) {
	feature := features.New("etcd-operator controller")

	const etcdClusterName = "etcd-cluster-test-1"
	const size = 1

	var etcdCluster = &ecv1alpha1.EtcdCluster{
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

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		// Check if the CRD exists
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
		
		// Create the etcd cluster resource
		ecv1alpha1.AddToScheme(client.Resources().GetScheme())
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create an etcd cluster resource of size 1: %s", err)
		}

		if err := wait.For(
			conditions.New(client.Resources()).ResourceMatch(etcdCluster, func(object k8s.Object) bool {
				return true
			}), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}
		return ctx
	})

	feature.Assess("Check if etcd cluster resource created with size=1", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		// ecv1alpha1.AddToScheme(client.Resources(namespace).GetScheme())
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

		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
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

	// Remove all the resources created 
	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to fetch etcd cluster while tearing down feature: %s", err)
		}
		// delete etcd cluster 
		if err := client.Resources().Delete(ctx, etcdCluster); err != nil {
			t.Fatalf("error in deleting etcd cluster while tearing down feature: %s", err)
		}
		// verify that no sts for the etcd cluster exists
		if err := wait.For(
			conditions.New(client.Resources()).ResourceDeleted(etcdCluster), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		log.Println("tore down etcd cluster test 1 resource successfully...")

		return ctx
	})

	_ = testenv.Test(t, feature.Feature())
}

func TestCreateAndDeleteThreeMemeberEtcdCluster(t *testing.T) {
	feature := features.New("etcd-operator controller")
	
	const etcdClusterName = "etcd-cluster-test-2"
	const size = 3

	var etcdCluster = &ecv1alpha1.EtcdCluster{
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

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		// Check if the CRD exists
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
		
		// Create the etcd cluster resource
		ecv1alpha1.AddToScheme(client.Resources().GetScheme())
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create an etcd cluster resource of size 3: %s", err)
		}

		if err := wait.For(
			conditions.New(client.Resources()).ResourceMatch(etcdCluster, func(object k8s.Object) bool {
				return true
			}), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		// fetch sts created via etcd cluster
		var sts appsv1.StatefulSet
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
			t.Fatalf("unable to fetch sts resource: %s", err)
		}

		// wait for sts to scale to 3
		if err := wait.For(
			conditions.New(client.Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
				return sts.Status.ReadyReplicas

			}, 3), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}
		return ctx
	})

	feature.Assess("Check if etcd cluster resource created with size=3", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		// ecv1alpha1.AddToScheme(client.Resources(namespace).GetScheme())
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

		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			if ec.Spec.Size != 3 {
				log.Fatalf("etcd cluster size is not equal to 3: %s", err)
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

	// Remove all the resources created 
	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to fetch etcd cluster while tearing down feature: %s", err)
		}
		// delete etcd cluster 
		if err := client.Resources().Delete(ctx, etcdCluster); err != nil {
			t.Fatalf("error in deleting etcd cluster while tearing down feature: %s", err)
		}
		// verify that no sts for the etcd cluster exists
		if err := wait.For(
			conditions.New(client.Resources()).ResourceDeleted(&ec), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		log.Println("tore down etcd cluster test 2 resource successfully...")

		return ctx
	})

	_ = testenv.Test(t, feature.Feature())
}

func TestCreateAndScaleOneMemberEtcdClusterToThree(t *testing.T) {
	feature := features.New("etcd-operator controller")

	const etcdClusterName = "etcd-cluster-test-3"
	
	var size = 1

	var etcdCluster = &ecv1alpha1.EtcdCluster{
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

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		// Check if the CRD exists
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
		
		// Create the etcd cluster resource
		ecv1alpha1.AddToScheme(client.Resources().GetScheme())
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create an etcd cluster resource of size 1: %s", err)
		}
		if err := wait.For(
			conditions.New(client.Resources()).ResourceMatch(etcdCluster, func(object k8s.Object) bool {
				return true
			}), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		// ensure that sts gets its pod in ready state
		var sts appsv1.StatefulSet
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
			t.Fatalf("sts does not exist: %s", err)
		}

		// if err := wait.For(
		// 	conditions.New(client.Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
		// 		return sts.Status.ReadyReplicas
		// 	}, 1),
		// 	wait.WithTimeout(3 * time.Minute), 
		// 	wait.WithInterval(30 * time.Second), 
		// ); err != nil {
		// 	t.Fatal(err)
		// }

		if err := wait.For(
			conditions.New(client.Resources()).ResourceMatch(&sts, func(object k8s.Object) bool {
				return sts.Status.ReadyReplicas == 1
			}),
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		return ctx
	})

	feature.Assess("Check if etcd cluster resource created with size=1", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		// ecv1alpha1.AddToScheme(client.Resources(namespace).GetScheme())
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

		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
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

		if int(*sts.Spec.Replicas) != etcdCluster.Spec.Size {
			t.Fatalf("sts does not have the same number of replicas as the required etcd cluster size")
		}
		return ctx
	})

	feature.Assess("Scale etcd cluster size to 3", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()

		// get etcd cluster resource 
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to get etcd cluster: %s", err)
		}

		// scale etcd cluster 
		etcdCluster.Spec.Size = 3

		if err := client.Resources().Update(ctx, etcdCluster); err != nil {
			t.Fatalf("failed to update etcd cluster size to 3: %s", err)
		}

		return ctx
	})

	feature.Assess("Check if the sts exists and is of the size 3", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		
		// get the sts 
		var sts appsv1.StatefulSet
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
			t.Fatalf("unable to fetch sts resource: %s", err)
		}

		// check the sts size
		// if not 3 then wait for it to scale
		if err := wait.For(
			conditions.New(client.Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
				return sts.Status.ReadyReplicas
			}, 3), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatalf("unable to scale sts to 3: %s", err)
		}

		return ctx
	})

	// Remove all the resources created
	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to fetch etcd cluster while tearing down feature: %s", err)
		}
		// delete etcd cluster 
		if err := client.Resources().Delete(ctx, etcdCluster); err != nil {
			t.Fatalf("error in deleting etcd cluster while tearing down feature: %s", err)
		}
		// verify that no sts for the etcd cluster exists
		if err := wait.For(
			conditions.New(client.Resources()).ResourceDeleted(&ec), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		log.Println("tore down etcd cluster test 3 resource successfully...")

		return ctx
	})

	_ = testenv.Test(t, feature.Feature())
}

func TestHealthCheck(t *testing.T) {
	feature := features.New("etcd-operator-controller")

	const etcdClusterName = "etcd-cluster-test-4"

	var size = 3

	var etcdCluster = &ecv1alpha1.EtcdCluster{
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

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		// Check if the CRD exists
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
		
		// Create the etcd cluster resource
		ecv1alpha1.AddToScheme(client.Resources().GetScheme())
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create an etcd cluster resource of size 1: %s", err)
		}
		if err := wait.For(
			conditions.New(client.Resources()).ResourceMatch(etcdCluster, func(object k8s.Object) bool {
				return true
			}), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		// ensure that sts gets its pod in ready state
		var sts appsv1.StatefulSet
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
			t.Fatalf("sts does not exist: %s", err)
		}

		if err := wait.For(
			conditions.New(client.Resources()).ResourceMatch(&sts, func(object k8s.Object) bool {
				return sts.Status.ReadyReplicas == 3
			}),
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		return ctx
	})

	feature.Assess("Check if etcd cluster resource created with size=3", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		// ecv1alpha1.AddToScheme(client.Resources(namespace).GetScheme())
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

		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			if ec.Spec.Size != 3 {
				log.Fatalf("etcd cluster size is not equal to 3: %s", err)
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

	// feature.Assess("Hit URL for the sts pod", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
	// 	client := c.Client()
	// 	log := klog.FromContext(ctx)

	// 	// fetch the sts resource 
	// 	var sts appsv1.StatefulSet
	// 	if err := client.Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
	// 		t.Fatalf("unable to fetch sts resource: %s", err)
	// 	}

		
	// 	if err := wait.For(
	// 		conditions.New(client.Resources()).ResourceMatch(
	// 			&sts, 
	// 			func(obj k8s.Object) bool {
	// 				// Cast to the just-refetched resource
	// 				statefulSet, ok := obj.(*appsv1.StatefulSet)
	// 				if !ok {
	// 					// If for some reason it isn't a StatefulSet, not healthy
	// 					return false
	// 				}
		
	// 				memberListResp, healthInfos, err := controller.HealthCheck(statefulSet, log)
	// 				if err != nil {
	// 					// Not healthy yet, keep waiting
	// 					return false
	// 				}
		
	// 				memberCount := 0
	// 				if memberListResp != nil {
	// 					memberCount = len(memberListResp.Members)
	// 				}
	// 				if memberCount != len(healthInfos) {
	// 					// t.Fatalf(
	// 					//   "memberCnt (%d) isn't equal to healthy member count (%d)",
	// 					//   memberCount, len(healthInfos),
	// 					// )
	// 					return false
	// 				}
		
	// 				log.Info("health check successful...")
	// 				return true
	// 			},
	// 		),
	// 		wait.WithTimeout(3*time.Minute),
	// 		wait.WithInterval(30*time.Second),
	// 	); err != nil {
	// 		t.Fatalf("timed out waiting for health check to succeed: %v", err)
	// 	}

	// 	return ctx
	// })

	// Remove all the resources created
	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client := c.Client()
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to fetch etcd cluster while tearing down feature: %s", err)
		}
		// delete etcd cluster 
		if err := client.Resources().Delete(ctx, etcdCluster); err != nil {
			t.Fatalf("error in deleting etcd cluster while tearing down feature: %s", err)
		}
		// verify that no sts for the etcd cluster exists
		if err := wait.For(
			conditions.New(client.Resources()).ResourceDeleted(&ec), 
			wait.WithTimeout(3 * time.Minute), 
			wait.WithInterval(30 * time.Second), 
		); err != nil {
			t.Fatal(err)
		}

		log.Println("tore down etcd cluster test 4 resource successfully...")

		return ctx
	})

	_ = testenv.Test(t, feature.Feature())

}

/*
func TestScaleOutExistingEtcdCluster(t *testing.T) {
	feature := features.New("etcd-operator controller")
	etcdCluster.Spec.Size = 3
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
*/
