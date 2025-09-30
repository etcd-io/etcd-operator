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
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/test/utils"
)

func TestDataPersistence(t *testing.T) {
	feature := features.New("data-persistence")

	const etcdClusterName = "etcd-cluster-test"
	const size = 1
	const key = "key"
	const input_value = "value"

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
			Size:    size,
			Version: "v3.5.18",
			StorageSpec: &ecv1alpha1.StorageSpec{
				AccessModes:       corev1.ReadWriteOnce,
				StorageClassName:  "standard",
				PVCName:           "test-pvc",
				VolumeSizeRequest: resource.MustParse("64Mi"),
				VolumeSizeLimit:   resource.MustParse("64Mi"),
			},
		},
	}

	objKey := runtimeClient.ObjectKeyFromObject(etcdCluster)

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		client := cfg.Client()

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

	feature.Assess("Check if sts exists with size 1",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			var sts appsv1.StatefulSet

			ops := []wait.Option{
				wait.WithTimeout(3 * time.Minute),
				wait.WithInterval(10 * time.Second),
			}

			if err := utils.GetKubernetesResource(ctx, client, objKey, &sts, ops...); err != nil {
				t.Fatalf("unable to get sts: %s", err)
			}

			if err := wait.For(
				conditions.New(client.Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
					return sts.Status.ReadyReplicas
				}, 1),
				ops...,
			); err != nil {
				t.Fatalf("unable to create sts with size 1: %s", err)
			}

			waitForClusterHealthyStatus(t, c, etcdClusterName, size)

			return ctx
		},
	)

	feature.Assess("Write data to the sts pod",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			// get sts pod
			var pod corev1.Pod
			if err := client.Resources().Get(ctx, fmt.Sprintf("%s-%d", etcdClusterName, 0), namespace, &pod); err != nil {
				log.Fatalf("unable to get sts pod: %s", err)
			}

			// write data to the pod
			if err := writeDataToPod(ctx, &pod, client, key, input_value); err != nil {
				t.Fatalf("unable to write into pod: %s", err)
			}

			return ctx
		},
	)

	feature.Assess("Delete the sts pod",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			// get sts pod
			var pod corev1.Pod
			if err := client.Resources().Get(ctx, fmt.Sprintf("%s-%d", etcdClusterName, 0), namespace, &pod); err != nil {
				log.Fatalf("unable to get sts pod: %s", err)
			}

			// delete the pod
			if err := client.Resources().Delete(ctx, &pod); err != nil {
				t.Fatalf("unable to delete pod")
			}

			return ctx
		},
	)

	feature.Assess("Read data from the newly created pod",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			// wait until sts's ReadyReplicas turn to 1
			var sts appsv1.StatefulSet
			if err := utils.GetKubernetesResource(ctx, client, objKey, &sts); err != nil {
				t.Fatalf("unable to get sts post pod deletion: %s", err)
			}

			if err := wait.For(
				conditions.New(client.Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
					return sts.Status.ReadyReplicas
				}, 1),
				wait.WithTimeout(3*time.Minute),
			); err != nil {
				t.Fatalf("unable to scale sts post pod deletion: %s", err)
			}

			// get sts pod
			var pod corev1.Pod
			if err := client.Resources().Get(ctx, fmt.Sprintf("%s-%d", etcdClusterName, 0), namespace, &pod); err != nil {
				log.Fatalf("unable to get sts pod: %s", err)
			}

			// read data from pod
			var err error
			var val string
			if val, err = readDataFromPod(ctx, &pod, client, key); err != nil {
				t.Logf("value: %s", val)
				t.Fatalf("unable to fetch data from sts pod: %s", err)
			}

			// compare the value read against the value written
			if val != input_value {
				t.Fatalf("value fetched does not match the input value...input value=%s, fetched value=%s", input_value, val)
			}

			return ctx
		},
	)

	_ = testEnv.Test(t, feature.Feature())
}

func writeDataToPod(ctx context.Context, pod *corev1.Pod, client klient.Client, key, input_value string) error {
	var stdout, stderr bytes.Buffer
	if err := client.Resources().ExecInPod(
		ctx,
		namespace,
		pod.GetObjectMeta().GetName(),
		pod.Spec.Containers[0].Name,
		[]string{"etcdctl", "put", key, input_value}, &stdout, &stderr); err != nil {
		return err
	}
	return nil
}

func readDataFromPod(ctx context.Context, pod *corev1.Pod, client klient.Client, key string) (string, error) {
	var stdout, stderr bytes.Buffer
	if err := client.Resources().ExecInPod(
		ctx,
		namespace,
		pod.GetObjectMeta().GetName(),
		pod.Spec.Containers[0].Name,
		[]string{"etcdctl", "get", key, "--print-value-only"}, &stdout, &stderr); err != nil {
		return strings.TrimSpace(stdout.String()), err
	}
	return strings.TrimSpace(stdout.String()), nil
}
