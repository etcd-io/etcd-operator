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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
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

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		client := cfg.Client()

		// create the etcd cluster
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create etcd cluster: %s", err)
		}

		// get the etcd cluster object
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to fetch etcd cluster: %s", err)
		}

		return ctx
	})

	feature.Assess("Check if there exists one replica of the etcd pod",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			if err := waitForAllEtcdMemberReady(t, c, etcdCluster); err != nil {
				t.Fatalf("unable to find the replica of the etcd pod: %s", err)
			}
			return ctx
		},
	)

	feature.Assess("Write data to the etcd pod",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			// get the etcd pod
			var pod corev1.Pod
			if err := client.Resources().Get(ctx, fmt.Sprintf("%s-%d", etcdClusterName, 0), namespace, &pod); err != nil {
				log.Fatalf("unable to get the etcd pod: %s", err)
			}

			// write data to the pod
			if err := writeDataToPod(ctx, &pod, client, key, input_value); err != nil {
				t.Fatalf("unable to write into pod: %s", err)
			}

			return ctx
		},
	)

	feature.Assess("Delete the etcd pod",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			// get the etcd pod
			var pod corev1.Pod
			if err := client.Resources().Get(ctx, fmt.Sprintf("%s-%d", etcdClusterName, 0), namespace, &pod); err != nil {
				log.Fatalf("unable to get the etcd pod: %s", err)
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
			// TODO: deleting a member's Pod is never noticed and repaired —
			// the live-health-to-EtcdMember mapping that would flip a Ready
			// member to Recreating, and the Recreating-phase repair step
			// itself, are both M3 (see
			// internal/controller/etcdcluster_controller.go dispatch, item 5
			// and its "not implemented yet" branches). Unskip once M3's
			// Pod-recovery ladder lands.
			t.Skip("blocked on M3 pod-recovery ladder; see internal/controller/etcdcluster_controller.go dispatch item 5")

			client := c.Client()

			if err := waitForAllEtcdMemberReady(t, c, etcdCluster); err != nil {
				t.Fatalf("unable to scale the etcd pod back post pod deletion: %s", err)
			}

			// get the etcd pod
			var pod corev1.Pod
			if err := client.Resources().Get(ctx, fmt.Sprintf("%s-%d", etcdClusterName, 0), namespace, &pod); err != nil {
				log.Fatalf("unable to get the etcd pod: %s", err)
			}

			// read data from pod
			var err error
			var val string
			if val, err = readDataFromPod(ctx, &pod, client, key); err != nil {
				t.Logf("value: %s", val)
				t.Fatalf("unable to fetch data from the etcd pod: %s", err)
			}

			// compare the value read against the value written
			if val != input_value {
				t.Fatalf("value fetched does not match the waitForPodReadinessinput value...input value=%s, fetched value=%s",
					input_value, val)
			}

			return ctx
		},
	)

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, etcdClusterName)
		return ctx
	})

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
