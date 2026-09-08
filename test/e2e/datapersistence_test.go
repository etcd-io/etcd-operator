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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestDataPersistence verifies that an etcd member's data survives its Pod
// being deleted and recreated, as long as the underlying PVC is left alone:
// it writes a key, deletes the Pod, recreates an equivalent one under the
// same name (the controller doesn't yet do this itself — see podForRecreate),
// and confirms the key reads back from the recreated Pod.
//
// TODO: add a ReadWriteMany sub case (shared PVC, per-member SubPath) once
// the e2e environment has a StorageClass that supports it.
func TestDataPersistence(t *testing.T) {
	feature := features.New("data-persistence")

	const etcdClusterName = "etcd-cluster-test"
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
			Size:    1,
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

	// podSpec is captured from the live Pod just before it's deleted, so the
	// next step can recreate an equivalent Pod by hand — the controller does
	// not yet notice or repair a member's Pod disappearing out-of-band, so
	// this test drives the recreation itself rather than waiting on that behavior.
	//
	// TODO: remove this after the Recreating workflow is supported
	var podSpec *corev1.Pod

	feature.Assess("Delete the etcd pod",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			// get the etcd pod
			var pod corev1.Pod
			if err := client.Resources().Get(ctx, fmt.Sprintf("%s-%d", etcdClusterName, 0), namespace, &pod); err != nil {
				log.Fatalf("unable to get the etcd pod: %s", err)
			}
			podSpec = podForRecreate(&pod)

			// delete the pod
			if err := client.Resources().Delete(ctx, &pod); err != nil {
				t.Fatalf("unable to delete pod")
			}

			// wait until the Pod is fully gone before recreating it under the
			// same name — its PVC is untouched, so the data directory on disk
			// survives and the recreated Pod picks the same etcd member state
			// back up.
			if err := wait.For(func(ctx context.Context) (bool, error) {
				var gone corev1.Pod
				err := client.Resources().Get(ctx, pod.Name, namespace, &gone)
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, nil
			}, wait.WithTimeout(2*time.Minute), wait.WithInterval(2*time.Second)); err != nil {
				t.Fatalf("pod %s was not fully deleted: %s", pod.Name, err)
			}

			return ctx
		},
	)

	feature.Assess("Recreate the pod and read data back from it",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()

			// TODO: remove this after the Recreating workflow is supported
			if err := client.Resources().Create(ctx, podSpec); err != nil {
				t.Fatalf("unable to recreate the etcd pod: %s", err)
			}

			if err := waitForAllEtcdMemberReady(t, c, etcdCluster); err != nil {
				t.Fatalf("recreated etcd pod never became ready: %s", err)
			}

			// get the recreated etcd pod
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
				t.Fatalf("value fetched does not match the input value...input value=%s, fetched value=%s",
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

// podForRecreate returns a new Pod, built from a live Pod's Name/Namespace/
// Labels/Annotations/OwnerReferences/Spec, suitable for a fresh Create call.
// It's used to manually recreate a member's Pod after deleting it, since
// server-populated fields (ResourceVersion, UID, Status, ...) would make
// Create reject a verbatim copy of the fetched object.
//
// TODO: remove this after the Recreating workflow is supported
func podForRecreate(pod *corev1.Pod) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            pod.Name,
			Namespace:       pod.Namespace,
			Labels:          pod.Labels,
			Annotations:     pod.Annotations,
			OwnerReferences: pod.OwnerReferences,
		},
		Spec: pod.Spec,
	}
}

func writeDataToPod(ctx context.Context, pod *corev1.Pod, client klient.Client, key, val string) error {
	var stdout, stderr bytes.Buffer
	if err := client.Resources().ExecInPod(
		ctx,
		namespace,
		pod.GetObjectMeta().GetName(),
		pod.Spec.Containers[0].Name,
		[]string{"etcdctl", "put", key, val}, &stdout, &stderr); err != nil {
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
