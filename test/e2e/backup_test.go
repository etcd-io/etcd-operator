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
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestEtcdBackupToS3 exercises the EtcdBackup snapshot+upload flow end-to-end
// against an in-cluster MinIO (S3-compatible) bucket, so it requires NO real
// cloud credentials. It is gated behind ETCD_E2E_BACKUP=true because it
// provisions extra workloads (MinIO) and is not part of the default e2e matrix.
//
// Every helper it uses is defined locally in this file so the test does not
// depend on helpers added by other in-flight e2e PRs.
//
// Flow:
//  1. Deploy a single-pod MinIO + Service, and create a bucket via a one-shot
//     mc pod.
//  2. Create a creds Secret for the operator to read.
//  3. Create a small EtcdCluster and wait for its StatefulSet ready, then seed
//     a key so the snapshot has observable data.
//  4. Create an EtcdBackup pointing at MinIO and wait for phase Completed,
//     asserting the status fields (location, size, completion time).
func TestEtcdBackupToS3(t *testing.T) {
	if os.Getenv("ETCD_E2E_BACKUP") != "true" {
		t.Skip("set ETCD_E2E_BACKUP=true to run the object-storage backup e2e (provisions MinIO)")
	}

	const (
		clusterName = "etcd-backup-e2e"
		backupName  = "etcd-backup-e2e-snap"
		minioName   = "minio-backup-e2e"
		bucket      = "etcd-backups"
		accessKey   = "minioadmin"
		secretKey   = "minioadmin"
		credsSecret = "backup-e2e-creds"
	)

	feature := features.New("etcd-backup-s3")

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		deployMinIO(ctx, t, cfg, minioName, accessKey, secretKey)
		createBucketPod(ctx, t, cfg, minioName, bucket, accessKey, secretKey)
		createBackupCredsSecret(ctx, t, cfg, credsSecret, accessKey, secretKey)
		createBackupTestCluster(ctx, t, cfg, clusterName, 1)
		waitStatefulSetReady(t, cfg, clusterName, 1)

		if _, _, err := backupExecInPod(t, cfg, clusterName+"-0",
			[]string{"etcdctl", "put", "backup-e2e-key", "backup-e2e-value"}); err != nil {
			t.Fatalf("seed key: %v", err)
		}

		backup := &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdBackupSpec{
				ClusterRef: clusterName,
				Destination: ecv1alpha1.BackupDestination{
					Provider:  ecv1alpha1.BackupProviderS3,
					Prefix:    "e2e",
					SecretRef: &corev1.LocalObjectReference{Name: credsSecret},
					S3: &ecv1alpha1.S3DestinationSpec{
						Bucket:         bucket,
						Region:         "us-east-1",
						Endpoint:       "http://" + minioName + "." + namespace + ".svc.cluster.local:9000",
						ForcePathStyle: true,
					},
				},
			},
		}
		if err := cfg.Client().Resources().Create(ctx, backup); err != nil {
			t.Fatalf("create EtcdBackup: %v", err)
		}
		return ctx
	})

	feature.Assess("backup reaches Completed", func(
		ctx context.Context, t *testing.T, cfg *envconf.Config,
	) context.Context {
		err := wait.For(func(ctx context.Context) (bool, error) {
			var got ecv1alpha1.EtcdBackup
			if err := cfg.Client().Resources().Get(ctx, backupName, namespace, &got); err != nil {
				return false, err
			}
			switch got.Status.Phase {
			case ecv1alpha1.BackupPhaseCompleted:
				return true, nil
			case ecv1alpha1.BackupPhaseFailed:
				t.Fatalf("backup failed: %+v", got.Status.Conditions)
			}
			return false, nil
		}, wait.WithTimeout(3*time.Minute), wait.WithInterval(5*time.Second))
		if err != nil {
			t.Fatalf("waiting for backup completion: %v", err)
		}

		var done ecv1alpha1.EtcdBackup
		if err := cfg.Client().Resources().Get(ctx, backupName, namespace, &done); err != nil {
			t.Fatalf("get completed backup: %v", err)
		}
		if done.Status.SnapshotSizeBytes <= 0 {
			t.Errorf("expected positive snapshot size, got %d", done.Status.SnapshotSizeBytes)
		}
		if done.Status.SnapshotLocation == "" {
			t.Errorf("expected snapshot location to be set")
		}
		if done.Status.CompletionTime == nil {
			t.Errorf("expected completion time to be set")
		}
		t.Logf("backup completed: location=%s size=%d", done.Status.SnapshotLocation, done.Status.SnapshotSizeBytes)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// --- locally-scoped helpers (intentionally self-contained) ------------------

func backupExecInPod(t *testing.T, cfg *envconf.Config, podName string, command []string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	var pod corev1.Pod
	if err := cfg.Client().Resources().Get(t.Context(), podName, namespace, &pod); err != nil {
		return "", "", err
	}
	containerName := pod.Spec.Containers[0].Name
	err := cfg.Client().Resources().ExecInPod(t.Context(), namespace, podName, containerName, command, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func createBackupTestCluster(ctx context.Context, t *testing.T, cfg *envconf.Config, name string, size int) {
	t.Helper()
	cluster := &ecv1alpha1.EtcdCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "operator.etcd.io/v1alpha1", Kind: "EtcdCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: size, Version: "v3.6.1"},
	}
	if err := cfg.Client().Resources().Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster %s: %v", name, err)
	}
}

func waitStatefulSetReady(t *testing.T, cfg *envconf.Config, name string, replicas int32) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var sts appsv1.StatefulSet
		if err := cfg.Client().Resources().Get(ctx, name, namespace, &sts); err != nil {
			return false, nil
		}
		return sts.Status.ReadyReplicas == replicas, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("statefulset %s not ready: %v", name, err)
	}
}

func createBackupCredsSecret(
	ctx context.Context, t *testing.T, cfg *envconf.Config, name, accessKey, secretKey string,
) {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data: map[string][]byte{
			"accessKeyID":     []byte(accessKey),
			"secretAccessKey": []byte(secretKey),
		},
	}
	if err := cfg.Client().Resources().Create(ctx, secret); err != nil {
		t.Fatalf("create creds secret: %v", err)
	}
}

func deployMinIO(ctx context.Context, t *testing.T, cfg *envconf.Config, name, accessKey, secretKey string) {
	t.Helper()
	labels := map[string]string{"app": name}
	replicas := int32(1)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "minio",
						Image: "quay.io/minio/minio:latest",
						Args:  []string{"server", "/data", "--address", ":9000"},
						Env: []corev1.EnvVar{
							{Name: "MINIO_ROOT_USER", Value: accessKey},
							{Name: "MINIO_ROOT_PASSWORD", Value: secretKey},
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 9000}},
					}},
				},
			},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, deploy); err != nil {
		t.Fatalf("create minio deployment: %v", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: 9000, TargetPort: intstr.FromInt(9000)}},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, svc); err != nil {
		t.Fatalf("create minio service: %v", err)
	}

	if err := wait.For(conditions.New(cfg.Client().Resources()).
		DeploymentConditionMatch(deploy, appsv1.DeploymentAvailable, corev1.ConditionTrue),
		wait.WithTimeout(2*time.Minute)); err != nil {
		t.Fatalf("minio not available: %v", err)
	}
}

func createBucketPod(
	ctx context.Context, t *testing.T, cfg *envconf.Config, minioName, bucket, accessKey, secretKey string,
) {
	t.Helper()
	podName := "mc-mkbucket"
	script := "mc alias set m http://" + minioName + ":9000 " + accessKey + " " + secretKey +
		" && mc mb --ignore-existing m/" + bucket
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyOnFailure,
			Containers: []corev1.Container{{
				Name:    "mc",
				Image:   "quay.io/minio/mc:latest",
				Command: []string{"sh", "-c", script},
			}},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, pod); err != nil {
		t.Fatalf("create bucket-create pod: %v", err)
	}
	if err := wait.For(func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		if err := cfg.Client().Resources().Get(ctx, podName, namespace, &p); err != nil {
			return false, nil
		}
		return p.Status.Phase == corev1.PodSucceeded, nil
	}, wait.WithTimeout(2*time.Minute), wait.WithInterval(3*time.Second)); err != nil {
		t.Fatalf("bucket-create pod did not succeed: %v", err)
	}
}
