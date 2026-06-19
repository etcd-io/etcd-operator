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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestEtcdRestoreFromBackup exercises the full backup -> restore round trip
// end-to-end against an in-cluster MinIO (S3-compatible) bucket, so it requires
// NO real cloud credentials. It is gated behind ETCD_E2E_BACKUP=true (the same
// gate as the backup e2e) because it provisions extra workloads (MinIO).
//
// It reuses the backup e2e's local helpers (deployMinIO, createBucketPod,
// createBackupCredsSecret, createBackupTestCluster, waitStatefulSetReady,
// backupExecInPod) so it does not depend on helpers from other in-flight PRs.
//
// Flow:
//  1. Deploy MinIO + bucket + creds secret.
//  2. Create a source EtcdCluster, seed a distinctive key, and back it up to
//     MinIO; wait for the EtcdBackup to reach Completed.
//  3. Create an EtcdRestore referencing that backup, targeting a NEW cluster.
//  4. Wait for the EtcdRestore to reach Completed, then assert the seeded key
//     is readable from the restored cluster's member.
func TestEtcdRestoreFromBackup(t *testing.T) {
	if os.Getenv("ETCD_E2E_BACKUP") != "true" {
		t.Skip("set ETCD_E2E_BACKUP=true to run the object-storage restore e2e (provisions MinIO)")
	}

	const (
		sourceCluster   = "etcd-restore-src-e2e"
		restoredCluster = "etcd-restore-dst-e2e"
		backupName      = "etcd-restore-e2e-snap"
		restoreName     = "etcd-restore-e2e"
		minioName       = "minio-restore-e2e"
		bucket          = "etcd-restores"
		accessKey       = "minioadmin"
		secretKey       = "minioadmin"
		credsSecret     = "restore-e2e-creds"
		seedKey         = "restore-e2e-key"
		seedValue       = "restore-e2e-value"
	)

	feature := features.New("etcd-restore-from-backup")

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		deployMinIO(ctx, t, cfg, minioName, accessKey, secretKey)
		createBucketPod(ctx, t, cfg, minioName, bucket, accessKey, secretKey)
		createBackupCredsSecret(ctx, t, cfg, credsSecret, accessKey, secretKey)
		createBackupTestCluster(ctx, t, cfg, sourceCluster, 1)
		waitStatefulSetReady(t, cfg, sourceCluster, 1)

		if _, _, err := backupExecInPod(t, cfg, sourceCluster+"-0",
			[]string{"etcdctl", "put", seedKey, seedValue}); err != nil {
			t.Fatalf("seed key: %v", err)
		}

		endpoint := "http://" + minioName + "." + namespace + ".svc.cluster.local:9000"
		backup := &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdBackupSpec{
				ClusterRef: sourceCluster,
				Destination: ecv1alpha1.BackupDestination{
					Provider:  ecv1alpha1.BackupProviderS3,
					Prefix:    "e2e",
					SecretRef: &corev1.LocalObjectReference{Name: credsSecret},
					S3: &ecv1alpha1.S3DestinationSpec{
						Bucket:         bucket,
						Region:         "us-east-1",
						Endpoint:       endpoint,
						ForcePathStyle: true,
					},
				},
			},
		}
		if err := cfg.Client().Resources().Create(ctx, backup); err != nil {
			t.Fatalf("create EtcdBackup: %v", err)
		}
		waitBackupCompleted(t, cfg, backupName)
		return ctx
	})

	feature.Assess("restore reaches Completed and data is present", func(
		ctx context.Context, t *testing.T, cfg *envconf.Config,
	) context.Context {
		restore := &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdRestoreSpec{
				Source: ecv1alpha1.SnapshotSource{
					BackupRef: &ecv1alpha1.BackupReference{Name: backupName},
				},
				Target: ecv1alpha1.RestoreTarget{
					Name:    restoredCluster,
					Size:    1,
					Version: "v3.6.1",
				},
			},
		}
		if err := cfg.Client().Resources().Create(ctx, restore); err != nil {
			t.Fatalf("create EtcdRestore: %v", err)
		}

		err := wait.For(func(ctx context.Context) (bool, error) {
			var got ecv1alpha1.EtcdRestore
			if err := cfg.Client().Resources().Get(ctx, restoreName, namespace, &got); err != nil {
				return false, err
			}
			switch got.Status.Phase {
			case ecv1alpha1.RestorePhaseCompleted:
				return true, nil
			case ecv1alpha1.RestorePhaseFailed:
				t.Fatalf("restore failed: %+v", got.Status.Conditions)
			}
			return false, nil
		}, wait.WithTimeout(3*time.Minute), wait.WithInterval(5*time.Second))
		if err != nil {
			t.Fatalf("waiting for restore completion: %v", err)
		}

		// The restored cluster's seed member should come up with the data.
		waitStatefulSetReady(t, cfg, restoredCluster, 1)
		stdout, _, err := backupExecInPod(t, cfg, restoredCluster+"-0",
			[]string{"etcdctl", "get", seedKey, "--print-value-only"})
		if err != nil {
			t.Fatalf("read restored key: %v", err)
		}
		if !strings.Contains(stdout, seedValue) {
			t.Errorf("restored cluster missing seeded data: got %q, want it to contain %q", stdout, seedValue)
		}
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// waitBackupCompleted blocks until the named EtcdBackup reaches Completed,
// failing the test on a Failed phase or timeout. It is a small local helper so
// the restore e2e does not depend on test ordering with the backup e2e.
func waitBackupCompleted(t *testing.T, cfg *envconf.Config, name string) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var got ecv1alpha1.EtcdBackup
		if err := cfg.Client().Resources().Get(ctx, name, namespace, &got); err != nil {
			return false, err
		}
		switch got.Status.Phase {
		case ecv1alpha1.BackupPhaseCompleted:
			return true, nil
		case ecv1alpha1.BackupPhaseFailed:
			t.Fatalf("source backup failed: %+v", got.Status.Conditions)
		}
		return false, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("waiting for source backup completion: %v", err)
	}
}
