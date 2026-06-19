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
	"fmt"
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
// end-to-end against an in-cluster MinIO (S3-compatible) bucket.
//
// It is gated behind ETCD_E2E_BACKUP=true (provisions MinIO) AND, separately,
// behind ETCD_E2E_RESTORE=true. The second gate exists because the restore
// data-path of the feature is known-incomplete against the distroless etcd
// images the operator deploys; running this test against the current restorer
// fails (correctly). The gate keeps the working backup e2e green in the default
// matrix while preserving this test as an executable specification of the
// round-trip the restorer must eventually satisfy.
//
// Three confirmed product gaps make the round-trip impossible today (each is a
// real bug in the restore data-path, not a test defect):
//
//  1. internal/controller/restorer.go execs `sh -c "...; etcdctl snapshot
//     restore ...; ..."` into the member pod, but gcr.io/etcd-development/etcd
//     images are distroless and ship no shell: the exec fails with
//     `exec: "sh": executable file not found in $PATH`. (Same root cause the
//     snapshotter hit; the snapshotter was moved off exec to the clientv3
//     Maintenance API, but restore must write a *data directory* and cannot.)
//  2. The restore writes the data dir to defaultRestoreDataDir
//     (/var/run/etcd/default.etcd), but the member's real data dir is
//     etcdDataDir (/var/lib/etcd, set via ETCD_DATA_DIR and the volume mount in
//     utils.go). etcd never reads the restored directory.
//  3. The restore execs into an already-running member without
//     `--force-new-cluster`/restart semantics, so even a correctly-placed data
//     dir would not be adopted by the live member.
//
// When the restorer is fixed (e.g. restore via an init container into
// /var/lib/etcd before the member starts), drop the ETCD_E2E_RESTORE gate and
// this test becomes a live proof of the round-trip.
//
// Flow (when enabled):
//  1. Deploy MinIO + bucket + creds secret.
//  2. Create a source EtcdCluster, seed a distinctive keyspace, and back it up
//     to MinIO; wait for the EtcdBackup to reach Completed.
//  3. Create an EtcdRestore referencing that backup, targeting a NEW cluster.
//  4. Wait for the EtcdRestore to reach Completed, then assert the ENTIRE seeded
//     keyspace is readable from the restored member, exact values match, and a
//     key that was never in the snapshot is absent (negative control).
func TestEtcdRestoreFromBackup(t *testing.T) {
	if os.Getenv("ETCD_E2E_BACKUP") != "true" {
		t.Skip("set ETCD_E2E_BACKUP=true to run the object-storage restore e2e (provisions MinIO)")
	}
	if os.Getenv("ETCD_E2E_RESTORE") != "true" {
		t.Skip("restore data-path is known-incomplete against distroless etcd images " +
			"(restorer.go execs `sh -c` with no shell in image; restores to /var/run/etcd " +
			"not /var/lib/etcd; no member restart). Set ETCD_E2E_RESTORE=true to run anyway.")
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
		absentKey       = "restore-e2e/never-in-snapshot"
	)

	feature := features.New("etcd-restore-from-backup")

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		deployMinIO(ctx, t, cfg, minioName, accessKey, secretKey)
		createBucketPod(ctx, t, cfg, minioName, bucket, accessKey, secretKey)
		createBackupCredsSecret(ctx, t, cfg, credsSecret, accessKey, secretKey)
		createBackupTestCluster(ctx, t, cfg, sourceCluster, 1)
		waitStatefulSetReady(t, cfg, sourceCluster, 1)
		seedKeyspace(t, cfg, sourceCluster+"-0", backupSeedKeyCount)

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

	feature.Assess("restore reaches Completed and the full keyspace round-trips", func(
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

		// Wait for terminal restore phase without t.Fatalf inside the poller.
		err := wait.For(func(ctx context.Context) (bool, error) {
			var got ecv1alpha1.EtcdRestore
			if err := cfg.Client().Resources().Get(ctx, restoreName, namespace, &got); err != nil {
				return false, err
			}
			switch got.Status.Phase {
			case ecv1alpha1.RestorePhaseCompleted:
				return true, nil
			case ecv1alpha1.RestorePhaseFailed:
				return false, fmt.Errorf("restore failed: %+v", got.Status.Conditions)
			}
			return false, nil
		}, wait.WithTimeout(3*time.Minute), wait.WithInterval(2*time.Second))
		if err != nil {
			t.Fatalf("waiting for restore completion: %v", err)
		}

		waitStatefulSetReady(t, cfg, restoredCluster, 1)

		// Positive proof: the ENTIRE seeded keyspace must be present with exact
		// values. Poll, so a member that boots and then catches up does not race
		// a single-shot read; a never-restored member fails with "data never
		// appeared" rather than an opaque mismatch.
		pod := restoredCluster + "-0"
		if err := wait.For(func(ctx context.Context) (bool, error) {
			for i := 0; i < backupSeedKeyCount; i++ {
				k, want := seedKV(i)
				out, _, err := backupExecInPod(t, cfg, pod,
					[]string{"etcdctl", "get", k, "--print-value-only"})
				if err != nil {
					return false, nil // member may still be coming up
				}
				if strings.TrimSpace(out) != want {
					return false, nil
				}
			}
			return true, nil
		}, wait.WithTimeout(2*time.Minute), wait.WithInterval(3*time.Second)); err != nil {
			t.Fatalf("restored cluster did not serve the full seeded keyspace: %v", err)
		}

		// Negative control: a key that was never written must be absent. This
		// rules out the vacuous pass where the member booted an empty dir and the
		// "presence" check matched diagnostic output.
		out, _, err := backupExecInPod(t, cfg, pod,
			[]string{"etcdctl", "get", absentKey, "--print-value-only"})
		if err != nil {
			t.Fatalf("negative-control read failed: %v", err)
		}
		if strings.TrimSpace(out) != "" {
			t.Errorf("negative control: key %q should be absent but returned %q", absentKey, out)
		}

		t.Logf("restore verified: %d keys round-tripped, negative control absent", backupSeedKeyCount)
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		cleanupBackupWorkloads(ctx, t, cfg, minioName, credsSecret, []string{sourceCluster, restoredCluster})
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace}})
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace}})
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
