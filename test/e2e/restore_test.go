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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestEtcdRestoreFromBackupS3 / ...GCS exercise the FULL backup -> restore round
// trip end-to-end against an in-cluster object store (MinIO for S3, fake-gcs-
// server for GCS), so they require NO real cloud credentials.
//
// They are gated ONLY behind ETCD_E2E_BACKUP=true (which provisions the
// backends). The restore data-path is now a real init-container restore (the
// operator image's `restore-localize` subcommand downloads the snapshot and runs
// etcd's snapshot-restore library in-process into the genesis member's data dir
// before etcd starts), so the round-trip runs live in the default backup matrix.
//
// Flow:
//  1. Deploy the backend + bucket (+ creds for S3).
//  2. Create a source EtcdCluster, seed a distinctive keyspace, back it up; wait
//     for the EtcdBackup to reach Completed.
//  3. Create an EtcdRestore referencing that backup, targeting a NEW cluster.
//  4. Wait for the EtcdRestore to reach Completed, then assert the ENTIRE seeded
//     keyspace is readable from the restored member with exact values, and a key
//     that was never in the snapshot is absent (negative control).
func TestEtcdRestoreFromBackupS3(t *testing.T) {
	requireBackupE2E(t)
	runRestoreRoundTrip(t,
		newMinIOBackend("minio-restore-e2e", "etcd-restores", "restore-e2e-creds"),
		"etcd-restore-s3",
		"etcd-restore-src-e2e", "etcd-restore-dst-e2e", "etcd-restore-e2e-snap", "etcd-restore-e2e")
}

func TestEtcdRestoreFromBackupGCS(t *testing.T) {
	requireBackupE2E(t)
	runRestoreRoundTrip(t,
		newFakeGCSBackend("fake-gcs-restore-e2e", "etcd-restores"),
		"etcd-restore-gcs",
		"etcd-restore-gcs-src", "etcd-restore-gcs-dst", "etcd-restore-gcs-snap", "etcd-restore-gcs")
}

// runRestoreRoundTrip drives one provider through the full backup->restore->byte-
// identical-readback proof.
func runRestoreRoundTrip(
	t *testing.T, backend backupBackend, featureName, sourceCluster, restoredCluster, backupName, restoreName string,
) {
	const absentKey = "restore-e2e/never-in-snapshot"

	feature := features.New(featureName)

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.deploy(ctx, t, cfg)
		backend.bootstrapBucket(ctx, t, cfg)
		createBackupTestCluster(ctx, t, cfg, sourceCluster, 1)
		waitStatefulSetReady(t, cfg, sourceCluster, 1)
		seedKeyspace(t, cfg, sourceCluster+"-0", backupSeedKeyCount)

		backup := &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdBackupSpec{
				ClusterRef:  sourceCluster,
				Destination: backend.destination("e2e"),
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
		backend.cleanup(ctx, t, cfg)
		deleteClusters(ctx, cfg, sourceCluster, restoredCluster)
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace}})
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace}})
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
