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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// These adversarial restore tests exercise the operator's SAFETY guards — the
// code paths whose silent regression is catastrophic (clobbering a live
// cluster, hanging forever on a missing object, restoring a corrupt snapshot).
// Crucially, each guard fires BEFORE or INDEPENDENTLY of the (known-incomplete)
// restore data-path, so they run live and green under ETCD_E2E_BACKUP=true even
// though the full byte-identical round-trip remains gated behind
// ETCD_E2E_RESTORE. The one exception — corrupted-snapshot rejection — can only
// be told apart from the broken restorer once the restorer is fixed, so it is
// honestly gated behind ETCD_E2E_RESTORE and documented as such below.

// TestEtcdRestoreRejectsPopulatedTarget proves the empty-target guarantee
// (ensureEmptyTargetCluster/assertClusterEmpty): a restore that targets a
// cluster which already has a ready member must FAIL terminally and must NOT
// clobber that cluster's existing data. This is the guard against a restore
// silently overwriting a live production cluster, and it currently has no e2e
// coverage.
func TestEtcdRestoreRejectsPopulatedTarget(t *testing.T) {
	requireBackupE2E(t)

	const (
		sourceCluster = "etcd-neg-pop-src"
		targetCluster = "etcd-neg-pop-tgt"
		backupName    = "etcd-neg-pop-snap"
		restoreName   = "etcd-neg-pop-restore"
		markerKey     = "neg-pop/precious"
		markerVal     = "do-not-clobber"
	)
	backend := newMinIOBackend("minio-neg-pop", "etcd-backups", "neg-pop-creds")

	feature := features.New("etcd-restore-rejects-populated-target")

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.deploy(ctx, t, cfg)
		backend.bootstrapBucket(ctx, t, cfg)

		// A real, completed backup so resolveSource succeeds and the restore
		// reaches the empty-target guard rather than failing earlier.
		createBackupTestCluster(ctx, t, cfg, sourceCluster, 1)
		waitStatefulSetReady(t, cfg, sourceCluster, 1)
		seedRichKeyspace(t, cfg, sourceCluster+"-0")
		mkBackup(ctx, t, cfg, backupName, sourceCluster, backend.destination("e2e"))
		waitBackupCompleted(t, cfg, backupName)

		// A POPULATED target cluster: ready member + a precious marker key.
		createBackupTestCluster(ctx, t, cfg, targetCluster, 1)
		waitStatefulSetReady(t, cfg, targetCluster, 1)
		if _, stderr, err := backupExecInPod(t, cfg, targetCluster+"-0",
			[]string{"etcdctl", "put", markerKey, markerVal}); err != nil {
			t.Fatalf("seed target marker: %v (stderr: %s)", err, stderr)
		}
		return ctx
	})

	feature.Assess("restore into a populated cluster is rejected and the data is preserved", func(
		ctx context.Context, t *testing.T, cfg *envconf.Config,
	) context.Context {
		restore := &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdRestoreSpec{
				Source: ecv1alpha1.SnapshotSource{BackupRef: &ecv1alpha1.BackupReference{Name: backupName}},
				Target: ecv1alpha1.RestoreTarget{Name: targetCluster, Size: 1, Version: "v3.6.1"},
			},
		}
		if err := cfg.Client().Resources().Create(ctx, restore); err != nil {
			t.Fatalf("create EtcdRestore: %v", err)
		}

		got := waitRestoreFailed(t, cfg, restoreName)
		if msg := failedConditionMessage(got); !strings.Contains(strings.ToLower(msg), "non-empty") &&
			!strings.Contains(strings.ToLower(msg), "ready member") {
			t.Errorf("expected a non-empty-target rejection reason, got: %q", msg)
		}

		// The precious data must be intact — the rejected restore must not have
		// clobbered the live cluster.
		out, _, err := backupExecInPod(t, cfg, targetCluster+"-0",
			[]string{"etcdctl", "get", markerKey, "--print-value-only"})
		if err != nil {
			t.Fatalf("re-read marker after rejected restore: %v", err)
		}
		if strings.TrimSpace(out) != markerVal {
			t.Errorf("marker key clobbered: got %q want %q", strings.TrimSpace(out), markerVal)
		}
		t.Logf("populated-target restore correctly rejected; existing data preserved")
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.cleanup(ctx, t, cfg)
		deleteClusters(ctx, cfg, sourceCluster, targetCluster)
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace}})
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace}})
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// TestEtcdRestoreMissingObjectFailsCleanly proves a restore that references an
// object which does not exist in the bucket fails TERMINALLY and PROMPTLY
// (ErrNotFound surfaced as a clean failure), rather than retrying or hanging
// until a timeout. The test's own bounded wait is the assertion: if the restore
// hung, waitRestoreFailed would time out and fail. This fires at the Download
// step, before the restore data-path, so it is live-green today.
func TestEtcdRestoreMissingObjectFailsCleanly(t *testing.T) {
	requireBackupE2E(t)

	const (
		targetCluster = "etcd-neg-missing-tgt"
		restoreName   = "etcd-neg-missing-restore"
	)
	backend := newMinIOBackend("minio-neg-missing", "etcd-backups", "neg-missing-creds")

	feature := features.New("etcd-restore-missing-object-clean-fail")

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.deploy(ctx, t, cfg)
		backend.bootstrapBucket(ctx, t, cfg)
		return ctx
	})

	feature.Assess("restore from a non-existent object fails cleanly within a bounded deadline", func(
		ctx context.Context, t *testing.T, cfg *envconf.Config,
	) context.Context {
		dst := backend.destination("e2e")
		restore := &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdRestoreSpec{
				Source: ecv1alpha1.SnapshotSource{
					Location: &ecv1alpha1.SnapshotLocation{
						Destination: dst,
						Key:         "this/object/does-not-exist.db",
					},
				},
				Target: ecv1alpha1.RestoreTarget{Name: targetCluster, Size: 1, Version: "v3.6.1"},
			},
		}
		if err := cfg.Client().Resources().Create(ctx, restore); err != nil {
			t.Fatalf("create EtcdRestore: %v", err)
		}

		got := waitRestoreFailed(t, cfg, restoreName)
		if msg := failedConditionMessage(got); !strings.Contains(strings.ToLower(msg), "download") &&
			!strings.Contains(strings.ToLower(msg), "not found") {
			t.Errorf("expected a download/not-found failure reason, got: %q", msg)
		}
		t.Logf("missing-object restore failed cleanly: %q", failedConditionMessage(got))
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.cleanup(ctx, t, cfg)
		deleteClusters(ctx, cfg, targetCluster)
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace}})
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// TestEtcdRestoreRejectsCorruptSnapshot uploads a deliberately corrupt object
// (random bytes that are not a valid bbolt/etcd snapshot) and asserts the
// restore fails rather than bootstrapping a garbage cluster.
//
// It is gated behind ETCD_E2E_RESTORE because, against the CURRENT restorer,
// a restore fails for the WRONG reason (the distroless `sh -c` exec bug, not
// snapshot-integrity rejection) — so the test cannot honestly distinguish
// "rejected the corruption" from "restore is broken" until the restore
// data-path is fixed to actually run `etcdutl snapshot restore`, which fails
// non-zero on a corrupt snapshot. When that fix lands, drop the gate and this
// becomes a true integrity-rejection proof.
func TestEtcdRestoreRejectsCorruptSnapshot(t *testing.T) {
	requireBackupE2E(t)
	requireRestoreE2E(t)

	const (
		targetCluster = "etcd-neg-corrupt-tgt"
		restoreName   = "etcd-neg-corrupt-restore"
		corruptKey    = "e2e/corrupt/garbage.db"
	)
	backend := newMinIOBackend("minio-neg-corrupt", "etcd-backups", "neg-corrupt-creds")

	feature := features.New("etcd-restore-rejects-corrupt-snapshot")

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.deploy(ctx, t, cfg)
		backend.bootstrapBucket(ctx, t, cfg)
		// Upload 64 KiB of non-snapshot bytes under the corrupt key (above the
		// 16 KiB floor, so size alone cannot save a naive check).
		uploadCorruptMinIOObject(t, cfg, backend, corruptKey, 64*1024)
		return ctx
	})

	feature.Assess("restore from a corrupt object is rejected", func(
		ctx context.Context, t *testing.T, cfg *envconf.Config,
	) context.Context {
		restore := &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdRestoreSpec{
				Source: ecv1alpha1.SnapshotSource{
					Location: &ecv1alpha1.SnapshotLocation{
						Destination: backend.destination(""),
						Key:         corruptKey,
					},
				},
				Target: ecv1alpha1.RestoreTarget{Name: targetCluster, Size: 1, Version: "v3.6.1"},
			},
		}
		if err := cfg.Client().Resources().Create(ctx, restore); err != nil {
			t.Fatalf("create EtcdRestore: %v", err)
		}
		got := waitRestoreFailed(t, cfg, restoreName)
		t.Logf("corrupt-snapshot restore failed as required: %q", failedConditionMessage(got))
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.cleanup(ctx, t, cfg)
		deleteClusters(ctx, cfg, targetCluster)
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace}})
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// --- shared negative-path helpers ------------------------------------------

func requireRestoreE2E(t *testing.T) {
	t.Helper()
	if !restoreE2EEnabled() {
		t.Skip("set ETCD_E2E_RESTORE=true to run restore data-path tests that " +
			"cannot be validated against the current (known-incomplete) restorer")
	}
}

func mkBackup(ctx context.Context, t *testing.T, cfg *envconf.Config, name, clusterRef string, dst ecv1alpha1.BackupDestination) {
	t.Helper()
	backup := &ecv1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       ecv1alpha1.EtcdBackupSpec{ClusterRef: clusterRef, Destination: dst},
	}
	if err := cfg.Client().Resources().Create(ctx, backup); err != nil {
		t.Fatalf("create EtcdBackup %q: %v", name, err)
	}
}

// waitRestoreFailed blocks until the named EtcdRestore reaches Failed and
// returns it. A restore that reaches Completed instead, or never reaches a
// terminal phase before the bounded deadline (e.g. it hung), fails the test —
// so this helper doubles as the "fails promptly, does not hang" assertion.
func waitRestoreFailed(t *testing.T, cfg *envconf.Config, name string) ecv1alpha1.EtcdRestore {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var got ecv1alpha1.EtcdRestore
		if err := cfg.Client().Resources().Get(ctx, name, namespace, &got); err != nil {
			return false, err
		}
		switch got.Status.Phase {
		case ecv1alpha1.RestorePhaseFailed:
			return true, nil
		case ecv1alpha1.RestorePhaseCompleted:
			return false, fmt.Errorf("restore %q unexpectedly Completed; it should have been rejected", name)
		}
		return false, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(2*time.Second))
	if err != nil {
		t.Fatalf("waiting for restore %q to fail: %v", name, err)
	}
	var done ecv1alpha1.EtcdRestore
	if err := cfg.Client().Resources().Get(t.Context(), name, namespace, &done); err != nil {
		t.Fatalf("get failed restore %q: %v", name, err)
	}
	return done
}

// failedConditionMessage returns the message of the Succeeded=False condition,
// where the controller records why a restore failed.
func failedConditionMessage(restore ecv1alpha1.EtcdRestore) string {
	for _, c := range restore.Status.Conditions {
		if c.Type == ecv1alpha1.RestoreConditionSucceeded {
			return c.Message
		}
	}
	return ""
}

func deleteClusters(ctx context.Context, cfg *envconf.Config, names ...string) {
	res := cfg.Client().Resources()
	for _, n := range names {
		_ = res.Delete(ctx, &ecv1alpha1.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: namespace}})
	}
}

// restoreE2EEnabled reports whether the (gated) restore data-path tests should
// run. The restore round-trip is known-incomplete against distroless etcd
// images, so it is opt-in behind ETCD_E2E_RESTORE.
func restoreE2EEnabled() bool {
	return os.Getenv("ETCD_E2E_RESTORE") == "true"
}

// uploadCorruptMinIOObject writes `size` bytes of non-snapshot data to the
// backend under key, via a one-shot mc pod, so a restore can be pointed at a
// deliberately corrupt object.
func uploadCorruptMinIOObject(t *testing.T, cfg *envconf.Config, backend *minioBackend, key string, size int) {
	t.Helper()
	// Generate `size` bytes from /dev/zero (the mc image has a shell + dd), then
	// pipe into mc. The content is not a valid bbolt DB, so an integrity-aware
	// restore must reject it.
	script := fmt.Sprintf(
		"mc alias set m http://%s:9000 %s %s >/dev/null && "+
			"head -c %d /dev/zero | mc pipe m/%s/%s",
		backend.instance, backend.accessKey, backend.secretKey,
		size, backend.bucket, key)
	_ = runMCJob(t, cfg, "mc-corrupt-"+backend.instance, script)
	// Sanity: confirm the object is present at the expected size, independent of
	// the upload's own success report.
	if got := backend.statObjectSize(t, cfg, key); got != int64(size) {
		t.Fatalf("corrupt object size %d != expected %d", got, size)
	}
}
