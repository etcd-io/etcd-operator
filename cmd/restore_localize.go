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

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"

	"go.etcd.io/etcd-operator/pkg/objectstore"
)

// The restore-localize subcommand runs as the SINGLE restore init-container the
// cluster controller injects into a restore-target member pod. It runs the
// operator image (the only image we control that already links the cloud SDKs
// and etcd's snapshot-restore library), so a restore needs no extra image and no
// shell. It performs, in order:
//
//  1. Ordinal guard: restore ONLY the genesis member (pod name ends in "-0").
//     Members 1..N exit 0 immediately and join via etcd's normal existing-
//     cluster path; restoring into every member would lay N divergent single-
//     member genesis dirs.
//  2. Idempotency guard: if the data dir already carries a marker file matching
//     this restore's generation, exit 0 without touching the data — so a pod
//     restart / reschedule never re-wipes-and-restores (which would silently
//     lose everything written since the restore).
//  3. Download the snapshot from object storage into a temp file.
//  4. In-process `snapshot.Restore` (the etcdutl snapshot-restore library) into
//     the etcd data dir, laying a single-member genesis whose member identity
//     matches exactly what the etcd container will advertise on boot. A corrupt
//     snapshot fails the library's hash check here, exiting non-zero — which is
//     how the corrupt-snapshot rejection surfaces for the RIGHT reason.
//  5. Write the marker so step (2) short-circuits future restarts.
//
// Env contract (stamped by applyRestoreInitContainers / the EtcdCluster
// annotation): see the RESTORE_* / cred env names below.
const (
	envRLProvider       = "RESTORE_PROVIDER"
	envRLBucket         = "RESTORE_BUCKET"
	envRLPrefix         = "RESTORE_PREFIX"
	envRLKey            = "RESTORE_KEY"
	envRLRegion         = "RESTORE_REGION"
	envRLEndpoint       = "RESTORE_ENDPOINT"
	envRLForcePathStyle = "RESTORE_FORCE_PATH_STYLE"
	envRLDataDir        = "RESTORE_DATA_DIR"
	envRLGeneration     = "RESTORE_GENERATION"
	envRLMemberName     = "RESTORE_MEMBER_NAME"
	envRLPeerURL        = "RESTORE_PEER_URL"
	envRLClusterToken   = "RESTORE_CLUSTER_TOKEN"
	envRLPodName        = "POD_NAME"

	// S3 credential env (optional => ambient identity).
	envRLS3AccessKeyID     = "RESTORE_S3_ACCESS_KEY_ID"
	envRLS3SecretAccessKey = "RESTORE_S3_SECRET_ACCESS_KEY"
	envRLS3SessionToken    = "RESTORE_S3_SESSION_TOKEN"
	// GCS credential env (optional => ambient/unauthenticated).
	envRLGCSServiceAccountJSON = "RESTORE_GCS_SERVICE_ACCOUNT_JSON"

	restoreDownloadTimeout = 10 * time.Minute
	// restoreMarkerFile lives in the data dir and records the restore generation
	// that produced it.
	restoreMarkerFile = ".etcd-operator-restore-complete"
)

// runRestoreLocalize is the `manager restore-localize` entrypoint. Any error
// exits non-zero so the init-container (and the member pod) fails loudly rather
// than the etcd container booting an empty / half-restored data dir.
func runRestoreLocalize() {
	if err := doRestoreLocalize(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "restore-localize: %v\n", err)
		os.Exit(1)
	}
}

func doRestoreLocalize(ctx context.Context) error {
	podName := os.Getenv(envRLPodName)
	dataDir := os.Getenv(envRLDataDir)
	generation := os.Getenv(envRLGeneration)
	memberName := os.Getenv(envRLMemberName)
	peerURL := os.Getenv(envRLPeerURL)
	if podName == "" || dataDir == "" || memberName == "" || peerURL == "" {
		return fmt.Errorf("missing required env (pod=%q dataDir=%q member=%q peerURL=%q)",
			podName, dataDir, memberName, peerURL)
	}

	// (1) Ordinal guard: only the genesis member (-0) is restored.
	if !strings.HasSuffix(podName, "-0") {
		fmt.Fprintf(os.Stdout, "restore-localize: %s is not the genesis member; skipping restore\n", podName)
		return nil
	}

	// (2) Idempotency guard: skip if a marker for this generation already exists.
	markerPath := filepath.Join(dataDir, restoreMarkerFile)
	if existing, err := os.ReadFile(markerPath); err == nil {
		if strings.TrimSpace(string(existing)) == generation {
			fmt.Fprintf(os.Stdout,
				"restore-localize: marker for generation %q already present; skipping restore\n", generation)
			return nil
		}
		// A marker for a DIFFERENT generation means this data dir was restored
		// from another snapshot and then (per the empty-target guard) should not
		// be re-restored under us. Refuse rather than clobber.
		return fmt.Errorf("data dir %q already restored for a different generation (have %q, want %q)",
			dataDir, strings.TrimSpace(string(existing)), generation)
	}

	// (3) Download the snapshot to a temp file.
	snapPath := filepath.Join(os.TempDir(), "restore-snapshot.db")
	if err := downloadSnapshot(ctx, snapPath); err != nil {
		return err
	}
	defer func() { _ = os.Remove(snapPath) }()

	// (4) In-process restore into the etcd data dir.
	if err := restoreSnapshotToDataDir(snapPath, dataDir, memberName, peerURL,
		os.Getenv(envRLClusterToken)); err != nil {
		return err
	}

	// (5) Mark success so a restart is a no-op.
	if err := os.WriteFile(markerPath, []byte(generation+"\n"), 0o600); err != nil {
		return fmt.Errorf("write restore marker %q: %w", markerPath, err)
	}
	fmt.Fprintf(os.Stdout, "restore-localize: restored genesis member %q into %s\n", memberName, dataDir)
	return nil
}

// downloadSnapshot streams the configured snapshot object into snapPath.
func downloadSnapshot(ctx context.Context, snapPath string) error {
	provider := os.Getenv(envRLProvider)
	bucket := os.Getenv(envRLBucket)
	key := os.Getenv(envRLKey)
	if provider == "" || bucket == "" || key == "" {
		return fmt.Errorf("missing snapshot source env (provider=%q bucket=%q key=%q)", provider, bucket, key)
	}

	forcePathStyle, _ := strconv.ParseBool(os.Getenv(envRLForcePathStyle))
	dst := objectstore.Destination{
		Provider:       objectstore.Provider(provider),
		Bucket:         bucket,
		Prefix:         os.Getenv(envRLPrefix),
		Region:         os.Getenv(envRLRegion),
		Endpoint:       os.Getenv(envRLEndpoint),
		ForcePathStyle: forcePathStyle,
	}

	var creds objectstore.Credentials
	switch objectstore.Provider(provider) {
	case objectstore.ProviderS3:
		creds.AccessKeyID = os.Getenv(envRLS3AccessKeyID)
		creds.SecretAccessKey = os.Getenv(envRLS3SecretAccessKey)
		creds.SessionToken = os.Getenv(envRLS3SessionToken)
	case objectstore.ProviderGCS:
		if v := os.Getenv(envRLGCSServiceAccountJSON); v != "" {
			creds.ServiceAccountJSON = []byte(v)
		}
	}

	dlCtx, cancel := context.WithTimeout(ctx, restoreDownloadTimeout)
	defer cancel()

	store, err := objectstore.New(dlCtx, dst, creds)
	if err != nil {
		return fmt.Errorf("build object store: %w", err)
	}
	body, err := store.Download(dlCtx, key)
	if err != nil {
		return fmt.Errorf("download snapshot %q from %s://%s: %w", key, provider, bucket, err)
	}
	defer func() { _ = body.Close() }()

	out, err := os.Create(snapPath)
	if err != nil {
		return fmt.Errorf("create %q: %w", snapPath, err)
	}
	n, copyErr := io.Copy(out, body)
	closeErr := out.Close()
	if copyErr != nil {
		return fmt.Errorf("stream snapshot to %q: %w", snapPath, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %q: %w", snapPath, closeErr)
	}
	if n == 0 {
		return fmt.Errorf("downloaded snapshot %q is empty", key)
	}
	return nil
}

// restoreSnapshotToDataDir runs the etcdutl snapshot-restore library in-process,
// laying a single-member genesis data directory into dataDir. The member name,
// peer URL and a single-member initial cluster must match exactly what the etcd
// container advertises on boot, or the bootstrapped member rejects itself. A
// corrupt/truncated snapshot fails the library's integrity check and returns an
// error here, which is the corrupt-snapshot rejection path.
func restoreSnapshotToDataDir(snapPath, dataDir, memberName, peerURL, clusterToken string) error {
	lg, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
	if err != nil {
		lg = zap.NewNop()
	}
	if clusterToken == "" {
		clusterToken = "etcd-cluster"
	}
	mgr := snapshot.NewV3(lg)
	return mgr.Restore(snapshot.RestoreConfig{
		SnapshotPath:        snapPath,
		Name:                memberName,
		OutputDataDir:       dataDir,
		PeerURLs:            []string{peerURL},
		InitialCluster:      fmt.Sprintf("%s=%s", memberName, peerURL),
		InitialClusterToken: clusterToken,
		SkipHashCheck:       false,
	})
}
