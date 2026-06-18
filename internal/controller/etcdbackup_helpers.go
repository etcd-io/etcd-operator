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

package controller

import (
	"fmt"
	"strings"
	"time"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/objectstore"
)

// snapshotKeyPrefix is the per-cluster prefix under which a backup's snapshots
// are written. Keeping all snapshots for a cluster under a shared prefix lets
// retention list and prune them as a group.
func snapshotKeyPrefix(backup *ecv1alpha1.EtcdBackup) string {
	return backup.Spec.ClusterRef
}

// snapshotObjectKey is the object key (relative to the destination prefix) for
// this backup's snapshot. It embeds the cluster, the backup name, and the
// creation time so keys are unique and sortable.
func snapshotObjectKey(backup *ecv1alpha1.EtcdBackup) string {
	ts := backup.CreationTimestamp.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	return fmt.Sprintf("%s/%s-%s.db",
		backup.Spec.ClusterRef,
		backup.Name,
		ts.UTC().Format("20060102T150405Z"),
	)
}

// relativeKey strips the destination prefix from an absolute (listed) object
// key so the result can be re-joined with the same prefix by Store.Delete
// without double-prefixing. Both sides are normalized for slashes.
func relativeKey(prefix, absKey string) string {
	prefix = strings.Trim(prefix, "/")
	absKey = strings.TrimLeft(absKey, "/")
	if prefix == "" {
		return absKey
	}
	if strings.HasPrefix(absKey, prefix+"/") {
		return absKey[len(prefix)+1:]
	}
	return absKey
}

// toObjectStoreDestination converts the API destination into the
// provider-agnostic objectstore.Destination, validating that the
// provider-specific block matches the selected provider.
func toObjectStoreDestination(dst ecv1alpha1.BackupDestination) (objectstore.Destination, error) {
	out := objectstore.Destination{
		Provider: objectstore.Provider(dst.Provider),
		Prefix:   dst.Prefix,
	}
	switch dst.Provider {
	case ecv1alpha1.BackupProviderS3:
		if dst.S3 == nil {
			return out, fmt.Errorf("destination.s3 is required when provider is %q", dst.Provider)
		}
		out.Bucket = dst.S3.Bucket
		out.Region = dst.S3.Region
		out.Endpoint = dst.S3.Endpoint
		out.ForcePathStyle = dst.S3.ForcePathStyle
	case ecv1alpha1.BackupProviderGCS:
		if dst.GCS == nil {
			return out, fmt.Errorf("destination.gcs is required when provider is %q", dst.Provider)
		}
		out.Bucket = dst.GCS.Bucket
	default:
		return out, fmt.Errorf("unsupported backup provider %q", dst.Provider)
	}
	return out, nil
}
