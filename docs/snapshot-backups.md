# EtcdBackup: on-demand snapshots to object storage

This document describes the `EtcdBackup` custom resource, which implements the
roadmap item *"Create on-demand backup of a cluster"* by taking a point-in-time
etcd snapshot and uploading it to an object store (S3, GCS, or a pluggable
provider).

## Custom resource

An `EtcdBackup` is a one-shot job: it references an `EtcdCluster` in the same
namespace and a destination, the controller snapshots the cluster and uploads
the result, and the resource then serves as the immutable record of that
snapshot.

```yaml
apiVersion: operator.etcd.io/v1alpha1
kind: EtcdBackup
metadata:
  name: nightly-2026-06-17
spec:
  clusterRef: my-cluster          # EtcdCluster in the same namespace
  snapshotTimeout: 10m            # optional, bounds the snapshot-save step
  destination:
    provider: s3                  # s3 | gcs
    prefix: etcd/backups          # optional key prefix within the bucket
    secretRef:                    # optional; omit to use ambient credentials
      name: s3-backup-credentials
    s3:
      bucket: my-etcd-backups
      region: us-east-1
      # endpoint + forcePathStyle enable S3-compatible stores (MinIO, Ceph RGW)
  retention:
    retainCount: 7                # keep the 7 newest snapshots under bucket/prefix
status:
  phase: Completed                # Pending | Snapshotting | Uploading | Completed | Failed
  snapshotLocation: s3://my-etcd-backups/etcd/backups/my-cluster/nightly-2026-06-17-20260617T010203Z.db
  snapshotSizeBytes: 20480
  completionTime: "2026-06-17T01:02:05Z"
  conditions:
    - type: Succeeded
      status: "True"
      reason: SnapshotUploaded
```

## Architecture

The backup feature is implemented as a **separate controller plus an isolated
provider package**, wired into the existing operator binary:

```
api/v1alpha1/etcdbackup_types.go        # EtcdBackup CRD
internal/controller/etcdbackup_*.go     # reconciler + snapshotter (no cloud SDK imports)
pkg/objectstore/{interface,s3,gcs}.go   # cloud SDKs isolated behind a Store interface
```

Two seams keep the cloud SDKs and the exec machinery out of the core
reconciliation path and make the orchestration unit-testable without cloud
credentials or a live cluster:

1. **`Snapshotter`** produces the snapshot byte stream. The default
   implementation execs `etcdctl snapshot save` inside a member pod via the
   Kubernetes exec subresource and streams its stdout.
2. **`pkg/objectstore.Store`** is the minimal object-storage surface
   (`Upload` / `List` / `Delete`). A `Factory` registry dispatches on the
   provider name, so a new backend is added by dropping a file in the package
   and calling `objectstore.Register`. The AWS and GCP SDKs are imported **only**
   from this package.

The snapshot is streamed from the snapshotter into the uploader through an
`io.Pipe`, giving backpressure so memory stays bounded.

### Why in-binary (vs a standalone backup-manager)

The heavy cloud SDKs are isolated to `pkg/objectstore` behind an interface, and
the controller is a self-contained file set, so the code is trivially liftable
into a future `cmd/backup-manager` if the CVE surface of the cloud SDKs becomes
a concern for the core operator. Shipping it in the existing binary now keeps
this a single, independently-mergeable PR with one Deployment, one RBAC set, and
one image — the standard kubebuilder multi-controller layout — and avoids the
operational cost of a second binary before there is a reason for it.

## Credentials

`secretRef` is optional. When omitted, providers use their ambient credential
chain — IRSA on AWS, Workload Identity on GCP — which is the recommended
production posture. When present, the expected secret keys are:

- **s3**: `accessKeyID`, `secretAccessKey`, and optionally `sessionToken`
- **gcs**: `serviceAccountJSON` (a GCP service-account key)

## Out of scope (follow-ups)

- Restore (`Create a new cluster from a backup`) and scheduled/periodic backups
  (`EtcdBackupSchedule`) are intentionally deferred to keep this PR focused; the
  `EtcdBackup` resource and provider interface are the foundation for both.
- TLS-enabled clusters: the snapshotter currently issues a plain `etcdctl
  snapshot save`; cert-flag injection for mTLS clusters is a small follow-up.
