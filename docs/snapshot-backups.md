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
api/v1alpha1/etcdrestore_types.go       # EtcdRestore CRD
internal/controller/etcdbackup_*.go     # backup reconciler + snapshotter (no cloud SDK imports)
internal/controller/etcdrestore_*.go    # restore reconciler + restorer (no cloud SDK imports)
internal/controller/objectstore_creds.go # shared, audit-logged credential seam
pkg/objectstore/{interface,s3,gcs}.go   # cloud SDKs isolated behind a Store interface
```

Two seams keep the cloud SDKs and the exec machinery out of the core
reconciliation path and make the orchestration unit-testable without cloud
credentials or a live cluster:

1. **`Snapshotter`** produces the snapshot byte stream. The default
   implementation execs `etcdctl snapshot save` inside a member pod via the
   Kubernetes exec subresource and streams its stdout.
2. **`pkg/objectstore.Store`** is the minimal object-storage surface
   (`Upload` / `Download` / `List` / `Delete`). A `Factory` registry dispatches
   on the provider name, so a new backend is added by dropping a file in the package
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

# EtcdRestore: restore a snapshot into a new cluster

`EtcdRestore` implements the roadmap item *"Create a new cluster from a
backup"*: it downloads a snapshot from object storage and bootstraps a **new,
empty** `EtcdCluster` from it. A restore is always a genesis — it never overlays
a snapshot onto a cluster that already has members.

## Custom resource

The snapshot source is one of two mutually-exclusive forms:

- **`backupRef`** — the name of a completed `EtcdBackup` in the same namespace.
  The destination (bucket/prefix/provider/secretRef), the object key, and a
  best-effort etcd version (inherited from the still-existing source cluster)
  are all derived from that backup. This is the convenient path for restoring a
  backup the operator itself produced.
- **`location`** — an explicit object-storage destination plus the object
  `key`, independent of any `EtcdBackup` (e.g. the originating backup was
  deleted). On this path `target.version` is required.

```yaml
apiVersion: operator.etcd.io/v1alpha1
kind: EtcdRestore
metadata:
  name: restore-2026-06-18
spec:
  source:
    backupRef:
      name: nightly-2026-06-17       # a Completed EtcdBackup
  target:
    name: my-cluster-restored        # NEW EtcdCluster to create
    size: 1                           # restore bootstraps a single seed member
    # version: v3.6.1                 # optional; inherited from source cluster
  restoreTimeout: 10m                 # optional, bounds download+restore
status:
  phase: Completed                    # Pending | Downloading | Restoring | Completed | Failed
  snapshotLocation: s3://my-etcd-backups/etcd/backups/my-cluster/nightly-...db
  restoredCluster: my-cluster-restored
  conditions:
    - type: Succeeded
      status: "True"
      reason: SnapshotRestored
```

## How it works

The restore controller mirrors the backup controller's two-seam design so its
orchestration is unit-testable with no cloud creds and no live etcd:

1. **`pkg/objectstore.Store.Download`** streams the snapshot back out of the
   bucket. Unlike the upload path (which spools to a temp file because S3
   `PutObject` needs a known length), `Download` streams the body, so the
   snapshot is never staged on the operator's disk on the read path.
2. **`Restorer`** writes the snapshot into the target member and bootstraps the
   new cluster. The default implementation streams the snapshot bytes into the
   genesis pod (`<cluster>-0`) over the exec subresource's stdin and runs
   `etcdctl snapshot restore` into a fresh data directory, stamping the member
   identity (`--name` / `--initial-advertise-peer-urls` / `--initial-cluster`)
   with the exact name and peer URL the bootstrapping pod will advertise.
   `etcdctl snapshot restore` refuses to overwrite a populated data directory,
   which is a second line of defence behind the controller's empty-target check.

**Empty-target guarantee.** Before restoring, the controller either creates the
target `EtcdCluster` fresh (owning it, so deleting the `EtcdRestore` can GC the
restored cluster) or, if a cluster of that name already exists, verifies its
backing StatefulSet reports **zero** ready members. Any ready member aborts the
restore — a restore must never silently diverge from a live cluster.

# Security

The backup and restore paths share one credential seam
(`resolveStoreCredentials`) with the following guarantees, asserted directly by
unit tests:

- **Credentials come only from `secretRef`** (or the operator's ambient
  identity when `secretRef` is omitted). No other source is consulted.
- **Credential values are never logged.** The resolver emits a structured audit
  log line recording the namespace, provider, and the secret *name* — never any
  key value — on every resolution (including the ambient-identity case). Secret
  bytes never reach status, conditions, or Events either.
- **Errors never echo secret material.** A missing or malformed secret yields an
  error mentioning only the secret name and the provider's required key names.
- **Validation before I/O.** The destination's provider-specific block must be
  present and consistent, a referenced secret name must be non-empty, and an
  `EtcdRestore` must set exactly one snapshot source — all checked before any
  network call so misconfiguration is a clean, terminal failure.

## Least-privilege IAM

The credentials a backup/restore needs are narrow; scope them accordingly rather
than reusing a broad cluster role.

**AWS S3.** Restore needs only read; backup needs write and (for retention)
list/delete. A combined policy, scoped to the backup prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BackupRestoreObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-etcd-backups/etcd/backups/*"
    },
    {
      "Sid": "BackupRetentionList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::my-etcd-backups",
      "Condition": {"StringLike": {"s3:prefix": ["etcd/backups/*"]}}
    }
  ]
}
```

A restore-only identity can drop `s3:PutObject` and `s3:DeleteObject`. Prefer
IRSA (no stored secret) over a long-lived access key whenever the operator runs
on EKS.

**GCS.** Grant `roles/storage.objectAdmin` on the bucket (or
`roles/storage.objectViewer` for a restore-only identity). Prefer Workload
Identity over a downloaded service-account key.

# Out of scope (follow-ups)

- **Cron / scheduled backups** (`EtcdBackupSchedule`) are deferred; `EtcdBackup`
  and the provider interface are the foundation for them.
- **Encryption at rest** of the snapshot object (SSE-KMS / CMEK or
  operator-side envelope encryption) is deferred; today the snapshot is written
  with the bucket's default encryption.
- TLS-enabled clusters: the snapshotter and restorer currently issue plain
  `etcdctl` commands; cert-flag injection for mTLS clusters is a small
  follow-up that both paths share.
