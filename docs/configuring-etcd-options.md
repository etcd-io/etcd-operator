# Configuration Etcd Options

Additional configuration options can be set using the `.spec.etcdOptions` field. The options provided are passed as command line arguments to the etcd container.

Options configured via `etcdOptions` have a higher priority than the default configured arguments. For example if one of the default arguments is `--listen-peer-urls=http://0.0.0.0:2380` and you specify `--listen-peer-urls=http://0.0.0.0:3200` using `etcdOptions`, then the argument `--listen-peer-urls=http://0.0.0.0:3200` will be used.

Information about the different configuration options is available from the etcd documentation page here: https://etcd.io/docs/latest/op-guide/configuration/.
## Database size and compaction

`spec.quotaBackendBytes`, `spec.autoCompactionMode` and `spec.autoCompactionRetention` render the `--quota-backend-bytes`, `--auto-compaction-mode` and `--auto-compaction-retention` flags. When unset, no flag is rendered and etcd defaults apply. A conflicting flag in `etcdOptions` still wins, following the precedence above.

The operator only writes these flags when it creates or scales the StatefulSet, so changing them on a running cluster takes effect on the next StatefulSet rollout — not immediately. In particular, raising `quotaBackendBytes` alone does not clear an active NOSPACE alarm: compact, defragment and `etcdctl alarm disarm` first.
