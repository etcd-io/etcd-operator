# Configuration Etcd Options

Additional configuration options can be set using the `.spec.etcdOptions` field. The options provided are passed as command line arguments to the etcd container.

Options configured via `etcdOptions` have a higher priority than the default configured arguments. For example if one of the default arguments is `--listen-peer-urls=http://0.0.0.0:2380` and you specify `--listen-peer-urls=http://0.0.0.0:3200` using `etcdOptions`, then the argument `--listen-peer-urls=http://0.0.0.0:3200` will be used.

Information about the different configuration options is available from the etcd documentation page here: https://etcd.io/docs/latest/op-guide/configuration/.
## Database size and compaction

`spec.quotaBackendBytes`, `spec.autoCompactionMode` and `spec.autoCompactionRetention` render the `--quota-backend-bytes`, `--auto-compaction-mode` and `--auto-compaction-retention` flags. When unset, no flag is rendered and etcd defaults apply. A conflicting flag in `etcdOptions` still wins, following the precedence above.

Changing these fields on a running, healthy cluster is not inert: the operator re-renders the StatefulSet template and immediately starts a rolling restart, replacing one pod at a time behind quorum and health gates (all members healthy, revisions caught up, every up-to-date pod Ready). Only apply such changes when a member-by-member restart is acceptable.

While a NOSPACE alarm is active those health gates hold the rollout, and raising `quotaBackendBytes` alone does not clear the alarm: compact, defragment and `etcdctl alarm disarm` first — the rollout then proceeds automatically. If space cannot be freed, delete pods one at a time (`kubectl delete pod`); the StatefulSet uses the OnDelete strategy, so each replacement starts from the already-rendered template carrying the new quota.

Rollback caveat: do not downgrade the operator below the version that introduced these fields while any cluster's DB exceeds etcd's 2GiB default quota. The older operator re-renders the template without `--quota-backend-bytes`, and the restarted pods raise a cluster-wide NOSPACE.
