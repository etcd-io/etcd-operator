# Configuration Etcd Options

Additional configuration options can be set using the `.spec.etcdOptions` field. The options provided are passed as command line arguments to the etcd container.

Options configured via `etcdOptions` have a higher priority than the default configured arguments. For example if one of the default arguments is `--listen-peer-urls=http://0.0.0.0:2380` and you specify `--listen-peer-urls=http://0.0.0.0:3200` using `etcdOptions`, then the argument `--listen-peer-urls=http://0.0.0.0:3200` will be used.

Information about the different configuration options is available from the etcd documentation page here: https://etcd.io/docs/latest/op-guide/configuration/.