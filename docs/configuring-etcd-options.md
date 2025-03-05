# Configuration Etcd Options

Additional configuration options can be set using the `.spec.etcdOptions` field. The options provided are passed as environment variables to the etcd pod.

Using a static value the `ETCD_NAME` configuration option will cause issues as it all members will end up being configured with the same name.

Information about the different configuration options is available from the etcd documentation page here: https://etcd.io/docs/v3.5/op-guide/configuration/. Please note that this link is for version 3.5 of etcd and you may need to adjust it for the version of etcd being run.