# ETCD Operator API References

## Packages
- [operator.etcd.io/v1alpha1](#operatoretcdiov1alpha1)


## operator.etcd.io/v1alpha1

Package v1alpha1 contains API Schema definitions for the operator v1alpha1 API group.

### Resource Types
- [EtcdCluster](#etcdcluster)
- [EtcdClusterList](#etcdclusterlist)



#### EtcdCluster



EtcdCluster is the Schema for the etcdclusters API.



_Appears in:_
- [EtcdClusterList](#etcdclusterlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdCluster` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[EtcdClusterSpec](#etcdclusterspec)_ |  |  |  |


#### EtcdClusterList



EtcdClusterList contains a list of EtcdCluster.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdClusterList` | | |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[EtcdCluster](#etcdcluster) array_ |  |  |  |


#### EtcdClusterSpec



EtcdClusterSpec defines the desired state of EtcdCluster.



_Appears in:_
- [EtcdCluster](#etcdcluster)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `size` _integer_ | Size is the expected size of the etcd cluster. |  | Minimum: 1 <br /> |
| `imageRegistry` _string_ | ImageRegistry specifies the container registry that hosts the etcd images.<br />If unset, it defaults to the value provided via the controller's<br />--image-registry flag, which itself defaults to "gcr.io/etcd-development/etcd". |  |  |
| `version` _string_ | Version is the expected version of the etcd container image. |  |  |
| `storageSpec` _[StorageSpec](#storagespec)_ | StorageSpec is the name of the StorageSpec to use for the etcd cluster. If not provided, then each POD just uses the temporary storage inside the container. |  |  |
| `tls` _[EtcdClusterTLS](#etcdclustertls)_ | TLS configures etcd's two independent TLS surfaces (peer and client/server).<br />Each surface is optional and configured fully independently; a nil surface<br />means that surface is served/dialed in cleartext. When TLS itself is nil, the<br />entire cluster (peer + client + operator client) is cleartext, byte-identical<br />to a TLS-free deployment.<br />TLS is effectively create-time: it flows into the pod template and cert mounts.<br />Toggling it on (or off) on a running cluster rolls the StatefulSet into a mixed<br />http/https membership whose peers cannot connect, dropping quorum. The supported<br />path is a NEW TLS cluster plus data migration, not an in-place flip. |  |  |
| `etcdOptions` _string array_ | etcd configuration options are passed as command line arguments to the etcd container, refer to etcd documentation for configuration options applicable for the version of etcd being used. |  |  |
| `podTemplate` _[PodTemplate](#podtemplate)_ | PodTemplate is the pod template to use for the etcd cluster. |  |  |




#### EtcdClusterTLS



EtcdClusterTLS configures etcd's two independent TLS surfaces. Each surface is
optional; a nil surface means that surface is served/dialed in cleartext (http).
The two surfaces are configured fully independently -- different providers,
issuers, and client-cert-auth policy are allowed and expected. Both surfaces nil
is legal and means fully-cleartext (today's default); it is intentional, not an
error, so there is no "at least one surface" validation.



_Appears in:_
- [EtcdClusterSpec](#etcdclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `peer` _[TLSSurface](#tlssurface)_ | Peer configures etcd<->etcd (peer) TLS. When nil, peer traffic is cleartext.<br />A configured peer surface REQUIRES a CA-capable issuer shared by all members<br />so members can mutually verify; a self-signed *leaf* issuer cannot form a<br />multi-member cluster. (CA-capability lives on the cert-manager Issuer object,<br />not on this spec, so that check is enforced at reconcile time, not via CEL.) |  | Optional: \{\} <br /> |
| `client` _[TLSSurface](#tlssurface)_ | Client configures client->etcd (server) TLS AND, transitively, the operator's<br />own etcd client identity (the operator authenticates to etcd as a client).<br />When nil, client traffic is cleartext and the operator dials cleartext. |  | Optional: \{\} <br /> |


#### MemberStatus



MemberStatus defines the observed state of a single etcd member.



_Appears in:_
- [EtcdClusterStatus](#etcdclusterstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name of the etcd member, typically the pod name (e.g., "etcd-cluster-example-0").<br />This can also be the name reported by etcd itself if set. |  | Optional: \{\} <br /> |
| `id` _string_ | ID is the hex-encoded member ID as reported by etcd.<br />This is the canonical identifier for an etcd member. |  |  |
| `version` _string_ | Version of etcd running on this member. |  | Optional: \{\} <br /> |
| `isHealthy` _boolean_ | IsHealthy indicates if the member is considered healthy.<br />A member is healthy if its etcd /health endpoint is reachable and reports OK,<br />and its Status endpoint does not report any 'Errors'. |  |  |
| `isLearner` _boolean_ | IsLearner indicates if the member is currently a learner in the etcd cluster. |  | Optional: \{\} <br /> |
| `isLeader` _boolean_ | IsLeader indicates if this member is currently the cluster leader. |  | Optional: \{\} <br /> |


#### PodMetadata







_Appears in:_
- [PodTemplate](#podtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `annotations` _object (keys:string, values:string)_ |  |  |  |
| `labels` _object (keys:string, values:string)_ |  |  |  |


#### PodSpec







_Appears in:_
- [PodTemplate](#podtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `affinity` _[Affinity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#affinity-v1-core)_ |  |  |  |
| `nodeSelector` _object (keys:string, values:string)_ |  |  |  |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#toleration-v1-core) array_ |  |  |  |


#### PodTemplate







_Appears in:_
- [EtcdClusterSpec](#etcdclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `metadata` _[PodMetadata](#podmetadata)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[PodSpec](#podspec)_ |  |  |  |


#### StorageSpec







_Appears in:_
- [EtcdClusterSpec](#etcdclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `accessModes` _[PersistentVolumeAccessMode](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#persistentvolumeaccessmode-v1-core)_ |  |  |  |
| `storageClassName` _string_ |  |  |  |
| `pvcName` _string_ |  |  |  |
| `volumeSizeRequest` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#quantity-resource-api)_ |  |  |  |
| `volumeSizeLimit` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#quantity-resource-api)_ |  |  |  |


#### TLSAutoProvider



TLSAutoProvider tunes the built-in self-signed provider for one surface.
All fields are optional; empty values derive defaults from the cluster.



_Appears in:_
- [TLSSurface](#tlssurface)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `commonName` _string_ | CommonName is the X509 subject CN. Keep to 64 characters or fewer to<br />avoid generating invalid CSRs. |  | MaxLength: 64 <br />Optional: \{\} <br /> |
| `organizations` _string array_ | Organizations are the X509 subject O values. |  | Optional: \{\} <br /> |
| `dnsNames` _string array_ | DNSNames are the DNS subject alternative names. Empty defaults to<br />*.<cluster>.<ns>.svc.cluster.local and <cluster>.<ns>.svc.cluster.local<br />(required for the operator's hostname verification of members). |  | Optional: \{\} <br /> |
| `ipAddresses` _string array_ | IPAddresses are IP subject alternative names, as literal IP strings. |  | Optional: \{\} <br /> |
| `duration` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#duration-v1-meta)_ | Duration is the requested certificate lifetime. The auto provider<br />requires at least 8760h (365 days); empty defaults to 8760h. Go duration<br />units only (h/m/s); day suffixes like "365d" are not accepted. |  | Optional: \{\} <br /> |
| `renewBefore` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#duration-v1-meta)_ | RenewBefore is reserved: the auto provider does not yet renew<br />certificates. Accepted for parity with the certManager block. |  | Optional: \{\} <br /> |


#### TLSCertManagerProvider



TLSCertManagerProvider configures cert-manager issuance for one surface.



_Appears in:_
- [TLSSurface](#tlssurface)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `issuerRef` _[IssuerReference](#issuerreference)_ | IssuerRef references the cert-manager Issuer or ClusterIssuer that signs<br />this surface's certificates. kind defaults to "Issuer"; group defaults to<br />"cert-manager.io". Point it at a CA Issuer to issue from a custom CA. |  |  |
| `commonName` _string_ | CommonName is the X509 subject CN. Keep to 64 characters or fewer to<br />avoid generating invalid CSRs. |  | MaxLength: 64 <br />Optional: \{\} <br /> |
| `organizations` _string array_ | Organizations are the X509 subject O values. |  | Optional: \{\} <br /> |
| `dnsNames` _string array_ | DNSNames are the DNS subject alternative names. Empty defaults to<br />*.<cluster>.<ns>.svc.cluster.local and <cluster>.<ns>.svc.cluster.local<br />(required for the operator's hostname verification of members). |  | Optional: \{\} <br /> |
| `ipAddresses` _string array_ | IPAddresses are IP subject alternative names, as literal IP strings. |  | Optional: \{\} <br /> |
| `duration` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#duration-v1-meta)_ | Duration is the requested certificate lifetime, passed through to<br />Certificate.spec.duration. Empty defaults to 2160h (90 days). Go duration<br />units only (h/m/s); day suffixes like "90d" are not accepted, and<br />cert-manager's minimum is 1h. |  | Optional: \{\} <br /> |
| `renewBefore` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#duration-v1-meta)_ | RenewBefore is passed through to Certificate.spec.renewBefore; empty<br />leaves cert-manager's default renewal policy in place. |  | Optional: \{\} <br /> |


#### TLSProvider

_Underlying type:_ _string_

TLSProvider names the certificate provider for one TLS surface. It is the
discriminator of the flattened union on TLSSurface. Values are domain-style
identifiers (cf. StorageClass.provisioner): the operator's built-in
self-signed provider is "auto"; cert-manager is addressed by its API group.

_Validation:_
- Enum: [auto cert-manager.io]

_Appears in:_
- [TLSSurface](#tlssurface)

| Field | Description |
| --- | --- |
| `auto` | TLSProviderAuto selects the operator's built-in self-signed provider.<br /> |
| `cert-manager.io` | TLSProviderCertManager selects cert-manager issuance.<br /> |


#### TLSSurface



TLSSurface is the full, independent TLS configuration for ONE surface (peer or
client): a provider-discriminated union (provider selects which member block
below is honored), the surface's mutual client-cert-auth policy, and an
optional additional trust bundle.

The XValidation rules below are the apply-time anti-misconfiguration
guardrails: they reject incoherent provider/member-block combinations and
mTLS-without-a-resolvable-CA at the API server, so a user "cannot
misconfigure" these from the spec alone. Rules that require reading cluster
objects (issuer existence, peer CA-capability, client/server CA match) cannot
be expressed in CEL and are enforced at reconcile time instead -- see
validateTLSSurface and the cert-manager provider's validateCertificateConfig.



_Appears in:_
- [EtcdClusterTLS](#etcdclustertls)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `provider` _[TLSProvider](#tlsprovider)_ | Provider selects the certificate provider for THIS surface and names<br />which member block below is honored. Defaults to "auto". | auto | Enum: [auto cert-manager.io] <br />Optional: \{\} <br /> |
| `auto` _[TLSAutoProvider](#tlsautoprovider)_ | Auto configures the operator's built-in self-signed provider for THIS<br />surface. Only valid when provider is "auto". |  | Optional: \{\} <br /> |
| `certManager` _[TLSCertManagerProvider](#tlscertmanagerprovider)_ | CertManager configures cert-manager issuance for THIS surface.<br />Required when provider is "cert-manager.io"; forbidden otherwise. |  | Optional: \{\} <br /> |
| `clientCertAuth` _boolean_ | ClientCertAuth toggles mutual cert auth for THIS surface (etcd's<br />--client-cert-auth for the client surface, --peer-client-cert-auth for the<br />peer surface). Defaults to true (mTLS). Set false to serve server-only TLS<br />where clients authenticate by other means (password/token). When true with<br />the cert-manager provider a trusted CA (issuerRef.name) is REQUIRED,<br />enforced by the XValidation rule above. | true | Optional: \{\} <br /> |


