# TLS

`spec.tls` configures etcd's two independent TLS surfaces. Each surface is
optional; a nil surface is served (and, for the client surface, dialed by the
operator) in cleartext. Omitting `tls` entirely yields a fully-cleartext
cluster, byte-identical to a TLS-free deployment — there is no separate opt-out
knob.

```yaml
spec:
  tls:
    peer:                          # etcd <-> etcd
      provider: cert-manager.io    # auto | cert-manager.io (default: auto)
      certManager:
        issuerRef:                 # cert-manager Issuer or ClusterIssuer
          name: etcd-ca-issuer
          kind: ClusterIssuer      # defaults to Issuer (namespaced!)
        commonName: my-etcd
        duration: 8760h            # Go units only; "365d" is rejected
        renewBefore: 360h
      trustBundleConfigMapRef:     # optional extra trusted CAs (see below)
        name: extra-cas
      clientCertAuth: true         # default true (mTLS)
    client:                        # client -> etcd, and the operator's own dial identity
      provider: auto               # built-in self-signed provider
      auto:
        duration: 8760h            # auto minimum is 8760h (365 days)
```

## Providers

`provider` is the union discriminator: exactly the matching member block
(`auto:` or `certManager:`) may be set, enforced at apply time by CEL. Provider
identifiers are domain-style (like `StorageClass.provisioner`): the built-in
self-signed provider is `auto`, cert-manager is addressed by its API group
`cert-manager.io`.

- **auto** — the operator mints a self-signed certificate per surface. Zero
  external dependencies; certificates are not renewed automatically
  (`renewBefore` is accepted but reserved).
- **cert-manager.io** — the operator creates cert-manager `Certificate` objects
  signed by the referenced `issuerRef`. To issue from a custom CA, point
  `issuerRef` at a cert-manager [CA `Issuer`](https://cert-manager.io/docs/configuration/ca/)
  whose Secret holds your CA keypair — the operator deliberately has no
  bring-your-own-CA input of its own.

Both member blocks accept the same curated cert-manager-style fields:
`commonName`, `organizations`, `dnsNames`, `ipAddresses` (literal IP strings),
`duration`, `renewBefore`.

**Durations use Go units (h/m/s) only.** Day suffixes like `365d` are rejected
at apply time. The CRD floors are 8760h (365 days) for `auto` — etcd's own
self-cert minimum — and 1h for `certManager`.

**`issuerRef.kind` defaults to `Issuer`**, which is namespaced. If you
previously set `issuerKind: ClusterIssuer` and drop the kind while porting a
spec, the operator will look for a namespaced Issuer and report
`IssuerNotFound`.

## Choose a dedicated CA per cluster

With `clientCertAuth: true` (the default) any certificate chaining to the
surface's trusted CA has full access to etcd — etcd's authorization is the CA
boundary. Point the client surface's `issuerRef` at a CA dedicated to this etcd
cluster, not a broad shared intermediate: with a shared CA, every workload that
can obtain a certificate from it can read and write all of etcd. The `auto`
provider's per-surface self-signed CA is the safe zero-config default.

## Trust bundles (`trustBundleConfigMapRef`)

Each surface optionally references a ConfigMap (same namespace, fixed key
`ca.crt`, one or more PEM certificates). The bundle is **appended to — never
replaces —** the surface's member-side trusted CAs: the operator composes
`<issued CA> + <bundle>` into a per-surface ConfigMap
(`<cluster>-server-trusted-ca` / `<cluster>-peer-trusted-ca`) and points
`--trusted-ca-file` / `--peer-trusted-ca-file` at it. This exists for
CA-rotation overlap windows: trust the incoming CA before certificates from it
appear.

Semantics worth knowing:

- The bundle broadens which client/peer certificates **etcd members accept**.
  It does not change which servers the **operator** trusts when dialing etcd —
  the operator keeps pinning only the issuing CA from the client-surface
  secret. (Anything else would let ConfigMap write access mint certificates
  that impersonate etcd to the operator.)
- On the client surface, a trust bundle widens the mTLS admission boundary
  described above — every CA in the bundle can mint credentials etcd accepts.
- **etcd reads trusted-CA files at process start only.** The operator keeps the
  composed ConfigMap current (including after issued-CA rotation), but a member
  honors trust changes only after its next restart:
  `kubectl rollout restart statefulset/<cluster>`. The operator deliberately
  does not auto-roll the StatefulSet on trust changes — rolling a
  quorum-sensitive workload on trust bytes is a separate decision.
- A malformed bundle (any non-CERTIFICATE or unparseable PEM block, or zero
  certificates) fails the reconcile and the composed ConfigMap is **not**
  updated. etcd itself hard-errors on any bad block in its CA file, so shipping
  it would crash-loop members at their next restart; keeping the last good
  composition is the safer failure mode.

## TLS is create-time

TLS configuration flows into the pod template and cert mounts. Toggling it on a
running cluster rolls the StatefulSet into a mixed http/https membership whose
peers cannot connect, dropping quorum. The supported path is a new TLS cluster
plus data migration, not an in-place flip.

## etcd RBAC is out of scope

The operator secures etcd with mTLS only. etcd's own auth system (`auth
enable`, users/roles, cert-CN-as-username) is **not supported for v1alpha1**:
the operator dials with a client certificate and no user identity, so enabling
etcd RBAC on a managed cluster breaks reconciliation (membership and
maintenance RPCs require root once auth is on). Access control is the CA
boundary described above. A future `spec.auth` block would be purely additive.
