# TLS Architecture & Cluster Access

**Status:** Draft — parts of this document describe a target design that is not
yet implemented. Every section is tagged:

| Tag | Meaning |
|-----|---------|
| ✅ | Implemented on `customized-controller` |
| ⚠️ | Implemented but incorrect or incomplete — see the note |
| ❌ | Proposed, not implemented |

**Scope note:** this document covers transport security (TLS) and cluster
access only. etcd's built-in authentication and per-identity authorization are
**out of scope**.

**Related:** [#447](https://github.com/etcd-io/etcd-operator/issues/447) (user
interface for cluster access),
[#420](https://github.com/etcd-io/etcd-operator/issues/420) (auto provider CA
model).

---

## 1. Scope

This document answers two questions:

1. **How does the operator model certificates** — who issues them, and what each
   certificate field means.
2. **How does a user reach an etcd cluster** — what endpoint to dial, what trust
   anchor to use, and how to obtain a client certificate.

The second question is the substance of #447. The two are inseparable: the
endpoint name determines the server certificate's SAN, so the naming decision
must be made before certificates are issued.

## 2. Consumers

| Consumer | Direction | Identity | Status |
|----------|-----------|----------|--------|
| etcd member ↔ etcd member | peer | `<cluster>-peer-tls` | ✅ |
| etcd-operator → etcd | client | `<cluster>-client-tls` | ❌ using `<cluster>-server-tls` as client cert |
| backup controller → etcd | client | `<cluster>-client-tls` | ❌ controller does not exist yet |
| **application → etcd** | client | `<cluster>-client-tls` | ❌ not published |

The backup controller is a component of etcd-operator, not a third-party client,
so it obtains its identity through the same internal code path and needs no
user-facing issuance interface.

All three client consumers share a single certificate — under §3.1 they would be
indistinguishable anyway. The only thing missing today is that its name is not
published — see §5.2 and §6.3.

## 3. Trust model

### 3.1 What a certificate authorizes

etcd members are started with `--client-cert-auth=true`
(`internal/controller/pods.go:260`), and etcd's built-in authentication is not
enabled. The consequence:

> **Any certificate signed by the cluster's trusted CA has full, unrestricted
> access to etcd.** All client certificates are equivalent in power. The Common
> Name carries no authorization meaning.

Two conclusions follow, and they shape the whole design:

1. **Issuing a distinct certificate per application provides no security
   benefit.** It cannot distinguish callers, cannot grant reduced privileges,
   and the impact of a leak is identical either way. A single per-cluster client
   certificate is therefore the right default, and no per-application issuance
   API is warranted. (This would change if etcd authentication is introduced.)

2. **The trust boundary is entirely "who can obtain a certificate from this
   CA."** There is no second line of defense behind it. The CA must therefore be
   scoped to exactly one cluster. §3.3 is a defect against this property, and it
   is the most serious issue in this document.

### 3.2 Per-cluster signing CA — `auto` provider ✅

The operator generates a self-signed CA and stores it in `<cluster>-ca-tls`
(`ca.crt` + `ca.key`). It is the sole issuer for the cluster, and etcd's
`--trusted-ca-file` / `--peer-trusted-ca-file` point at it and nothing else.

```
<cluster>-ca-tls  (self-signed root, operator-generated, ❌)
   ├── <cluster>-server-tls
   ├── <cluster>-peer-tls
   └── <cluster>-client-tls            (operator, backup controller, applications)
```

### 3.3 Per-cluster signing CA — `cert-manager` provider ⚠️

The operator references the **user's** `Issuer` (or `ClusterIssuer`) directly on
the cert-manager `Certificate` it creates
(`pkg/certificate/cert_manager/provider.go:313-346`); cert-manager writes the
resulting leaf Secret with `tls.crt` / `tls.key` / `ca.crt`, and etcd's
`--trusted-ca-file` mounts that leaf `ca.crt`. There is no per-cluster
intermediate CA between the user's Issuer and the leaves — the leaf `ca.crt`
IS the user's Issuer CA.

Combined with §3.1, this is a serious trust-boundary defect. If the user points
`issuerRef` at an organization-wide `ClusterIssuer` — a common setup — then
**every certificate that CA has ever signed, for any purpose anywhere in the
organization, is accepted as a fully privileged etcd client.** A certificate
minted for an unrelated web service would grant complete access to the cluster's
data.

The fix is for the operator to interpose a per-cluster intermediate CA:

```
user's Issuer / ClusterIssuer
   └── Certificate{isCA: true}  →  <cluster>-ca-tls        (intermediate, ❌)
          └── Issuer{ca: {secretName: <cluster>-ca-tls}}   (per-cluster issuance point, ❌)
                 ├── <cluster>-server-tls
                 ├── <cluster>-peer-tls
                 └── <cluster>-client-tls
```

`--trusted-ca-file` points at the intermediate only. Go's `x509` accepts any
certificate with `CA:true` in the roots pool as a chain terminator, so
verification stops at the intermediate and the parent CA's other leaves are
**not** trusted.

This also makes the two providers structurally symmetric: both end up with a
per-cluster CA that signs exactly this cluster's leaves and nothing else. The
`Issuer` is an internal issuance point for the operator; it is not part of the
user-facing interface.

> **Breaking change.** It alters the meaning of `<cluster>-ca-tls` for existing
> cert-manager clusters. Requires a migration note and maintainer sign-off.

## 4. Certificate field semantics

Most of the current defects come from applying the same field values to all
three certificate roles. The three fields behave very differently.

### 4.1 Common Name

CN plays **no role in TLS verification** for any certificate. Hostname
verification uses SANs exclusively: Go removed the CN-as-hostname fallback in
1.15 and deleted the `x509ignoreCN` escape hatch in 1.17, and etcd is pure Go.

Within the scope of this document, CN is therefore **descriptive only**. It
should still be set to something meaningful, for two reasons:

- It is what appears in etcd's TLS error logs and in `openssl x509` output;
  a distinguishable CN makes handshake failures diagnosable.
- If etcd authentication is introduced later, CN becomes the username.
  Choosing sensible values now avoids re-issuing every certificate then.

⚠️ Today all three roles default to CN `<cluster>.<ns>.svc.cluster.local`
(`internal/controller/utils.go:255-313`), which is both uninformative and
hostname-shaped. Suggested values:

| Role | CN |
|------|-----|
| server | `<cluster>-server` |
| peer | `<cluster>-peer` |
| client | `<cluster>-client` |

For completeness: etcd exposes `--peer-cert-allowed-cn` to allowlist peer CNs.
There is no client equivalent (`--client-cert-allowed-hostname` exists and
matches SANs, not CN). The operator does not set either flag.

### 4.2 Subject Alternative Names

| | SANs required? |
|---|---|
| Server certificate | **Yes** — clients verify the dialed hostname against them |
| Peer certificate | **Yes** — member FQDN and Pod IP |
| Client certificates | **No** — leave empty |

When etcd verifies a client certificate, Go's `crypto/tls` server path calls
`x509.Verify` with `KeyUsages: [ExtKeyUsageClientAuth]` and **no `DNSName`**.
SANs are never consulted. The only exception is the opt-in
`--client-cert-allowed-hostname`, which the operator does not set.

Putting cluster hostnames in a client certificate's SANs is actively harmful:
it makes that certificate usable as a server identity for the cluster if it ever
gains `ServerAuth`, which is an impersonation primitive. Roles stay disjoint.

⚠️ Client certificates currently carry the same DNS SANs as server certificates.

**Server SAN set** — must be complete before certificates are issued against
a published endpoint:

```
<cluster>.<ns>.svc.<domain>            # headless Service
*.<cluster>.<ns>.svc.<domain>          # per-member FQDNs (each pod)
localhost, 127.0.0.1                   # loopback / probes
```

`<domain>` comes from the operator's `--service-dns-domain` flag
([#444](https://github.com/etcd-io/etcd-operator/issues/444)), not a hardcoded
`cluster.local`.

The `*.<cluster>.<ns>.svc.<domain>` pattern is the user-visible surface: every URL in
`status.endpoints` resolves to one of those SANs, and no others.

### 4.3 Extended Key Usage

| Role | EKU | Status |
|------|-----|--------|
| Server | `ServerAuth` + `ClientAuth` | ⚠️ `ServerAuth` only today |
| Peer | `ServerAuth` + `ClientAuth` | ✅ |
| Client | `ClientAuth` | ✅ |

**Why the server certificate needs `ClientAuth`.** etcd's gRPC gateway dials its
own server, presenting the *server* certificate as a client certificate
(`server/embed/etcd.go:818-832`):

```go
tlscfg, tlsErr := e.cfg.ClientTLSInfo.ServerConfig()   // server cert
dtls := tlscfg.Clone()
dtls.InsecureSkipVerify = true                          // dial side only
opts = append(opts, grpc.WithTransportCredentials(...))
```

`InsecureSkipVerify` suppresses only the gateway's verification of the server;
the server still verifies the presented client certificate. With
`--enable-grpc-gateway` defaulting to true and `--client-cert-auth=true` set, a
`ServerAuth`-only server certificate makes this loopback handshake fail, and the
v3 HTTP/JSON API is unusable on TLS clusters. `etcdctl` speaks gRPC directly, so
end-to-end tests do not catch it.

Go still enforces leaf EKU during client-certificate verification
(`crypto/x509/verify.go`, `checkChainForKeyUsage`), so this is not
version-dependent.

The alternative — `--enable-grpc-gateway=false` — removes the loopback entirely
but disables the v3 HTTP/JSON API. Too costly for a general-purpose operator.

## 5. Artifacts

### 5.1 Secrets

| Name | Kind | Contents | Audience |
|------|------|----------|----------|
| `<cluster>-ca-tls` | Secret (Opaque) | `ca.crt` (+ `ca.key` for `auto`) | **operator only — never referenced by users** ❌ |
| `<cluster>-server-tls` | Secret (`kubernetes.io/tls`) | `tls.crt`, `tls.key`, `ca.crt` | etcd member Pods |
| `<cluster>-peer-tls` | Secret (`kubernetes.io/tls`) | `tls.crt`, `tls.key`, `ca.crt` | etcd member Pods |
| `<cluster>-client-tls` | Secret (`kubernetes.io/tls`) | `tls.crt`, `tls.key`, `ca.crt` | operator, backup controller, **and users** ❌ |

**Every leaf Secret carries the trust anchor.** `ca.crt` is written alongside
`tls.crt` / `tls.key` in each leaf (`pkg/certificate/auto/provider.go:336-341`),
and `verifySecretHasCA` (`internal/controller/utils.go:399-413`) rejects a leaf
Secret that lacks it, for both providers. A user who mounts
`<cluster>-client-tls` therefore has everything needed to connect: identity,
key, and the CA to verify the server against. This is the whole distribution
mechanism — no additional artifact is required.

**`<cluster>-ca-tls` is not a user-facing artifact.** Its contents differ by
provider: the `auto` provider stores the CA **private key** in it, cert-manager
does not. Documentation and `status` must never point users at it; the CA they
need is already in the leaf Secret they are mounting anyway.

> An earlier draft proposed publishing a separate `<cluster>-ca-bundle`
> ConfigMap. It was dropped: no consumer requires the trust anchor without also
> requiring a credential (`--client-cert-auth=true` admits no such client),
> ConfigMaps are namespace-scoped so it solves no cross-namespace problem, and
> duplicating `ca.crt` across two objects creates a second thing to keep in sync
> during CA rotation while raising the question of which copy is authoritative.
> If a trust-anchor-only consumer ever appears, this is the point to revisit.

### 5.2 `EtcdClusterStatus` must expose connection information ❌

An application needs three things to reach a cluster: where to dial, what
identity to present, and what to trust. All three exist today, but none of them
is discoverable — `EtcdClusterStatus` currently reports only replica counts,
member health, version, leader ID, and conditions
(`api/v1alpha1/etcdcluster_types.go:138-186`). Nothing tells a user the endpoint
or the Secret name.

The consequence is that anyone integrating with an operator-managed cluster must
reconstruct the Secret name and the per-member URL list by reading operator
source. Those values then become a de-facto frozen API — the operator can
never change them, without ever having agreed to support them. Publishing them
in `status` is what turns the naming convention into an *interface*, which is
what #447 asks for.

Proposed additions:

```yaml
status:
  endpoints:
    - https://mycluster-0.mycluster.default.svc.cluster.local:2379
    - https://mycluster-1.mycluster.default.svc.cluster.local:2379
    - https://mycluster-2.mycluster.default.svc.cluster.local:2379
  tls:
    clientCertSecret: mycluster-client-tls      # carries tls.crt, tls.key, ca.crt
```

| Field | Purpose |
|-------|---------|
| `endpoints` | The per-member client URLs reported by etcd via `--advertise-client-urls` (§6.1). One entry per member. The list is what clientv3 needs to construct one sub-conn per member, which is what makes failover happen at the gRPC balancer layer rather than the kernel conntrack layer. |
| `tls.clientCertSecret` | The Secret holding the client identity **and** the trust anchor (§5.1). One reference covers `--cert`, `--key`, and `--cacert`. |

Two fields are enough because a client needs exactly one Secret and one address
(§6.3). The cert-manager `Issuer` from §3.3 is deliberately **not** published:
with no supported user-facing issuance path, exposing it would advertise an
interface the operator does not intend to support.

These fields are populated by the same reconcile pass that ensures the
certificates and Services, and they are the contract that §6 documents against.
This is a status and documentation change — the underlying Secret and
certificate already exist.

## 6. Cluster access

### 6.1 Network endpoint

**Today** ✅ there is exactly one Service per cluster: a headless Service named
`<cluster>` with `publishNotReadyAddresses: true`
(`internal/controller/utils.go:148-190`). The operator already sets each
member's `--advertise-client-urls` to its per-pod FQDN on this Service
(`internal/controller/pods.go:251`):

```
--advertise-client-urls=https://$(POD_NAME).<cluster>.<ns>.svc.cluster.local:2379
```

So etcd is **already** publishing per-member client URLs that round-trip
through the headless Service's DNS. The server certificate already needs
`*.{cluster}.{ns>.svc.<domain>` in its SAN for these URLs to verify.

**The intended user endpoint is therefore a list of per-member client URLs,
not a single address.** The headless Service provides the DNS machinery; the
operator simply has to publish the URL list in `status.endpoints` once the
member list converges. No second Service is required.

This choice is the result of an earlier design passing through a `<cluster>-client`
ClusterIP Service. That variant was rejected on connection-resilience grounds;
§6.1.1 explains why. The current §6.1, the resulting `status.endpoints` shape,
and §6.4's `etcdctl` snippet are all consistent with the rejection.

#### 6.1.1 Why not a single ClusterIP endpoint

A single `https://<cluster>-client.<ns>.svc.<domain>:2379` address — translated
to a ClusterIP and load-balanced by kube-proxy / CNI at TCP level — is what
many K8s services look like, and would be the natural shape for a generic
operator. For etcd it has a concrete failure mode that is invisible to
clientv3.

`clientv3` does not use gRPC's DNS resolver. It installs a custom manual
resolver (`client/v3/internal/resolver/resolver.go`) that constructs one
`sub-conn` per entry in its `Endpoints` list and configures a `round_robin`
balancer. With one entry, the balancer degenerates to "always use the only
sub-conn," and the host is resolved to an A-record at TCP connect time, never
seen by gRPC itself.

When the picked backend dies:

- gRPC's sub-conn goes `TRANSIENT_FAILURE` and begins reconnecting.
- The new TCP SYN hits kube-proxy / CNI conntrack, which still has the dead
  pod mapped. The SYN goes to the dead pod and times out.
- gRPC's `DialTimeout` (2s by default in etcd) fires; another reconnect
  attempt, same outcome.
- After `nf_conntrack_tcp_timeout_established` (5 minutes by default) the
  conntrack entry finally expires, the next SYN reaches a live pod, and
  service resumes.

This is an outage of **up to ~5 minutes per affected client** for any single
member failure, bounded by kernel state, not by clientv3's retry machinery.
clientv3's retry interceptor handles "RPC failed with `Unavailable`" and
similar, not "TCP SYN silently routed to a dead pod."

With three per-member endpoints, gRPC has three sub-conns and the
`round_robin` balancer skips any in `TRANSIENT_FAILURE` immediately — the
isolation is at the balancer layer, not the kernel layer, and the recovery
time drops to milliseconds.

There is a partial escape: clientv3's `AutoSync` (`client/v3/client.go:186-211`)
periodically replaces the endpoint list with the per-member URLs etcd reports
via `MemberList`. After one `AutoSyncInterval` (default 1 minute for etcdctl,
disabled in the Go SDK by default — `client.go:208-209`), a client that
started with one ClusterIP endpoint ends up with three real ones. But the
**first `AutoSyncInterval` window is real**, and turning AutoSync on is not
something every Go SDK consumer knows to do.

The per-member URL list sidesteps this entirely: the client has the correct
shape from the first call, and AutoSync becomes an internal optimization
rather than a correctness requirement.

#### 6.1.2 `status.endpoints` is a snapshot, not a live view

The operator republishes `status.endpoints` only when **membership changes**
(join, leave, replacement). The per-pod FQDN is stable across Pod restarts, so
ordinary Pod churn does not invalidate the list; a restart simply means the
sub-conn built to that FQDN now resolves to a different IP. This is the
correct semantics — etcd membership is what application clients care about,
not individual pod identities.

Application clients are not notified of `status.endpoints` updates out of band.
There is no watcher, no informer, no push. The gRPC `round_robin` balancer
that clientv3 uses treats the endpoint list as a static configuration for the
lifetime of a `Client`; new members added after the client is constructed are
invisible until the client rebuilds its resolver state.

The practical consequences:

- **Scale-out load-spreading is delayed.** A new member is reachable to
  existing clients only through Raft forwarding from the old members, not
  directly. Load does not redistribute to the new member until clients
  restart, or until `AutoSync` runs (§6.1.1).
- **Scale-in is handled correctly.** A removed member's sub-conn goes
  `TRANSIENT_FAILURE`, the balancer skips it on the next pick, no manual
  action is needed.
- **A pod that crashes and is replaced at the same FQDN is invisible.** The
  sub-conn's underlying TCP connection breaks, gRPC reconnects, the new
  pod's IP is reached on retry. No `status.endpoints` update, no client-side
  action.

This means `status.endpoints` is the **bootstrap configuration** — it answers
"what should the client have on first call?" — not a live channel. Keeping
`AutoSyncInterval` short (30 s is a good default) is the only mechanism that
makes the running configuration track membership changes. The operator
publishes the truth at reconcile time; the protocol layer is responsible for
keeping client state close enough to that truth in between.

#### 6.1.3 The headless Service is internal

The headless Service `<cluster>` carries port 2380 and has
`publishNotReadyAddresses: true`. Both are deliberate and **not** part of the
user-facing contract:

- Port 2380 is for peer communication. The Service does not listen on it; the
  port is declared only so DNS-SRV records are complete. clientv3 does not
  use SRV, so the declaration is inert for users.
- `publishNotReadyAddresses: true` is required for peer bootstrap — etcd 3.6
  resolves peer addresses before members are Ready
  (`internal/controller/utils.go:163-174`). Flipping it breaks cluster
  formation, not client access.

Users do not dial the headless Service's name. They dial the per-member FQDNs
published in `status.endpoints`, which are pre-Ready by the time they exist
(`--advertise-client-urls` is set by etcd at startup). A user who copies the
Service name anyway gets exactly the failure mode §6.1.1 warns about — that
choice is theirs, not the operator's.

### 6.2 Trust anchor

Use the `ca.crt` key of `<cluster>-client-tls` — the same Secret that carries
the client identity (§5.1). Clients must never be told to read
`<cluster>-ca-tls`, which holds the CA private key on `auto` clusters.

### 6.3 Client certificate

The operator already issues one client certificate per cluster,
`<cluster>-client-tls` ✅. What is missing is publishing it: its name must appear
in `status.tls.clientCertSecret` and be documented ❌. Users then mount that
Secret directly. No new certificate and no new code path is required — this is a
status and documentation change.

It is the same Secret the operator itself uses. Since every client certificate
carries identical privileges (§3.1), a second, separately-named copy for users
would isolate nothing.

This is the entire user-facing issuance story. Per §3.1, a per-application
certificate would carry exactly the same privileges as this one, so the
additional machinery — a CRD, a request/approve flow, per-identity lifecycle —
would buy nothing while the cluster has no authorization model.

What users must be told plainly:

- The certificate grants **full access** to the cluster. It is not a
  reduced-privilege credential.
- It is **shared**. etcd cannot distinguish callers holding it.
- It **cannot be revoked** before expiry (§7). Containing a leak means rotating
  the CA and reissuing every certificate.
- Kubernetes RBAC on the Secret is the access control mechanism. Treat it with
  the same care as a database root password.

**Scope: same-namespace clients only.** A client must run in the same namespace
as the `EtcdCluster`, because Secrets are namespace-scoped and the credential is
distributed as a Secret. Reaching a cluster from another namespace is **not
supported** — the operator does not replicate the Secret, and users who need it
must arrange their own distribution, outside anything this design guarantees.

This bounds the problem deliberately. It also means no per-application issuance
mechanism is needed at any tier: a same-namespace client can mount
`<cluster>-client-tls` directly, and under §3.1 a certificate it minted for
itself would carry identical privileges anyway.

### 6.4 Worked example

```bash
# Everything a client needs is in status:
kubectl get etcdcluster mycluster \
  -o jsonpath='{range .status.endpoints[*]}{.}{" "}{end}'
# https://mycluster-0.mycluster.default.svc.cluster.local:2379 \
# https://mycluster-1.mycluster.default.svc.cluster.local:2379 \
# https://mycluster-2.mycluster.default.svc.cluster.local:2379

kubectl get etcdcluster mycluster -o jsonpath='{.status.tls.clientCertSecret}'
# mycluster-client-tls

# From a Pod mounting mycluster-client-tls at /etc/etcd-client:
ENDPOINTS=$(kubectl get etcdcluster mycluster \
  -o jsonpath='{range .status.endpoints[*]}{.}{","}{end}')
etcdctl \
  --endpoints=$ENDPOINTS \
  --cacert=/etc/etcd-client/ca.crt \
  --cert=/etc/etcd-client/tls.crt \
  --key=/etc/etcd-client/tls.key \
  put foo bar
```

This is also the acceptance test for #447 ❌: a Pod that is **not** an etcd
member, holding only the published client certificate Secret, reaching
the cluster through the published endpoint. It exercises both halves of the
issue in one test.

⚠️ The current e2e helper passes the **server** certificate as `--cert`
(`test/e2e/helpers_test.go`) from inside an etcd Pod. That proves only that a
member can reach itself, and — given §4.3 — should be rejected outright by the
EKU check. Worth confirming whether that assertion is actually running.

## 7. Lifecycle

| Event | `auto` | `cert-manager` |
|-------|--------|----------------|
| Leaf renewal | ❌ operator-driven reissue before expiry | ✅ cert-manager renews in place |
| CA rotation | ❌ | ❌ needs dual-anchor bundle + coordinated reissue |
| Revocation | ❌ not possible — see below | ❌ not possible — see below |

**Revocation is not available.** etcd is not configured with a CRL, and OCSP is
not part of the design, so an issued certificate is valid until it expires.
There is no mechanism to invalidate a single leaked credential.

The available mitigations are therefore preventive:

- Keep certificate validity short enough that expiry is a meaningful bound.
- Restrict Secret `get`/`list` through RBAC; enable encryption at rest.
- To contain a confirmed leak, rotate the CA and reissue every certificate —
  a full-cluster operation.

CA rotation requires a window during which both the outgoing and incoming
anchors are trusted, so the `ca.crt` key of each leaf Secret must be able to
hold more than one PEM block during that window.

## 8. Summary of required changes

| # | Change | Breaking | Motivation |
|---|--------|----------|------------|
| 1 | Server EKU += `ClientAuth` | No | gRPC gateway is broken on TLS clusters (§4.3) |
| 2 | Client certificates: no SANs | No | §4.2 |
| 3 | Per-role descriptive CNs | No | diagnosability; future-ready if etcd auth lands |
| 4 | Publish `<cluster>-client-tls` in `status.tls` + document it | No | §6.3 — status/docs only, no new artifact |
| 5 | Per-cluster intermediate CA for cert-manager | **Yes** | trust boundary (§3.3) — highest severity |
| 6 | Populate `status.endpoints` with the per-member client URLs once membership converges | No | §6.1 — uses URLs etcd already advertises; no new Service |
| 7 | Server SANs honor `--service-dns-domain` (the headless-Service wildcard is already set) | No | #444, #447 aspect 1 |
| 8 | Non-member Pod e2e using the published client certificate **and the published endpoint list** | No | acceptance of #447 |

Suggested sequencing: (1–3) as a correctness fix; (4, 6) as the publication
change; (5) separately, since it is breaking and needs sign-off; (7) as the
#444 / #447 aspect-1 change, which can proceed in parallel; (8) last.

No second Service, no rename, and no new trust-anchor artifact (§5.1) are
required.
