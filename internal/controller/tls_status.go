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
	"context"
	"fmt"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// tlsReadyConditionType is the status condition Type that reflects the health of
// the cluster's configured TLS surfaces (peer and/or client). A single condition
// reflects BOTH surfaces; its Reason/Message carry which surface failed.
const tlsReadyConditionType = "TLSReady"

// TLSReady reason vocabulary. These strings are shared verbatim between the
// status condition Reason and the CR Event Reason, so a `kubectl describe` shows
// the same vocabulary in both the conditions table and the events list.
//
// State -> reason mapping (see evaluateTLSReadiness):
//
//	both surfaces nil ............................... TLSNotConfigured (omit condition)
//	all configured surfaces healthy ................ TLSReady           (Status=True)
//	cert-manager issuer missing/wrong-kind ......... IssuerNotFound     (Status=False)
//	peer issuer is a self-signed leaf (no shared CA) PeerCANotShared    (Status=False)
//	operator client cert/secret not usable ......... ClientCertificateError (Status=False)
//	operator-client CA != server CA ................ ClientServerCAMismatch (Status=False)
//	a configured surface's cert/secret not yet ready SurfaceNotReady    (Status=False)
const (
	// reasonTLSNotConfigured indicates no TLS surface is configured (cleartext).
	// When this is the reason the TLSReady condition is omitted entirely (no TLS,
	// no TLS condition) rather than reported as a failure.
	reasonTLSNotConfigured = "TLSNotConfigured"
	// reasonTLSReady indicates every configured surface is provisioned, the
	// issuer(s) are CA-capable, and (for the client surface) the operator's own
	// client *tls.Config built and verified.
	reasonTLSReady = "TLSReady"
	// reasonIssuerNotFound indicates a cert-manager surface references an Issuer
	// or ClusterIssuer that does not exist (or is the wrong kind).
	reasonIssuerNotFound = "IssuerNotFound"
	// reasonPeerCANotShared indicates the peer surface uses a non-CA / self-signed
	// *leaf* issuer that cannot establish shared peer trust by chain verification.
	// This is a conservative WARNING, not a hard cap: the operator mounts ONE shared
	// peer secret (getPeerCertName) into every pod, so even a self-signed leaf is in
	// every member's --peer-trusted-ca-file and a multi-member quorum still forms.
	// We still flag it because that arrangement breaks the moment peer certs are
	// rotated per-member or the secret stops being shared.
	reasonPeerCANotShared = "PeerCANotShared"
	// reasonClientServerCAMismatch indicates the operator's client CA does not
	// match the CA that signs the etcd server cert.
	reasonClientServerCAMismatch = "ClientServerCAMismatch"
	// reasonClientCertificateError indicates the operator's own client cert/secret
	// could not be provisioned or built into a usable *tls.Config.
	reasonClientCertificateError = "ClientCertificateError"
	// reasonSurfaceNotReady indicates a configured surface's cert/secret has not
	// yet materialized (a transient bring-up state, resolved by requeue).
	reasonSurfaceNotReady = "SurfaceNotReady"
)

// tlsReadiness is the Recorder-free evaluation of a cluster's TLS surfaces. It is
// the single source of truth that both updateConditions (the TLSReady condition)
// and the controller boundary (CR Events) consume, so the condition Reason and the
// Event Reason can never drift. A nil *tlsReadiness (configured == false) means no
// TLS surface is configured and the condition is omitted.
type tlsReadiness struct {
	// configured is true iff at least one surface (peer or client) is set.
	configured bool
	// ready is true iff every configured surface is healthy.
	ready bool
	// reason is one of the reason* constants above.
	reason string
	// message is a human-readable detail for the condition/event.
	message string
}

// condition renders the tlsReadiness as a metav1.Condition for SetStatusCondition.
// It must only be called when configured == true (the caller omits the condition
// otherwise).
func (t tlsReadiness) condition(generation int64) metav1.Condition {
	status := metav1.ConditionFalse
	if t.ready {
		status = metav1.ConditionTrue
	}
	return metav1.Condition{
		Type:               tlsReadyConditionType,
		Status:             status,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
		Reason:             t.reason,
		Message:            t.message,
	}
}

// evaluateTLSReadiness inspects a cluster's configured TLS surfaces against live
// cluster objects (cert-manager Issuers and the operator's client secret) and
// returns a typed verdict. It is deliberately Recorder-free and read-only: it
// performs Gets but emits no Events and mutates nothing, so it is safe to call
// from both the reconcile boundary (which turns the verdict into Events) and from
// unit tests (which assert the verdict directly).
//
// The runtime checks implemented here are the ones the independence reshape
// deferred because they are NOT expressible in CEL (they read issuer/secret
// objects, not the EtcdCluster spec):
//
//   - PeerCANotShared: a configured cert-manager peer surface whose issuer is a
//     self-signed leaf (Issuer.Spec.SelfSigned != nil) cannot establish shared
//     peer trust. We detect it from the Issuer object's kind/spec.
//   - ClientServerCAMismatch: see note below — with the independent-surface shape
//     the operator-client and server certs both flow through the SINGLE client
//     surface's issuer, so they share a CA by construction. A genuine cross-issuer
//     mismatch is therefore not reachable from a single client surface; the
//     residual runtime compare is left as a documented TODO.
func evaluateTLSReadiness(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) tlsReadiness {
	if !peerTLSEnabled(ec) && !clientTLSEnabled(ec) {
		return tlsReadiness{configured: false, reason: reasonTLSNotConfigured, message: "no TLS surface configured"}
	}

	// Peer surface: require a CA-capable shared issuer. A self-signed leaf issuer
	// mints CA:FALSE certs that are each their own root, so members cannot mutually
	// verify (the silent "stuck at 1 member" hazard).
	if peerTLSEnabled(ec) {
		if r, ok := checkPeerCAShared(ctx, ec, c); !ok {
			return r
		}
	}

	// Client surface: the operator must be able to build a usable client *tls.Config
	// from its own client secret (keypair + pinned server CA). A missing/unusable
	// secret during bring-up surfaces as SurfaceNotReady; a malformed cert surfaces
	// as ClientCertificateError.
	if clientTLSEnabled(ec) {
		// First confirm the cert-manager issuer (if any) for the client surface
		// resolves, so a missing issuer reports IssuerNotFound rather than the
		// downstream "secret not ready".
		if r, ok := checkSurfaceIssuer(ctx, ec, c, "client", ec.Spec.TLS.Client); !ok {
			return r
		}
		if r, ok := checkClientSecretUsable(ctx, ec, c); !ok {
			return r
		}
	} else if peerTLSEnabled(ec) {
		// Peer-only cluster: also confirm the peer issuer resolves (the CA-shared
		// check above already read it, but checkSurfaceIssuer reports a missing
		// issuer distinctly from a non-CA one).
		if r, ok := checkSurfaceIssuer(ctx, ec, c, "peer", ec.Spec.TLS.Peer); !ok {
			return r
		}
	}

	return tlsReadiness{
		configured: true,
		ready:      true,
		reason:     reasonTLSReady,
		message:    tlsReadyMessage(ec),
	}
}

// tlsReadyMessage describes which surfaces are healthy.
func tlsReadyMessage(ec *ecv1alpha1.EtcdCluster) string {
	switch {
	case peerTLSEnabled(ec) && clientTLSEnabled(ec):
		return "peer and client TLS surfaces provisioned and verified"
	case peerTLSEnabled(ec):
		return "peer TLS surface provisioned and verified"
	default:
		return "client TLS surface provisioned and verified; operator client identity verified"
	}
}

// checkSurfaceIssuer confirms that a cert-manager surface's issuer exists and is a
// supported kind. Non-cert-manager (auto) surfaces always pass (the operator mints
// a CA itself). Returns (verdict, ok=false) on failure.
func checkSurfaceIssuer(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client,
	surfaceName string, surface *ecv1alpha1.TLSSurface) (tlsReadiness, bool) {
	cm := surface.ProviderCfg.CertManagerCfg
	if surface.Provider != "cert-manager" || cm == nil {
		return tlsReadiness{}, true
	}
	if err := issuerResolves(ctx, c, cm.IssuerName, cm.IssuerKind, ec.Namespace); err != nil {
		return tlsReadiness{
			configured: true,
			ready:      false,
			reason:     reasonIssuerNotFound,
			message:    fmt.Sprintf("%s surface: %v", surfaceName, err),
		}, false
	}
	return tlsReadiness{}, true
}

// checkPeerCAShared inspects the peer surface's cert-manager issuer and fails with
// PeerCANotShared when it is a self-signed leaf issuer (no shared CA). Auto-provider
// peer surfaces pass: the operator's auto provider mints a shared CA itself.
func checkPeerCAShared(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) (tlsReadiness, bool) {
	peer := ec.Spec.TLS.Peer
	cm := peer.ProviderCfg.CertManagerCfg
	if peer.Provider != "cert-manager" || cm == nil {
		return tlsReadiness{}, true
	}

	caCapable, err := issuerIsCACapable(ctx, c, cm.IssuerName, cm.IssuerKind, ec.Namespace)
	if err != nil {
		// Issuer missing/unreadable: report it as IssuerNotFound (checkSurfaceIssuer
		// would say the same); fold it here so a single read drives both.
		return tlsReadiness{
			configured: true,
			ready:      false,
			reason:     reasonIssuerNotFound,
			message:    fmt.Sprintf("peer surface: %v", err),
		}, false
	}
	if !caCapable {
		return tlsReadiness{
			configured: true,
			ready:      false,
			reason:     reasonPeerCANotShared,
			message: fmt.Sprintf("peer TLS issuer %q (%s) is a self-signed leaf; members cannot mutually "+
				"verify. Use a CA-capable issuer (SelfSigned -> CA Certificate isCA:true -> CA Issuer)",
				cm.IssuerName, cm.IssuerKind),
		}, false
	}
	return tlsReadiness{}, true
}

// checkClientSecretUsable confirms the operator's client secret yields a usable
// *tls.Config. A NotFound/transient bring-up state reports SurfaceNotReady (resolved
// by requeue); a malformed secret reports ClientCertificateError.
func checkClientSecretUsable(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) (tlsReadiness, bool) {
	_, err := buildClientTLSConfig(ctx, ec, c)
	if err == nil {
		return tlsReadiness{}, true
	}
	// A not-yet-materialized secret is a transient bring-up state, not a misconfig.
	secretName := getClientCertName(ec.Name)
	notReady := &corev1.Secret{}
	getErr := c.Get(ctx, client.ObjectKey{Name: secretName, Namespace: ec.Namespace}, notReady)
	if k8serrors.IsNotFound(getErr) {
		return tlsReadiness{
			configured: true,
			ready:      false,
			reason:     reasonSurfaceNotReady,
			message:    fmt.Sprintf("client surface: operator client secret %q not yet materialized", secretName),
		}, false
	}
	return tlsReadiness{
		configured: true,
		ready:      false,
		reason:     reasonClientCertificateError,
		message:    fmt.Sprintf("client surface: operator client TLS not usable: %v", err),
	}, false
}

// issuerResolves returns nil iff the named cert-manager (cluster)issuer exists and
// the kind is supported.
func issuerResolves(ctx context.Context, c client.Client, name, kind, namespace string) error {
	switch kind {
	case "Issuer":
		issuer := &certmanagerv1.Issuer{}
		if err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, issuer); err != nil {
			if k8serrors.IsNotFound(err) {
				return fmt.Errorf("issuer %q not found in namespace %s", name, namespace)
			}
			return fmt.Errorf("reading issuer %q: %w", name, err)
		}
	case "ClusterIssuer":
		clusterIssuer := &certmanagerv1.ClusterIssuer{}
		if err := c.Get(ctx, client.ObjectKey{Name: name}, clusterIssuer); err != nil {
			if k8serrors.IsNotFound(err) {
				return fmt.Errorf("clusterIssuer %q not found", name)
			}
			return fmt.Errorf("reading clusterIssuer %q: %w", name, err)
		}
	default:
		return fmt.Errorf("unsupported issuer kind: %q", kind)
	}
	return nil
}

// issuerIsCACapable reports whether the named cert-manager (cluster)issuer can act
// as a shared CA for peer trust. A SelfSigned issuer (Spec.SelfSigned != nil) mints
// self-signed *leaf* certs and is NOT CA-capable; every other issuer config (CA,
// ACME, Vault, Venafi, external) issues certs chained to a CA shared across members
// and IS considered CA-capable. The error is non-nil only when the issuer cannot be
// read (treated as IssuerNotFound by the caller).
func issuerIsCACapable(ctx context.Context, c client.Client, name, kind, namespace string) (bool, error) {
	var spec certmanagerv1.IssuerSpec
	switch kind {
	case "Issuer":
		issuer := &certmanagerv1.Issuer{}
		if err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, issuer); err != nil {
			if k8serrors.IsNotFound(err) {
				return false, fmt.Errorf("issuer %q not found in namespace %s", name, namespace)
			}
			return false, fmt.Errorf("reading issuer %q: %w", name, err)
		}
		spec = issuer.Spec
	case "ClusterIssuer":
		clusterIssuer := &certmanagerv1.ClusterIssuer{}
		if err := c.Get(ctx, client.ObjectKey{Name: name}, clusterIssuer); err != nil {
			if k8serrors.IsNotFound(err) {
				return false, fmt.Errorf("clusterIssuer %q not found", name)
			}
			return false, fmt.Errorf("reading clusterIssuer %q: %w", name, err)
		}
		spec = clusterIssuer.Spec
	default:
		return false, fmt.Errorf("unsupported issuer kind: %q", kind)
	}
	// A self-signed issuer mints CA:FALSE leaves that are each their own root.
	if spec.SelfSigned != nil {
		return false, nil
	}
	return true, nil
}
