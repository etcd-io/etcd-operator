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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

// SECURITY: this file is the single seam through which EtcdMirror credential
// Secrets are read. The discipline (the objectstore credential-handling
// precedent):
//
//   - Log only namespace/side/secret NAMES — never a key's value, never
//     certificate or credential bytes.
//   - Errors wrap only object names, key names, and API errors — never data.
//   - No secret VALUE is ever returned to the render/status/event paths:
//     rendering needs only presence booleans (sideCredsLayout); the raw
//     bytes flow exclusively into resolveFinalizerTarget's in-memory
//     CheckpointTarget for the short-lived finalizer client.

const (
	tlsSecretCertKey = "tls.crt"
	tlsSecretKeyKey  = "tls.key"
	tlsSecretCAKey   = "ca.crt"
	authUsernameKey  = "username"
	authPasswordKey  = "password"
)

// credsError is a validation failure with an API-facing condition reason.
// Its message carries object/key names only, never secret data.
type credsError struct {
	Reason string
	msg    string
}

func (e *credsError) Error() string { return e.msg }

// sideCredsLayout is the presence-only view of one side's credential
// material, everything the Deployment render needs (file flags are
// presence-conditional) without ever seeing a byte of it.
type sideCredsLayout struct {
	HasClientCert bool
	HasCA         bool
	HasAuth       bool
	// AuthSecretRV is the auth Secret's resourceVersion, rolled into a pod
	// template annotation so an auth rotation restarts the pod (auth is read
	// once at agent startup; TLS reloads live and needs no roll).
	AuthSecretRV string
}

// resolveSideCreds validates one side's referenced Secrets/ConfigMaps and
// returns their presence layout. Never logs or returns secret values.
func resolveSideCreds(
	ctx context.Context, c client.Client, logger logr.Logger,
	ns, side string, ep ecv1alpha1.EtcdMirrorEndpoint,
) (sideCredsLayout, error) {
	var layout sideCredsLayout
	if ep.TLS != nil && ep.TLS.SecretRef != nil {
		name := ep.TLS.SecretRef.Name
		secret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return layout, &credsError{Reason: reasonSecretNotFound,
					msg: fmt.Sprintf("%s TLS secret %s/%s not found", side, ns, name)}
			}
			return layout, err
		}
		_, hasCert := secret.Data[tlsSecretCertKey]
		_, hasKey := secret.Data[tlsSecretKeyKey]
		if hasCert != hasKey {
			return layout, &credsError{Reason: reasonInvalidTLSSecret,
				msg: fmt.Sprintf("%s TLS secret %s/%s has one of %s/%s without the other",
					side, ns, name, tlsSecretCertKey, tlsSecretKeyKey)}
		}
		layout.HasClientCert = hasCert
		_, layout.HasCA = secret.Data[tlsSecretCAKey]
		logger.V(1).Info("resolved TLS secret", "side", side, "secret", name,
			"hasClientCert", layout.HasClientCert, "hasCA", layout.HasCA)
	}
	if ep.TLS != nil && ep.TLS.CABundleRef != nil {
		if _, err := readCABundle(ctx, c, ns, side, ep.TLS.CABundleRef); err != nil {
			return layout, err
		}
	}
	if ep.Auth != nil {
		name := ep.Auth.SecretRef.Name
		secret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return layout, &credsError{Reason: reasonSecretNotFound,
					msg: fmt.Sprintf("%s auth secret %s/%s not found", side, ns, name)}
			}
			return layout, err
		}
		if len(secret.Data[authUsernameKey]) == 0 || len(secret.Data[authPasswordKey]) == 0 {
			return layout, &credsError{Reason: reasonInvalidAuthSecret,
				msg: fmt.Sprintf("%s auth secret %s/%s must hold non-empty %q and %q keys",
					side, ns, name, authUsernameKey, authPasswordKey)}
		}
		layout.HasAuth = true
		layout.AuthSecretRV = secret.ResourceVersion
		logger.V(1).Info("resolved auth secret", "side", side, "secret", name)
	}
	return layout, nil
}

// readCABundle fetches the PEM trust bundle a caBundleRef points at (Secret
// or ConfigMap). The returned bytes are used only by resolveFinalizerTarget;
// validation callers discard them.
func readCABundle(
	ctx context.Context, c client.Client, ns, side string, ref *ecv1alpha1.EtcdMirrorCABundleRef,
) ([]byte, error) {
	key := ref.Key
	if key == "" {
		key = tlsSecretCAKey
	}
	kind := ref.Kind
	if kind == "" {
		kind = "ConfigMap"
	}
	nn := types.NamespacedName{Namespace: ns, Name: ref.Name}
	var data []byte
	var found bool
	if kind == "Secret" {
		secret := &corev1.Secret{}
		if err := c.Get(ctx, nn, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, &credsError{Reason: reasonSecretNotFound,
					msg: fmt.Sprintf("%s caBundleRef Secret %s/%s not found", side, ns, ref.Name)}
			}
			return nil, err
		}
		data, found = secret.Data[key]
	} else {
		cm := &corev1.ConfigMap{}
		if err := c.Get(ctx, nn, cm); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, &credsError{Reason: reasonSecretNotFound,
					msg: fmt.Sprintf("%s caBundleRef ConfigMap %s/%s not found", side, ns, ref.Name)}
			}
			return nil, err
		}
		var s string
		s, found = cm.Data[key]
		data = []byte(s)
	}
	if !found {
		return nil, &credsError{Reason: reasonInvalidTLSSecret,
			msg: fmt.Sprintf("%s caBundleRef %s %s/%s has no key %q", side, kind, ns, ref.Name, key)}
	}
	return data, nil
}

// checkpointKeyForMirror mirrors the agent's checkpoint-key defaulting
// (cmd/mirror-agent passes --checkpoint-key only when spec.checkpoint.key is
// set; the engine default is prefix + DefaultCheckpointKeySuffix).
func checkpointKeyForMirror(em *ecv1alpha1.EtcdMirror) string {
	if em.Spec.Checkpoint != nil && em.Spec.Checkpoint.Key != "" {
		return em.Spec.Checkpoint.Key
	}
	return effectiveDestPrefix(em) + mirroragent.DefaultCheckpointKeySuffix
}

// resolveFinalizerTarget builds the in-memory dial material for the
// finalizer's one-key delete. Secret values flow only into the returned
// struct — same never-log contract as resolveSideCreds.
func resolveFinalizerTarget(
	ctx context.Context, c client.Client, logger logr.Logger, em *ecv1alpha1.EtcdMirror,
) (CheckpointTarget, error) {
	tgt := CheckpointTarget{Key: checkpointKeyForMirror(em), LinkUID: string(em.UID)}
	ep := em.Spec.Target

	switch {
	case len(ep.EndpointList) > 0:
		tgt.Endpoints = ep.EndpointList
	case ep.ServiceRef != nil:
		addr, err := serviceRefEndpoints(ctx, c, em.Namespace, ep.ServiceRef)
		if err != nil {
			return CheckpointTarget{}, err
		}
		scheme := "http://"
		if ep.TLS != nil {
			scheme = "https://"
		}
		tgt.Endpoints = []string{scheme + addr}
	default:
		return CheckpointTarget{}, &credsError{Reason: reasonInvalidConfig,
			msg: "target has neither endpointList nor serviceRef"}
	}

	if ep.TLS != nil {
		tlsCfg, err := finalizerTLSConfig(ctx, c, em.Namespace, ep.TLS)
		if err != nil {
			return CheckpointTarget{}, err
		}
		tgt.TLS = tlsCfg
	}

	if ep.Auth != nil {
		name := ep.Auth.SecretRef.Name
		secret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: em.Namespace, Name: name}, secret); err != nil {
			return CheckpointTarget{}, fmt.Errorf("reading target auth secret %s/%s: %w", em.Namespace, name, err)
		}
		tgt.Username = strings.TrimRight(string(secret.Data[authUsernameKey]), "\r\n")
		tgt.Password = strings.TrimRight(string(secret.Data[authPasswordKey]), "\r\n")
	}
	logger.V(1).Info("resolved finalizer target", "endpoints", tgt.Endpoints, "key", tgt.Key)
	return tgt, nil
}

// certExpiry names one piece of referenced TLS material and when it expires.
// Only the side, a human-readable kind, and NotAfter escape this file — never
// certificate bytes.
type certExpiry struct {
	Side     string
	Kind     string
	NotAfter time.Time
}

// earliestNotAfter parses every CERTIFICATE block in pemData and returns the
// earliest NotAfter (zero when nothing parses — best-effort: expiry warnings
// must never fail a reconcile, and parse errors are surfaced by the agent's
// own TLS handshake failures).
func earliestNotAfter(pemData []byte) time.Time {
	var earliest time.Time
	for len(pemData) > 0 {
		var block *pem.Block
		block, pemData = pem.Decode(pemData)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		if earliest.IsZero() || cert.NotAfter.Before(earliest) {
			earliest = cert.NotAfter
		}
	}
	return earliest
}

// sideCertExpiries collects the NotAfter of each cert material one side
// references (client leaf, ca.crt, caBundle). Best-effort: unreadable objects
// or unparseable PEM yield no entry rather than an error.
func sideCertExpiries(
	ctx context.Context, c client.Client, ns, side string, ep ecv1alpha1.EtcdMirrorEndpoint,
) []certExpiry {
	if ep.TLS == nil {
		return nil
	}
	var out []certExpiry
	if ep.TLS.SecretRef != nil {
		secret := &corev1.Secret{}
		nn := types.NamespacedName{Namespace: ns, Name: ep.TLS.SecretRef.Name}
		if err := c.Get(ctx, nn, secret); err == nil {
			if t := earliestNotAfter(secret.Data[tlsSecretCertKey]); !t.IsZero() {
				out = append(out, certExpiry{Side: side, Kind: "client certificate (tls.crt)", NotAfter: t})
			}
			if t := earliestNotAfter(secret.Data[tlsSecretCAKey]); !t.IsZero() {
				out = append(out, certExpiry{Side: side, Kind: "CA (ca.crt)", NotAfter: t})
			}
		}
	}
	if ep.TLS.CABundleRef != nil {
		if data, err := readCABundle(ctx, c, ns, side, ep.TLS.CABundleRef); err == nil {
			if t := earliestNotAfter(data); !t.IsZero() {
				out = append(out, certExpiry{Side: side, Kind: "CA bundle (caBundleRef)", NotAfter: t})
			}
		}
	}
	return out
}

// finalizerTLSConfig assembles a one-shot *tls.Config from the referenced
// Secret bytes (the finalizer client lives seconds; no live reload needed).
func finalizerTLSConfig(
	ctx context.Context, c client.Client, ns string, spec *ecv1alpha1.EtcdMirrorTLS,
) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         spec.ServerName,
		InsecureSkipVerify: spec.InsecureSkipVerify, // #nosec G402 -- CR opted in via acknowledged spec field
	}
	if spec.SecretRef != nil {
		secret := &corev1.Secret{}
		nn := types.NamespacedName{Namespace: ns, Name: spec.SecretRef.Name}
		if err := c.Get(ctx, nn, secret); err != nil {
			return nil, fmt.Errorf("reading target TLS secret %s/%s: %w", ns, spec.SecretRef.Name, err)
		}
		certPEM, keyPEM := secret.Data[tlsSecretCertKey], secret.Data[tlsSecretKeyKey]
		if len(certPEM) > 0 && len(keyPEM) > 0 {
			pair, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, fmt.Errorf("parsing client certificate in secret %s/%s: %w",
					ns, spec.SecretRef.Name, err)
			}
			cfg.Certificates = []tls.Certificate{pair}
		}
		if caPEM := secret.Data[tlsSecretCAKey]; len(caPEM) > 0 && spec.CABundleRef == nil {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("no CA certificate parsed from secret %s/%s key %s",
					ns, spec.SecretRef.Name, tlsSecretCAKey)
			}
			cfg.RootCAs = pool
		}
	}
	if spec.CABundleRef != nil {
		caPEM, err := readCABundle(ctx, c, ns, "target", spec.CABundleRef)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("no CA certificate parsed from caBundleRef %s", spec.CABundleRef.Name)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}
