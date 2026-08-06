package controller

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// Trust-bundle mechanics. etcd takes exactly ONE --trusted-ca-file per surface,
// so "append a user bundle to the trusted CAs" is implemented by composing a
// per-surface ConfigMap: issued CA (from the cert secret) + "\n" + user bundle,
// re-composed every reconcile so cert-secret CA rotation is picked up. The
// composed ConfigMap is mounted next to the cert secret and the surface's
// --*trusted-ca-file flag points at it instead of the secret's ca.crt.
//
// NOTE: etcd builds its trust pools once at process start (only the member's
// own keypair hot-reloads), so composition keeps the FILE current but a member
// only honors trust changes after its next restart. Deliberately no automatic
// StatefulSet roll on trust change -- rolling a quorum-sensitive workload on
// trust bytes is its own behavior change.

const (
	serverTrustVolumeName = "server-trust"
	peerTrustVolumeName   = "peer-trust"

	serverTrustMountPath = "/etc/etcd/server-trust"
	peerTrustMountPath   = "/etc/etcd/peer-trust"

	// trustBundleKey is the fixed ConfigMap key for both the user bundle and
	// the composed output.
	trustBundleKey = "ca.crt"
)

func getServerTrustName(etcdClusterName string) string {
	return fmt.Sprintf("%s-server-trusted-ca", etcdClusterName)
}

func getPeerTrustName(etcdClusterName string) string {
	return fmt.Sprintf("%s-peer-trusted-ca", etcdClusterName)
}

// trustBundleEnabled reports whether a surface requests an additional trust
// bundle.
func trustBundleEnabled(s *ecv1alpha1.TLSSurface) bool {
	return s != nil && s.TrustBundleConfigMapRef != nil
}

// validateTrustBundlePEM strictly validates a user-supplied trust bundle: every
// PEM block must be a parseable certificate and at least one certificate must
// be present. etcd's own CA-file loader (tlsutil.NewCertPool) hard-errors on
// any bad block, so a lenient check here (e.g. AppendCertsFromPEM, which skips
// bad blocks) would admit a bundle that crash-loops members at their next
// restart.
func validateTrustBundlePEM(data []byte) error {
	rest := data
	count := 0
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return fmt.Errorf("trust bundle contains a %q PEM block; only CERTIFICATE blocks are allowed", block.Type)
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return fmt.Errorf("trust bundle contains an unparseable certificate: %w", err)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("trust bundle contains no certificates")
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return fmt.Errorf("trust bundle contains trailing non-PEM data")
	}
	return nil
}

// reconcileTrustConfigMap composes the surface's trusted-CA ConfigMap from the
// issued cert secret's ca.crt and the user bundle. An invalid or missing user
// bundle is an error and the composed ConfigMap is NOT written, preserving the
// last good trust set.
func reconcileTrustConfigMap(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client,
	surface *ecv1alpha1.TLSSurface, certSecretName, trustCMName string) error {
	userCM := &corev1.ConfigMap{}
	userCMKey := client.ObjectKey{Name: surface.TrustBundleConfigMapRef.Name, Namespace: ec.Namespace}
	if err := c.Get(ctx, userCMKey, userCM); err != nil {
		return fmt.Errorf("trust bundle ConfigMap %s/%s: %w", userCMKey.Namespace, userCMKey.Name, err)
	}
	bundle, ok := userCM.Data[trustBundleKey]
	if !ok || len(bundle) == 0 {
		return fmt.Errorf("trust bundle invalid: ConfigMap %s/%s missing key %q", userCMKey.Namespace, userCMKey.Name, trustBundleKey)
	}
	if err := validateTrustBundlePEM([]byte(bundle)); err != nil {
		return fmt.Errorf("trust bundle invalid: ConfigMap %s/%s: %w", userCMKey.Namespace, userCMKey.Name, err)
	}

	certSecret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: certSecretName, Namespace: ec.Namespace}, certSecret); err != nil {
		return fmt.Errorf("cert secret %s/%s for trust composition: %w", ec.Namespace, certSecretName, err)
	}
	issuedCA, ok := certSecret.Data[tlsCAFile]
	if !ok || len(issuedCA) == 0 {
		return fmt.Errorf("cert secret %s/%s missing %q; cannot compose trust bundle", ec.Namespace, certSecretName, tlsCAFile)
	}

	composed := string(bytes.TrimRight(issuedCA, "\n")) + "\n" + bundle

	trustCM := &corev1.ConfigMap{}
	trustCM.Name = trustCMName
	trustCM.Namespace = ec.Namespace
	_, err := controllerutil.CreateOrUpdate(ctx, c, trustCM, func() error {
		trustCM.Data = map[string]string{trustBundleKey: composed}
		return controllerutil.SetControllerReference(ec, trustCM, c.Scheme())
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile trust ConfigMap %s/%s: %w", ec.Namespace, trustCMName, err)
	}
	return nil
}

// applyTrustBundles composes the per-surface trust ConfigMaps for every surface
// that requests one. Runs after the member certs are provisioned (the composed
// output embeds the issued CA) and before the StatefulSet references the
// ConfigMaps.
func applyTrustBundles(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	if ec.Spec.TLS == nil {
		return nil
	}
	if clientTLSEnabled(ec) && trustBundleEnabled(ec.Spec.TLS.Client) {
		if err := reconcileTrustConfigMap(ctx, ec, c, ec.Spec.TLS.Client,
			getServerCertName(ec.Name), getServerTrustName(ec.Name)); err != nil {
			return err
		}
	}
	if peerTLSEnabled(ec) && trustBundleEnabled(ec.Spec.TLS.Peer) {
		if err := reconcileTrustConfigMap(ctx, ec, c, ec.Spec.TLS.Peer,
			getPeerCertName(ec.Name), getPeerTrustName(ec.Name)); err != nil {
			return err
		}
	}
	return nil
}
