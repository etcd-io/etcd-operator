package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/certificate"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

const (
	etcdDataDir = "/var/lib/etcd"
	volumeName  = "etcd-data"
)

type etcdClusterState string

const (
	etcdClusterStateNew      etcdClusterState = "new"
	etcdClusterStateExisting etcdClusterState = "existing"
)

// pvcNameForMember returns the PVC name for a given pod, matching the naming
// convention that StatefulSet VolumeClaimTemplates would have produced.
func pvcNameForMember(podName string) string {
	return fmt.Sprintf("%s-%s", volumeName, podName)
}

// etcdClusterLabels returns the label set applied to every member pod and used
// by the headless Service selector.
func etcdClusterLabels(ec *ecv1alpha1.EtcdCluster) map[string]string {
	return map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}
}

// createPVCForMember creates a PVC for the given pod if one does not already
// exist.  Naming mirrors StatefulSet VolumeClaimTemplates: "{volumeName}-{podName}".
func createPVCForMember(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster, podName string, scheme *runtime.Scheme) error {
	pvcName := pvcNameForMember(podName)

	existing := &corev1.PersistentVolumeClaim{}
	err := c.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: ec.Namespace}, existing)
	if err == nil {
		return nil // already exists
	}
	if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to check PVC %s: %w", pvcName, err)
	}

	if ec.Spec.StorageSpec.VolumeSizeRequest.Cmp(resource.MustParse("1Mi")) < 0 {
		return fmt.Errorf("VolumeSizeRequest must be at least 1Mi")
	}

	volumeSizeLimit := ec.Spec.StorageSpec.VolumeSizeLimit
	if volumeSizeLimit.IsZero() {
		volumeSizeLimit = ec.Spec.StorageSpec.VolumeSizeRequest
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: ec.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: ec.Spec.StorageSpec.VolumeSizeRequest,
				},
				Limits: corev1.ResourceList{
					corev1.ResourceStorage: volumeSizeLimit,
				},
			},
		},
	}

	if ec.Spec.StorageSpec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &ec.Spec.StorageSpec.StorageClassName
	}

	if err := controllerutil.SetControllerReference(ec, pvc, scheme); err != nil {
		return err
	}

	return c.Create(ctx, pvc)
}

func RemoveStringFromSlice(s []string, str string) []string {
	for i := range s {
		defaultArg := getArgName(s[i])
		if defaultArg == str {
			s = slices.Delete(s, i, i+1)
			break
		}
	}
	return s
}

func getArgName(s string) string {
	idx := strings.Index(s, "=")
	if idx != -1 {
		return s[:idx]
	}
	idx = strings.Index(s, " ")
	if idx != -1 {
		return s[:idx]
	}
	return strings.TrimSpace(s)
}

func createArgs(name string, etcdOptions []string, tlsEnabled bool) []string {
	defaultArgs := defaultArgs(name, tlsEnabled)
	if len(etcdOptions) > 0 {
		for i := range etcdOptions {
			argName := getArgName(etcdOptions[i])
			defaultArgs = RemoveStringFromSlice(defaultArgs, argName)
		}
	}
	defaultArgs = append(defaultArgs, etcdOptions...)
	return defaultArgs
}

// ---------------------------------------------------------------------------
// Kubernetes resource helpers
// ---------------------------------------------------------------------------

func createHeadlessServiceIfNotExist(ctx context.Context, logger logr.Logger, c client.Client, ec *ecv1alpha1.EtcdCluster, scheme *runtime.Scheme) error {
	service := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, service)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("Headless service does not exist. Creating headless service")
			headlessSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ec.Name,
					Namespace: ec.Namespace,
					Labels:    etcdClusterLabels(ec),
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "None",
					Selector:  etcdClusterLabels(ec),
					// PublishNotReadyAddresses keeps each member's Pod IP in the
					// headless Service's DNS A record set even while the Pod is
					// NotReady (e.g. a newly created member whose etcd process is
					// still bootstrapping). etcd v3.6's peer-port TLS handler runs
					// checkCertSAN, which forward-DNS-resolves the cert's DNSName
					// and checks the connecting Pod IP against the returned A
					// records. Without this flag CoreDNS returns only Ready Pod
					// IPs, so a new member's own IP is absent from the lookup
					// result, every peer handshake it initiates is rejected
					// ("tls: \"<ip>\" does not match any of DNSNames"), its etcd
					// bootstrap dies, and the Pod stays NotReady forever — a
					// chicken-and-egg deadlock during cluster scale-out.
					PublishNotReadyAddresses: true,
				},
			}
			if err := controllerutil.SetControllerReference(ec, headlessSvc, scheme); err != nil {
				return err
			}
			if createErr := c.Create(ctx, headlessSvc); createErr != nil {
				return fmt.Errorf("failed to create headless service: %w", createErr)
			}
			logger.Info("Headless service created successfully")
			return nil
		}
		return fmt.Errorf("failed to get headless service: %w", err)
	}
	return nil
}

// peerEndpointForOrdinalIndex returns the member name and peer URL for a given
// ordinal, used both to build ETCD_INITIAL_CLUSTER and to call AddMember. The
// peer URL scheme reflects the cluster's TLS configuration (https when TLS is
// configured, http otherwise).
func peerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("%s://%s-%d.%s.%s.svc.cluster.local:2380",
		clusterScheme(clusterTLSEnabled(ec)), ec.Name, index, ec.Name, ec.Namespace)
}

// ---------------------------------------------------------------------------
// Certificate helpers (unchanged from original implementation)
// ---------------------------------------------------------------------------

func getClientCertName(etcdClusterName string) string {
	return fmt.Sprintf("%s-%s-tls", etcdClusterName, "client")
}

func getServerCertName(etcdClusterName string) string {
	return fmt.Sprintf("%s-%s-tls", etcdClusterName, "server")
}

func getPeerCertName(etcdClusterName string) string {
	return fmt.Sprintf("%s-%s-tls", etcdClusterName, "peer")
}

func parseValidityDuration(customizedDuration string, defaultDuration time.Duration) (time.Duration, error) {
	if customizedDuration == "" {
		return defaultDuration, nil
	}
	duration, err := time.ParseDuration(customizedDuration)
	if err != nil {
		return 0, fmt.Errorf("failed to parse ValidityDuration: %w", err)
	}
	return duration, nil
}

func createCMCertificateConfig(ec *ecv1alpha1.EtcdCluster) (*certInterface.Config, error) {
	cmConfig := ec.Spec.TLS.ProviderCfg.CertManagerCfg
	if cmConfig == nil {
		return nil, fmt.Errorf("cert-manager configuration is not present")
	}

	duration, err := parseValidityDuration(cmConfig.ValidityDuration, certInterface.DefaultCertManagerValidity)
	if err != nil {
		return nil, err
	}

	var getAltNames certInterface.AltNames
	if cmConfig.AltNames.DNSNames != nil {
		getAltNames = certInterface.AltNames{
			DNSNames: cmConfig.AltNames.DNSNames,
			IPs:      make([]net.IP, len(cmConfig.AltNames.DNSNames)),
		}
	} else {
		defaultDNSNames := []string{
			fmt.Sprintf("*.%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
			fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
		}
		getAltNames = certInterface.AltNames{DNSNames: defaultDNSNames}
	}

	return &certInterface.Config{
		CommonName:       cmConfig.CommonName,
		Organization:     cmConfig.Organization,
		ValidityDuration: duration,
		AltNames:         getAltNames,
		ExtraConfig: map[string]any{
			"issuerName": cmConfig.IssuerName,
			"issuerKind": cmConfig.IssuerKind,
		},
	}, nil
}

func createAutoCertificateConfig(ec *ecv1alpha1.EtcdCluster) (*certInterface.Config, error) {
	autoConfig := ec.Spec.TLS.ProviderCfg.AutoCfg
	if autoConfig == nil {
		autoConfig = &ecv1alpha1.ProviderAutoConfig{
			CommonConfig: ecv1alpha1.CommonConfig{
				CommonName:       fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
				ValidityDuration: certInterface.DefaultAutoValidity.String(),
			},
		}
	}

	duration, err := parseValidityDuration(autoConfig.ValidityDuration, certInterface.DefaultAutoValidity)
	if err != nil {
		return nil, err
	}

	var altNames certInterface.AltNames
	if autoConfig.AltNames.DNSNames != nil {
		altNames = certInterface.AltNames{
			DNSNames: autoConfig.AltNames.DNSNames,
			IPs:      make([]net.IP, len(autoConfig.AltNames.DNSNames)),
		}
	} else {
		defaultDNSNames := []string{
			fmt.Sprintf("*.%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
			fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
		}
		altNames = certInterface.AltNames{DNSNames: defaultDNSNames}
	}

	return &certInterface.Config{
		CommonName:       autoConfig.CommonName,
		Organization:     autoConfig.Organization,
		ValidityDuration: duration,
		AltNames:         altNames,
	}, nil
}

func createCertificate(ec *ecv1alpha1.EtcdCluster, ctx context.Context, c client.Client, certName string) error {
	providerName := ec.Spec.TLS.Provider
	if providerName == "" {
		providerName = string(certificate.Auto)
	}

	cert, certErr := certificate.NewProvider(certificate.ProviderType(providerName), c)
	if certErr != nil {
		return certErr
	}
	_, getCertError := cert.GetCertificateConfig(ctx, client.ObjectKey{Name: certName, Namespace: ec.Namespace})
	if getCertError != nil {
		if k8serrors.IsNotFound(getCertError) {
			log.Printf("Creating certificate: %s for etcd-operator: %s\n", certName, ec.Name)
			secretKey := client.ObjectKey{Name: certName, Namespace: ec.Namespace}

			switch certificate.ProviderType(providerName) {
			case certificate.Auto:
				autoConfig, err := createAutoCertificateConfig(ec)
				if err != nil {
					return fmt.Errorf("error creating auto certificate config: %w", err)
				}
				if createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, autoConfig); createCertErr != nil {
					return fmt.Errorf("error creating auto certificate: %w", createCertErr)
				}
				return nil
			case certificate.CertManager:
				cmConfig, err := createCMCertificateConfig(ec)
				if err != nil {
					return fmt.Errorf("error creating cert-manager certificate config: %w", err)
				}
				if createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, cmConfig); createCertErr != nil {
					return fmt.Errorf("error creating cert-manager certificate: %w", createCertErr)
				}
				return nil
			default:
				log.Printf("Error creating certificate, valid certificate provider not defined.")
				return nil
			}
		}
		return fmt.Errorf("%s:Error getting certificate", getCertError)
	}
	return nil
}

func createClientCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	certName := getClientCertName(ec.Name)
	if err := createCertificate(ec, ctx, c, certName); err != nil {
		return err
	}
	return patchCertificateSecret(ctx, ec, c, certName)
}

func createServerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	serverCertName := getServerCertName(ec.Name)
	if err := createCertificate(ec, ctx, c, serverCertName); err != nil {
		return err
	}
	return patchCertificateSecret(ctx, ec, c, serverCertName)
}

func createPeerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	peerCertName := getPeerCertName(ec.Name)
	if err := createCertificate(ec, ctx, c, peerCertName); err != nil {
		return err
	}
	return patchCertificateSecret(ctx, ec, c, peerCertName)
}

func applyEtcdMemberCerts(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	if ec.Spec.TLS != nil {
		if err := createServerCertificate(ctx, ec, c); err != nil {
			return err
		}
		return createPeerCertificate(ctx, ec, c)
	}
	return nil
}

func patchCertificateSecret(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client, certSecretName string) error {
	getCertSecret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: certSecretName, Namespace: ec.Namespace}, getCertSecret); err != nil {
		return err
	}

	log.Printf("Setting ownerReference for certificate secret: %s", certSecretName)
	if err := controllerutil.SetControllerReference(ec, getCertSecret, c.Scheme()); err != nil {
		return err
	}
	if err := c.Update(ctx, getCertSecret); err != nil {
		return fmt.Errorf("failed to update certificate secret with ownerReference: %w", err)
	}
	return nil
}

// verifySecretHasCA returns an error if the supplied certificate Secret lacks
// a ca.crt data key. etcd requires --trusted-ca-file / --peer-trusted-ca-file,
// so a Secret without the CA cannot drive a TLS-enabled cluster. The auto
// provider always writes ca.crt; cert-manager only does so for CA-type Issuers
// (SelfSigned/CA), so the missing-key case surfaces an actionable error rather
// than mounting a non-existent file.
func verifySecretHasCA(secret *corev1.Secret, provider string) error {
	if _, ok := secret.Data[corev1.ServiceAccountRootCAKey]; !ok {
		// corev1.ServiceAccountRootCAKey == "ca.crt".
		return fmt.Errorf("certificate Secret %s (provider %s) is missing the ca.crt key; "+
			"for cert-manager use a SelfSigned or CA Issuer (ACME does not emit ca.crt)",
			secret.Name, provider)
	}
	return nil
}

// buildClientTLSConfig assembles the operator's TLS config from the cluster's
// server certificate Secret. The operator reuses the server identity (tls.crt/tls.key)
// and trusts the server CA (ca.crt), so the control plane can reach a TLS-enabled etcd client listener.
func buildClientTLSConfig(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) (*tls.Config, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: getServerCertName(ec.Name), Namespace: ec.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to get server certificate secret for client TLS: %w", err)
	}

	if err := verifySecretHasCA(secret, ec.Spec.TLS.Provider); err != nil {
		return nil, err
	}

	certData, ok := secret.Data[corev1.TLSCertKey]
	if !ok || len(certData) == 0 {
		return nil, fmt.Errorf("server certificate secret %s is missing tls.crt", secret.Name)
	}
	keyData, ok := secret.Data[corev1.TLSPrivateKeyKey]
	if !ok || len(keyData) == 0 {
		return nil, fmt.Errorf("server certificate secret %s is missing tls.key", secret.Name)
	}

	keyPair, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return nil, fmt.Errorf("failed to load server keypair for client TLS: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(secret.Data[corev1.ServiceAccountRootCAKey]) {
		return nil, fmt.Errorf("failed to parse ca.crt from server certificate secret %s", secret.Name)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ---------------------------------------------------------------------------
// Version validation
// ---------------------------------------------------------------------------

// validateEtcdUpgradePath checks whether upgrading from current to target is
// permitted by the official etcd upgrade policy. If canParse is false, one of
// the version strings could not be parsed as semver.
func validateEtcdUpgradePath(etcdVersions []semver.Version, current, target string) (canParse bool, err error) {
	var (
		currentVer            *semver.Version
		targetVer             *semver.Version
		currentIdx, targetIdx = -1, -1
	)

	currentVer, err = semver.NewVersion(current)
	if err != nil {
		return false, fmt.Errorf("failed to parse current version %s: %w", current, err)
	}
	targetVer, err = semver.NewVersion(target)
	if err != nil {
		return false, fmt.Errorf("failed to parse target version %s: %w", target, err)
	}

	for idx, v := range etcdVersions {
		if v.Major == currentVer.Major && v.Minor == currentVer.Minor {
			currentIdx = idx
		}
		if v.Major == targetVer.Major && v.Minor == targetVer.Minor {
			targetIdx = idx
		}
		if currentIdx != -1 && targetIdx != -1 {
			break
		}
	}

	switch {
	case currentIdx == -1:
		return true, fmt.Errorf("unknown current version %s", currentVer)
	case targetIdx == -1:
		return true, fmt.Errorf("unknown target version %s", targetVer)
	case currentIdx > targetIdx || (currentIdx == targetIdx && currentVer.Patch > targetVer.Patch):
		return true, fmt.Errorf("downgrading from version %s to version %s is not allowed",
			currentVer, targetVer)
	case targetIdx > currentIdx+1:
		return true, fmt.Errorf("upgrading from version %s to version %s is not allowed",
			currentVer, targetVer)
	}

	return true, nil
}
