package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd-operator/pkg/certificate"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdDataDir = "/var/lib/etcd"
	volumeName  = "etcd-data"

	// Cert mount points inside the etcd container. The server (client-surface) and
	// peer secrets are mounted independently so a single-surface cluster only mounts
	// the secret it actually needs.
	serverCertVolumeName = "server-secret"
	peerCertVolumeName   = "peer-secret"
	serverCertMountPath  = "/etc/etcd/server-tls"
	peerCertMountPath    = "/etc/etcd/peer-tls"

	// Standard cert-secret keys (cert-manager and the auto provider both write these).
	tlsCertFile = "tls.crt"
	tlsKeyFile  = "tls.key"
	tlsCAFile   = "ca.crt"

	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// peerScheme returns the URL scheme etcd serves/dials on its peer port. It is
// "https" iff the peer TLS surface is configured, otherwise "http". This is the
// single source of truth for the peer scheme: peer args and every peer-endpoint
// generator derive from it so the operator can never compute a peer URL with a
// scheme etcd does not serve.
func peerScheme(ec *ecv1alpha1.EtcdCluster) string {
	if ec.Spec.TLS != nil && ec.Spec.TLS.Peer != nil {
		return schemeHTTPS
	}
	return schemeHTTP
}

// clientScheme returns the URL scheme etcd serves/dials on its client port. It is
// "https" iff the client TLS surface is configured, otherwise "http". Single source
// of truth for the client scheme (client args, client endpoints, and the operator's
// own dial scheme all derive from it).
func clientScheme(ec *ecv1alpha1.EtcdCluster) string {
	if ec.Spec.TLS != nil && ec.Spec.TLS.Client != nil {
		return schemeHTTPS
	}
	return schemeHTTP
}

// peerTLSEnabled reports whether the peer TLS surface is configured.
func peerTLSEnabled(ec *ecv1alpha1.EtcdCluster) bool {
	return ec.Spec.TLS != nil && ec.Spec.TLS.Peer != nil
}

// clientTLSEnabled reports whether the client TLS surface is configured.
func clientTLSEnabled(ec *ecv1alpha1.EtcdCluster) bool {
	return ec.Spec.TLS != nil && ec.Spec.TLS.Client != nil
}

// clientCertAuthEnabled resolves a surface's ClientCertAuth toggle, defaulting to
// true (mTLS) when unset, matching the CRD default. A nil surface returns false.
func clientCertAuthEnabled(s *ecv1alpha1.TLSSurface) bool {
	if s == nil {
		return false
	}
	if s.ClientCertAuth == nil {
		return true
	}
	return *s.ClientCertAuth
}

// validateTLS is the reconcile-time anti-misconfiguration backstop for the two TLS
// surfaces. It re-checks the spec-only coherence rules that are *also* expressed as
// CEL XValidation markers on TLSSurface (api/v1alpha1/etcdcluster_types.go), so the
// operator still rejects an invalid spec on an apiserver that does not enforce CEL
// (e.g. pre-1.25, CEL feature-gated off, or a CR written directly to a fake client
// in tests). These are the spec-derivable rules ONLY:
//
//   - provider 'cert-manager'  => providerCfg.certManagerCfg must be set
//   - provider auto/empty      => providerCfg.certManagerCfg must NOT be set
//   - clientCertAuth (mTLS) with cert-manager => issuerName must be set (a CA source)
//
// Rules that require reading cluster objects are intentionally NOT here: issuer
// existence and kind are enforced when the cert is provisioned (the cert-manager
// provider's validateCertificateConfig -> checkIssuerExists returns an error that
// requeues), and peer CA-capability / client-server CA equality live on the
// cert-manager Issuer and secret objects, not on this spec, so they cannot be
// checked purely from the EtcdCluster (plan Decision 2.5-2.7 / Decision 3). For the
// client surface specifically, the operator-client cert and the server cert both
// flow through the SINGLE client surface's issuer, so they share a CA by
// construction -- the cheap form of the client/server CA-match rule is satisfied
// without a runtime compare.
func validateTLS(ec *ecv1alpha1.EtcdCluster) field.ErrorList {
	var errs field.ErrorList
	if ec.Spec.TLS == nil {
		return errs
	}
	base := field.NewPath("spec", "tls")
	errs = append(errs, validateTLSSurface(base.Child("peer"), ec.Spec.TLS.Peer)...)
	errs = append(errs, validateTLSSurface(base.Child("client"), ec.Spec.TLS.Client)...)
	return errs
}

func validateTLSSurface(path *field.Path, s *ecv1alpha1.TLSSurface) field.ErrorList {
	var errs field.ErrorList
	if s == nil {
		return errs
	}
	hasCM := s.ProviderCfg.CertManagerCfg != nil
	isCertManager := s.Provider == string(certificate.CertManager)
	switch {
	case isCertManager && !hasCM:
		errs = append(errs, field.Required(path.Child("providerCfg", "certManagerCfg"),
			"provider 'cert-manager' requires providerCfg.certManagerCfg"))
	case !isCertManager && hasCM:
		errs = append(errs, field.Invalid(path.Child("providerCfg", "certManagerCfg"), "<set>",
			"providerCfg.certManagerCfg may only be set when provider is 'cert-manager'"))
	}
	// mTLS requires a resolvable CA. With cert-manager that means an issuerName.
	if clientCertAuthEnabled(s) && isCertManager && hasCM && s.ProviderCfg.CertManagerCfg.IssuerName == "" {
		errs = append(errs, field.Required(path.Child("providerCfg", "certManagerCfg", "issuerName"),
			"clientCertAuth requires a trusted CA: set providerCfg.certManagerCfg.issuerName"))
	}
	return errs
}

type etcdClusterState string

const (
	etcdClusterStateNew      etcdClusterState = "new"
	etcdClusterStateExisting etcdClusterState = "existing"
)

func reconcileStatefulSet(ctx context.Context, logger logr.Logger, ec *ecv1alpha1.EtcdCluster, c client.Client, replicas int32, scheme *runtime.Scheme) (*appsv1.StatefulSet, error) {

	// prepare/update configmap for StatefulSet
	err := applyEtcdClusterState(ctx, ec, int(replicas), c, scheme, logger)
	if err != nil {
		return nil, err
	}

	// Add server and peer certificate
	err = applyEtcdMemberCerts(ctx, ec, c)
	if err != nil {
		return nil, err
	}

	// Create Update StatefulSet
	err = createOrPatchStatefulSet(ctx, logger, ec, c, replicas, scheme)
	if err != nil {
		return nil, err
	}

	// Wait for statefulset to be ready
	err = waitForStatefulSetReady(ctx, logger, c, ec.Name, ec.Namespace)
	if err != nil {
		return nil, err
	}

	// Return latest Stateful set. (This is to ensure that we return the latest statefulset for next operation to act on)
	return getStatefulSet(ctx, c, ec.Name, ec.Namespace)
}

// tlsArgs captures the per-surface TLS decisions that drive defaultArgs. Each
// surface is independent: the peer flag group and peer https scheme are gated on
// peerEnabled, the server flag group and client https scheme on clientEnabled, and
// each --*client-cert-auth flag on its own *CertAuth toggle. The zero value
// (everything false) yields byte-identical cleartext output to the pre-TLS args.
type tlsArgs struct {
	peerEnabled    bool
	clientEnabled  bool
	peerCertAuth   bool
	clientCertAuth bool
}

// tlsArgsFor derives tlsArgs from an EtcdCluster's two TLS surfaces.
func tlsArgsFor(ec *ecv1alpha1.EtcdCluster) tlsArgs {
	var a tlsArgs
	if ec.Spec.TLS != nil {
		a.peerEnabled = ec.Spec.TLS.Peer != nil
		a.clientEnabled = ec.Spec.TLS.Client != nil
		a.peerCertAuth = clientCertAuthEnabled(ec.Spec.TLS.Peer)
		a.clientCertAuth = clientCertAuthEnabled(ec.Spec.TLS.Client)
	}
	return a
}

func defaultArgs(name string, tls tlsArgs) []string {
	peerScheme := schemeHTTP
	if tls.peerEnabled {
		peerScheme = schemeHTTPS
	}
	clientScheme := schemeHTTP
	if tls.clientEnabled {
		clientScheme = schemeHTTPS
	}

	args := []string{
		"--name=$(POD_NAME)",
		fmt.Sprintf("--listen-peer-urls=%s://0.0.0.0:2380", peerScheme),     // TODO: only listen on 127.0.0.1 and host IP
		fmt.Sprintf("--listen-client-urls=%s://0.0.0.0:2379", clientScheme), // TODO: only listen on 127.0.0.1 and host IP
		fmt.Sprintf("--initial-advertise-peer-urls=%s://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", peerScheme, name),
		fmt.Sprintf("--advertise-client-urls=%s://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", clientScheme, name),
	}

	// Server (client-surface) TLS flag group: emitted iff the client surface is set.
	if tls.clientEnabled {
		args = append(args,
			fmt.Sprintf("--cert-file=%s/%s", serverCertMountPath, tlsCertFile),
			fmt.Sprintf("--key-file=%s/%s", serverCertMountPath, tlsKeyFile),
			fmt.Sprintf("--trusted-ca-file=%s/%s", serverCertMountPath, tlsCAFile),
		)
		if tls.clientCertAuth {
			args = append(args, "--client-cert-auth")
		}
	}

	// Peer TLS flag group: emitted iff the peer surface is set.
	if tls.peerEnabled {
		args = append(args,
			fmt.Sprintf("--peer-cert-file=%s/%s", peerCertMountPath, tlsCertFile),
			fmt.Sprintf("--peer-key-file=%s/%s", peerCertMountPath, tlsKeyFile),
			fmt.Sprintf("--peer-trusted-ca-file=%s/%s", peerCertMountPath, tlsCAFile),
		)
		if tls.peerCertAuth {
			args = append(args, "--peer-client-cert-auth")
		}
	}

	return args
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

	// Assume arg is bool switch if idx is still -1
	return strings.TrimSpace(s)
}

func createArgs(name string, etcdOptions []string, tls tlsArgs) []string {
	defaultArgs := defaultArgs(name, tls)
	if len(etcdOptions) > 0 {
		var argName string
		// Remove default arguments if conflicts with user supplied
		for i := range etcdOptions {
			argName = getArgName(etcdOptions[i])
			defaultArgs = RemoveStringFromSlice(defaultArgs, argName)
		}
	}
	defaultArgs = append(defaultArgs, etcdOptions...)
	return defaultArgs
}

func createOrPatchStatefulSet(ctx context.Context, logger logr.Logger, ec *ecv1alpha1.EtcdCluster, c client.Client, replicas int32, scheme *runtime.Scheme) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ec.Name,
			Namespace: ec.Namespace,
		},
	}

	labels := map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:    "etcd",
				Command: []string{"/usr/local/bin/etcd"},
				Args:    createArgs(ec.Name, ec.Spec.EtcdOptions, tlsArgsFor(ec)),
				Image:   fmt.Sprintf("%s:%s", ec.Spec.ImageRegistry, ec.Spec.Version),
				Env: []corev1.EnvVar{
					{
						Name: "POD_NAME",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.name",
							},
						},
					},
					{
						Name: "POD_NAMESPACE",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.namespace",
							},
						},
					},
				},
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: configMapNameForEtcdCluster(ec),
							},
						},
					},
				},
				Ports: []corev1.ContainerPort{
					{
						Name:          "client",
						ContainerPort: 2379,
					},
					{
						Name:          "peer",
						ContainerPort: 2380,
					},
				},
			},
		},
	}

	// Mount the server and peer certificate secrets per surface, independently: the
	// server (client-surface) secret iff the client surface is configured, the peer
	// secret iff the peer surface is configured. A single-surface cluster only mounts
	// the secret it needs. VolumeMounts are appended (not assigned) so they coexist
	// with the storage data-dir mount added later.
	var certVolume []corev1.Volume
	var certMounts []corev1.VolumeMount
	if clientTLSEnabled(ec) {
		certVolume = append(certVolume, corev1.Volume{
			Name: serverCertVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: getServerCertName(ec.Name)},
			},
		})
		certMounts = append(certMounts, corev1.VolumeMount{
			Name:      serverCertVolumeName,
			MountPath: serverCertMountPath,
			ReadOnly:  true,
		})
	}
	if peerTLSEnabled(ec) {
		certVolume = append(certVolume, corev1.Volume{
			Name: peerCertVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: getPeerCertName(ec.Name)},
			},
		})
		certMounts = append(certMounts, corev1.VolumeMount{
			Name:      peerCertVolumeName,
			MountPath: peerCertMountPath,
			ReadOnly:  true,
		})
	}
	if len(certVolume) != 0 {
		podSpec.Volumes = certVolume
	}
	if len(certMounts) != 0 {
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, certMounts...)
	}

	// Prepare pod template metadata
	podTemplateMetadata := metav1.ObjectMeta{
		Labels:      make(map[string]string),
		Annotations: make(map[string]string),
	}

	// Pod Scheduling specs
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Spec != nil {
		podSpec.Affinity = ec.Spec.PodTemplate.Spec.Affinity
		podSpec.NodeSelector = ec.Spec.PodTemplate.Spec.NodeSelector
		podSpec.Tolerations = ec.Spec.PodTemplate.Spec.Tolerations
	}

	// Apply custom metadata from PodTemplate if provided
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Metadata != nil {
		// Apply custom labels
		if len(ec.Spec.PodTemplate.Metadata.Labels) > 0 {
			maps.Copy(podTemplateMetadata.Labels, ec.Spec.PodTemplate.Metadata.Labels)
		}

		// Apply annotations
		if len(ec.Spec.PodTemplate.Metadata.Annotations) > 0 {
			podTemplateMetadata.Annotations = ec.Spec.PodTemplate.Metadata.Annotations
		}
	}

	// Apply default labels
	maps.Copy(podTemplateMetadata.Labels, labels)

	stsSpec := appsv1.StatefulSetSpec{
		Replicas:    &replicas,
		ServiceName: ec.Name,
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: podTemplateMetadata,
			Spec:       podSpec,
		},
	}

	if ec.Spec.StorageSpec != nil {

		// Append the storage data-dir mount so it coexists with any TLS cert mounts
		// added above (assigning here would clobber them).
		stsSpec.Template.Spec.Containers[0].VolumeMounts = append(
			stsSpec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:        volumeName,
				MountPath:   etcdDataDir,
				SubPathExpr: "$(POD_NAME)",
			})
		// Create a new volume claim template
		if ec.Spec.StorageSpec.VolumeSizeRequest.Cmp(resource.MustParse("1Mi")) < 0 {
			return fmt.Errorf("VolumeSizeRequest must be at least 1Mi")
		}

		if ec.Spec.StorageSpec.VolumeSizeLimit.IsZero() {
			logger.Info("VolumeSizeLimit is not set. Setting it to VolumeSizeRequest")
			ec.Spec.StorageSpec.VolumeSizeLimit = ec.Spec.StorageSpec.VolumeSizeRequest
		}

		pvcResources := corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceStorage: ec.Spec.StorageSpec.VolumeSizeRequest,
			},
			Limits: corev1.ResourceList{
				corev1.ResourceStorage: ec.Spec.StorageSpec.VolumeSizeLimit,
			},
		}

		switch ec.Spec.StorageSpec.AccessModes {
		case corev1.ReadWriteOnce, "":
			stsSpec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: volumeName},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources:   pvcResources,
					},
				},
			}

			if ec.Spec.StorageSpec.StorageClassName != "" {
				stsSpec.VolumeClaimTemplates[0].Spec.StorageClassName = &ec.Spec.StorageSpec.StorageClassName
			}
		case corev1.ReadWriteMany:
			if ec.Spec.StorageSpec.PVCName == "" {
				return fmt.Errorf("PVCName must be set when AccessModes is ReadWriteMany")
			}
			stsSpec.Template.Spec.Volumes = append(stsSpec.Template.Spec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: ec.Spec.StorageSpec.PVCName,
					},
				},
			})
		default:
			return fmt.Errorf("AccessMode %s is not supported", ec.Spec.StorageSpec.AccessModes)
		}
	}

	logger.Info("Now creating/updating statefulset", "name", ec.Name, "namespace", ec.Namespace, "replicas", replicas)
	_, err := controllerutil.CreateOrPatch(ctx, c, sts, func() error {
		// Define or update the desired spec
		sts.ObjectMeta = metav1.ObjectMeta{
			Name:      ec.Name,
			Namespace: ec.Namespace,
		}
		sts.Spec = stsSpec

		// Set ower reference
		if err := controllerutil.SetControllerReference(ec, sts, scheme); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("Stateful set created/updated", "name", ec.Name, "namespace", ec.Namespace, "replicas", replicas)
	return nil
}

func waitForStatefulSetReady(ctx context.Context, logger logr.Logger, r client.Client, name, namespace string) error {
	logger.Info("Now checking the readiness of statefulset", "name", name, "namespace", namespace)

	backoff := wait.Backoff{
		Duration: 3 * time.Second,
		Factor:   2.0,
		Steps:    5,
	}

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		// Fetch the StatefulSet
		sts, err := getStatefulSet(ctx, r, name, namespace)
		if err != nil {
			return false, err
		}

		// Check if the StatefulSet is ready
		if sts.Status.ReadyReplicas == *sts.Spec.Replicas {
			// StatefulSet is ready
			logger.Info("StatefulSet is ready", "name", name, "namespace", namespace)
			return true, nil
		}

		// Log the current status
		logger.Info("StatefulSet is not ready", "ReadyReplicas", strconv.Itoa(int(sts.Status.ReadyReplicas)), "DesiredReplicas", strconv.Itoa(int(*sts.Spec.Replicas)))
		return false, nil
	})

	if err != nil {
		return fmt.Errorf("StatefulSet %s/%s did not become ready: %w", namespace, name, err)
	}

	return nil
}

func createHeadlessServiceIfNotExist(ctx context.Context, logger logr.Logger, c client.Client, ec *ecv1alpha1.EtcdCluster, scheme *runtime.Scheme) error {
	service := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, service)

	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("Headless service does not exist. Creating headless service")

			labels := map[string]string{
				"app":        ec.Name,
				"controller": ec.Name,
			}
			// Create the headless service
			headlessSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ec.Name,
					Namespace: ec.Namespace,
					Labels:    labels,
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "None", // Key for headless service
					Selector:  labels,
					// PublishNotReadyAddresses makes the headless service publish A
					// records for pods that are not yet Ready. This is REQUIRED for a
					// peer-TLS cluster to form: with --peer-client-cert-auth, etcd
					// verifies a joining peer's source IP against its cert SANs via
					// isHostInDNS (client/pkg transport), which forward-resolves the
					// cluster's DNS name. A joining member is not Ready until it has
					// joined, so without this its IP is absent from the service's A
					// record, the peer cert SAN check fails ("does not match any of
					// DNSNames"), the peer handshake is rejected, and the member
					// crashloops -- deadlocking the cluster at one member. (Cleartext
					// peer traffic does not hit this path, so the pre-TLS behaviour is
					// unchanged for non-TLS clusters.)
					PublishNotReadyAddresses: true,
				},
			}

			// Set owner reference
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

func checkStatefulSetControlledByEtcdOperator(ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) error {
	if !metav1.IsControlledBy(sts, ec) {
		return fmt.Errorf("StatefulSet %s/%s is not controlled by EtcdCluster %s/%s", sts.Namespace, sts.Name, ec.Namespace, ec.Name)
	}
	return nil
}

func configMapNameForEtcdCluster(ec *ecv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s-state", ec.Name)
}

func peerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("%s://%s-%d.%s.%s.svc.cluster.local:2380",
		peerScheme(ec), ec.Name, index, ec.Name, ec.Namespace)
}

func newEtcdClusterState(ec *ecv1alpha1.EtcdCluster, replica int) *corev1.ConfigMap {
	// We always add members one by one, so the state is always
	// "existing" if replica > 1.

	state := etcdClusterStateNew
	if replica > 1 {
		state = etcdClusterStateExisting
	}

	var initialCluster []string
	for i := 0; i < replica; i++ {
		name, peerURL := peerEndpointForOrdinalIndex(ec, i)
		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, peerURL))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapNameForEtcdCluster(ec),
			Namespace: ec.Namespace,
		},
		Data: map[string]string{
			"ETCD_INITIAL_CLUSTER_STATE": string(state),
			"ETCD_INITIAL_CLUSTER":       strings.Join(initialCluster, ","),
			"ETCD_DATA_DIR":              etcdDataDir,
		},
	}
}

func applyEtcdClusterState(ctx context.Context, ec *ecv1alpha1.EtcdCluster, replica int, c client.Client, scheme *runtime.Scheme, logger logr.Logger) error {
	cm := newEtcdClusterState(ec, replica)

	// Set owner reference
	if err := controllerutil.SetControllerReference(ec, cm, scheme); err != nil {
		return err
	}

	logger.Info("Now updating configmap", "name", configMapNameForEtcdCluster(ec), "namespace", ec.Namespace)
	err := c.Get(ctx, types.NamespacedName{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, &corev1.ConfigMap{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			createErr := c.Create(ctx, cm)
			return createErr
		}
		return err
	}

	updateErr := c.Update(ctx, cm)
	return updateErr
}

// clientEndpointForOrdinalIndex builds the operator-facing client URL for one
// member. scheme ("http"/"https") MUST be the client surface's scheme (clientScheme)
// so the operator dials the same scheme etcd serves on its client port.
func clientEndpointForOrdinalIndex(sts *appsv1.StatefulSet, index int, scheme string) string {
	return fmt.Sprintf("%s://%s-%d.%s.%s.svc.cluster.local:2379",
		scheme, sts.Name, index, sts.Name, sts.Namespace)
}

func getStatefulSet(ctx context.Context, c client.Client, name, namespace string) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, sts)
	if err != nil {
		return nil, err
	}
	return sts, nil
}

// clientEndpointsFromStatefulsets builds the operator's client endpoints. scheme
// MUST be the client surface's scheme (clientScheme of the owning EtcdCluster).
func clientEndpointsFromStatefulsets(sts *appsv1.StatefulSet, scheme string) []string {
	var endpoints []string
	replica := int(*sts.Spec.Replicas)
	if replica > 0 {
		for i := 0; i < replica; i++ {
			endpoints = append(endpoints, clientEndpointForOrdinalIndex(sts, i, scheme))
		}
	}
	return endpoints
}

func areAllMembersHealthy(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client, sts *appsv1.StatefulSet, logger logr.Logger) (bool, error) {
	_, health, err := healthCheck(ctx, ec, c, sts, logger)
	if err != nil {
		return false, err
	}

	for _, h := range health {
		if !h.Health {
			return false, nil
		}
	}
	return true, nil
}

// healthCheck returns a memberList and an error.
// If any member (excluding not yet started or already removed member)
// is unhealthy, the error won't be nil. The operator dials the client surface's
// scheme and, when the client surface is configured, presents its client TLS
// config; both derive from ec so the operator never mismatches what etcd serves.
func healthCheck(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client, sts *appsv1.StatefulSet, lg klog.Logger) (*clientv3.MemberListResponse, []etcdutils.EpHealth, error) {
	replica := int(*sts.Spec.Replicas)
	if replica == 0 {
		return nil, nil, nil
	}

	tlsConfig, err := buildClientTLSConfig(ctx, ec, c)
	if err != nil {
		return nil, nil, err
	}

	endpoints := clientEndpointsFromStatefulsets(sts, clientScheme(ec))

	memberlistResp, err := etcdutils.MemberList(endpoints, tlsConfig)
	if err != nil {
		return nil, nil, err
	}
	memberCnt := len(memberlistResp.Members)

	// Usually replica should be equal to memberCnt. If it isn't, then
	// it means previous reconcile loop somehow interrupted right after
	// adding (replica < memberCnt) or removing (replica > memberCnt)
	// a member from the cluster. In that case, we shouldn't run health
	// check on the not yet started or already removed member.
	cnt := min(replica, memberCnt)

	lg.Info("health checking", "replica", replica, "len(members)", memberCnt)
	endpoints = endpoints[:cnt]

	healthInfos, err := etcdutils.ClusterHealth(endpoints, tlsConfig)
	if err != nil {
		return memberlistResp, nil, err
	}

	var memberErrors []error
	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			// TODO: also update metrics?
			memberErrors = append(memberErrors, errors.New(healthInfo.String()))
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, utilerrors.NewAggregate(memberErrors)
}

func getClientCertName(etcdClusterName string) string {
	clientCertName := fmt.Sprintf("%s-%s-tls", etcdClusterName, "client")
	return clientCertName
}

func getServerCertName(etcdClusterName string) string {
	serverCertName := fmt.Sprintf("%s-%s-tls", etcdClusterName, "server")
	return serverCertName
}

func getPeerCertName(etcdClusterName string) string {
	peerCertName := fmt.Sprintf("%s-%s-tls", etcdClusterName, "peer")
	return peerCertName
}

// parseValidityDuration parses a duration string and returns the parsed duration.
// If the customizedDuration is empty, it returns the defaultDuration.
// Returns an error if the duration string cannot be parsed.
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

func createCMCertificateConfig(ec *ecv1alpha1.EtcdCluster, surface *ecv1alpha1.TLSSurface) (*certInterface.Config, error) {
	cmConfig := surface.ProviderCfg.CertManagerCfg
	if cmConfig == nil {
		return nil, fmt.Errorf("cert-manager configuration is not present")
	}

	// Set default duration to 90 days for cert-manager if not provided
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
		// Use wildcard DNS for the cluster's headless service to cover all pods
		// This allows the certificate to work for pod-0, pod-1, etc.
		defaultDNSNames := []string{
			fmt.Sprintf("*.%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
			fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
		}
		getAltNames = certInterface.AltNames{
			DNSNames: defaultDNSNames,
		}
	}

	config := &certInterface.Config{
		CommonName:       cmConfig.CommonName,
		Organization:     cmConfig.Organization,
		ValidityDuration: duration,
		AltNames:         getAltNames,
		ExtraConfig: map[string]any{
			"issuerName":  cmConfig.IssuerName,
			"issuerKind":  cmConfig.IssuerKind,
			"issuerGroup": cmConfig.IssuerGroup,
		},
	}
	return config, nil
}

func createAutoCertificateConfig(ec *ecv1alpha1.EtcdCluster, surface *ecv1alpha1.TLSSurface) (*certInterface.Config, error) {
	autoConfig := surface.ProviderCfg.AutoCfg
	// Set default values for auto configuration if not present
	if autoConfig == nil {
		autoConfig = &ecv1alpha1.ProviderAutoConfig{
			CommonConfig: ecv1alpha1.CommonConfig{
				CommonName:       fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
				ValidityDuration: certInterface.DefaultAutoValidity.String(),
			},
		}
	}

	// Set default duration to 365 days for auto provider if not provided
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
		// Use wildcard DNS for the cluster's headless service to cover all pods
		// This allows the certificate to work for pod-0, pod-1, etc.
		defaultDNSNames := []string{
			fmt.Sprintf("*.%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
			fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
		}
		altNames = certInterface.AltNames{
			DNSNames: defaultDNSNames,
		}
	}

	config := &certInterface.Config{
		CommonName:       autoConfig.CommonName,
		Organization:     autoConfig.Organization,
		ValidityDuration: duration,
		AltNames:         altNames,
	}
	return config, nil
}

// createCertificate provisions the certificate named certName from a SPECIFIC TLS
// surface (peer or client). The surface carries its own provider and provider
// config, so peer and client certs can flow through different issuers.
func createCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client, surface *ecv1alpha1.TLSSurface, certName string) error {
	logger := log.FromContext(ctx)

	// An empty provider on the surface defaults to the auto provider.
	providerName := surface.Provider
	if providerName == "" {
		providerName = string(certificate.Auto)
	}

	cert, certErr := certificate.NewProvider(certificate.ProviderType(providerName), c)
	if certErr != nil {
		// TODO: instead of error, set default autoConfig
		return certErr
	}
	_, getCertError := cert.GetCertificateConfig(ctx, client.ObjectKey{Name: certName, Namespace: ec.Namespace})
	if getCertError != nil {
		if k8serrors.IsNotFound(getCertError) {
			logger.Info("Creating certificate", "certificate", certName, "etcdCluster", ec.Name)
			secretKey := client.ObjectKey{Name: certName, Namespace: ec.Namespace}

			switch certificate.ProviderType(providerName) {
			case certificate.Auto:
				autoConfig, err := createAutoCertificateConfig(ec, surface)
				if err != nil {
					return fmt.Errorf("error creating auto certificate config: %w", err)
				}
				createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, autoConfig)
				if createCertErr != nil {
					return fmt.Errorf("error creating auto certificate: %w", createCertErr)
				}
				return nil
			case certificate.CertManager:
				cmConfig, err := createCMCertificateConfig(ec, surface)
				if err != nil {
					return fmt.Errorf("error creating cert-manager certificate config: %w", err)
				}
				createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, cmConfig)
				if createCertErr != nil {
					return fmt.Errorf("error creating cert-manager certificate: %w", createCertErr)
				}
				return nil
			default: // This should never happen
				// TODO: Use AuthProvider, since both AutoCfg and CertManagerCfg is not present
				logger.Info("Error creating certificate, valid certificate provider not defined", "certificate", certName)
				return nil
			}
		} else {
			return fmt.Errorf("%s:Error getting certificate", getCertError)
		}
	}

	return nil
}

// createClientCertificate provisions the operator's own client identity from the
// CLIENT surface. The operator authenticates to etcd's client port as a client.
func createClientCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	certName := getClientCertName(ec.Name)
	err := createCertificate(ctx, ec, c, ec.Spec.TLS.Client, certName)
	if err != nil {
		return err
	}
	err = patchCertificateSecret(ctx, ec, c, certName)
	if err != nil {
		return fmt.Errorf("patching certificate secret: %s with ownerReference failed: %w", certName, err)
	}
	return err
}

// createServerCertificate provisions etcd's server (client-facing) identity from
// the CLIENT surface.
func createServerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	serverCertName := getServerCertName(ec.Name)
	err := createCertificate(ctx, ec, c, ec.Spec.TLS.Client, serverCertName)
	if err != nil {
		return err
	}
	err = patchCertificateSecret(ctx, ec, c, serverCertName)
	if err != nil {
		return fmt.Errorf("patching certificate secret: %s with ownerReference failed: %w", serverCertName, err)
	}
	return nil
}

// createPeerCertificate provisions etcd's peer identity from the PEER surface.
func createPeerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	peerCertName := getPeerCertName(ec.Name)
	err := createCertificate(ctx, ec, c, ec.Spec.TLS.Peer, peerCertName)
	if err != nil {
		return err
	}
	err = patchCertificateSecret(ctx, ec, c, peerCertName)
	if err != nil {
		return fmt.Errorf("patching certificate secret: %s with ownerReference failed: %w", peerCertName, err)
	}
	return nil
}

// applyEtcdMemberCerts provisions per-surface member certs independently: the
// server cert iff the client surface is configured, the peer cert iff the peer
// surface is configured. (The operator's own client cert is provisioned separately
// in fetchAndValidateState, also gated on the client surface.)
func applyEtcdMemberCerts(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	if clientTLSEnabled(ec) {
		if err := createServerCertificate(ctx, ec, c); err != nil {
			return err
		}
	}
	if peerTLSEnabled(ec) {
		if err := createPeerCertificate(ctx, ec, c); err != nil {
			return err
		}
	}
	return nil
}

// buildClientTLSConfig builds the operator's etcd-client *tls.Config from the
// CLIENT surface's secret. It returns (nil, nil) when the client surface is not
// configured, so the operator dials cleartext (byte-identical to the pre-TLS path).
// When configured it loads the operator client keypair and pins the server's CA;
// a missing ca.crt is an explicit error (rather than silently falling through to
// the system trust store, which would verify against the wrong roots and fail
// opaquely later).
//
// No ServerName is set, so Go verifies the dialed pod FQDN against the server
// cert's SANs. The default SANs cover the pod FQDNs; if a user supplies custom
// AltNames they MUST still include *.{name}.{ns}.svc.cluster.local or every
// operator health check fails verification.
func buildClientTLSConfig(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) (*tls.Config, error) {
	if !clientTLSEnabled(ec) {
		return nil, nil
	}

	secretName := getClientCertName(ec.Name)
	secret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: secretName, Namespace: ec.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to get operator client TLS secret %s/%s: %w", ec.Namespace, secretName, err)
	}

	certPEM, ok := secret.Data[tlsCertFile]
	if !ok || len(certPEM) == 0 {
		return nil, fmt.Errorf("operator client TLS secret %s/%s missing %q", ec.Namespace, secretName, tlsCertFile)
	}
	keyPEM, ok := secret.Data[tlsKeyFile]
	if !ok || len(keyPEM) == 0 {
		return nil, fmt.Errorf("operator client TLS secret %s/%s missing %q", ec.Namespace, secretName, tlsKeyFile)
	}
	keyPair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to build operator client keypair from secret %s/%s: %w", ec.Namespace, secretName, err)
	}

	caPEM, ok := secret.Data[tlsCAFile]
	if !ok || len(caPEM) == 0 {
		return nil, fmt.Errorf("operator client TLS secret %s/%s missing %q (cannot verify the etcd server)", ec.Namespace, secretName, tlsCAFile)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("operator client TLS secret %s/%s has an unparseable %q", ec.Namespace, secretName, tlsCAFile)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func patchCertificateSecret(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client, certSecretName string) error {
	getCertSecret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: certSecretName, Namespace: ec.Namespace}, getCertSecret); err != nil {
		return err
	}

	log.FromContext(ctx).Info("Setting ownerReference for certificate secret", "secret", certSecretName)
	if err := controllerutil.SetControllerReference(ec, getCertSecret, c.Scheme()); err != nil {
		return err
	}
	if err := c.Update(ctx, getCertSecret); err != nil {
		return fmt.Errorf("failed to update certificate secret with ownerReference: %w", err)
	}

	return nil
}

// validateEtcdUpgradePat can be used to check if the current and target versions align
// with the official upgrade paths for etcd. If one of the versions cannot be parsed
// canParse is false. If the versions are equal the function will not return an error
// but that should be handled on the call site.
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
