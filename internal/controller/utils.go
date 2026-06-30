package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"slices"
	"sort"
	"strconv"
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
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd-operator/pkg/certificate"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// memberPodName returns the deterministic name for an etcd member pod.
// The naming convention mirrors StatefulSet so that headless-service DNS is identical.
func memberPodName(clusterName string, ordinal int) string {
	return fmt.Sprintf("%s-%d", clusterName, ordinal)
}

// pvcNameForMember returns the PVC name for a given pod, matching the naming
// convention that StatefulSet VolumeClaimTemplates would have produced.
func pvcNameForMember(podName string) string {
	return fmt.Sprintf("%s-%s", volumeName, podName)
}

// podOrdinal extracts the numeric ordinal from a pod name of the form
// "{clusterName}-{ordinal}".  Returns -1 on parse failure.
func podOrdinal(podName, clusterName string) int {
	suffix := strings.TrimPrefix(podName, clusterName+"-")
	ordinal, err := strconv.Atoi(suffix)
	if err != nil {
		return -1
	}
	return ordinal
}

// listOwnedPods returns all Pods that are owned (via OwnerReference) by ec,
// sorted in ascending ordinal order.
func listOwnedPods(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster) ([]*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(ec.Namespace),
		client.MatchingLabels(etcdClusterLabels(ec)),
	); err != nil {
		return nil, fmt.Errorf("failed to list pods for cluster %s: %w", ec.Name, err)
	}

	var owned []*corev1.Pod
	for i := range podList.Items {
		if metav1.IsControlledBy(&podList.Items[i], ec) {
			owned = append(owned, &podList.Items[i])
		}
	}

	sort.Slice(owned, func(i, j int) bool {
		return podOrdinal(owned[i].Name, ec.Name) < podOrdinal(owned[j].Name, ec.Name)
	})
	return owned, nil
}

// etcdClusterLabels returns the label set applied to every member pod and used
// by the headless Service selector.
func etcdClusterLabels(ec *ecv1alpha1.EtcdCluster) map[string]string {
	return map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}
}

// createMemberPod creates a single etcd member Pod (and, if needed, its PVC)
// for the given ordinal index.  It does not wait for the pod to become ready;
// the caller is responsible for requeueing until the pod is healthy.
func createMemberPod(ctx context.Context, logger logr.Logger, c client.Client, ec *ecv1alpha1.EtcdCluster, ordinal int, scheme *runtime.Scheme) error {
	podName := memberPodName(ec.Name, ordinal)

	// Ensure TLS certificates exist before the pod mounts them.
	if err := applyEtcdMemberCerts(ctx, ec, c); err != nil {
		return err
	}

	// Create per-member PVC for ReadWriteOnce storage.
	if ec.Spec.StorageSpec != nil && ec.Spec.StorageSpec.AccessModes != corev1.ReadWriteMany {
		if err := createPVCForMember(ctx, c, ec, podName, scheme); err != nil {
			return err
		}
	}

	state := etcdClusterStateExisting
	if ordinal == 0 {
		state = etcdClusterStateNew
	}

	// Build the initial-cluster value: all peers from ordinal 0 to this one.
	var clusterParts []string
	for i := 0; i <= ordinal; i++ {
		name, peerURL := peerEndpointForOrdinalIndex(ec, i)
		clusterParts = append(clusterParts, fmt.Sprintf("%s=%s", name, peerURL))
	}

	pod := buildMemberPod(ec, podName, state, strings.Join(clusterParts, ","))
	if err := controllerutil.SetControllerReference(ec, pod, scheme); err != nil {
		return err
	}

	logger.Info("Creating member pod", "name", podName, "ordinal", ordinal, "state", state)
	return c.Create(ctx, pod)
}

// buildMemberPod constructs the Pod object for a single etcd member.
func buildMemberPod(ec *ecv1alpha1.EtcdCluster, podName string, state etcdClusterState, initialCluster string) *corev1.Pod {
	// Start with custom labels then overwrite with the mandatory defaults so
	// that the headless-service selector is always satisfied.
	labels := make(map[string]string)
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Metadata != nil {
		maps.Copy(labels, ec.Spec.PodTemplate.Metadata.Labels)
	}
	maps.Copy(labels, etcdClusterLabels(ec))

	// Annotations are purely from the PodTemplate; nil when not provided.
	var annotations map[string]string
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Metadata != nil &&
		len(ec.Spec.PodTemplate.Metadata.Annotations) > 0 {
		annotations = ec.Spec.PodTemplate.Metadata.Annotations
	}

	envVars := []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
		{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: string(state)},
		{Name: "ETCD_INITIAL_CLUSTER", Value: initialCluster},
		{Name: "ETCD_DATA_DIR", Value: etcdDataDir},
	}

	container := corev1.Container{
		Name:    "etcd",
		Image:   fmt.Sprintf("%s:%s", ec.Spec.ImageRegistry, ec.Spec.Version),
		Command: []string{"/usr/local/bin/etcd"},
		Args:    createArgs(ec.Name, ec.Spec.EtcdOptions),
		Env:     envVars,
		Ports: []corev1.ContainerPort{
			{Name: "client", ContainerPort: 2379},
			{Name: "peer", ContainerPort: 2380},
		},
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{container},
	}

	// Pod scheduling customisation.
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Spec != nil {
		podSpec.Affinity = ec.Spec.PodTemplate.Spec.Affinity
		podSpec.NodeSelector = ec.Spec.PodTemplate.Spec.NodeSelector
		podSpec.Tolerations = ec.Spec.PodTemplate.Spec.Tolerations
	}

	// Persistent storage volumes.
	if ec.Spec.StorageSpec != nil {
		podSpec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
			Name:      volumeName,
			MountPath: etcdDataDir,
		}}

		switch ec.Spec.StorageSpec.AccessModes {
		case corev1.ReadWriteMany:
			// All pods share a single pre-existing PVC.
			podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: ec.Spec.StorageSpec.PVCName,
					},
				},
			})
		default: // ReadWriteOnce
			podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcNameForMember(podName),
					},
				},
			})
		}
	}

	// TLS certificate volumes (mounted as secrets).
	if ec.Spec.TLS != nil {
		podSpec.Volumes = append(podSpec.Volumes,
			corev1.Volume{
				Name: "server-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: getServerCertName(ec.Name)},
				},
			},
			corev1.Volume{
				Name: "peer-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: getPeerCertName(ec.Name)},
				},
			},
		)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   ec.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: podSpec,
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

// waitForPodReady polls until the given Pod has its Ready condition set to True,
// using an exponential back-off.  It is provided as a utility; the primary
// reconcile paths do not block on it, relying on natural requeueing instead.
func waitForPodReady(ctx context.Context, logger logr.Logger, c client.Client, podName, namespace string) error {
	logger.Info("Waiting for pod to become ready", "name", podName, "namespace", namespace)

	backoff := wait.Backoff{
		Duration: 3 * time.Second,
		Factor:   2.0,
		Steps:    5,
	}

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		pod := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod); err != nil {
			return false, err
		}
		if isPodReady(pod) {
			logger.Info("Pod is ready", "name", podName, "namespace", namespace)
			return true, nil
		}
		logger.Info("Pod is not ready yet", "name", podName, "namespace", namespace)
		return false, nil
	})

	if err != nil {
		return fmt.Errorf("pod %s/%s did not become ready: %w", namespace, podName, err)
	}
	return nil
}

// isPodReady returns true when the Pod's Ready condition is True.
func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// clientEndpointsFromPods builds the client endpoint URL for every pod in the
// slice, in the same order.  The DNS form is:
//
//	http://{podName}.{clusterName}.{namespace}.svc.cluster.local:2379
func clientEndpointsFromPods(clusterName, namespace string, pods []*corev1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	eps := make([]string, 0, len(pods))
	for _, pod := range pods {
		eps = append(eps, clientEndpointForOrdinal(clusterName, namespace, podOrdinal(pod.Name, clusterName)))
	}
	return eps
}

// clientEndpointForOrdinal returns the client endpoint URL for a member at the
// given ordinal index.
func clientEndpointForOrdinal(clusterName, namespace string, ordinal int) string {
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
		clusterName, ordinal, clusterName, namespace)
}

// areAllMembersHealthy returns true when every entry in the supplied health
// slice reports healthy.  It uses already-fetched health data and does not make
// additional network calls.
func areAllMembersHealthy(memberHealth []etcdutils.EpHealth) bool {
	for _, h := range memberHealth {
		if !h.Health {
			return false
		}
	}
	return true
}

// healthCheck returns a MemberListResponse and per-endpoint health information
// for the etcd cluster reachable through the given pods.
func healthCheck(clusterName, namespace string, pods []*corev1.Pod, lg klog.Logger) (*clientv3.MemberListResponse, []etcdutils.EpHealth, error) {
	if len(pods) == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromPods(clusterName, namespace, pods)

	memberlistResp, err := etcdutils.MemberList(endpoints)
	if err != nil {
		return nil, nil, err
	}
	memberCnt := len(memberlistResp.Members)

	// Use the smaller of the two counts: pods that are starting up may not yet
	// appear in the member list and already-removed members may have no pod.
	cnt := min(len(pods), memberCnt)
	lg.Info("health checking", "podCount", len(pods), "len(members)", memberCnt)
	endpoints = endpoints[:cnt]

	healthInfos, err := etcdutils.ClusterHealth(endpoints)
	if err != nil {
		return memberlistResp, nil, err
	}

	var memberErrors []error
	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			memberErrors = append(memberErrors, errors.New(healthInfo.String()))
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, utilerrors.NewAggregate(memberErrors)
}

// ---------------------------------------------------------------------------
// etcd argument helpers
// ---------------------------------------------------------------------------

func defaultArgs(name string) []string {
	return []string{
		"--name=$(POD_NAME)",
		"--listen-peer-urls=http://0.0.0.0:2380",
		"--listen-client-urls=http://0.0.0.0:2379",
		fmt.Sprintf("--initial-advertise-peer-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", name),
		fmt.Sprintf("--advertise-client-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", name),
	}
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

func createArgs(name string, etcdOptions []string) []string {
	defaultArgs := defaultArgs(name)
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
// ordinal, used both to build ETCD_INITIAL_CLUSTER and to call AddMember.
func peerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2380",
		ec.Name, index, ec.Name, ec.Namespace)
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
