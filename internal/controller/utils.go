package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

func defaultArgs(name string) []string {
	return []string{
		"--name=$(POD_NAME)",
		"--listen-peer-urls=http://0.0.0.0:2380",   // TODO: only listen on 127.0.0.1 and host IP
		"--listen-client-urls=http://0.0.0.0:2379", // TODO: only listen on 127.0.0.1 and host IP
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

	// Assume arg is bool switch if idx is still -1
	return strings.TrimSpace(s)
}

func createArgs(name string, etcdOptions []string) []string {
	defaultArgs := defaultArgs(name)
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
				Args:    createArgs(ec.Name, ec.Spec.EtcdOptions),
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

	// mount server and peer certificate secret to each pods of the statefulset via PodSpec
	var certVolume []corev1.Volume
	serverCertName := getServerCertName(ec.Name)
	peerCertName := getPeerCertName(ec.Name)
	if ec.Spec.TLS != nil {
		serverCertVolume := corev1.Volume{
			Name: "server-secret",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: serverCertName},
			},
		}
		peerCertVolume := corev1.Volume{
			Name: "peer-secret",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: peerCertName},
			},
		}
		certVolume = append(certVolume, serverCertVolume, peerCertVolume)
	}
	if len(certVolume) != 0 {
		podSpec.Volumes = certVolume
	}

	// Prepare pod template metadata
	podTemplateMetadata := metav1.ObjectMeta{
		Labels:      make(map[string]string),
		Annotations: make(map[string]string),
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

		stsSpec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
			Name:        volumeName,
			MountPath:   etcdDataDir,
			SubPathExpr: "$(POD_NAME)",
		}}
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
	return name, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2380",
		ec.Name, index, ec.Name, ec.Namespace)
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

func clientEndpointForOrdinalIndex(sts *appsv1.StatefulSet, index int) string {
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
		sts.Name, index, sts.Name, sts.Namespace)
}

func getStatefulSet(ctx context.Context, c client.Client, name, namespace string) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, sts)
	if err != nil {
		return nil, err
	}
	return sts, nil
}

func clientEndpointsFromStatefulsets(sts *appsv1.StatefulSet) []string {
	var endpoints []string
	replica := int(*sts.Spec.Replicas)
	if replica > 0 {
		for i := 0; i < replica; i++ {
			endpoints = append(endpoints, clientEndpointForOrdinalIndex(sts, i))
		}
	}
	return endpoints
}

func areAllMembersHealthy(sts *appsv1.StatefulSet, logger logr.Logger) (bool, error) {
	_, health, err := healthCheck(sts, logger)
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
// is unhealthy, the error won't be nil.
func healthCheck(sts *appsv1.StatefulSet, lg klog.Logger) (*clientv3.MemberListResponse, []etcdutils.EpHealth, error) {
	replica := int(*sts.Spec.Replicas)
	if replica == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromStatefulsets(sts)

	memberlistResp, err := etcdutils.MemberList(endpoints)
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

	healthInfos, err := etcdutils.ClusterHealth(endpoints)
	if err != nil {
		return memberlistResp, nil, err
	}

	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			// TODO: also update metrics?
			return memberlistResp, healthInfos, errors.New(healthInfo.String())
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, nil
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

func createCMCertificateConfig(ec *ecv1alpha1.EtcdCluster) *certInterface.Config {
	cmConfig := ec.Spec.TLS.ProviderCfg.CertManagerCfg
	duration, err := time.ParseDuration(cmConfig.ValidityDuration)
	if err != nil {
		log.Printf("Failed to parse ValidityDuration: %s", err)
	}

	var getAltNames certInterface.AltNames
	if cmConfig.AltNames.DNSNames != nil {
		getAltNames = certInterface.AltNames{
			DNSNames: cmConfig.AltNames.DNSNames,
			IPs:      make([]net.IP, len(cmConfig.AltNames.DNSNames)),
		}
	} else {
		defaultDNSNames := []string{fmt.Sprintf("%s.svc.cluster.local", cmConfig.CommonName)}
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
			"issuerName": cmConfig.IssuerName,
			"issuerKind": cmConfig.IssuerKind,
		},
	}
	return config
}

func createAutoCertificateConfig(ec *ecv1alpha1.EtcdCluster) *certInterface.Config {
	autoConfig := ec.Spec.TLS.ProviderCfg.AutoCfg
	duration, err := time.ParseDuration(autoConfig.ValidityDuration)
	if err != nil {
		log.Printf("Failed to parse ValidityDuration: %s", err)
	}

	var altNames certInterface.AltNames
	if autoConfig.AltNames.DNSNames != nil {
		altNames = certInterface.AltNames{
			DNSNames: autoConfig.AltNames.DNSNames,
			IPs:      make([]net.IP, len(autoConfig.AltNames.DNSNames)),
		}
	} else {
		defaultDNSNames := []string{fmt.Sprintf("%s.svc.cluster.local", autoConfig.CommonName)}
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
	return config
}

func createCertificate(ec *ecv1alpha1.EtcdCluster, ctx context.Context, c client.Client, certName string) error {
	// The TLS field is present but spec is empty
	providerName := ec.Spec.TLS.Provider
	if providerName == "" {
		providerName = string(certificate.Auto)
	}

	cert, certErr := certificate.NewProvider(certificate.ProviderType(providerName), c)
	if certErr != nil {
		// TODO: instead of error, set default autoConfig
		return certErr
	}
	_, getCertError := cert.GetCertificateConfig(ctx, certName, ec.Namespace)
	if getCertError != nil {
		if k8serrors.IsNotFound(getCertError) {
			log.Printf("Creating certificate: %s for etcd-operator: %s\n", certName, ec.Name)
			secretKey := client.ObjectKey{Name: certName, Namespace: ec.Namespace}
			switch {
			case ec.Spec.TLS.ProviderCfg.AutoCfg != nil:
				autoConfig := createAutoCertificateConfig(ec)
				createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, autoConfig)
				if createCertErr != nil {
					log.Printf("Error creating certificate: %s", createCertErr)
				}
				return nil
			case ec.Spec.TLS.ProviderCfg.CertManagerCfg != nil:
				cmConfig := createCMCertificateConfig(ec)
				createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, cmConfig)
				if createCertErr != nil {
					log.Printf("Error creating certificate: %s", createCertErr)
				}
				return nil
			default:
				// TODO: Use AuthProvider, since both AutoCfg and CertManagerCfg is not present
				log.Printf("Error creating certificate, valid certificate provider not defined.")
				return nil
			}
		} else {
			return fmt.Errorf("%s:Error getting certificate", getCertError)
		}
	}

	return nil
}

func createClientCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	certName := getClientCertName(ec.Name)
	err := createCertificate(ec, ctx, c, certName)
	if err != nil {
		return err
	}
	err = patchCertificateSecret(ctx, ec, c, certName)
	if err != nil {
		return fmt.Errorf("patching certificate secret: %s with ownerReference failed: %w", certName, err)
	}
	return err
}

func createServerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	serverCertName := getServerCertName(ec.Name)
	err := createCertificate(ec, ctx, c, serverCertName)
	if err != nil {
		return err
	}
	err = patchCertificateSecret(ctx, ec, c, serverCertName)
	if err != nil {
		return fmt.Errorf("patching certificate secret: %s with ownerReference failed: %w", serverCertName, err)
	}
	return nil
}

func createPeerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	peerCertName := getPeerCertName(ec.Name)
	err := createCertificate(ec, ctx, c, peerCertName)
	if err != nil {
		return err
	}
	err = patchCertificateSecret(ctx, ec, c, peerCertName)
	if err != nil {
		return fmt.Errorf("patching certificate secret: %s with ownerReference failed: %w", peerCertName, err)
	}
	return nil
}

func applyEtcdMemberCerts(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	if ec.Spec.TLS != nil {
		err := createServerCertificate(ctx, ec, c)
		if err != nil {
			return err
		}
		err = createPeerCertificate(ctx, ec, c)
		if err != nil {
			return err
		}
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
