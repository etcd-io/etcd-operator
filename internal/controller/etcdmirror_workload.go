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
	"maps"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// agentHTTPPort is the fixed /statusz-/healthz-/readyz-/metrics port; the
	// rendered args pin --http-bind-address to it so the status poller and
	// the probes always agree with the binary.
	agentHTTPPort = 8080

	// authSecretsRVAnnotation carries the referenced auth Secrets'
	// resourceVersions on the pod template: auth credentials are read once at
	// agent startup (unlike TLS material, which reloads per handshake), so an
	// auth rotation must roll the pod, and this annotation is what makes the
	// rendered template differ.
	authSecretsRVAnnotation = "operator.etcd.io/auth-secrets-rv"
)

const (
	appLabelKey        = "app"
	controllerLabelKey = "controller"
	// defaultClientPortName is EtcdMirrorServiceRef.Port's documented default.
	defaultClientPortName = "client"
)

func deploymentNameForEtcdMirror(em *ecv1alpha1.EtcdMirror) string {
	return em.Name + "-mirror-agent"
}

func etcdMirrorAgentLabels(em *ecv1alpha1.EtcdMirror) map[string]string {
	return map[string]string{
		appLabelKey:        deploymentNameForEtcdMirror(em),
		controllerLabelKey: em.Name,
	}
}

// agentWorkloadInput is everything the Deployment render needs beyond the CR
// itself: resolved endpoints and the presence-only credential layout (never
// secret values).
type agentWorkloadInput struct {
	image           string
	sourceEndpoints string
	targetEndpoints string
	sourceCreds     sideCredsLayout
	targetCreds     sideCredsLayout
}

// agentArgs renders the CR spec into the rung-4 mirror-agent flag surface
// (cmd/mirror-agent/config.go is the contract). Field order is fixed and
// excludePrefixes keep spec order, so re-renders of an unchanged spec are
// byte-identical and never cause a spurious rollout.
func agentArgs(em *ecv1alpha1.EtcdMirror, in agentWorkloadInput) []string {
	spec := em.Spec
	mode := spec.Mode
	if mode == "" {
		mode = ecv1alpha1.EtcdMirrorModeSync
	}
	args := []string{
		"--link-uid=" + string(em.UID),
		// The epoch is the CR generation: every pod-template-changing
		// re-deploy is spec-driven (TLS rotates in place; an auth-annotation
		// roll is a legal same-epoch fence resume), and generation is >= 1.
		fmt.Sprintf("--epoch=%d", em.Generation),
		"--mode=" + string(mode),
	}
	if spec.Source.Prefix != "" {
		args = append(args, "--source-prefix="+spec.Source.Prefix)
	}
	if spec.Target.Prefix != "" {
		args = append(args, "--target-prefix="+spec.Target.Prefix)
	}
	if spec.Sync.DestPrefix != "" {
		args = append(args, "--dest-prefix="+spec.Sync.DestPrefix)
	}
	for _, p := range spec.Sync.ExcludePrefixes {
		args = append(args, "--exclude-prefix="+p)
	}
	if spec.InitialSync != nil {
		if spec.InitialSync.Mode != "" {
			args = append(args, "--initial-sync-mode="+string(spec.InitialSync.Mode))
		}
		if spec.InitialSync.StartRevision > 0 {
			args = append(args, fmt.Sprintf("--start-revision=%d", spec.InitialSync.StartRevision))
		}
	}
	if spec.Checkpoint != nil && spec.Checkpoint.Key != "" {
		args = append(args, "--checkpoint-key="+spec.Checkpoint.Key)
	}
	if spec.Sync.MaxTxnOps > 0 {
		args = append(args, fmt.Sprintf("--max-txn-ops=%d", spec.Sync.MaxTxnOps))
	}
	if spec.Sync.TxnFlushBytes != nil {
		args = append(args, "--txn-flush-bytes="+spec.Sync.TxnFlushBytes.String())
	}
	if spec.Sync.PageKeyLimit > 0 {
		args = append(args, fmt.Sprintf("--page-key-limit=%d", spec.Sync.PageKeyLimit))
	}
	if spec.Sync.PageBytes != nil {
		args = append(args, "--page-bytes="+spec.Sync.PageBytes.String())
	}
	if spec.Sync.WatchBufferBytes != nil {
		args = append(args, "--watch-buffer-bytes="+spec.Sync.WatchBufferBytes.String())
	}
	if spec.Sync.MaxOpsPerSecond > 0 {
		args = append(args, fmt.Sprintf("--max-ops-per-second=%d", spec.Sync.MaxOpsPerSecond))
	}
	if spec.Sync.RequestTimeout != nil {
		args = append(args, "--request-timeout="+spec.Sync.RequestTimeout.Duration.String())
	}
	if spec.Sync.DialTimeout != nil {
		args = append(args, "--dial-timeout="+spec.Sync.DialTimeout.Duration.String())
	}
	if spec.Sync.ReconnectBackoff != nil {
		if spec.Sync.ReconnectBackoff.InitialDelay != nil {
			args = append(args, "--backoff-initial-delay="+spec.Sync.ReconnectBackoff.InitialDelay.Duration.String())
		}
		if spec.Sync.ReconnectBackoff.MaxDelay != nil {
			args = append(args, "--backoff-max-delay="+spec.Sync.ReconnectBackoff.MaxDelay.Duration.String())
		}
	}
	if spec.Reconciliation != nil && spec.Reconciliation.Enabled {
		args = append(args, "--reconcile-enabled")
		if spec.Reconciliation.Interval != nil {
			args = append(args, "--reconcile-interval="+spec.Reconciliation.Interval.Duration.String())
		}
		if spec.Reconciliation.DeleteOrphans {
			args = append(args, "--reconcile-delete-orphans")
		}
	}
	args = append(args, fmt.Sprintf("--http-bind-address=:%d", agentHTTPPort))
	args = append(args, sideArgs(sideSourceName, spec.Source, in.sourceEndpoints, in.sourceCreds)...)
	args = append(args, sideArgs(sideTargetName, spec.Target, in.targetEndpoints, in.targetCreds)...)
	return args
}

const (
	sideSourceName = "source"
	sideTargetName = "target"
)

func sideMountBase(side string) string { return "/etc/mirror-agent/" + side }

// sideArgs renders one side's flag block. File flags are presence-conditional
// on the resolved Secret layout so the agent never gets a path to a key the
// Secret does not hold.
func sideArgs(side string, ep ecv1alpha1.EtcdMirrorEndpoint, endpoints string, creds sideCredsLayout) []string {
	base := sideMountBase(side)
	args := []string{"--" + side + "-endpoints=" + endpoints}
	if ep.TLS != nil {
		args = append(args, "--"+side+"-tls")
		if creds.HasClientCert {
			args = append(args,
				"--"+side+"-cert-file="+base+"/tls/"+tlsSecretCertKey,
				"--"+side+"-key-file="+base+"/tls/"+tlsSecretKeyKey)
		}
		if creds.HasCA {
			args = append(args, "--"+side+"-ca-file="+base+"/tls/"+tlsSecretCAKey)
		}
		if ep.TLS.CABundleRef != nil {
			args = append(args, "--"+side+"-ca-bundle-file="+base+"/ca/"+caBundleKey(ep.TLS.CABundleRef))
		}
		if ep.TLS.ServerName != "" {
			args = append(args, "--"+side+"-server-name="+ep.TLS.ServerName)
		}
		if ep.TLS.InsecureSkipVerify {
			args = append(args, "--"+side+"-insecure-skip-verify")
		}
	}
	if ep.Auth != nil {
		args = append(args,
			"--"+side+"-username-file="+base+"/auth/"+authUsernameKey,
			"--"+side+"-password-file="+base+"/auth/"+authPasswordKey)
	}
	return args
}

func caBundleKey(ref *ecv1alpha1.EtcdMirrorCABundleRef) string {
	if ref.Key != "" {
		return ref.Key
	}
	return tlsSecretCAKey
}

// sideVolumes renders one side's Secret/ConfigMap mounts. TLS volumes are
// direct mounts (the agent re-reads them per handshake, so rotation needs no
// pod roll); the auth Secret additionally drives the pod-template
// resourceVersion annotation.
func sideVolumes(side string, ep ecv1alpha1.EtcdMirrorEndpoint) ([]corev1.Volume, []corev1.VolumeMount) {
	var vols []corev1.Volume
	var mounts []corev1.VolumeMount
	base := sideMountBase(side)
	mode := ptr.To[int32](0o400)
	if ep.TLS != nil && ep.TLS.SecretRef != nil {
		vols = append(vols, corev1.Volume{
			Name: side + "-tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  ep.TLS.SecretRef.Name,
					DefaultMode: mode,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: side + "-tls", MountPath: base + "/tls", ReadOnly: true})
	}
	if ep.TLS != nil && ep.TLS.CABundleRef != nil {
		ref := ep.TLS.CABundleRef
		vol := corev1.Volume{Name: side + "-ca-bundle"}
		if ref.Kind == "Secret" {
			vol.VolumeSource = corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: ref.Name, DefaultMode: mode},
			}
		} else {
			vol.VolumeSource = corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
					DefaultMode:          mode,
				},
			}
		}
		vols = append(vols, vol)
		mounts = append(mounts, corev1.VolumeMount{Name: side + "-ca-bundle", MountPath: base + "/ca", ReadOnly: true})
	}
	if ep.Auth != nil {
		vols = append(vols, corev1.Volume{
			Name: side + "-auth",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  ep.Auth.SecretRef.Name,
					DefaultMode: mode,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: side + "-auth", MountPath: base + "/auth", ReadOnly: true})
	}
	return vols, mounts
}

// renderAgentDeployment renders the size-1 stateless agent Deployment.
// Strategy is Recreate: the fence tolerates an overlap window, but a dual
// writer on every spec rollout buys nothing.
func renderAgentDeployment(em *ecv1alpha1.EtcdMirror, in agentWorkloadInput, replicas int32) *appsv1.Deployment {
	labels := etcdMirrorAgentLabels(em)

	srcVols, srcMounts := sideVolumes(sideSourceName, em.Spec.Source)
	tgtVols, tgtMounts := sideVolumes(sideTargetName, em.Spec.Target)

	container := corev1.Container{
		Name: "mirror-agent",
		// The operator image ships the binary at /mirror-agent but its
		// ENTRYPOINT is /manager, so the command must be explicit.
		Command:      []string{"/mirror-agent"},
		Args:         agentArgs(em, in),
		Image:        in.image,
		Ports:        []corev1.ContainerPort{{Name: "http", ContainerPort: agentHTTPPort}},
		VolumeMounts: append(srcMounts, tgtMounts...),
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http")},
			},
			PeriodSeconds: 10,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
			},
		},
	}
	if em.Spec.Resources != nil {
		container.Resources = *em.Spec.Resources
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{container},
		Volumes:    append(srcVols, tgtVols...),
		// The agent has zero Kubernetes API dependency; don't hand it a token.
		AutomountServiceAccountToken: ptr.To(false),
	}

	podMeta := metav1.ObjectMeta{
		Labels:      make(map[string]string),
		Annotations: make(map[string]string),
	}
	if em.Spec.PodTemplate != nil && em.Spec.PodTemplate.Spec != nil {
		podSpec.Affinity = em.Spec.PodTemplate.Spec.Affinity
		podSpec.NodeSelector = em.Spec.PodTemplate.Spec.NodeSelector
		podSpec.Tolerations = em.Spec.PodTemplate.Spec.Tolerations
	}
	if em.Spec.PodTemplate != nil && em.Spec.PodTemplate.Metadata != nil {
		maps.Copy(podMeta.Labels, em.Spec.PodTemplate.Metadata.Labels)
		maps.Copy(podMeta.Annotations, em.Spec.PodTemplate.Metadata.Annotations)
	}
	maps.Copy(podMeta.Labels, labels)
	if em.Spec.Source.Auth != nil || em.Spec.Target.Auth != nil {
		podMeta.Annotations[authSecretsRVAnnotation] =
			in.sourceCreds.AuthSecretRV + "/" + in.targetCreds.AuthSecretRV
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentNameForEtcdMirror(em),
			Namespace: em.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: podMeta,
				Spec:       podSpec,
			},
		},
	}
}

// serviceRefEndpoints resolves a serviceRef to one schemeless
// "<name>.<ns>.svc.cluster.local:<port>" endpoint. The port may be a name
// ("client" by default), which cannot appear in a dial URL, so a named port
// is resolved against the Service's spec.
func serviceRefEndpoints(
	ctx context.Context, c client.Client, ns string, ref *ecv1alpha1.EtcdMirrorServiceRef,
) (string, error) {
	svcNS := ref.Namespace
	if svcNS == "" {
		svcNS = ns
	}
	portSpec := ref.Port
	if portSpec == "" {
		portSpec = defaultClientPortName
	}
	svc := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: svcNS, Name: ref.Name}, svc); err != nil {
		if apierrors.IsNotFound(err) {
			return "", &credsError{Reason: reasonServiceNotFound,
				msg: fmt.Sprintf("serviceRef Service %s/%s not found", svcNS, ref.Name)}
		}
		return "", err
	}
	if n, err := strconv.Atoi(portSpec); err == nil {
		return fmt.Sprintf("%s.%s.svc.cluster.local:%d", ref.Name, svcNS, n), nil
	}
	for _, p := range svc.Spec.Ports {
		if p.Name == portSpec {
			return fmt.Sprintf("%s.%s.svc.cluster.local:%d", ref.Name, svcNS, p.Port), nil
		}
	}
	return "", &credsError{Reason: reasonServiceNotFound,
		msg: fmt.Sprintf("Service %s/%s has no port named %q", svcNS, ref.Name, portSpec)}
}

// etcdctlDelCommand renders the exact recovery command for an
// EmptyTargetViolation: the offending effective destination range as etcd
// sees it. The empty prefix is the whole keyspace, which has no prefix range
// end — only the --from-key form expresses it.
func etcdctlDelCommand(effectiveDestPrefix string) string {
	if effectiveDestPrefix == "" {
		return `etcdctl del "" --from-key`
	}
	return fmt.Sprintf("etcdctl del %q %q", effectiveDestPrefix, clientv3.GetPrefixRangeEnd(effectiveDestPrefix))
}
