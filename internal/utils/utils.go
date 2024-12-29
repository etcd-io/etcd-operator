package utils

import (
	"context"
	"fmt"
	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func CreateOrUpdateStatefulSet(ctx context.Context, client client.Client, scheme *runtime.Scheme, ec *ecv1alpha1.EtcdCluster, replica int32) (string, error) {
	var sts appsv1.StatefulSet
	result, err := controllerutil.CreateOrUpdate(ctx, client, &sts, func() error {
		errSetControllerRef := controllerutil.SetControllerReference(ec, &sts, scheme)
		if errSetControllerRef != nil {
			return errSetControllerRef
		}
		sts.Spec = newStatefulSet(ec, replica).Spec
		return nil
	})
	if err != nil {
		return "", err
	}
	return string(result), err

}

func newStatefulSet(ec *ecv1alpha1.EtcdCluster, replica int32) *appsv1.StatefulSet {
	labels := map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ec.Name,
			Namespace: ec.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replica,
			ServiceName: ec.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "etcd",
							Command: []string{"/usr/local/bin/etcd"},
							Args: []string{
								"--name=$(POD_NAME)",
								"--listen-peer-urls=http://0.0.0.0:2380",   //TODO: only listen on 127.0.0.1 and host IP
								"--listen-client-urls=http://0.0.0.0:2379", //TODO: only listen on 127.0.0.1 and host IP
								fmt.Sprintf("--initial-advertise-peer-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", ec.Name),
								fmt.Sprintf("--advertise-client-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", ec.Name),
							},
							Image: fmt.Sprintf("gcr.io/etcd-development/etcd:%s", ec.Spec.Version),
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
											Name: ConfigMapNameForEtcdCluster(ec),
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
				},
			},
		},
	}
}

func ConfigMapNameForEtcdCluster(ec *ecv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s-state", ec.Name)
}

func PeerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2380",
		ec.Name, index, ec.Name, ec.Namespace)
}
