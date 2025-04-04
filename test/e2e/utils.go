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

package e2e

import (
	"context"
	"fmt"
	"log"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

func getContainerByName(containers []corev1.Container, name string) (corev1.Container, error) {
	var container corev1.Container
	for c := range containers {
		if containers[c].Name == name {
			container = containers[c]
			return container, nil
		}
	}
	return container, fmt.Errorf("container named %s not found", name)
}

func setupTestRun(ctx context.Context,
	cfg *envconf.Config,
	client klient.Client,
	etcdCluster *ecv1alpha1.EtcdCluster) (
	context.Context,
	error) {

	ctx, err := envfuncs.CreateNamespace(etcdCluster.Namespace)(ctx, cfg)
	if err != nil {
		log.Printf("failed to create namespace: %s", err)
		return ctx, nil
	}

	// create etcd cluster
	if err := client.Resources().Create(ctx, etcdCluster); err != nil {
		return ctx, fmt.Errorf("unable to create etcd cluster: %s", err)
	}

	// get etcd cluster object
	var ec ecv1alpha1.EtcdCluster

	if err := client.Resources().Get(ctx, etcdCluster.Name, etcdCluster.Namespace, &ec); err != nil {
		return ctx, fmt.Errorf("unable to fetch etcd cluster: %s", err)
	}

	return ctx, nil
}

// Gets K8S object from cluster, accepts wait options. Returns error if resource does not exist
func getKubernetesResource(
	object k8s.Object,
	ctx context.Context,
	client klient.Client,
	etcdCluster *ecv1alpha1.EtcdCluster,
	outObj k8s.Object, options ...wait.Option) (
	context.Context,
	error) {

	if err := wait.For(
		conditions.New(client.Resources()).ResourceMatch(object,
			func(object k8s.Object) bool {
				err := client.Resources().Get(ctx, etcdCluster.Name, etcdCluster.Namespace, outObj)
				return err == nil
			}),
		options...,
	); err != nil {
		fmt.Println(outObj)
		return ctx, fmt.Errorf("unable to get sts: %s", err)
	}

	return ctx, nil
}
