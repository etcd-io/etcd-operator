/*
Copyright 2025.

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

	corev1 "k8s.io/api/core/v1"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
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

// Gets K8S object from cluster, accepts wait options. Returns error if resource does not exist
func getKubernetesResource(
	ctx context.Context,
	object k8s.Object,
	client klient.Client,
	outObj k8s.Object, options ...wait.Option) error {
	objectKey := runtimeClient.ObjectKeyFromObject(object)

	if err := wait.For(
		conditions.New(client.Resources()).ResourceMatch(object,
			func(object k8s.Object) bool {
				err := client.Resources().Get(ctx, objectKey.Name, objectKey.Namespace, outObj)
				return err == nil
			}),
		options...,
	); err != nil {
		return fmt.Errorf("unable to find %s: %s", &objectKey, err)
	}
	return nil
}
