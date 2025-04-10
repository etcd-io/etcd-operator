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
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestClusterHealthy(t *testing.T) {
	feature := features.New("etcd-operator-controller")

	feature.Assess("ensure the etcd-operator pod is running",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()

			// get the etcd controller deployment
			var deployment appsv1.Deployment
			if err := client.Resources().Get(ctx, "etcd-operator-controller-manager", namespace, &deployment); err != nil {
				t.Fatalf("Failed to get deployment: %s", err)
			}

			// check for deployment to become available with minimum replicas
			if err := wait.For(
				conditions.New(client.Resources()).
					DeploymentConditionMatch(&deployment, appsv1.DeploymentAvailable, corev1.ConditionTrue),
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				t.Fatal(err)
			}

			return ctx
		})

	// 'testEnv' is the env.Environment you set up in TestMain
	_ = testEnv.Test(t, feature.Feature())
}
