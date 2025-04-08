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
	"os"
	"os/exec"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/support/kind"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"

	test_utils "go.etcd.io/etcd-operator/test/utils"
)

var (
	testEnv     env.Environment
	dockerImage = "etcd-operator:v0.1"
	namespace   = "etcd-operator-system"
)

func TestMain(m *testing.M) {
	testEnv = env.New()
	kindClusterName := "etcd-cluster"
	kindCluster := kind.NewCluster(kindClusterName)
	clusterVersion := kind.WithImage("kindest/node:v1.32.0")

	log.Println("Creating KinD cluster...")
	testEnv.Setup(
		// create KinD cluster
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			// create KinD cluster
			var err error
			ctx, err = envfuncs.CreateClusterWithOpts(kindCluster, kindClusterName, clusterVersion)(ctx, cfg)
			if err != nil {
				log.Printf("failed to create cluster: %s", err)
				return ctx, err
			}

			return ctx, nil
		},

		// prepare the resources
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			// Build docker image
			log.Println("Building docker image...")
			cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", dockerImage))
			if _, err := test_utils.Run(cmd); err != nil {
				log.Printf("Failed to build docker image: %s", err)
				return ctx, err
			}

			// Load docker image into kind
			log.Println("Loading docker image into kind cluster...")
			if err := kindCluster.LoadImage(ctx, dockerImage); err != nil {
				log.Printf("Failed to load image into kind: %s", err)
				return ctx, err
			}

			return ctx, nil
		},

		// install prometheus and cert-manager
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			log.Println("Installing prometheus operator...")
			if err := test_utils.InstallPrometheusOperator(); err != nil {
				log.Printf("Unable to install Prometheus operator: %s", err)
			}

			log.Println("Installing cert-manager...")
			if err := test_utils.InstallCertManager(); err != nil {
				log.Printf("Unable to install Cert Manager: %s", err)
			}

			return ctx, nil
		},

		// set up environment
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			// create namespace
			var err error
			ctx, err = envfuncs.CreateNamespace(namespace)(ctx, cfg)
			if err != nil {
				log.Printf("failed to create namespace: %s", err)
				return ctx, err
			}

			// install crd
			log.Println("Install crd...")
			cmd := exec.Command("make", "install")
			if _, err := test_utils.Run(cmd); err != nil {
				log.Printf("Failed to install crd: %s", err)
				return ctx, err
			}

			// Deploy components
			log.Println("Deploying components...")

			log.Println("Deploying controller-manager resources...")
			cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", dockerImage))
			if _, err := test_utils.Run(cmd); err != nil {
				log.Printf("Failed to deploy resource configurations: %s", err)
				return ctx, err
			}

			// wait for controller to get ready
			client := cfg.Client()

			log.Println("Waiting for controller-manager deployment to be available...")
			if err := wait.For(
				conditions.New(client.Resources()).DeploymentAvailable("etcd-operator-controller-manager", "etcd-operator-system"),
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				log.Printf("Timed out while waiting for etcd-operator-controller-manager deployment: %s", err)
				return ctx, err
			}

			// Add schemes
			_ = appsv1.AddToScheme(client.Resources().GetScheme())
			_ = corev1.AddToScheme(client.Resources().GetScheme())
			_ = metav1.AddMetaToScheme(client.Resources().GetScheme())
			_ = ecv1alpha1.AddToScheme(client.Resources().GetScheme())

			return ctx, nil
		},
	)

	// Use the Environment.Finish method to define clean up steps
	testEnv.Finish(
		// cleanup environment
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			log.Println("Finishing tests, cleaning cluster ...")

			// undeploy etcd operator
			log.Println("Undeploy etcd controller...")
			cmd := exec.Command("make", "undeploy", "ignore-not-found=true")
			if _, err := test_utils.Run(cmd); err != nil {
				log.Printf("Warning: Failed to undeploy controller: %s", err)
			}

			// uninstall crd
			log.Println("Uninstalling crd...")
			cmd = exec.Command("make", "uninstall", "ignore-not-found=true")
			if _, err := test_utils.Run(cmd); err != nil {
				log.Printf("Warning: Failed to install crd: %s", err)
			}

			// remove namespace
			var err error
			log.Println("Destroying namespace...")
			ctx, err = envfuncs.DeleteNamespace(namespace)(ctx, cfg)
			if err != nil {
				log.Printf("failed to delete namespace: %s", err)
			}

			return ctx, nil
		},

		// remove the installed dependencies
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			log.Println("Removing dependencies...")

			// remove prometheus
			test_utils.UninstallPrometheusOperator()

			// remove cert-manager
			test_utils.UninstallCertManager()

			return ctx, nil
		},

		// Destroy environment
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			var err error

			log.Println("Destroying cluster...")
			ctx, err = envfuncs.DestroyCluster(kindClusterName)(ctx, cfg)
			if err != nil {
				log.Printf("failed to delete cluster: %s", err)
			}

			return ctx, nil
		},
	)

	// Use Environment.Run to launch the test
	os.Exit(testEnv.Run(m))
}
