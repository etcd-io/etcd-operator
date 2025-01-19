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
	"testing"
	"time"

	// "sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/support/kind"
	"sigs.k8s.io/e2e-framework/support/utils"
	// "go.etcd.io/etcd-operator/test/utils"
)

var (
	testenv env.Environment
	namespace = "etcd-operator-system"
	kindClusterName string
	kustomizeVer = "v5.5.0"
	ctrlGenVer   = "v0.16.4"
	dockerImage = "razashahid107/etcd-operator:v0.1"

)

func TestMain(m *testing.M) {
	testenv = env.New()
	kindClusterName = "etcd-cluster"
	kindCluster := kind.NewCluster(kindClusterName)
	log.Println("Creating Kind cluster...")

	// Use pre-defined environment funcs to create a kind cluster prior to test run
	testenv.Setup(
		envfuncs.CreateCluster(kindCluster, kindClusterName),
		envfuncs.CreateNamespace(namespace),
		// install tool dependencies
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			log.Println("Installing bin tools...")
			if p := utils.RunCommand(fmt.Sprintf("go install sigs.k8s.io/kustomize/kustomize/v5@%s", kustomizeVer)); p.Err() != nil {
				log.Printf("Failed to install kustomize binary: %s: %s", p.Err(), p.Out())
				return ctx, p.Err()
			}
			if p := utils.RunCommand(fmt.Sprintf("go install sigs.k8s.io/controller-tools/cmd/controller-gen@%s", ctrlGenVer)); p.Err() != nil {
				log.Printf("Failed to install controller-gen binary: %s: %s", p.Err(), p.Out())
				return ctx, p.Err()
			}
			return ctx, nil
		},
		// generate and deploy resource configurations
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			log.Println("Building source components...")
			origWd, _ := os.Getwd()

			// change dir for Make file or it will fail
			if err := os.Chdir("../"); err != nil {
				log.Printf("Unable to set working directory: %s", err)
				return ctx, err
			}

			// gen manifest files
			log.Println("Generate manifests...")
			if p := utils.RunCommand(`controller-gen rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases`); p.Err() != nil {
				log.Printf("Failed to generate manifests: %s: %s", p.Err(), p.Out())
				return ctx, p.Err()
			}

			// gen api objects
			log.Println("Generate API objects...")
			if p := utils.RunCommand(`controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."`); p.Err() != nil {
				log.Printf("Failed to generate API objects: %s: %s", p.Err(), p.Out())
				return ctx, p.Err()
			}

			// Build docker image
			log.Println("Building docker image...")
			if p := utils.RunCommand(fmt.Sprintf("docker build -t %s .", dockerImage)); p.Err() != nil {
				log.Printf("Failed to build docker image: %s: %s", p.Err(), p.Out())
				return ctx, p.Err()
			}

			// Load docker image into kind
			log.Println("Loading docker image into kind cluster...")
			if err := kindCluster.LoadImage(ctx, dockerImage); err != nil {
				log.Printf("Failed to load image into kind: %s", err)
				return ctx, err
			}

			// Deploy components
			log.Println("Deploying controller-manager resources...")
			if p := utils.RunCommand(`bash -c "kustomize build config/default | kubectl apply --server-side -f -"`); p.Err() != nil {
				log.Printf("Failed to deploy resource configurations: %s: %s", p.Err(), p.Out())
				return ctx, p.Err()
			}

			// wait for controller-manager to be ready
			log.Println("Waiting for controller-manager deployment to be available...")
			client := cfg.Client()
			if err := wait.For(
				conditions.New(client.Resources()).DeploymentAvailable("etcd-operator-controller-manager", "etcd-operator-system"),
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				log.Printf("Timed out while waiting for etcd-operator-controller-manager deployment: %s", err)
				return ctx, err
			}

			if err := os.Chdir(origWd); err != nil {
				log.Printf("Unable to set working directory: %s", err)
				return ctx, err
			}
			log.Println("Finished setting up controller-manager")
			return ctx, nil
		},

	)

	// Use pre-defined environment funcs to teardown kind cluster after tests
	testenv.Finish(
		func(ctx context.Context, c *envconf.Config) (context.Context, error) {
			log.Println("Finishing tests, cleaning cluster ...")
			return ctx, nil
		},
		envfuncs.DeleteNamespace(namespace),
		envfuncs.TeardownCRDs("./testdata/crd", "*"),
		envfuncs.DestroyCluster(kindClusterName),
	)

	// launch package tests
	os.Exit(testenv.Run(m))
}
