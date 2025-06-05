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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	operatorv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	// +kubebuilder:scaffold:imports
)

var (
	k8sClient client.Client
)

func TestMain(m *testing.M) {
	logger := log.Default()

	// read ENVTEST_K8S_VERSION
	envTestK8sVersion := os.Getenv("ENVTEST_K8S_VERSION")
	if envTestK8sVersion == "" {
		envTestK8sVersion = "1.31.0"
	}

	// Set up the test environment
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join(
			"..", "..", "bin", "k8s",
			fmt.Sprintf("%s-%s-%s", envTestK8sVersion, runtime.GOOS, runtime.GOARCH),
		),
	}

	// Start the envtest environment.
	var err error
	cfg, err := testEnv.Start()
	if err != nil {
		logger.Fatalf("Failed to start test environment: %v\n", err)
	}
	if cfg == nil {
		logger.Fatalf("Test environment started with nil config")
	}

	ctrl.SetLogger(zap.New())

	// Teardown after tests
	defer func() {
		if err := testEnv.Stop(); err != nil {
			logger.Fatalf("Failed to stop test environment: %v\n", err)
		}
	}()

	// Add custom resource’s scheme.
	err = operatorv1alpha1.AddToScheme(scheme.Scheme)
	if err != nil {
		logger.Fatalf("Failed to add operator scheme: %v\n", err)
	}

	// +kubebuilder:scaffold:scheme

	// Create a Kubernetes client to be used in tests.
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		logger.Fatalf("Failed to create k8s client: %v\n", err)
	}

	// Run all tests.
	code := m.Run()

	// Exit with the test code.
	os.Exit(code)
}
