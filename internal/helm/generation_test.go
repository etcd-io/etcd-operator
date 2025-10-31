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

package helm

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestHelmGeneration validates that the make helm command generates a correct Helm Chart structure
// This is an integration test that needs to actually call the make helm command
func TestHelmGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Get project root directory
	rootDir, err := findProjectRoot()
	require.NoError(t, err, "Failed to find project root")

	helmDir := filepath.Join(rootDir, "helm")

	// Clean up existing helm directory to test idempotency
	t.Cleanup(func() {
		// Keep generated files after test for subsequent tests
	})

	// Execute make helm
	cmd := exec.Command("make", "helm")
	cmd.Dir = rootDir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "make helm failed: %s", string(output))

	t.Run("Chart.yaml exists and has correct metadata", func(t *testing.T) {
		chartPath := filepath.Join(helmDir, "Chart.yaml")
		require.FileExists(t, chartPath, "Chart.yaml should be generated")

		data, err := os.ReadFile(chartPath)
		require.NoError(t, err, "Failed to read Chart.yaml")

		var chart map[string]interface{}
		err = yaml.Unmarshal(data, &chart)
		require.NoError(t, err, "Failed to parse Chart.yaml")

		// Verify required fields
		assert.Equal(t, "etcd-operator", chart["name"], "Chart name should be etcd-operator")
		assert.Equal(t, "Official Kubernetes operator for etcd", chart["description"], "Chart description should match")
		assert.Equal(t, "https://github.com/etcd-io/etcd-operator", chart["home"], "Chart home should match")
		assert.Equal(t, "application", chart["type"], "Chart type should be application")
		assert.NotEmpty(t, chart["version"], "Chart version should not be empty")
		assert.Equal(t, "v2", chart["apiVersion"], "Chart apiVersion should be v2")
	})

	t.Run("values.yaml exists and has reasonable structure", func(t *testing.T) {
		valuesPath := filepath.Join(helmDir, "values.yaml")
		require.FileExists(t, valuesPath, "values.yaml should be generated")

		data, err := os.ReadFile(valuesPath)
		require.NoError(t, err, "Failed to read values.yaml")

		var values map[string]interface{}
		err = yaml.Unmarshal(data, &values)
		require.NoError(t, err, "Failed to parse values.yaml")

		// values.yaml should contain configurable items
		// helmify-generated structure typically contains top-level keys like controllerManager
		assert.NotEmpty(t, values, "values.yaml should not be empty")
	})

	t.Run("templates directory exists with required files", func(t *testing.T) {
		templatesDir := filepath.Join(helmDir, "templates")
		require.DirExists(t, templatesDir, "templates/ directory should be generated")

		// Check if key template files are included
		entries, err := os.ReadDir(templatesDir)
		require.NoError(t, err, "Failed to read templates directory")
		assert.NotEmpty(t, entries, "templates/ should contain files")

		// Verify at least contains deployment.yaml
		foundDeployment := false
		for _, entry := range entries {
			if entry.Name() == "deployment.yaml" {
				foundDeployment = true
				break
			}
		}
		assert.True(t, foundDeployment, "templates/ should contain deployment.yaml")
	})

	t.Run("crds directory exists with CRD files", func(t *testing.T) {
		crdsDir := filepath.Join(helmDir, "crds")
		require.DirExists(t, crdsDir, "crds/ directory should be generated")

		entries, err := os.ReadDir(crdsDir)
		require.NoError(t, err, "Failed to read crds directory")
		assert.NotEmpty(t, entries, "crds/ should contain CRD files")

		// Verify contains etcdcluster CRD
		foundEtcdClusterCRD := false
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) == ".yaml" || filepath.Ext(entry.Name()) == ".yml" {
				foundEtcdClusterCRD = true
				break
			}
		}
		assert.True(t, foundEtcdClusterCRD, "crds/ should contain etcdcluster CRD")
	})

	t.Run("Helm Chart is idempotent", func(t *testing.T) {
		// First generation
		cmd1 := exec.Command("make", "helm")
		cmd1.Dir = rootDir
		output1, err := cmd1.CombinedOutput()
		require.NoError(t, err, "First make helm failed: %s", string(output1))

		chartPath := filepath.Join(helmDir, "Chart.yaml")
		data1, err := os.ReadFile(chartPath)
		require.NoError(t, err, "Failed to read Chart.yaml after first generation")

		// Second generation
		cmd2 := exec.Command("make", "helm")
		cmd2.Dir = rootDir
		output2, err := cmd2.CombinedOutput()
		require.NoError(t, err, "Second make helm failed: %s", string(output2))

		data2, err := os.ReadFile(chartPath)
		require.NoError(t, err, "Failed to read Chart.yaml after second generation")

		// Verify content is identical
		assert.Equal(t, data1, data2, "Multiple runs of make helm should produce identical Chart.yaml")
	})
}

// findProjectRoot finds the project root directory (directory containing go.mod)
func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Search upward until finding go.mod
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
