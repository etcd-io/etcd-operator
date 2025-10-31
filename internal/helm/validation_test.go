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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHelmLint validates that the generated Helm Chart passes helm lint checks
func TestHelmLint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	rootDir, err := findProjectRoot()
	require.NoError(t, err, "Failed to find project root")

	helmDir := filepath.Join(rootDir, "helm")

	// Ensure Helm Chart is generated
	cmd := exec.Command("make", "helm")
	cmd.Dir = rootDir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "make helm failed: %s", string(output))

	t.Run("helm lint passes", func(t *testing.T) {
		cmd := exec.Command("helm", "lint", helmDir)
		cmd.Dir = rootDir
		output, err := cmd.CombinedOutput()

		outputStr := string(output)
		t.Logf("helm lint output:\n%s", outputStr)

		require.NoError(t, err, "helm lint should pass: %s", outputStr)

		// Verify output contains success messages
		assert.Contains(t, outputStr, "1 chart(s) linted", "Should report 1 chart linted")
		assert.Contains(t, outputStr, "0 chart(s) failed", "Should report 0 charts failed")
	})

	t.Run("helm lint detects no errors", func(t *testing.T) {
		cmd := exec.Command("helm", "lint", helmDir)
		cmd.Dir = rootDir
		output, err := cmd.CombinedOutput()

		outputStr := string(output)

		// helm lint returns 0 even with warnings, only errors return non-zero
		require.NoError(t, err, "helm lint should not error")

		// Verify no [ERROR] marks
		assert.NotContains(t, strings.ToUpper(outputStr), "[ERROR]", "Should not contain ERROR messages")
	})
}

// TestHelmTemplate validates that the generated Helm Chart can be rendered successfully
func TestHelmTemplate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	rootDir, err := findProjectRoot()
	require.NoError(t, err, "Failed to find project root")

	helmDir := filepath.Join(rootDir, "helm")

	// Ensure Helm Chart is generated
	cmd := exec.Command("make", "helm")
	cmd.Dir = rootDir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "make helm failed: %s", string(output))

	t.Run("helm template renders successfully", func(t *testing.T) {
		cmd := exec.Command("helm", "template", "test", helmDir)
		cmd.Dir = rootDir
		output, err := cmd.CombinedOutput()

		outputStr := string(output)

		require.NoError(t, err, "helm template should succeed: %s", outputStr)
		assert.NotEmpty(t, outputStr, "helm template should produce output")
	})

	t.Run("helm template output contains expected resources", func(t *testing.T) {
		cmd := exec.Command("helm", "template", "test", helmDir)
		cmd.Dir = rootDir
		output, err := cmd.CombinedOutput()

		outputStr := string(output)
		require.NoError(t, err, "helm template should succeed")

		// Verify output contains key resource types
		assert.Contains(t, outputStr, "kind: Deployment", "Should contain Deployment")
		assert.Contains(t, outputStr, "kind: ServiceAccount", "Should contain ServiceAccount")
		assert.Contains(t, outputStr, "kind: Role", "Should contain Role or ClusterRole")
		assert.Contains(t, outputStr, "apiVersion:", "Should contain apiVersion fields")
		assert.Contains(t, outputStr, "metadata:", "Should contain metadata fields")
	})

	t.Run("helm template with custom values renders successfully", func(t *testing.T) {
		// Test rendering with custom values
		// Create a simple temporary values file
		customValues := `
controllerManager:
  replicas: 2
`
		tmpValuesPath := filepath.Join(t.TempDir(), "custom-values.yaml")
		err := writeFile(tmpValuesPath, []byte(customValues))
		require.NoError(t, err, "Failed to write custom values file")

		cmd := exec.Command("helm", "template", "test", helmDir, "-f", tmpValuesPath)
		cmd.Dir = rootDir
		output, err := cmd.CombinedOutput()

		outputStr := string(output)

		require.NoError(t, err, "helm template with custom values should succeed: %s", outputStr)
		assert.NotEmpty(t, outputStr, "helm template should produce output")
	})
}
func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0644)
}
