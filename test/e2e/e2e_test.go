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
/*

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	"go.etcd.io/etcd-operator/test/utils"
)

// serviceAccountName created for the project
const serviceAccountName = "etcd-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "etcd-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that allows metrics access
const metricsRoleBindingName = "etcd-operator-metrics-binding"

func TestManager(t *testing.T) {
	feat := features.New("Manager End-to-End Tests")

	feat.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		t.Log("[Setup] creating manager namespace ...")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		if _, err := utils.Run(cmd); err != nil {
			t.Fatalf("Failed to create namespace: %v", err)
		}

		t.Log("[Setup] installing CRDs (make install) ...")
		cmd = exec.Command("make", "install")
		if _, err := utils.Run(cmd); err != nil {
			t.Fatalf("Failed to install CRDs: %v", err)
		}

		t.Log("[Setup] deploying the controller-manager (make deploy) ...")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
		if _, err := utils.Run(cmd); err != nil {
			t.Fatalf("Failed to deploy the controller-manager: %v", err)
		}

		return ctx
	})
	
	feat.Assess("manager pod should run successfully", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var controllerPodName string

		t.Log("validating that the controller-manager pod is running as expected")

		// Find the controller-manager pod
		cmd := exec.Command("kubectl", "get",
			"pods", "-l", "control-plane=controller-manager",
			"-o", "go-template={{ range .items }}"+
				"{{ if not .metadata.deletionTimestamp }}"+
				"{{ .metadata.name }}"+
				"{{ \"\\n\" }}{{ end }}{{ end }}",
			"-n", namespace,
		)
		podOutput, err := utils.Run(cmd)
		if err != nil {
			t.Fatalf("Failed to retrieve controller-manager pod info: %v", err)
		}
		podNames := utils.GetNonEmptyLines(podOutput)
		if len(podNames) != 1 {
			t.Fatalf("Expected exactly 1 controller pod running, got %d", len(podNames))
		}
		controllerPodName = podNames[0]
		if controllerPodName == "" {
			t.Fatalf("Got empty controllerPodName")
		}
		if !containsString(controllerPodName, "controller-manager") {
			t.Fatalf("Pod name %q did not contain 'controller-manager'", controllerPodName)
		}

		// Verify the pod's status is "Running"
		cmd = exec.Command("kubectl", "get", "pods", controllerPodName,
			"-o", "jsonpath={.status.phase}",
			"-n", namespace)
		output, err := utils.Run(cmd)
		if err != nil {
			t.Fatalf("Failed to get controller-manager pod status: %v", err)
		}
		if output != "Running" {
			t.Fatalf("Expected 'Running' status, got %q", output)
		}

		// Store the pod name in context for next steps
		return context.WithValue(ctx, "controllerPodName", controllerPodName)
	})

	feat.Assess("should ensure the metrics endpoint is serving metrics", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var controllerPodName string
		if cpn, ok := ctx.Value("controllerPodName").(string); ok {
			controllerPodName = cpn
		} else {
			t.Fatalf("No controllerPodName found in context")
		}

		t.Log("creating a ClusterRoleBinding to allow metrics access")
		cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
			"--clusterrole=etcd-operator-metrics-reader",
			fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
		)
		if _, err := utils.Run(cmd); err != nil {
			t.Fatalf("Failed to create ClusterRoleBinding: %v", err)
		}

		t.Log("validating metrics service is available")
		cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
		if _, err := utils.Run(cmd); err != nil {
			t.Fatalf("Metrics service should exist: %v", err)
		}

		t.Log("checking ServiceMonitor for Prometheus is applied in the namespace")
		cmd = exec.Command("kubectl", "get", "ServiceMonitor", "-n", namespace)
		if _, err := utils.Run(cmd); err != nil {
			t.Fatalf("ServiceMonitor should exist: %v", err)
		}

		t.Log("getting the service account token")
		token, err := serviceAccountToken(t)
		if err != nil {
			t.Fatalf("Failed to get service account token: %v", err)
		}
		if token == "" {
			t.Fatalf("Got empty token from serviceAccountToken()")
		}

		t.Log("waiting for the metrics endpoint to be ready (endpoints includes port 8443)")
		if err := eventuallyCheck(2*time.Minute, time.Second, func() error {
			cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
			out, er := utils.Run(cmd)
			if er != nil {
				return er
			}
			if !containsString(out, "8443") {
				return fmt.Errorf("metrics endpoint not ready; no '8443' found in endpoints")
			}
			return nil
		}); err != nil {
			t.Fatalf("Metrics endpoint never became ready: %v", err)
		}

		t.Log("verifying the manager is serving the metrics server")
		if err := eventuallyCheck(2*time.Minute, 2*time.Second, func() error {
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			out, er := utils.Run(cmd)
			if er != nil {
				return er
			}
			if !containsString(out, "controller-runtime.metrics\tServing metrics server") {
				return fmt.Errorf("metrics server not started yet")
			}
			return nil
		}); err != nil {
			t.Fatalf("controller manager did not serve metrics server in logs: %v", err)
		}

		t.Log("creating the curl-metrics pod to access the metrics endpoint")
		curlCmd := exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
			"--namespace", namespace,
			"--image=curlimages/curl:7.78.0",
			"--", "/bin/sh", "-c",
			fmt.Sprintf("curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics",
				token, metricsServiceName, namespace),
		)
		if _, err := utils.Run(curlCmd); err != nil {
			t.Fatalf("Failed to create curl-metrics pod: %v", err)
		}

		t.Log("waiting for the curl-metrics pod to complete (Succeeded)")
		if err := eventuallyCheck(5*time.Minute, 3*time.Second, func() error {
			cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
				"-o", "jsonpath={.status.phase}",
				"-n", namespace)
			out, er := utils.Run(cmd)
			if er != nil {
				return er
			}
			if out != "Succeeded" {
				return fmt.Errorf("curl pod not yet Succeeded, got %q", out)
			}
			return nil
		}); err != nil {
			t.Fatalf("curl-metrics pod never reached Succeeded: %v", err)
		}

		t.Log("verifying the metrics logs contain the reconcile metric")
		output, err := getMetricsOutput(t)
		if err != nil {
			t.Fatalf("Failed to get metrics from logs: %v", err)
		}
		if !containsString(output, "controller_runtime_reconcile_total") {
			t.Fatalf("Expected 'controller_runtime_reconcile_total' in metrics, not found")
		}
		return ctx
	})

	feat.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		t.Log("[Teardown] cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		t.Log("[Teardown] undeploying the controller-manager (make undeploy)")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		t.Log("[Teardown] uninstalling CRDs (make uninstall)")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		t.Log("[Teardown] removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)

		return ctx
	})

	// Run the scenario
	if err := testenv.Test(t, feat.Feature()); err != nil {
		t.Fatal(err)
	}
}

func serviceAccountToken(t *testing.T) (string, error) {
	const tokenRequestRawString = `{
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest"
    }`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	if err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), 0o644); err != nil {
		return "", err
	}

	// We'll retry this creation to mimic the "Eventually" approach
	var token string
	err := eventuallyCheck(2*time.Minute, 2*time.Second, func() error {
		cmd := exec.Command("kubectl", "create",
			"--raw",
			fmt.Sprintf("/api/v1/namespaces/%s/serviceaccounts/%s/token", namespace, serviceAccountName),
			"-f", tokenRequestFile,
		)
		output, e := cmd.CombinedOutput()
		if e != nil {
			return fmt.Errorf("error creating token: %w (output: %s)", e, string(output))
		}

		var tr tokenRequest
		if umErr := json.Unmarshal(output, &tr); umErr != nil {
			return fmt.Errorf("error unmarshaling token request: %w", umErr)
		}
		if tr.Status.Token == "" {
			return fmt.Errorf("token is empty in response")
		}
		token = tr.Status.Token
		return nil
	})
	return token, err
}

func getMetricsOutput(t *testing.T) (string, error) {
	t.Log("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	out, err := utils.Run(cmd)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve logs from curl-metrics: %w", err)
	}
	if !containsString(out, "< HTTP/1.1 200 OK") {
		return "", fmt.Errorf("metrics response did not contain 'HTTP/1.1 200 OK':\n%s", out)
	}
	return out, nil
}

// tokenRequest is the same minimal structure from your Ginkgo code
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}

func eventuallyCheck(timeout, interval time.Duration, condition func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := condition()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %s: last error: %w", timeout, err)
		}
		time.Sleep(interval)
	}
}

// Utility: naive substring check
func containsString(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && (stringIndex(haystack, needle) >= 0)
}

// Utility: a plain substring search
func stringIndex(s, sub string) int {
	return len([]rune(s)) - len([]rune((s + sub))) // naive placeholder or you can use strings.Index
}
*/