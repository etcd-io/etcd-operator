/*
Copyright 2026.

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
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

const (
	noSpaceClusterName   = "etcd-nospace"
	noSpaceClusterSize   = 3
	noSpaceQuotaBytes    = 64 << 10 // 65536
	noSpaceValueBytes    = 4 << 10  // 4096
	noSpaceRemedyTimeout = 1 * time.Minute
)

// TestNoSpaceAlarmRemediation fills every member's backend past a small quota,
// waits for NOSPACE, and checks the operator's §4.7 remedy clears it.
func TestNoSpaceAlarmRemediation(t *testing.T) {
	feature := features.New("nospace-alarm-remediation")

	etcdCluster := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      noSpaceClusterName,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    noSpaceClusterSize,
			Version: etcdVersion,
			EtcdOptions: []string{
				fmt.Sprintf("--quota-backend-bytes=%d", noSpaceQuotaBytes),
			},
		},
	}

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		if err := cfg.Client().Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("failed to create EtcdCluster %s: %v", noSpaceClusterName, err)
		}
		if err := waitForAllEtcdMemberReady(t, cfg, etcdCluster); err != nil {
			t.Fatalf("EtcdCluster %s never became ready: %v", noSpaceClusterName, err)
		}
		return ctx
	})

	feature.Assess("every member serves writes and enforces the configured quota",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			verifyDataOperations(t, cfg, noSpaceClusterName, "nospace-before", "ok", false)

			for _, status := range getBackendStatuses(t, cfg) {
				if status.Status.DbSizeQuota != noSpaceQuotaBytes {
					t.Fatalf("%s backend quota is %d bytes, want %d",
						status.Endpoint, status.Status.DbSizeQuota, noSpaceQuotaBytes)
				}
			}
			return ctx
		})

	feature.Assess("filling the backends raises NOSPACE and blocks writes",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			pauseEtcdCluster(ctx, t, cfg)

			podName := noSpaceClusterName + "-0"
			value := strings.Repeat("x", noSpaceValueBytes)
			for {
				cmd := append(etcdctlCmd(podName, noSpaceClusterName, namespace, false),
					"put", "nospace-fill", value)
				_, stderr, err := execInPod(t, cfg, podName, namespace, cmd)
				if err == nil {
					continue
				}
				if alarms := etcdctlAlarmList(t, cfg); !hasNoSpaceAlarm(alarms) {
					t.Fatalf("write failed with %v (stderr: %s) without raising NOSPACE; alarms: %v",
						err, stderr, alarms)
				}
				t.Logf("write rejected with %v (stderr: %s); NOSPACE is now active",
					err, strings.TrimSpace(stderr))
				break
			}

			return ctx
		})

	feature.Assess("the operator's remedy clears the alarm and restores writes",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			unpauseEtcdCluster(ctx, t, cfg)

			var alarms []*etcdserverpb.AlarmMember
			if err := wait.For(func(ctx context.Context) (bool, error) {
				alarms = etcdctlAlarmList(t, cfg)
				return !hasNoSpaceAlarm(alarms), nil
			}, wait.WithTimeout(noSpaceRemedyTimeout), wait.WithInterval(5*time.Second)); err != nil {
				t.Fatalf("the operator never disarmed the NOSPACE alarm: %v; last alarms: %v", err, alarms)
			}

			var sizes string
			if err := wait.For(func(ctx context.Context) (bool, error) {
				underQuota := true
				details := make([]string, 0, noSpaceClusterSize)
				for _, status := range getBackendStatuses(t, cfg) {
					if status.Status.DbSize >= noSpaceQuotaBytes*9/10 {
						underQuota = false
					}
					details = append(details, fmt.Sprintf("%s dbSize=%d",
						status.Endpoint, status.Status.DbSize))
				}
				sizes = strings.Join(details, "; ")
				return underQuota, nil
			}, wait.WithTimeout(noSpaceRemedyTimeout), wait.WithInterval(5*time.Second)); err != nil {
				t.Fatalf("every member's backend never fell under 90%% of the quota: %v; last observed: %s",
					err, sizes)
			}

			verifyDataOperations(t, cfg, noSpaceClusterName, "nospace-recovered", "ok", false)
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, cfg, noSpaceClusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

type endpointStatus struct {
	Endpoint string                       `json:"Endpoint"`
	Status   *etcdserverpb.StatusResponse `json:"Status"`
}

func getBackendStatuses(t *testing.T, cfg *envconf.Config) []endpointStatus {
	t.Helper()

	podName := noSpaceClusterName + "-0"
	endpoints := make([]string, 0, noSpaceClusterSize)
	for ordinal := range noSpaceClusterSize {
		endpoints = append(endpoints, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
			noSpaceClusterName, ordinal, noSpaceClusterName, namespace))
	}

	cmd := append(etcdctlCmd(podName, noSpaceClusterName, namespace, false),
		"--endpoints="+strings.Join(endpoints, ","), "endpoint", "status", "-w", "json")
	response, errResp, err := execInPod(t, cfg, podName, namespace, cmd)
	if err != nil {
		t.Fatalf("etcdctl endpoint status failed: %v (stderr: %s)", err, errResp)
	}

	var statuses []endpointStatus
	if err := json.Unmarshal([]byte(response), &statuses); err != nil {
		t.Fatalf("failed to parse endpoint status %q: %v", response, err)
	}
	if len(statuses) != noSpaceClusterSize {
		t.Fatalf("expected %d endpoint statuses, got %d: %s", noSpaceClusterSize, len(statuses), response)
	}
	return statuses
}

func etcdctlAlarmList(t *testing.T, cfg *envconf.Config) []*etcdserverpb.AlarmMember {
	t.Helper()

	podName := noSpaceClusterName + "-0"
	cmd := append(etcdctlCmd(podName, noSpaceClusterName, namespace, false),
		"alarm", "list", "-w", "json")
	resp, errResp, err := execInPod(t, cfg, podName, namespace, cmd)
	if err != nil {
		t.Fatalf("etcdctl alarm list failed: %v (stderr: %s)", err, errResp)
	}

	var response etcdserverpb.AlarmResponse
	if err := json.Unmarshal([]byte(resp), &response); err != nil {
		t.Fatalf("failed to parse alarm list %q: %v", resp, err)
	}
	return response.Alarms
}

func hasNoSpaceAlarm(alarms []*etcdserverpb.AlarmMember) bool {
	return slices.ContainsFunc(alarms, func(a *etcdserverpb.AlarmMember) bool {
		return a.Alarm == etcdserverpb.AlarmType_NOSPACE
	})
}

func pauseEtcdCluster(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	updateEtcdCluster(ctx, t, cfg, noSpaceClusterName, func(ec *ecv1alpha1.EtcdCluster) {
		ec.Spec.Paused = true
	})
}

func unpauseEtcdCluster(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	updateEtcdCluster(ctx, t, cfg, noSpaceClusterName, func(ec *ecv1alpha1.EtcdCluster) {
		ec.Spec.Paused = false
	})
}
