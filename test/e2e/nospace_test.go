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
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestNoSpaceAlarm verifies the NOSPACE self-healing path: a cluster whose
// backend exceeds a deliberately small quota is compacted, defragmented and
// disarmed by the controller, after which writes are accepted again — and no
// member's Pod or PVC is ever replaced, since the remedy only touches etcd's
// data plane (issue #475).
func TestNoSpaceAlarm(t *testing.T) {
	feature := features.New("nospace-alarm")
	clusterName := "etcd-nospace"
	podName := clusterName + "-0"

	// A small backend quota so ~64 rewrites of one 16 KiB value
	// exceed it. Writing the same key repeatedly leaves a small
	// live keyspace but a large reclaimable history — exactly what
	// the compact + defragment remedy reclaims, so the post-cycle
	// db-size check can succeed. The value must stay well under
	// the 128 KiB exec argv limit (MAX_ARG_STRLEN).
	const quotaBytes = 1024 * 1024 // 1 MiB
	bigValue := strings.Repeat("x", 16*1024)

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdClusterWithPVCOptions(ctx, t, c, clusterName, 3,
			[]string{fmt.Sprintf("--quota-backend-bytes=%d", quotaBytes)})
		if err := waitForAllEtcdMemberReady(t, c, etcdClusterRef(clusterName, 3)); err != nil {
			t.Fatalf("cluster %s did not become Ready: %v", clusterName, err)
		}
		return ctx
	})

	feature.Assess("a NOSPACE alarm is remediated and writes work again",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			before := make(map[string]resourceIDSnapshot)
			for ordinal := range 3 {
				name := fmt.Sprintf("%s-%d", clusterName, ordinal)
				before[name] = takeResourceIDSnapshot(t, c, name, "etcd-data-"+name)
			}

			// Trip the quota by writing the same key until etcd rejects a
			// write with its quota-exceeded error; any other failure is fatal.
			putCmd := append(etcdctlCmd(podName, clusterName, namespace, false), "put", "big", bigValue)
			quotaReached := false
			for i := 0; i < 256; i++ {
				if _, stderr, err := execInPod(t, c, podName, namespace, putCmd); err != nil {
					if !strings.Contains(stderr, "etcdserver: mvcc: database space exceeded") {
						t.Fatalf("unexpected failure on write %d: %v, stderr: %s", i, err, stderr)
					}
					t.Logf("write %d rejected with the quota-exceeded error", i)
					quotaReached = true
					break
				}
			}
			if !quotaReached {
				t.Errorf("etcd never rejected a write; the %d-byte quota was not reached within 256 writes", quotaBytes)
			}

			if err := waitForAlarmPresent(t, c, podName); err != nil {
				t.Fatalf("no etcd alarm appeared after filling the quota: %v", err)
			}
			// Nothing enqueues a reconcile when an etcd alarm appears, so
			// the operator may not handle the alarm until an unrelated
			// event or requeue lands.
			requestReconcile(ctx, t, c, clusterName)

			if err := waitForNoAlarms(t, c, podName); err != nil {
				t.Fatalf("the NOSPACE alarm was not disarmed: %v", err)
			}
			if err := waitForAllEtcdMemberReady(t, c, etcdClusterRef(clusterName, 3)); err != nil {
				t.Fatalf("cluster %s did not recover all members to Ready: %v", clusterName, err)
			}

			for name, snapshot := range before {
				if after := takeResourceIDSnapshot(t, c, name, "etcd-data-"+name); after != snapshot {
					t.Errorf("NOSPACE remediation must not replace member %s: before %+v, after %+v", name, snapshot, after)
				}
			}

			var ec ecv1alpha1.EtcdCluster
			if err := c.Client().Resources().Get(ctx, clusterName, namespace, &ec); err != nil {
				t.Fatalf("get EtcdCluster %s: %v", clusterName, err)
			}
			if ec.Status.NoSpaceRemediation != nil {
				t.Errorf("NoSpaceRemediation should be cleared after the cycle, got %+v", ec.Status.NoSpaceRemediation)
			}
			if ec.Status.LastCompactTime == nil {
				t.Error("LastCompactTime should be recorded after a compaction")
			}

			verifyDataOperations(t, c, clusterName, "after-nospace", "ok", false)
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, clusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
