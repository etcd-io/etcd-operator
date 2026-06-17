//go:build stress

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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

// These helpers are intentionally composed from the primitives already living
// in helpers_test.go (createEtcdClusterWithPVC, waitForNoLearners,
// getEtcdMemberListPB, getClusterEndpointHashKVs, execInPod, ...). They add the
// higher-level invariants the stress suite asserts (full health, leader
// identity, hashkv consistency, a continuous quorum watcher, and a timed
// bring-up) without re-implementing any of the low-level etcdctl plumbing.

// endpointHealthAllHealthy runs `etcdctl endpoint health --cluster` from the
// given pod and returns whether every reported endpoint is healthy, along with
// the healthy/total counts. It never fails the test itself so callers (e.g. the
// quorum watcher) can poll it.
func endpointHealthAllHealthy(t *testing.T, c *envconf.Config, podName string) (healthy, total int, ok bool) {
	t.Helper()
	cmd := []string{"etcdctl", "endpoint", "health", "--cluster"}
	stdout, _, err := execInPod(t, c, podName, namespace, cmd)
	if err != nil {
		// Treat an exec/etcdctl error as "not currently healthy" rather than a
		// hard failure; transient unavailability during churn is expected.
		return 0, 0, false
	}
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		total++
		if strings.Contains(line, "is healthy") {
			healthy++
		}
	}
	return healthy, total, total > 0 && healthy == total
}

// waitForClusterHealthy blocks until the cluster reachable through podName has
// exactly `size` voting members (no learners) AND every endpoint reports
// healthy, or the timeout elapses.
func waitForClusterHealthy(t *testing.T, c *envconf.Config, podName string, size int, timeout time.Duration) {
	t.Helper()
	// First require the steady-state membership (size members, no learners).
	waitForNoLearners(t, c, podName, size, timeout)

	// Then require every endpoint to be healthy.
	err := wait.For(func(ctx context.Context) (bool, error) {
		healthy, total, ok := endpointHealthAllHealthy(t, c, podName)
		if !ok {
			return false, nil
		}
		return total == size && healthy == size, nil
	}, wait.WithTimeout(timeout), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("cluster %s never reached %d healthy endpoints: %v", podName, size, err)
	}
}

// endpointStatusEntry mirrors the shape of `etcdctl endpoint status --cluster -w json`.
type endpointStatusEntry struct {
	Endpoint string `json:"Endpoint"`
	Status   struct {
		Header struct {
			// member_id is the ID of the member that answered this endpoint.
			MemberID uint64 `json:"member_id"`
		} `json:"header"`
		// leader is the member ID the responding member currently believes is leader.
		Leader uint64 `json:"leader"`
	} `json:"Status"`
}

// getEtcdLeader parses `etcdctl endpoint status --cluster -w json` and returns
// the leader's member name and its StatefulSet pod ordinal. The member name for
// etcd pods is the pod name (e.g. "etcd-cluster-2"), so the ordinal is the
// suffix after the final '-'.
func getEtcdLeader(t *testing.T, c *envconf.Config, podName string) (leaderName string, ordinal int) {
	t.Helper()
	cmd := []string{"etcdctl", "endpoint", "status", "--cluster", "-w", "json"}
	stdout, stderr, err := execInPod(t, c, podName, namespace, cmd)
	if err != nil {
		t.Fatalf("Failed to get endpoint status from %s: %v, stderr: %s", podName, err, stderr)
	}

	var entries []endpointStatusEntry
	if err := json.Unmarshal([]byte(stdout), &entries); err != nil {
		t.Fatalf("Failed to parse endpoint status JSON: %v. Raw: %s", err, stdout)
	}
	if len(entries) == 0 {
		t.Fatalf("endpoint status returned no entries from %s", podName)
	}

	// Any entry reports the same leader ID; take the first non-zero one.
	var leaderID uint64
	for _, e := range entries {
		if e.Status.Leader != 0 {
			leaderID = e.Status.Leader
			break
		}
	}
	if leaderID == 0 {
		t.Fatalf("no leader reported by endpoint status from %s (cluster leaderless?)", podName)
	}

	// Map the leader member ID back to a member name via the member list.
	for name, id := range getEtcdMembersName2IDMapping(t, c, podName) {
		if id == leaderID {
			leaderName = name
			break
		}
	}
	if leaderName == "" {
		t.Fatalf("leader ID %d not found in member list from %s", leaderID, podName)
	}

	ordinal = podOrdinal(t, leaderName)
	return leaderName, ordinal
}

// podOrdinal extracts the StatefulSet ordinal from a pod/member name like
// "etcd-cluster-3" -> 3.
func podOrdinal(t *testing.T, name string) int {
	t.Helper()
	idx := strings.LastIndex(name, "-")
	if idx < 0 || idx == len(name)-1 {
		t.Fatalf("cannot derive ordinal from name %q", name)
	}
	var ord int
	if _, err := fmt.Sscanf(name[idx+1:], "%d", &ord); err != nil {
		t.Fatalf("cannot parse ordinal from name %q: %v", name, err)
	}
	return ord
}

// assertHashKVConsistent wraps getClusterEndpointHashKVs and fails the test if
// the members disagree on their key-value hash (a data-divergence signal).
func assertHashKVConsistent(t *testing.T, c *envconf.Config, podName string) {
	t.Helper()
	responses := getClusterEndpointHashKVs(t, c, podName)
	if len(responses) == 0 {
		t.Fatalf("hashkv returned no responses from %s", podName)
	}
	hashes := make(map[uint32]struct{})
	for _, r := range responses {
		hashes[r.Hash] = struct{}{}
	}
	if len(hashes) != 1 {
		t.Errorf("hashkv divergence across %d members reachable from %s: %d distinct hashes",
			len(responses), podName, len(hashes))
	}
}

// quorumWatcher polls endpoint health every ~2s in the background and records
// any window during which the cluster was not fully healthy (a proxy for
// quorum-loss / write-stall). Call the returned stop func when the churn under
// test is complete; it stops the watcher and fails the test if any unhealthy
// window was observed.
//
// The watcher tolerates a single transient unhealthy poll (one pod momentarily
// restarting is normal during scale steps); it only fails on a *sustained*
// unhealthy window (>= unhealthyTolerance consecutive bad polls), which is what
// a real quorum loss looks like.
func quorumWatcher(ctx context.Context, t *testing.T, c *envconf.Config, podName string) (stop func()) {
	t.Helper()

	const pollInterval = 2 * time.Second
	const unhealthyTolerance = 3 // consecutive bad polls (~6s) before we call it a quorum-loss window

	wctx, cancel := context.WithCancel(ctx)
	var (
		wg             sync.WaitGroup
		mu             sync.Mutex
		windows        int // number of distinct sustained unhealthy windows
		worstStreak    int // longest consecutive unhealthy streak observed
		totalUnhealthy int // total unhealthy polls (informational)
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		streak := 0
		inWindow := false
		for {
			select {
			case <-wctx.Done():
				return
			case <-ticker.C:
				_, _, ok := endpointHealthAllHealthy(t, c, podName)
				mu.Lock()
				if ok {
					streak = 0
					inWindow = false
				} else {
					streak++
					totalUnhealthy++
					if streak > worstStreak {
						worstStreak = streak
					}
					if streak >= unhealthyTolerance && !inWindow {
						windows++
						inWindow = true
					}
				}
				mu.Unlock()
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		if windows > 0 {
			t.Errorf("quorumWatcher(%s): observed %d sustained quorum-loss window(s) "+
				"(worst streak %d polls @ %s, %d unhealthy polls total)",
				podName, windows, worstStreak, pollInterval, totalUnhealthy)
		} else {
			t.Logf("quorumWatcher(%s): no quorum-loss window (worst streak %d poll(s), %d unhealthy polls total)",
				podName, worstStreak, totalUnhealthy)
		}
	}
}

// timeToHealthy creates a cluster of the given size and blocks until it is fully
// healthy, logging the elapsed wall-clock time. This is the "efficient spin-up"
// baseline the plan calls for: the log line is the number to watch as the
// blocking-reconcile de-block work lands later.
func timeToHealthy(ctx context.Context, t *testing.T, c *envconf.Config, name string, size int, timeout time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	createEtcdClusterWithPVC(ctx, t, c, name, size)
	waitForSTSReadiness(t, c, name, size)
	podName := fmt.Sprintf("%s-0", name)
	waitForClusterHealthy(t, c, podName, size, timeout)
	elapsed := time.Since(start)
	t.Logf("timeToHealthy: cluster %q size=%d reached full health in %s", name, size, elapsed)
	return elapsed
}
