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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// startTestEtcd boots an embedded etcd on loopback and returns its client
// URL (http://...).
func startTestEtcd(t *testing.T) string {
	t.Helper()
	loopbackURL := func() *url.URL {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { require.NoError(t, l.Close()) }()
		u, err := url.Parse("http://" + l.Addr().String())
		require.NoError(t, err)
		return u
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger, cfg.LogLevel = "zap", "error"
	client, peer := loopbackURL(), loopbackURL()
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*client}, []url.URL{*client}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*peer}, []url.URL{*peer}
	cfg.InitialCluster = cfg.Name + "=" + peer.String()
	etcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(etcd.Close)
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(time.Minute):
		t.Fatal("embedded etcd took too long to start")
	}
	return client.String()
}

// TestLiveAgentEndpoints exercises the real buildConfig → clients → New →
// Run → HTTP path end to end against two embedded etcds, cleartext.
func TestLiveAgentEndpoints(t *testing.T) {
	crlog.SetLogger(logr.Discard())
	srcURL := startTestEtcd(t)
	dstURL := startTestEtcd(t)

	src, err := clientv3.New(mirroragent.NewClientConfig([]string{srcURL}, nil, 5*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	for i := range 5 {
		_, err = src.Put(t.Context(), fmt.Sprintf("/live/key-%d", i), fmt.Sprintf("val-%d", i))
		require.NoError(t, err)
	}

	f := parseArgs(t,
		"--link-uid=live-test", "--epoch=1",
		"--source-endpoints="+srcURL, "--target-endpoints="+dstURL,
		"--source-prefix=/live/", "--target-prefix=/mirror/",
		"--request-timeout=5s", "--dial-timeout=5s",
		"--http-bind-address=127.0.0.1:0",
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	addrCh := make(chan net.Addr, 1)
	runDone := make(chan error, 1)
	go func() { runDone <- run(ctx, f, func(a net.Addr) { addrCh <- a }) }()

	var base string
	select {
	case addr := <-addrCh:
		base = "http://" + addr.String()
	case <-time.After(15 * time.Second):
		t.Fatal("run never bound the HTTP listener")
	}

	// Poll /statusz until the engine reaches steady state with applies.
	// assert (not require) so the failure message can read snap AFTER the
	// polling — require.Eventually's msgAndArgs are copied before it.
	var snap mirroragent.Snapshot
	if !assert.Eventually(t, func() bool {
		snap = getStatusz(t, base)
		return snap.Phase == mirroragent.PhaseSyncing && snap.Watermark > 0 && snap.KeysAppliedTotal > 0
	}, 30*time.Second, 100*time.Millisecond) {
		t.Fatalf("engine never reached steady state; last snapshot: %+v", snap)
	}
	require.NotZero(t, snap.SourceClusterID)
	require.NotZero(t, snap.TargetClusterID)
	require.False(t, snap.LastProgressTime.IsZero())

	resp, err := http.Get(base + "/readyz")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(base + "/metrics")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	metrics := string(body)
	require.Contains(t, metrics, "etcd_mirror_agent_watermark_revision")
	require.Contains(t, metrics, "etcd_mirror_agent_keys_applied_total")
	require.NotContains(t, metrics, "etcd_mirror_agent_keys_applied_total 0\n",
		"keys_applied_total must be nonzero after the scan")

	// SIGTERM-equivalent: cancel the run context; the engine stops and the
	// server drains, and the whole run resolves to the exit-0 path (nil).
	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func getStatusz(t *testing.T, base string) mirroragent.Snapshot {
	t.Helper()
	resp, err := http.Get(base + "/statusz")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json"))
	var snap mirroragent.Snapshot
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&snap))
	return snap
}
