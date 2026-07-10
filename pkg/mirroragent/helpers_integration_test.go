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

package mirroragent_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// startEtcd boots one in-process embedded etcd on unique loopback ports and
// returns a client wired the way the engine requires (NewClientConfig).
func startEtcd(t *testing.T, mutate func(*embed.Config)) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cu := freeURL(t)
	pu := freeURL(t)
	cfg.ListenClientUrls = []url.URL{*cu}
	cfg.AdvertiseClientUrls = []url.URL{*cu}
	cfg.ListenPeerUrls = []url.URL{*pu}
	cfg.AdvertisePeerUrls = []url.URL{*pu}
	cfg.InitialCluster = cfg.Name + "=" + pu.String()
	if mutate != nil {
		mutate(cfg)
	}
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		t.Fatal("embedded etcd took too long to start")
	}
	cli, err := clientv3.New(mirroragent.NewClientConfig([]string{cu.String()}, nil, 5*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func freeURL(t *testing.T) *url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	u, err := url.Parse("http://" + addr)
	require.NoError(t, err)
	return u
}

// baseCfg returns an engine config with intervals shrunk for test runtime.
func baseCfg(srcPrefix, dstPrefix string) mirroragent.Config {
	return mirroragent.Config{
		LinkUID:             "test-link",
		Epoch:               1,
		SourcePrefix:        srcPrefix,
		TargetPrefix:        dstPrefix,
		RequestTimeout:      5 * time.Second,
		BackoffInitialDelay: 50 * time.Millisecond,
		BackoffMaxDelay:     500 * time.Millisecond,
		ProgressInterval:    200 * time.Millisecond,
		QuotaProbeInterval:  250 * time.Millisecond,
	}
}

func checkpointKey(cfg mirroragent.Config) string {
	return cfg.TargetPrefix + cfg.DestPrefix + mirroragent.DefaultCheckpointKeySuffix
}

type agentRun struct {
	agent  *mirroragent.Agent
	cancel context.CancelFunc
	done   chan error
}

// startAgent builds and runs an Agent in a goroutine, cleaned up with the
// test.
func startAgent(t *testing.T, cfg mirroragent.Config, src, dst mirroragent.Client) *agentRun {
	t.Helper()
	agent, err := mirroragent.New(cfg, src, dst)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()
	r := &agentRun{agent: agent, cancel: cancel, done: done}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("agent did not stop within 15s of cancel")
		}
	})
	return r
}

// stop cancels the agent and waits for Run to return.
func (r *agentRun) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	return r.waitErr(t, 15*time.Second)
}

// waitErr waits for Run to return and hands back its error.
func (r *agentRun) waitErr(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case err := <-r.done:
		r.done <- err // allow repeated reads / cleanup
		return err
	case <-time.After(timeout):
		t.Fatalf("agent Run did not return within %v", timeout)
		return nil
	}
}

// waitSnap polls Snapshot until cond holds.
func waitSnap(
	t *testing.T, a *mirroragent.Agent, timeout time.Duration,
	what string, cond func(mirroragent.Snapshot) bool,
) mirroragent.Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var s mirroragent.Snapshot
	for time.Now().Before(deadline) {
		s = a.Snapshot()
		if cond(s) {
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waiting for %s: condition not met within %v; last snapshot: %+v", what, timeout, s)
	return s
}

// putN writes n sequential keys under prefix and returns the resulting
// source data as the expected target map under dstPrefix.
func putN(t *testing.T, cli *clientv3.Client, srcPrefix, dstPrefix string, n int) map[string]string {
	t.Helper()
	want := make(map[string]string, n)
	for i := range n {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		_, err := cli.Put(t.Context(), srcPrefix+k, v)
		require.NoError(t, err)
		want[dstPrefix+k] = v
	}
	return want
}

// targetData reads every key under the destination prefix except the
// reserved checkpoint key.
func targetData(t *testing.T, cli *clientv3.Client, cfg mirroragent.Config) map[string]string {
	t.Helper()
	resp, err := cli.Get(t.Context(), cfg.TargetPrefix+cfg.DestPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	out := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if string(kv.Key) == checkpointKey(cfg) {
			continue
		}
		out[string(kv.Key)] = string(kv.Value)
	}
	return out
}

// waitTargetData polls until the destination data (reserved key excluded)
// equals want exactly.
func waitTargetData(
	t *testing.T, cli *clientv3.Client, cfg mirroragent.Config,
	timeout time.Duration, want map[string]string,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got map[string]string
	for time.Now().Before(deadline) {
		got = targetData(t, cli, cfg)
		if mapsEqual(got, want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("target data never converged: got %d keys, want %d keys\ngot:  %v\nwant: %v",
		len(got), len(want), summarize(got), summarize(want))
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func summarize(m map[string]string) string {
	if len(m) <= 12 {
		return fmt.Sprintf("%v", m)
	}
	keys := make([]string, 0, 12)
	for k := range m {
		keys = append(keys, k)
		if len(keys) == 12 {
			break
		}
	}
	return fmt.Sprintf("{%s, ...}", strings.Join(keys, ", "))
}

// readFence reads and decodes the reserved checkpoint key.
func readFence(
	t *testing.T, cli *clientv3.Client, cfg mirroragent.Config,
) (mirroragent.FenceValue, int64) {
	t.Helper()
	resp, err := cli.Get(t.Context(), checkpointKey(cfg))
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1, "reserved checkpoint key missing")
	f, err := mirroragent.DecodeFenceValue(resp.Kvs[0].Value)
	require.NoError(t, err)
	return f, resp.Kvs[0].ModRevision
}

// sourceRevision returns the source cluster's current revision.
func sourceRevision(t *testing.T, cli *clientv3.Client, prefix string) int64 {
	t.Helper()
	resp, err := cli.Get(t.Context(), prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	return resp.Header.Revision
}

// countingClient wraps a Client and counts Get/Txn calls, for asserting that
// resume paths do not rescan and quota parking does not hot-loop.
type countingClient struct {
	mirroragent.Client
	gets atomic.Int64
	txns atomic.Int64
}

func (c *countingClient) Get(
	ctx context.Context, key string, opts ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
	c.gets.Add(1)
	return c.Client.Get(ctx, key, opts...)
}

func (c *countingClient) Txn(ctx context.Context) clientv3.Txn {
	c.txns.Add(1)
	return c.Client.Txn(ctx)
}

// watchRevRecordingClient records the OpOption-resolved start revision of
// every Watch call, for pinning resume revisions.
type watchRevRecordingClient struct {
	mirroragent.Client
	mu   sync.Mutex
	revs []int64
}

func (c *watchRevRecordingClient) Watch(
	ctx context.Context, key string, opts ...clientv3.OpOption,
) clientv3.WatchChan {
	op := clientv3.OpGet(key, opts...)
	c.mu.Lock()
	c.revs = append(c.revs, op.Rev())
	c.mu.Unlock()
	return c.Client.Watch(ctx, key, opts...)
}

func (c *watchRevRecordingClient) recorded() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.revs...)
}
