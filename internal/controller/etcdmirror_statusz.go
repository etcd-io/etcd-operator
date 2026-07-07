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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// statuszTimeout bounds one /statusz poll end to end.
const statuszTimeout = 5 * time.Second

// AgentStatusClient fetches one agent pod's /statusz snapshot. It is an
// injectable seam because envtest has no kubelet: agent pods never run
// there, so tests substitute a fake returning fixture Snapshots.
type AgentStatusClient interface {
	Snapshot(ctx context.Context, addr string) (*mirroragent.Snapshot, error)
}

// httpAgentStatusClient is the production impl: GET http://<addr>/statusz
// with a 5s overall timeout, json.Decode into mirroragent.Snapshot (the
// tagged type IS the wire contract — the agent marshals the same Go type).
type httpAgentStatusClient struct{ client *http.Client }

func newHTTPAgentStatusClient() *httpAgentStatusClient {
	return &httpAgentStatusClient{client: &http.Client{Timeout: statuszTimeout}}
}

func (c *httpAgentStatusClient) Snapshot(ctx context.Context, addr string) (*mirroragent.Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/statusz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("statusz returned %s", resp.Status)
	}
	var snap mirroragent.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return nil, &snapshotDecodeError{err: err}
	}
	return &snap, nil
}

// snapshotDecodeError distinguishes "reached the agent but its /statusz body
// did not decode" (condition reason SnapshotDecodeFailed) from a transport
// failure (AgentStatusUnreachable).
type snapshotDecodeError struct{ err error }

func (e *snapshotDecodeError) Error() string { return "decoding /statusz snapshot: " + e.err.Error() }
func (e *snapshotDecodeError) Unwrap() error { return e.err }

// CheckpointTarget is everything the finalizer needs to reach the target
// etcd and delete the reserved checkpoint key. Credential values flow only
// through this struct into the short-lived client — never into logs, status,
// or events.
type CheckpointTarget struct {
	Endpoints []string
	// TLS is nil for a cleartext target.
	TLS      *tls.Config
	Username string
	Password string
	// Key is the reserved checkpoint key (exactly one key, never a prefix).
	Key string
}

// CheckpointCleaner deletes the reserved checkpoint key from the target etcd
// during finalization — a short-lived client per call, seamed for envtest
// (no reachable target etcd there).
type CheckpointCleaner interface {
	DeleteCheckpoint(ctx context.Context, tgt CheckpointTarget) error
}

const (
	checkpointCleanerDialTimeout    = 10 * time.Second
	checkpointCleanerRequestTimeout = 30 * time.Second
)

// etcdCheckpointCleaner is the production CheckpointCleaner: one clientv3
// client per call, one exact-key Delete (0 deleted keys — already gone — is
// success), Close.
type etcdCheckpointCleaner struct{}

func (etcdCheckpointCleaner) DeleteCheckpoint(ctx context.Context, tgt CheckpointTarget) error {
	cfg := mirroragent.NewClientConfig(tgt.Endpoints, tgt.TLS, checkpointCleanerDialTimeout)
	cfg.Username, cfg.Password = tgt.Username, tgt.Password
	cfg.Context = ctx
	cli, err := clientv3.New(cfg)
	if err != nil {
		return fmt.Errorf("creating target client: %w", err)
	}
	defer func() { _ = cli.Close() }()
	rctx, cancel := context.WithTimeout(ctx, checkpointCleanerRequestTimeout)
	defer cancel()
	if _, err := cli.Delete(rctx, tgt.Key); err != nil {
		return fmt.Errorf("deleting checkpoint key: %w", err)
	}
	return nil
}
