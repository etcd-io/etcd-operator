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
	// LinkUID is the deleted CR's UID — the fence owner the cleaner requires
	// before deleting anything (a PrefixConflict loser's default key is the
	// WINNER's live fence; a blind delete would destroy it).
	LinkUID string
}

// CheckpointCleaner deletes the reserved checkpoint key from the target etcd
// during finalization — a short-lived client per call, seamed for envtest
// (no reachable target etcd there). The key is deleted only when its stored
// fence is owned by tgt.LinkUID; an absent key is success, and a foreign or
// undecodable fence is left in place with the reason returned in skipReason
// ("" when the key was deleted or already absent).
type CheckpointCleaner interface {
	DeleteCheckpoint(ctx context.Context, tgt CheckpointTarget) (skipReason string, err error)
}

const (
	checkpointCleanerDialTimeout    = 10 * time.Second
	checkpointCleanerRequestTimeout = 30 * time.Second
)

// etcdCheckpointCleaner is the production CheckpointCleaner: one clientv3
// client per call, an ownership-checked exact-key delete, Close.
type etcdCheckpointCleaner struct{}

func (etcdCheckpointCleaner) DeleteCheckpoint(ctx context.Context, tgt CheckpointTarget) (string, error) {
	cfg := mirroragent.NewClientConfig(tgt.Endpoints, tgt.TLS, checkpointCleanerDialTimeout)
	cfg.Username, cfg.Password = tgt.Username, tgt.Password
	cfg.Context = ctx
	cli, err := clientv3.New(cfg)
	if err != nil {
		return "", fmt.Errorf("creating target client: %w", err)
	}
	defer func() { _ = cli.Close() }()
	rctx, cancel := context.WithTimeout(ctx, checkpointCleanerRequestTimeout)
	defer cancel()

	resp, err := cli.Get(rctx, tgt.Key)
	if err != nil {
		return "", fmt.Errorf("reading checkpoint key: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	kv := resp.Kvs[0]
	stored, derr := mirroragent.DecodeFenceValue(kv.Value)
	if derr != nil {
		return "the stored fence is undecodable, so ownership is unprovable; " +
			"leaving the key in place (fail-closed, matching the agent)", nil
	}
	if stored.LinkUID != tgt.LinkUID {
		return fmt.Sprintf("the stored fence is owned by link %q, not this mirror; "+
			"leaving the key in place", stored.LinkUID), nil
	}
	// Mod-revision compare so a fence written between the ownership check and
	// the delete (an agent straggler, a takeover) is never deleted blind.
	txn, err := cli.Txn(rctx).
		If(clientv3.Compare(clientv3.ModRevision(tgt.Key), "=", kv.ModRevision)).
		Then(clientv3.OpDelete(tgt.Key)).
		Commit()
	if err != nil {
		return "", fmt.Errorf("deleting checkpoint key: %w", err)
	}
	if !txn.Succeeded {
		return "", fmt.Errorf("checkpoint key moved between ownership check and delete; retrying")
	}
	return "", nil
}
