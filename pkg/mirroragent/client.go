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

package mirroragent

import (
	"context"
	"crypto/tls"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Client is the subset of *clientv3.Client the engine uses on each side.
// The source side needs KV (Get) + Watcher + Status; the target side needs
// KV (Get/Txn) + Status. *clientv3.Client satisfies it directly.
type Client interface {
	clientv3.KV
	clientv3.Watcher
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
	Endpoints() []string
}

var _ Client = (*clientv3.Client)(nil)

// Keepalive settings the engine's liveness machinery assumes: without
// client-driven keepalives, NLB (350s) and Cloud NAT (1200s) idle timeouts
// silently kill quiet watches on the cross-cloud path this engine exists
// for.
const (
	DialKeepAliveTime    = 25 * time.Second
	DialKeepAliveTimeout = 10 * time.Second
)

// NewClientConfig returns a clientv3.Config wired the way the engine
// requires: keepalives on (including without active streams) and a bounded
// dial. Callers own TLS/auth material — this library never reads Secrets.
// Per-unary request deadlines are applied inside the engine from
// Config.RequestTimeout, not here.
func NewClientConfig(endpoints []string, tlsConfig *tls.Config, dialTimeout time.Duration) clientv3.Config {
	return clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    DialKeepAliveTime,
		DialKeepAliveTimeout: DialKeepAliveTimeout,
		PermitWithoutStream:  true,
		TLS:                  tlsConfig,
	}
}
