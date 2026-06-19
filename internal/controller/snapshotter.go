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
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Snapshotter abstracts "produce an etcd snapshot stream from a running
// member". It exists as an interface so the backup controller's
// snapshot+upload orchestration can be unit-tested with a fake that returns
// canned bytes, with no live etcd required.
type Snapshotter interface {
	// Snapshot writes a consistent point-in-time etcd snapshot of the cluster
	// reachable via pod to w. It returns the number of bytes written.
	//
	// Implementations run the equivalent of `etcdctl snapshot save` against a
	// healthy member and stream the resulting file to w.
	Snapshot(ctx context.Context, pod types.NamespacedName, w io.Writer) (int64, error)
}

// clientSnapshotter implements Snapshotter using the etcd v3 Maintenance
// Snapshot API directly from the operator process, with no in-pod exec.
//
// This is deliberately NOT exec-based: the etcd images the operator deploys
// (e.g. gcr.io/etcd-development/etcd:v3.6.x) are distroless and ship no shell
// or coreutils, so `kubectl exec -- sh -c "etcdctl snapshot save ... | cat"`
// fails with `exec: "sh": executable file not found in $PATH`. The Maintenance
// API streams the snapshot over the client port the operator can already reach
// in-cluster, so it works regardless of what binaries the member image carries.
type clientSnapshotter struct {
	// endpointForPod resolves the etcd client URL for the target member pod.
	// Parameterized so tests can point it at a local listener and so TLS/port
	// specifics can evolve without touching the streaming logic.
	endpointForPod func(pod types.NamespacedName) string
	// dialTimeout bounds establishing the client connection.
	dialTimeout time.Duration
}

// newClientSnapshotter constructs the default, shell-free Snapshotter backed by
// the etcd v3 Maintenance Snapshot API.
func newClientSnapshotter() *clientSnapshotter {
	return &clientSnapshotter{
		endpointForPod: clientURLForMemberPod,
		dialTimeout:    10 * time.Second,
	}
}

// clientURLForMemberPod returns the in-cluster client URL of a member pod.
// StatefulSet pods are named <cluster>-<ordinal> and addressable through the
// cluster's headless Service (named after the cluster) at
// <pod>.<cluster>.<namespace>.svc.cluster.local:2379, which is exactly the
// --advertise-client-urls the member is started with (see utils.go).
func clientURLForMemberPod(pod types.NamespacedName) string {
	cluster := clusterNameFromPod(pod.Name)
	return fmt.Sprintf("http://%s.%s.%s.svc.cluster.local:2379",
		pod.Name, cluster, pod.Namespace)
}

// clusterNameFromPod strips the trailing -<ordinal> from a StatefulSet pod name
// to recover the cluster (and headless Service) name.
func clusterNameFromPod(podName string) string {
	for i := len(podName) - 1; i >= 0; i-- {
		if podName[i] == '-' {
			return podName[:i]
		}
		if podName[i] < '0' || podName[i] > '9' {
			break
		}
	}
	return podName
}

func (s *clientSnapshotter) Snapshot(ctx context.Context, pod types.NamespacedName, w io.Writer) (int64, error) {
	lg, err := logutil.CreateDefaultZapLogger(zap.WarnLevel)
	if err != nil {
		lg = zap.NewNop()
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.endpointForPod(pod)},
		DialTimeout: s.dialTimeout,
		Context:     ctx,
		Logger:      lg,
	})
	if err != nil {
		return 0, fmt.Errorf("snapshotter: new etcd client for %s: %w", pod, err)
	}
	defer func() { _ = cli.Close() }()

	rc, err := cli.Snapshot(ctx)
	if err != nil {
		return 0, fmt.Errorf("snapshotter: open snapshot stream for %s: %w", pod, err)
	}
	defer func() { _ = rc.Close() }()

	cw := &countingWriter{w: w}
	if _, err := io.Copy(cw, rc); err != nil {
		return cw.n, fmt.Errorf("snapshotter: stream snapshot from %s: %w", pod, err)
	}
	if cw.n == 0 {
		return 0, fmt.Errorf("snapshotter: empty snapshot produced for %s", pod)
	}
	return cw.n, nil
}

// countingWriter counts bytes passing through to the wrapped writer.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
