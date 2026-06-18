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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Snapshotter abstracts "produce an etcd snapshot stream from a running
// member". It exists as an interface so the backup controller's
// snapshot+upload orchestration can be unit-tested with a fake that returns
// canned bytes, with no Kubernetes exec or live etcd required.
type Snapshotter interface {
	// Snapshot writes a consistent point-in-time etcd snapshot of the cluster
	// reachable via pod to w. It returns the number of bytes written.
	//
	// Implementations run the equivalent of `etcdctl snapshot save` against a
	// healthy member and stream the resulting file to w.
	Snapshot(ctx context.Context, pod types.NamespacedName, w io.Writer) (int64, error)
}

// execSnapshotter implements Snapshotter by streaming
// `etcdctl snapshot save /dev/stdout` from a member pod via the Kubernetes
// exec subresource. The snapshot file never lands on the operator's disk; it
// is piped straight into the object-store uploader.
type execSnapshotter struct {
	cfg           *rest.Config
	containerName string
	// command builds the in-pod command. Parameterized so TLS-enabled clusters
	// can inject cert flags without changing the streaming logic.
	command func() []string
}

// newExecSnapshotter constructs an execSnapshotter. The command writes the
// snapshot to stdout so it can be streamed; etcdctl 3.4+ supports
// `snapshot save` to an arbitrary path including /dev/stdout.
func newExecSnapshotter(cfg *rest.Config, containerName string) *execSnapshotter {
	return &execSnapshotter{
		cfg:           cfg,
		containerName: containerName,
		command: func() []string {
			// ETCDCTL_API=3 is the default on etcd 3.4+, but set it explicitly
			// for older images. Endpoints default to the local member.
			return []string{
				"sh", "-c",
				"ETCDCTL_API=3 etcdctl snapshot save /tmp/snapshot.db >/dev/null 2>&1 && cat /tmp/snapshot.db && rm -f /tmp/snapshot.db",
			}
		},
	}
}

func (e *execSnapshotter) Snapshot(ctx context.Context, pod types.NamespacedName, w io.Writer) (int64, error) {
	clientset, err := kubernetes.NewForConfig(e.cfg)
	if err != nil {
		return 0, fmt.Errorf("snapshotter: build clientset: %w", err)
	}

	req := clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: e.containerName,
			Command:   e.command(),
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.cfg, "POST", req.URL())
	if err != nil {
		return 0, fmt.Errorf("snapshotter: new executor: %w", err)
	}

	cw := &countingWriter{w: w}
	var stderr writerBuffer
	streamErr := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: cw,
		Stderr: &stderr,
	})
	if streamErr != nil {
		return cw.n, fmt.Errorf("snapshotter: exec stream failed: %w (stderr: %s)", streamErr, stderr.String())
	}
	if cw.n == 0 {
		return 0, fmt.Errorf("snapshotter: empty snapshot produced (stderr: %s)", stderr.String())
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

// writerBuffer is a tiny bounded buffer used to capture exec stderr for error
// messages without pulling in bytes.Buffer's full surface; it caps growth so a
// chatty member cannot balloon operator memory.
type writerBuffer struct {
	b []byte
}

const maxStderrCapture = 4 << 10 // 4 KiB

func (w *writerBuffer) Write(p []byte) (int, error) {
	if len(w.b) < maxStderrCapture {
		room := maxStderrCapture - len(w.b)
		if room > len(p) {
			room = len(p)
		}
		w.b = append(w.b, p[:room]...)
	}
	return len(p), nil
}

func (w *writerBuffer) String() string { return string(w.b) }
