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

// RestoreTarget identifies the member pod a snapshot is restored into and the
// etcd member identity the restored data directory must be initialised with.
// It is the restore-side analogue of the (pod) argument to Snapshotter, but
// carries the extra cluster-membership knobs `etcdctl snapshot restore` needs
// to lay down a valid genesis data directory.
type RestoreParams struct {
	// Pod is the target member pod (typically <cluster>-0) the snapshot is
	// streamed into and restored on.
	Pod types.NamespacedName

	// MemberName is the etcd member name the restored data dir is stamped with
	// (etcdctl --name). It must match the name the bootstrapped member will use
	// to introduce itself, or the new cluster will reject it.
	MemberName string

	// PeerURL is the advertised peer URL for the restored member
	// (etcdctl --initial-advertise-peer-urls / --initial-cluster). It must
	// match the peer URL the member advertises on startup.
	PeerURL string

	// DataDir is the absolute path of the etcd data directory inside the pod
	// the restored snapshot is written to (etcdctl --data-dir). This is the
	// directory the bootstrapping etcd then starts from.
	DataDir string
}

// Restorer abstracts "write an etcd snapshot stream into a fresh member's data
// directory and bootstrap a new single-member cluster from it". It mirrors
// Snapshotter: a seam so the restore controller's download+restore
// orchestration is unit-testable with a fake that records what it was handed,
// with no Kubernetes exec or live etcd required.
type Restorer interface {
	// Restore consumes the snapshot bytes from r and runs the equivalent of
	// `etcdctl snapshot restore` against the target member described by params,
	// producing a data directory the member can boot a new cluster from. It
	// returns the number of snapshot bytes consumed.
	Restore(ctx context.Context, params RestoreParams, r io.Reader) (int64, error)
}

const (
	// defaultRestoreDataDir is where the restored data directory is written in
	// the member pod when the caller does not override it. It matches the
	// conventional etcd data-dir mount.
	defaultRestoreDataDir = "/var/run/etcd/default.etcd"
	// snapshotStagePath is the in-pod path the streamed snapshot bytes are
	// staged to before `etcdctl snapshot restore` reads them. It lives in the
	// member pod's own scratch space, not on the operator's disk.
	snapshotStagePath = "/tmp/restore-snapshot.db"
)

// execRestorer implements Restorer by streaming the snapshot bytes into a
// member pod over the exec subresource's stdin, staging them to a temp file,
// and running `etcdctl snapshot restore` to produce a fresh data directory.
//
// The restore writes a genesis data dir for a single-member cluster (the
// snapshot's data plus a new member identity). The member then boots from it;
// the EtcdCluster controller grows the cluster to the requested size from that
// seed member. `--force-new-cluster` semantics are achieved by restoring into a
// clean data dir with a fresh cluster token, which is exactly what
// `etcdctl snapshot restore` does — it refuses to overwrite an existing dir, so
// a non-empty target is rejected by etcdctl itself, a second line of defence
// behind the controller's empty-target check.
type execRestorer struct {
	cfg           *rest.Config
	containerName string
	// command builds the in-pod command from the restore params. Parameterized
	// so TLS/peer-URL specifics can be adjusted without touching streaming.
	command func(params RestoreParams) []string
}

// newExecRestorer constructs an execRestorer. The command reads the staged
// snapshot file and runs `etcdctl snapshot restore` with the member identity
// flags. etcdctl's own diagnostics go to stderr (captured for error messages);
// nothing is written to stdout, so an empty stdout is the success signal.
func newExecRestorer(cfg *rest.Config, containerName string) *execRestorer {
	return &execRestorer{
		cfg:           cfg,
		containerName: containerName,
		command: func(p RestoreParams) []string {
			dataDir := p.DataDir
			if dataDir == "" {
				dataDir = defaultRestoreDataDir
			}
			// Stage stdin to a file, then restore. `cat > file` consumes the
			// streamed snapshot; the restore reads it back. The data dir is
			// removed first only if empty (rmdir fails on a non-empty dir, so a
			// populated target aborts the restore rather than clobbering data).
			// etcdctl's output is sent to stderr (1>&2) so success leaves stdout
			// empty.
			script := fmt.Sprintf(
				"set -e; cat > %s; "+
					"ETCDCTL_API=3 etcdctl snapshot restore %s "+
					"--name %s "+
					"--initial-advertise-peer-urls %s "+
					"--initial-cluster %s=%s "+
					"--data-dir %s 1>&2; "+
					"rm -f %s",
				snapshotStagePath,
				snapshotStagePath,
				p.MemberName,
				p.PeerURL,
				p.MemberName, p.PeerURL,
				dataDir,
				snapshotStagePath,
			)
			return []string{"sh", "-c", script}
		},
	}
}

func (e *execRestorer) Restore(ctx context.Context, params RestoreParams, r io.Reader) (int64, error) {
	clientset, err := kubernetes.NewForConfig(e.cfg)
	if err != nil {
		return 0, fmt.Errorf("restorer: build clientset: %w", err)
	}

	req := clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(params.Pod.Name).
		Namespace(params.Pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: e.containerName,
			Command:   e.command(params),
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.cfg, "POST", req.URL())
	if err != nil {
		return 0, fmt.Errorf("restorer: new executor: %w", err)
	}

	cr := &countingReader{r: r}
	var stderr writerBuffer
	streamErr := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  cr,
		Stderr: &stderr,
	})
	if streamErr != nil {
		return cr.n, fmt.Errorf("restorer: exec stream failed: %w (stderr: %s)", streamErr, stderr.String())
	}
	if cr.n == 0 {
		return 0, fmt.Errorf("restorer: empty snapshot stream (stderr: %s)", stderr.String())
	}
	return cr.n, nil
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

// countingReader counts bytes read from the wrapped reader. It is the read-side
// analogue of countingWriter (snapshotter.go).
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
