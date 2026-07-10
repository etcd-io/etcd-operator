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
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// statusStubClient stubs only the maintenance Status probe (and Endpoints);
// the embedded Client is never touched by the probe path.
type statusStubClient struct {
	Client
	learner bool
}

func (c *statusStubClient) Endpoints() []string { return []string{"stub:2379"} }

func (c *statusStubClient) Status(_ context.Context, _ string) (*clientv3.StatusResponse, error) {
	return &clientv3.StatusResponse{
		Header:    &etcdserverpb.ResponseHeader{ClusterId: 42},
		Version:   "3.5.9",
		IsLearner: c.learner,
	}, nil
}

// TestProbeRecordsLearner: the connect-time Status probe must record
// IsLearner into the snapshot for its side — the LearnerEndpoint condition's
// only input.
func TestProbeRecordsLearner(t *testing.T) {
	a := newTestAgent(t)

	_, id, err := a.probe(t.Context(), "source", &statusStubClient{learner: true})
	require.NoError(t, err)
	require.EqualValues(t, 42, id)
	snap := a.Snapshot()
	require.True(t, snap.SourceLearner)
	require.False(t, snap.TargetLearner, "the target side must be untouched by a source probe")

	_, _, err = a.probe(t.Context(), "target", &statusStubClient{learner: false})
	require.NoError(t, err)
	snap = a.Snapshot()
	require.True(t, snap.SourceLearner)
	require.False(t, snap.TargetLearner)

	_, _, err = a.probe(t.Context(), "target", &statusStubClient{learner: true})
	require.NoError(t, err)
	require.True(t, a.Snapshot().TargetLearner)
}
