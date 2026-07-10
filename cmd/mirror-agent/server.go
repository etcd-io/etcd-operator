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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

// snapshotFn supplies the engine snapshot the HTTP surface serves
// (Agent.Snapshot in production, stubs in tests).
type snapshotFn func() mirroragent.Snapshot

// readHeaderTimeout bounds header reads on the local listener. Plaintext
// net/http never negotiates HTTP/2, consistent with the operator's h2-off
// stance.
const readHeaderTimeout = 10 * time.Second

// newMux serves the agent's observability surface:
//
//   - /statusz: JSON dump of the engine Snapshot. The tagged
//     mirroragent.Snapshot IS the wire shape — the controller decodes into
//     the same Go type, so the surface cannot drift from the engine.
//   - /healthz: pure process liveness, always 200. A Degraded/backing-off
//     agent must not be killed by the kubelet — backoff is normal operation.
//   - /readyz: 200 once the engine has connected and owns (or completed)
//     the replication loop; see readyPhase for the exact semantics.
//   - /metrics: Prometheus exposition over the agent's standalone registry.
func newMux(snapshot snapshotFn, reg *prometheus.Registry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/statusz", func(w http.ResponseWriter, _ *http.Request) {
		body, err := json.Marshal(snapshot())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if p := snapshot().Phase; !readyPhase(p) {
			http.Error(w, fmt.Sprintf("not ready: phase %q", p), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// readyPhase defines /readyz: ready = "the engine has connected and owns
// (or completed) the replication loop". Degraded IS ready — transient
// backoff must not flap the Deployment's availability. Drained IS ready —
// terminal success. Connecting ("" before Run starts) is not, gating
// rollout until both sides are reachable; Failed is not — permanent.
func readyPhase(p mirroragent.Phase) bool {
	switch p {
	case "", mirroragent.PhaseConnecting, mirroragent.PhaseFailed:
		return false
	default:
		return true
	}
}
