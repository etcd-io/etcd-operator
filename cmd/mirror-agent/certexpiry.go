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
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// certExpiryRefreshInterval is how often the expiry gauges re-read the cert
// files (mounted Secrets update in place on rotation).
const certExpiryRefreshInterval = 5 * time.Minute

const (
	certKindClient = "client"
	certKindCA     = "ca"
)

// certFile is one PEM file feeding the expiry gauge.
type certFile struct {
	side, kind, path string
}

// certExpiryFiles lists the cert files the flags actually configured:
// kind="client" is the leaf certificate, kind="ca" the effective trust file
// (the bundle wins over the ca file — the same precedence the dial path
// uses). Only configured files produce series.
func certExpiryFiles(sides ...*sideFlags) []certFile {
	var out []certFile
	for _, s := range sides {
		if s.certFile != "" {
			out = append(out, certFile{side: s.side, kind: certKindClient, path: s.certFile})
		}
		trust := s.caFile
		if s.caBundleFile != "" {
			trust = s.caBundleFile
		}
		if trust != "" {
			out = append(out, certFile{side: s.side, kind: certKindCA, path: trust})
		}
	}
	return out
}

// certExpiryTracker maintains tls_cert_expiry_timestamp_seconds{side,kind}:
// the EARLIEST NotAfter among each file's certificates (bundle-safe). On a
// read/parse error the series is deleted — a stale expiry gauge must never
// reassure. The expiry lead-window Warning event is the controller's job;
// only the gauge lives in the agent.
type certExpiryTracker struct {
	gauge *prometheus.GaugeVec
	files []certFile
}

func newCertExpiryTracker(files []certFile) *certExpiryTracker {
	return &certExpiryTracker{
		gauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricPrefix + "tls_cert_expiry_timestamp_seconds",
			Help: "Earliest NotAfter (unix seconds) among the certificates in the configured " +
				"file. Absent while the file is missing or unparseable.",
		}, []string{"side", "kind"}),
		files: files,
	}
}

// refresh re-reads every configured file and updates the gauges.
func (t *certExpiryTracker) refresh() {
	for _, f := range t.files {
		expiry, err := earliestNotAfter(f.path)
		if err != nil {
			setupLog.Error(err, "reading certificate for the expiry gauge",
				"side", f.side, "kind", f.kind, "path", f.path)
			t.gauge.DeleteLabelValues(f.side, f.kind)
			continue
		}
		t.gauge.WithLabelValues(f.side, f.kind).Set(float64(expiry.Unix()))
	}
}

// loop refreshes on a fixed ticker until ctx is cancelled.
func (t *certExpiryTracker) loop(ctx context.Context) {
	tick := time.NewTicker(certExpiryRefreshInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			t.refresh()
		}
	}
}

// earliestNotAfter parses every CERTIFICATE PEM block in the file and
// returns the earliest expiry.
func earliestNotAfter(path string) (time.Time, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	var earliest time.Time
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, cerr := x509.ParseCertificate(block.Bytes)
		if cerr != nil {
			return time.Time{}, fmt.Errorf("parsing certificate in %s: %w", path, cerr)
		}
		if earliest.IsZero() || cert.NotAfter.Before(earliest) {
			earliest = cert.NotAfter
		}
	}
	if earliest.IsZero() {
		return time.Time{}, fmt.Errorf("no CERTIFICATE PEM block in %s", path)
	}
	return earliest, nil
}
