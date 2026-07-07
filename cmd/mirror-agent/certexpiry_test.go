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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// writeCertFile writes one self-signed certificate per notAfter to path, as
// a concatenated PEM bundle.
func writeCertFile(t *testing.T, path string, notAfters ...time.Time) {
	t.Helper()
	pemData := make([]byte, 0, 2048)
	for i, notAfter := range notAfters {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 1)),
			Subject:      pkix.Name{CommonName: "mirror-agent-test"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     notAfter,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		require.NoError(t, err)
		pemData = append(pemData, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	require.NoError(t, os.WriteFile(path, pemData, 0o600))
}

func TestCertExpiryGauge(t *testing.T) {
	dir := t.TempDir()
	leafPath := filepath.Join(dir, "tls.crt")
	bundlePath := filepath.Join(dir, "ca-bundle.crt")
	near := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	far := time.Now().Add(90 * 24 * time.Hour).Truncate(time.Second)
	writeCertFile(t, leafPath, far)
	writeCertFile(t, bundlePath, far, near) // overlapping bundle: two CAs

	tracker := newCertExpiryTracker(certExpiryFiles(
		&sideFlags{side: sideSource, tls: true, certFile: leafPath, keyFile: "unused",
			caFile: filepath.Join(dir, "ignored-ca.crt"), caBundleFile: bundlePath},
	))
	tracker.refresh()

	// kind="client" reads the leaf; kind="ca" reads the bundle (precedence
	// over ca-file) and reports the EARLIEST NotAfter of its certificates.
	require.Equal(t, float64(far.Unix()),
		testutil.ToFloat64(tracker.gauge.WithLabelValues(sideSource, certKindClient)))
	require.Equal(t, float64(near.Unix()),
		testutil.ToFloat64(tracker.gauge.WithLabelValues(sideSource, certKindCA)))

	// Rotation: rewrite the leaf, refresh, gauge moves.
	rotated := time.Now().Add(48 * time.Hour).Truncate(time.Second)
	writeCertFile(t, leafPath, rotated)
	tracker.refresh()
	require.Equal(t, float64(rotated.Unix()),
		testutil.ToFloat64(tracker.gauge.WithLabelValues(sideSource, certKindClient)))

	// A vanished file deletes its series — a stale expiry must not reassure.
	require.NoError(t, os.Remove(leafPath))
	tracker.refresh()
	require.Equal(t, 1, testutil.CollectAndCount(tracker.gauge),
		"only the ca series should remain")
}

func TestCertExpiryFilesOnlyConfigured(t *testing.T) {
	files := certExpiryFiles(
		&sideFlags{side: sideSource, tls: true, caFile: "/mnt/ca.crt"},
		&sideFlags{side: sideTarget},
	)
	require.Equal(t, []certFile{{side: sideSource, kind: certKindCA, path: "/mnt/ca.crt"}}, files)

	// A tls=false side never feeds the gauge, even with file flags set
	// (buildTLSInfo rejects that combination before the tracker exists).
	require.Empty(t, certExpiryFiles(
		&sideFlags{side: sideSource, certFile: "/mnt/tls.crt", caFile: "/mnt/ca.crt"}))
}

func TestEarliestNotAfterNoCertificate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.crt")
	require.NoError(t, os.WriteFile(path, []byte("not pem"), 0o600))
	_, err := earliestNotAfter(path)
	require.ErrorContains(t, err, "no CERTIFICATE PEM block")
}
