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
	"fmt"

	"go.etcd.io/etcd/client/pkg/v3/transport"
)

// buildTLSInfo translates one side's TLS flags into a transport.TLSInfo;
// enabled is false when --<side>-tls is off (dial cleartext, nil tls.Config).
//
// ROTATION: TLSInfo.ClientConfig installs a per-handshake
// GetClientCertificate that re-reads the leaf pair from the mounted paths,
// so client certificate rotation needs no restart — the contract pinned on
// EtcdMirrorTLS. The CA pool from TrustedCAFile is loaded once at client
// construction; CA rotation relies on the standard overlapping-bundle
// practice, which is what the separate --<side>-ca-bundle-file exists for.
//
// A fully empty TLSInfo (tls on, no file flags) yields server-auth TLS
// verified against the system trust roots, matching a tls block with a nil
// secretRef in the CRD.
func buildTLSInfo(side *sideFlags) (transport.TLSInfo, bool, error) {
	if !side.tls {
		return transport.TLSInfo{}, false, nil
	}
	if (side.certFile == "") != (side.keyFile == "") {
		return transport.TLSInfo{}, false, fmt.Errorf(
			"--%s-cert-file and --%s-key-file must be set together", side.side, side.side)
	}
	trust := side.caFile
	if side.caBundleFile != "" {
		if side.caFile != "" {
			setupLog.Info("CA bundle takes precedence over the CA file",
				"side", side.side, "caBundleFile", side.caBundleFile, "caFile", side.caFile)
		}
		trust = side.caBundleFile
	}
	return transport.TLSInfo{
		CertFile:           side.certFile,
		KeyFile:            side.keyFile,
		TrustedCAFile:      trust,
		ServerName:         side.serverName,
		InsecureSkipVerify: side.insecureSkipVerify,
	}, true, nil
}
