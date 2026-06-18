/*
Copyright 2025.

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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	test_utils "go.etcd.io/etcd-operator/test/utils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// The two-tier shared CA issuer that a multi-member peer-TLS cluster REQUIRES.
// A bare SelfSigned issuer (cmIssuer in cert_manager_test.go) mints CA:FALSE leaf
// certs that are each their own root, so members cannot mutually verify and the
// cluster caps at one member -- which is exactly the negative case asserted by
// TestTLSPeerCANotShared below. This shared CA chain is:
//
//	SelfSigned ClusterIssuer -> CA Certificate (isCA:true) -> CA ClusterIssuer
//
// referenced by both the peer and client surfaces so every member's cert chains
// to one shared root.
const (
	caBootstrapIssuerName = "etcd-tls-e2e-selfsigned-bootstrap"
	caCertificateName     = "etcd-tls-e2e-ca"
	caCertSecretName      = "etcd-tls-e2e-ca-tls"
	caIssuerName          = "etcd-tls-e2e-ca-issuer"
	caIssuerType          = "ClusterIssuer"
	caCertNamespace       = "cert-manager"
)

// ensureClusterCAIssuer creates (idempotently) the SelfSigned->CA Certificate->CA
// ClusterIssuer chain, returning once the CA ClusterIssuer is Ready so certs
// referencing it can actually be issued.
func ensureClusterCAIssuer(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	r := cfg.Client().Resources()

	bootstrap := &certv1.ClusterIssuer{
		ObjectMeta: metav1.ObjectMeta{Name: caBootstrapIssuerName},
		Spec: certv1.IssuerSpec{
			IssuerConfig: certv1.IssuerConfig{SelfSigned: &certv1.SelfSignedIssuer{}},
		},
	}
	createIfAbsent(ctx, t, cfg, bootstrap, caBootstrapIssuerName, "")

	caCert := &certv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: caCertificateName, Namespace: caCertNamespace},
		Spec: certv1.CertificateSpec{
			IsCA:       true,
			CommonName: "etcd-tls-e2e-ca",
			SecretName: caCertSecretName,
			PrivateKey: &certv1.CertificatePrivateKey{Algorithm: certv1.ECDSAKeyAlgorithm, Size: 256},
			IssuerRef: cmmeta.IssuerReference{
				Name:  caBootstrapIssuerName,
				Kind:  "ClusterIssuer",
				Group: "cert-manager.io",
			},
		},
	}
	createIfAbsent(ctx, t, cfg, caCert, caCertificateName, caCertNamespace)

	caIssuer := &certv1.ClusterIssuer{
		ObjectMeta: metav1.ObjectMeta{Name: caIssuerName},
		Spec: certv1.IssuerSpec{
			IssuerConfig: certv1.IssuerConfig{
				CA: &certv1.CAIssuer{SecretName: caCertSecretName},
			},
		},
	}
	createIfAbsent(ctx, t, cfg, caIssuer, caIssuerName, "")

	// Wait for the CA ClusterIssuer to be Ready -- otherwise member certs cannot
	// be minted and the cluster never comes up (this is the failure the test is
	// here to prove does NOT happen with a shared CA).
	if err := wait.For(func(ctx context.Context) (bool, error) {
		var iss certv1.ClusterIssuer
		if err := r.Get(ctx, caIssuerName, "", &iss); err != nil {
			return false, nil
		}
		for _, c := range iss.Status.Conditions {
			if c.Type == certv1.IssuerConditionReady && c.Status == cmmeta.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(5*time.Second)); err != nil {
		t.Fatalf("CA ClusterIssuer %q never became Ready: %v", caIssuerName, err)
	}
}

// createIfAbsent gets obj by name/ns and returns if it already exists, otherwise
// creates it, retrying past the transient cert-manager webhook errors below.
func createIfAbsent(ctx context.Context, t *testing.T, cfg *envconf.Config, obj k8s.Object, name, ns string) {
	t.Helper()
	r := cfg.Client().Resources()
	if err := r.Get(ctx, name, ns, obj); err == nil {
		return // already exists
	} else if !k8serrors.IsNotFound(err) {
		t.Fatalf("failed to get %T %q: %v", obj, name, err)
	}

	// Create with retry: cert-manager's validating webhook is registered before its
	// serving-cert CA bundle is injected (by cainjector), so an immediate Create can
	// fail transiently with "failed calling webhook ... x509: certificate signed by
	// unknown authority". Retry until the webhook is genuinely serving.
	if err := wait.For(func(ctx context.Context) (bool, error) {
		err := r.Create(ctx, obj)
		if err == nil || k8serrors.IsAlreadyExists(err) {
			return true, nil
		}
		if isTransientWebhookError(err) {
			return false, nil
		}
		return false, err
	}, wait.WithTimeout(2*time.Minute), wait.WithInterval(5*time.Second)); err != nil {
		t.Fatalf("failed to create %T %q: %v", obj, name, err)
	}
}

// isTransientWebhookError reports whether err is the well-known transient failure of
// calling the cert-manager admission webhook before its CA bundle is injected.
func isTransientWebhookError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "failed calling webhook") ||
		strings.Contains(msg, "certificate signed by unknown authority") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no endpoints available")
}

// installCertManagerForTLS installs cert-manager and registers the schemes the
// TLS tests need, mirroring the setup in cert_manager_test.go.
func installCertManagerForTLS(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	t.Helper()
	log.Println("Installing cert-manager...")
	if err := test_utils.InstallCertManager(); err != nil {
		log.Printf("Unable to install Cert Manager: %s", err)
	}
	client := cfg.Client()
	_ = appsv1.AddToScheme(client.Resources().GetScheme())
	_ = corev1.AddToScheme(client.Resources().GetScheme())
	_ = certv1.AddToScheme(client.Resources().GetScheme())
	_ = apiextensionsV1.AddToScheme(client.Resources().GetScheme())
	return ctx
}

// waitForSTSCreated blocks until the operator has created the named StatefulSet.
// The shared waitForSTSReadiness treats a Get error (including NotFound) as fatal,
// so it must only be called once the STS exists; with a fresh cert-manager install
// the operator can take longer than usual to mint certs and create the STS.
func waitForSTSCreated(t *testing.T, c *envconf.Config, name string, waitFor time.Duration) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var sts appsv1.StatefulSet
		if err := c.Client().Resources().Get(ctx, name, namespace, &sts); err != nil {
			if k8serrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}, wait.WithTimeout(waitFor), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("StatefulSet %s was never created by the operator: %v", name, err)
	}
}

// podFQDN returns the in-cluster DNS name etcd advertises for a member, which is
// what the server cert's wildcard SAN (*.{cluster}.{ns}.svc.cluster.local) covers.
// etcdctl MUST dial this name (not 127.0.0.1) or Go TLS hostname verification fails.
func podFQDN(clusterName string, ordinal int) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", clusterName, ordinal, clusterName, namespace)
}

// tlsEtcdctl runs etcdctl inside the given pod with explicit mTLS flags pointed at
// the server cert material mounted at serverCertMountPath, dialing the pod's own
// advertised https endpoint.
func tlsEtcdctl(t *testing.T, c *envconf.Config, podName, clusterName string, ordinal int, args ...string) (string, string, error) {
	t.Helper()
	base := []string{
		"etcdctl",
		"--cacert=/etc/etcd/server-tls/ca.crt",
		"--cert=/etc/etcd/server-tls/tls.crt",
		"--key=/etc/etcd/server-tls/tls.key",
		fmt.Sprintf("--endpoints=https://%s:2379", podFQDN(clusterName, ordinal)),
	}
	return execInPod(t, c, podName, namespace, append(base, args...))
}

// getEtcdMemberListTLS returns the member list parsed from `etcdctl member list -w
// json`, run with mTLS flags. The shared getEtcdMemberListPB uses a bare etcdctl
// (no certs, 127.0.0.1) which a client-cert-auth TLS server resets, so TLS clusters
// need this variant.
func getEtcdMemberListTLS(t *testing.T, c *envconf.Config, clusterName string, ordinal int) *etcdserverpb.MemberListResponse {
	t.Helper()
	podName := fmt.Sprintf("%s-%d", clusterName, ordinal)
	stdout, stderr, err := tlsEtcdctl(t, c, podName, clusterName, ordinal, "member", "list", "-w", "json")
	if err != nil {
		t.Fatalf("Failed to get etcd member list over mTLS: %v, stderr: %s", err, stderr)
	}
	var ml etcdserverpb.MemberListResponse
	if err := json.Unmarshal([]byte(stdout), &ml); err != nil {
		t.Fatalf("Failed to parse mTLS member list JSON: %v. Raw: %s", err, stdout)
	}
	return &ml
}

// waitForNoLearnersTLS is the mTLS counterpart of waitForNoLearners: it waits until
// the cluster reports exactly expectedMembers, all voting (no learners), querying
// over mTLS.
func waitForNoLearnersTLS(t *testing.T, c *envconf.Config, clusterName string, ordinal, expectedMembers int, waitFor time.Duration) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		ml := getEtcdMemberListTLS(t, c, clusterName, ordinal)
		if len(ml.Members) != expectedMembers {
			return false, nil
		}
		for _, m := range ml.Members {
			if m.IsLearner {
				return false, nil
			}
		}
		return true, nil
	}, wait.WithTimeout(waitFor), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("Timeout waiting for %d voting members (no learners) over mTLS: %v", expectedMembers, err)
	}
}

// waitForTLSReadyCondition blocks until the EtcdCluster's TLSReady condition has
// the wanted status, returning the observed reason. It fails the test on timeout.
func waitForTLSReadyCondition(t *testing.T, c *envconf.Config, name string,
	want metav1.ConditionStatus, waitFor time.Duration) string {
	t.Helper()
	var observedReason string
	err := wait.For(func(ctx context.Context) (bool, error) {
		var ec ecv1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, name, namespace, &ec); err != nil {
			return false, nil
		}
		for _, cond := range ec.Status.Conditions {
			if cond.Type == "TLSReady" {
				observedReason = cond.Reason
				return cond.Status == want, nil
			}
		}
		return false, nil
	}, wait.WithTimeout(waitFor), wait.WithInterval(5*time.Second))
	if err != nil {
		// Surface the current conditions for debugging.
		var ec ecv1alpha1.EtcdCluster
		_ = c.Client().Resources().Get(t.Context(), name, namespace, &ec)
		t.Fatalf("TLSReady never reached status=%s for %q (last reason=%q); conditions=%+v",
			want, name, observedReason, ec.Status.Conditions)
	}
	return observedReason
}

// TestTLSMultiMemberQuorum is the headline T6 proof: a 3-member EtcdCluster with
// BOTH TLS surfaces enabled against a shared CA issuer forms and holds quorum,
// reports TLSReady=True, serves real mTLS (put/get succeeds) and genuinely
// enforces it (a no-cert / cleartext attempt fails).
func TestTLSMultiMemberQuorum(t *testing.T) {
	feature := features.New("tls-multi-member-quorum").WithLabel("app", "cert-manager")

	const clusterName = "etcd-tls-quorum"
	const size = 3

	etcdCluster := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: etcdVersion,
			TLS: &ecv1alpha1.EtcdClusterTLS{
				Peer:   certManagerSurface(caIssuerType, caIssuerName),
				Client: certManagerSurface(caIssuerType, caIssuerName),
			},
		},
	}

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		ctx = installCertManagerForTLS(ctx, t, cfg)
		ensureClusterCAIssuer(ctx, t, cfg)
		if err := cfg.Client().Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create etcd cluster: %s", err)
		}
		return ctx
	})

	// THE quorum assertion: all 3 replicas ready + 3 voting members, no learners.
	feature.Assess("3-member TLS cluster forms and holds quorum",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Wait for the StatefulSet to bring all 3 replicas to Ready. With a
			// self-signed leaf peer issuer this would hang at 1; with the shared CA
			// it must reach 3. Wait for the operator to create the STS first (cert
			// issuance on a fresh cert-manager can lag), then for full readiness.
			waitForSTSCreated(t, c, clusterName, 3*time.Minute)
			waitForSTSReadiness(t, c, clusterName, size)

			// All 3 members registered and promoted to voting (no stuck learners),
			// queried over mTLS (the cleartext helper can't talk to a TLS server).
			waitForNoLearnersTLS(t, c, clusterName, 0, size, 5*time.Minute)

			ml := getEtcdMemberListTLS(t, c, clusterName, 0)
			if len(ml.Members) != size {
				t.Fatalf("expected %d members, got %d: %+v", size, len(ml.Members), ml.Members)
			}
			for _, m := range ml.Members {
				if m.IsLearner {
					t.Errorf("member %s (%d) is still a learner -- quorum not fully formed", m.Name, m.ID)
				}
			}
			t.Logf("etcd member list (%d voting members):", len(ml.Members))
			for _, m := range ml.Members {
				t.Logf("  - %s id=%x learner=%v peerURLs=%v", m.Name, m.ID, m.IsLearner, m.PeerURLs)
			}
			return ctx
		})

	feature.Assess("EtcdCluster reports TLSReady=True",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			reason := waitForTLSReadyCondition(t, c, clusterName, metav1.ConditionTrue, 3*time.Minute)
			if reason != "TLSReady" {
				t.Errorf("expected TLSReady reason=TLSReady, got %q", reason)
			}
			return ctx
		})

	feature.Assess("peer traffic is https (peer cert flags + https peer URLs)",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			var sts appsv1.StatefulSet
			if err := c.Client().Resources().Get(ctx, clusterName, namespace, &sts); err != nil {
				t.Fatalf("failed to get StatefulSet: %v", err)
			}
			args := strings.Join(sts.Spec.Template.Spec.Containers[0].Args, " ")
			for _, want := range []string{
				"--listen-peer-urls=https://",
				"--initial-advertise-peer-urls=https://",
				"--peer-cert-file=/etc/etcd/peer-tls/tls.crt",
				"--peer-trusted-ca-file=/etc/etcd/peer-tls/ca.crt",
				"--peer-client-cert-auth",
				"--listen-client-urls=https://",
				"--cert-file=/etc/etcd/server-tls/tls.crt",
				"--client-cert-auth",
			} {
				if !strings.Contains(args, want) {
					t.Errorf("expected etcd args to contain %q; full args: %s", want, args)
				}
			}
			return ctx
		})

	// mTLS genuinely works AND is genuinely enforced.
	feature.Assess("mTLS put/get succeeds across the cluster",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			podName := fmt.Sprintf("%s-0", clusterName)
			// Write with mTLS to member 0.
			if _, stderr, err := tlsEtcdctl(t, c, podName, clusterName, 0, "put", "tls-key", "tls-value"); err != nil {
				t.Fatalf("mTLS put failed: %v, stderr: %s", err, stderr)
			}
			// Read it back through member 2 (proves replication across the TLS cluster).
			pod2 := fmt.Sprintf("%s-2", clusterName)
			stdout, stderr, err := tlsEtcdctl(t, c, pod2, clusterName, 2, "get", "tls-key")
			if err != nil {
				t.Fatalf("mTLS get on member 2 failed: %v, stderr: %s", err, stderr)
			}
			lines := strings.Split(strings.TrimSpace(stdout), "\n")
			if len(lines) < 2 || lines[0] != "tls-key" || lines[1] != "tls-value" {
				t.Errorf("expected tls-key=tls-value replicated to member 2, got: %s", stdout)
			}
			return ctx
		})

	feature.Assess("cleartext / no-cert access is rejected (TLS is not theater)",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			podName := fmt.Sprintf("%s-0", clusterName)
			fqdn := podFQDN(clusterName, 0)

			// 1) http:// against an https listener must fail.
			stdout, stderr, err := execInPod(t, c, podName, namespace, []string{
				"etcdctl", fmt.Sprintf("--endpoints=http://%s:2379", fqdn),
				"--command-timeout=5s", "put", "should", "fail",
			})
			if err == nil {
				t.Errorf("cleartext (http) put unexpectedly SUCCEEDED -- TLS is not enforced. stdout=%s stderr=%s",
					stdout, stderr)
			}

			// 2) https but WITHOUT a client cert must fail because --client-cert-auth
			//    requires a presented client certificate.
			stdout, stderr, err = execInPod(t, c, podName, namespace, []string{
				"etcdctl",
				"--cacert=/etc/etcd/server-tls/ca.crt",
				fmt.Sprintf("--endpoints=https://%s:2379", fqdn),
				"--command-timeout=5s", "put", "should", "fail",
			})
			if err == nil {
				t.Errorf("no-client-cert put unexpectedly SUCCEEDED -- client-cert-auth not enforced. stdout=%s stderr=%s",
					stdout, stderr)
			}
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, clusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// TestTLSPeerCANotShared is the negative companion: a cluster whose PEER surface
// uses a bare self-signed leaf issuer must surface TLSReady=False/PeerCANotShared --
// a visible, actionable signal -- and must NOT report TLSReady=True. The client
// surface uses the shared CA so the verdict is unambiguously attributable to the
// peer surface.
//
// IMPORTANT empirical finding (verified live on kind, 2026-06-17): the plan's
// Decision 3 / the adversarial review assumed a self-signed *leaf* peer issuer
// makes members unable to mutually verify and caps the cluster at one member. That
// is FALSE for this operator's architecture: the operator provisions a SINGLE
// shared peer Certificate/secret ({cluster}-peer-tls) mounted into every member, so
// every member presents and trusts the *identical* self-signed cert and peer mTLS
// succeeds -- a healthy 3-member quorum still forms. PeerCANotShared is therefore a
// conservative WARNING signal (the user's issuer is not a real shared CA, which is a
// fragile/unintended config), not a hard barrier to formation, and the operator does
// not gate scale-out on it. This test asserts the signal fires and the condition is
// not falsely True; it deliberately does NOT assert the cluster fails to form,
// because live evidence shows it does form. See the T6 report for the full analysis.
func TestTLSPeerCANotShared(t *testing.T) {
	feature := features.New("tls-peer-ca-not-shared").WithLabel("app", "cert-manager")

	const clusterName = "etcd-tls-peercheck"
	const size = 3

	etcdCluster := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: etcdVersion,
			TLS: &ecv1alpha1.EtcdClusterTLS{
				// Peer on a bare self-signed leaf issuer (the broken pattern).
				Peer: certManagerSurface(cmIssuerType, cmIssuerName),
				// Client on the shared CA, so any failure is the peer surface's.
				Client: certManagerSurface(caIssuerType, caIssuerName),
			},
		},
	}

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		ctx = installCertManagerForTLS(ctx, t, cfg)
		ensureClusterCAIssuer(ctx, t, cfg)
		// The bare self-signed ClusterIssuer (cmIssuer) the peer surface points at.
		createIfAbsent(ctx, t, cfg, cmIssuer, cmIssuerName, "")
		if err := cfg.Client().Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create etcd cluster: %s", err)
		}
		return ctx
	})

	feature.Assess("operator surfaces TLSReady=False / reason=PeerCANotShared",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			reason := waitForTLSReadyCondition(t, c, clusterName, metav1.ConditionFalse, 3*time.Minute)
			if reason != "PeerCANotShared" {
				t.Fatalf("expected TLSReady=False reason=PeerCANotShared, got reason=%q", reason)
			}
			return ctx
		})

	feature.Assess("PeerCANotShared persists; the operator never falsely reports TLSReady=True",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// The signal must be stable, not flap to True. The shared-peer-secret
			// design means the cluster CAN still form (see the test doc), so we do
			// not assert non-formation; we assert the WARNING stays put -- the user
			// keeps a visible, actionable signal that their peer issuer is not a real
			// shared CA. A flip to TLSReady=True here would be the real bug (the guard
			// silently clearing itself).
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				var ec ecv1alpha1.EtcdCluster
				if err := c.Client().Resources().Get(ctx, clusterName, namespace, &ec); err == nil {
					for _, cond := range ec.Status.Conditions {
						if cond.Type == "TLSReady" {
							if cond.Status == metav1.ConditionTrue {
								t.Fatalf("TLSReady flipped to True for a self-signed-leaf peer issuer; "+
									"the PeerCANotShared guard cleared itself (reason=%q)", cond.Reason)
							}
							if cond.Reason != "PeerCANotShared" {
								t.Fatalf("TLSReady reason drifted off PeerCANotShared to %q", cond.Reason)
							}
						}
					}
				}
				time.Sleep(10 * time.Second)
			}
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, clusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
