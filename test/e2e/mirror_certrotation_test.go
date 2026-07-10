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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// Scenario 4: cert rotation under load, on a TLS SOURCE. The source etcd is
// hand-rendered (operator-managed EtcdClusters hardcode cleartext client
// listeners — internal/controller/utils.go — so serving TLS is a product
// change out of this rung's scope); the AGENT stays on the real CR path.
// x509 material is generated in-test: no cert-manager dependency, and gen-1's
// lifetime is exact. Margins: gen-1 lifetime 5m >> first-assessment worst
// case (~3m of ceilings) + kubelet Secret sync (~90s), and the agent's
// per-handshake TLSInfo reload retries through backoff, so late propagation
// self-heals inside the reconnect ceilings.
const (
	tlsNamespace    = "mirror-tls"
	tlsSourceName   = "tls-src"
	tlsTargetName   = "tls-tgt"
	tlsServerSecret = "tls-src-server"
	tlsClientSecret = "mirror-client"
	tlsMountPath    = "/etc/etcd/tls"
	tlsPrefix       = "/tlsrot/"
	// tlsGen1Lifetime is the deliberately short first client-cert lifetime;
	// the schedule is fixed rather than polled off the agent's cert-expiry
	// gauge (whose 5m refresh would cost more wall clock). The fuse starts
	// at issuance (first assessment), so the lifetime must absorb that
	// assessment's full worst case and still leave tlsRotationFloor for the
	// rotation loop.
	tlsGen1Lifetime = 5 * time.Minute
	// tlsRotationFloor is the minimum gen-1 lifetime that must remain when
	// the rotation assessment starts; below it the loop could run too few
	// iterations to mean anything, so the test fails loudly instead.
	tlsRotationFloor = 90 * time.Second
	tlsLoadInterval  = 5 * time.Second
	// tlsStallWindow: the watermark must advance at least once per window
	// while the rotation load loop runs.
	tlsStallWindow = time.Minute
)

func tlsSourceAdvertiseURL() string {
	return fmt.Sprintf("https://%s-0.%s.%s.svc.cluster.local:2379", tlsSourceName, tlsSourceName, tlsNamespace)
}

func tlsSrcRef() etcdPodRef {
	return etcdPodRef{ns: tlsNamespace, pod: tlsSourceName + "-0", extraArgs: []string{
		"--endpoints=https://127.0.0.1:2379",
		"--cacert=" + tlsMountPath + "/ca.crt",
		"--cert=" + tlsMountPath + "/tls.crt",
		"--key=" + tlsMountPath + "/tls.key",
	}}
}

func tlsTgtRef() etcdPodRef { return etcdPodRef{ns: tlsNamespace, pod: tlsTargetName + "-0"} }

// mirrorTestCA is a throwaway in-test CA (P-256).
type mirrorTestCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newMirrorTestCA(t *testing.T) *mirrorTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mirror-e2e-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}
	return &mirrorTestCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// issue mints a leaf with both serverAuth and clientAuth EKUs (the server
// cert doubles as in-pod etcdctl's client cert against --client-cert-auth).
func (ca *mirrorTestCA) issue(
	t *testing.T, cn string, dnsSANs []string, ipSANs []net.IP, notAfter time.Time,
) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key for %s: %v", cn, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatalf("generating serial for %s: %v", cn, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dnsSANs,
		IPAddresses:  ipSANs,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating certificate for %s: %v", cn, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key for %s: %v", cn, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// tlsMaterialSecret shapes a Secret the way mirror_creds.go expects:
// tls.crt/tls.key plus ca.crt.
func tlsMaterialSecret(name string, certPEM, keyPEM, caPEM []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tlsNamespace},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM, "ca.crt": caPEM},
	}
}

// tlsSourceWorkload renders the single-member TLS etcd StatefulSet and its
// headless Service. The PVC is required: the post-expiry step restarts the
// pod, and losing the data dir would change the cluster ID — turning the
// intended resume into a CheckpointInvalidated genesis that fails
// RequireEmpty for the wrong reason.
func tlsSourceWorkload() (*appsv1.StatefulSet, *corev1.Service) {
	labels := map[string]string{"app": tlsSourceName}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: tlsSourceName, Namespace: tlsNamespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labels,
			Ports:     []corev1.ServicePort{{Name: "client", Port: 2379}},
		},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: tlsSourceName, Namespace: tlsNamespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    ptr.To[int32](1),
			ServiceName: tlsSourceName,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:    "etcd",
						Image:   "gcr.io/etcd-development/etcd:" + etcdVersion,
						Command: []string{"/usr/local/bin/etcd"},
						Args: []string{
							"--name=" + tlsSourceName + "-0",
							"--data-dir=/var/lib/etcd",
							"--listen-peer-urls=http://127.0.0.1:2380",
							"--initial-advertise-peer-urls=http://127.0.0.1:2380",
							"--initial-cluster=" + tlsSourceName + "-0=http://127.0.0.1:2380",
							"--initial-cluster-state=new",
							"--initial-cluster-token=" + tlsSourceName,
							"--listen-client-urls=https://0.0.0.0:2379",
							"--advertise-client-urls=" + tlsSourceAdvertiseURL(),
							"--cert-file=" + tlsMountPath + "/tls.crt",
							"--key-file=" + tlsMountPath + "/tls.key",
							"--client-cert-auth",
							"--trusted-ca-file=" + tlsMountPath + "/ca.crt",
						},
						Ports: []corev1.ContainerPort{{Name: "client", ContainerPort: 2379}},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/etcd"},
							{Name: "tls", MountPath: tlsMountPath, ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "tls",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: tlsServerSecret},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}},
		},
	}
	return sts, svc
}

// waitForTLSSourceServing waits for the TLS source pod to be Running and
// answering authenticated etcdctl calls (no HTTP readiness probe: the client
// port demands TLS client certs).
func waitForTLSSourceServing(ctx context.Context, t *testing.T, cfg *envconf.Config, ceiling time.Duration) {
	t.Helper()
	waitForPodRunning(ctx, t, cfg, tlsNamespace, tlsSourceName+"-0", ceiling)
	if err := wait.For(func(context.Context) (bool, error) {
		_, eerr := execEtcdctl(t, cfg, tlsSrcRef(), "endpoint", "status")
		return eerr == nil, nil
	}, wait.WithContext(ctx), wait.WithTimeout(ceiling), wait.WithInterval(mirrorPollInterval)); err != nil {
		t.Fatalf("TLS source never answered an authenticated etcdctl call within %s: %v", ceiling, err)
	}
}

// TestMirrorCertRotation exercises scenario 4: a TLS source with in-test
// x509 material, the client-cert Secret rotated in place mid-replication.
// The contract under test is the agent's per-handshake TLSInfo file reload:
// the live session survives rotation untouched, and the first post-expiry
// re-handshake (forced by a source pod restart) can only succeed with the
// rotated cert (etcd rejects the expired gen-1).
func TestMirrorCertRotation(t *testing.T) {
	feature := features.New("mirror-cert-rotation")

	var ca *mirrorTestCA
	var gen1Expiry time.Time

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		ctx = setupMirrorNamespace(ctx, t, cfg, tlsNamespace)

		ca = newMirrorTestCA(t)
		serverCert, serverKey := ca.issue(t, tlsSourceName,
			[]string{
				fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", tlsSourceName, tlsSourceName, tlsNamespace),
				"localhost",
			},
			[]net.IP{net.ParseIP("127.0.0.1")},
			time.Now().Add(24*time.Hour))
		if err := cfg.Client().Resources().Create(ctx,
			tlsMaterialSecret(tlsServerSecret, serverCert, serverKey, ca.certPEM)); err != nil {
			t.Fatalf("failed to create server TLS secret: %v", err)
		}

		sts, svc := tlsSourceWorkload()
		if err := cfg.Client().Resources().Create(ctx, svc); err != nil {
			t.Fatalf("failed to create TLS source Service: %v", err)
		}
		if err := cfg.Client().Resources().Create(ctx, sts); err != nil {
			t.Fatalf("failed to create TLS source StatefulSet: %v", err)
		}
		waitForTLSSourceServing(ctx, t, cfg, clusterReadyWait)

		createEtcdClusterInNS(ctx, t, cfg, tlsNamespace, tlsTargetName)
		waitForSTSReadyInNS(ctx, t, cfg, tlsNamespace, tlsTargetName)
		return ctx
	})

	feature.Assess("mirror converges over TLS",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			gen1Expiry = time.Now().Add(tlsGen1Lifetime)
			gen1Cert, gen1Key := ca.issue(t, "mirror-agent", nil, nil, gen1Expiry)
			if err := cfg.Client().Resources().Create(ctx,
				tlsMaterialSecret(tlsClientSecret, gen1Cert, gen1Key, ca.certPEM)); err != nil {
				t.Fatalf("failed to create client TLS secret: %v", err)
			}

			createMirror(ctx, t, cfg, newEtcdMirror(tlsNamespace, "tlsrot", tlsSourceName, tlsTargetName,
				tlsPrefix, func(em *ecv1alpha1.EtcdMirror) {
					em.Spec.Source.EndpointList = []string{tlsSourceAdvertiseURL()}
					em.Spec.Source.TLS = &ecv1alpha1.EtcdMirrorTLS{
						SecretRef: &corev1.LocalObjectReference{Name: tlsClientSecret},
					}
				}))
			waitForMirrorSyncingAvailable(ctx, t, cfg, tlsNamespace, "tlsrot", 2*time.Minute)

			putKeys(t, cfg, tlsSrcRef(), tlsPrefix+"key-", "val-", 0, 20)
			waitForMirrorDataMatch(ctx, t, cfg, tlsSrcRef(), tlsTgtRef(), tlsPrefix, 30*time.Second,
				250*time.Millisecond)
			return ctx
		})

	feature.Assess("rotation under load: watermark advances, never Failed",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// A blown fuse must fail loudly here — not surface as an
			// unrelated handshake failure later, and never let the loop
			// below run zero iterations and pass vacuously.
			if margin := time.Until(gen1Expiry); margin < tlsRotationFloor {
				t.Fatalf("only %s of gen-1 lifetime left at rotation start (floor %s); "+
					"fixture too slow — raise tlsGen1Lifetime", margin.Round(time.Second), tlsRotationFloor)
			}
			gen2Cert, gen2Key := ca.issue(t, "mirror-agent", nil, nil, time.Now().Add(24*time.Hour))
			updateSecretData(ctx, t, cfg, tlsNamespace, tlsClientSecret,
				map[string][]byte{"tls.crt": gen2Cert, "tls.key": gen2Key, "ca.crt": ca.certPEM})

			// Live-session half of the rotation contract: keep writing until
			// past gen-1 expiry; the established connection must be
			// undisturbed (no Failed phase, watermark never stalls a full
			// window).
			em, err := getMirror(ctx, cfg, tlsNamespace, "tlsrot")
			if err != nil {
				t.Fatalf("failed to get EtcdMirror tlsrot: %v", err)
			}
			lastRev := em.Status.LastAppliedRevision
			lastAdvance := time.Now()
			deadline := gen1Expiry.Add(30 * time.Second)
			iterations := 0
			for ; time.Now().Before(deadline); iterations++ {
				if _, perr := execEtcdctl(t, cfg, tlsSrcRef(), "put",
					fmt.Sprintf("%sload-%03d", tlsPrefix, iterations), "l"); perr != nil {
					t.Fatalf("load put failed mid-rotation: %v", perr)
				}
				em, err = getMirror(ctx, cfg, tlsNamespace, "tlsrot")
				if err == nil {
					if em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseFailed {
						dumpMirrorDiagnostics(ctx, t, cfg, tlsNamespace, "tlsrot")
						t.Fatalf("mirror went Failed during cert rotation: %+v", em.Status)
					}
					if em.Status.LastAppliedRevision > lastRev {
						lastRev = em.Status.LastAppliedRevision
						lastAdvance = time.Now()
					}
				}
				if time.Since(lastAdvance) > tlsStallWindow {
					dumpMirrorDiagnostics(ctx, t, cfg, tlsNamespace, "tlsrot")
					t.Fatalf("watermark stalled for over %s during cert rotation (revision %d)",
						tlsStallWindow, lastRev)
				}
				time.Sleep(tlsLoadInterval)
			}
			// Defense-in-depth behind the floor guard: the loop must have
			// exercised the contract at least once.
			if iterations == 0 {
				t.Fatal("rotation load loop ran zero iterations")
			}
			return ctx
		})

	feature.Assess("post-expiry reconnect uses the rotated cert",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// Agent identity before the reconnect: a crashed-and-restarted
			// agent would also pass the data checks (it reads gen-2 from the
			// mount at startup and resumes the fenced checkpoint), so pod
			// name + container restart count are what pin the handshake to
			// the IN-PROCESS TLSInfo reload.
			pre, err := getMirror(ctx, cfg, tlsNamespace, "tlsrot")
			if err != nil {
				t.Fatalf("failed to get EtcdMirror tlsrot: %v", err)
			}
			agentPod := pre.Status.AgentPod
			if agentPod == "" {
				t.Fatal("status.agentPod is empty on a syncing mirror")
			}
			restartsBefore := agentRestartCount(ctx, t, cfg, tlsNamespace, agentPod)

			// gen-1 is now expired; a successful handshake after the source
			// restart is only possible with gen-2 — the per-handshake TLSInfo
			// file reload proven end-to-end.
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: tlsSourceName + "-0", Namespace: tlsNamespace}}
			if err := cfg.Client().Resources().Delete(ctx, pod); err != nil {
				t.Fatalf("failed to delete TLS source pod: %v", err)
			}
			waitForTLSSourceServing(ctx, t, cfg, 2*time.Minute)

			putKeys(t, cfg, tlsSrcRef(), tlsPrefix+"key-", "val-", 20, 25)
			waitForMirrorDataMatch(ctx, t, cfg, tlsSrcRef(), tlsTgtRef(), tlsPrefix, 90*time.Second, time.Second)

			// The status mirror lags the 15s poll cadence behind the data
			// plane; give it one cycle to report the healthy steady state.
			em := waitForMirrorSyncingAvailable(ctx, t, cfg, tlsNamespace, "tlsrot", time.Minute)
			if em.Status.ForcedResyncCount != 0 {
				t.Fatalf("reconnect forced a resync (count %d); the PVC-backed identity should have resumed cleanly",
					em.Status.ForcedResyncCount)
			}
			if em.Status.AgentPod != agentPod {
				t.Fatalf("agent pod changed across the reconnect: %q -> %q", agentPod, em.Status.AgentPod)
			}
			if after := agentRestartCount(ctx, t, cfg, tlsNamespace, agentPod); after != restartsBefore {
				t.Fatalf("agent container restarted across the reconnect (%d -> %d); "+
					"the handshake was not survived in-process", restartsBefore, after)
			}
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		deleteMirrorFixture(ctx, t, cfg, tlsNamespace)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
