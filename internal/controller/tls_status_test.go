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
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// schemeWithCertManager extends the base test scheme with cert-manager Issuer /
// ClusterIssuer so the fake client can serve the runtime issuer reads.
func schemeWithCertManager(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := newScheme(t)
	require.NoError(t, certmanagerv1.AddToScheme(scheme))
	return scheme
}

// caClusterIssuer is a CA-capable ClusterIssuer (peer trust can be shared).
func caClusterIssuer(name string) *certmanagerv1.ClusterIssuer {
	return &certmanagerv1.ClusterIssuer{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certmanagerv1.IssuerSpec{
			IssuerConfig: certmanagerv1.IssuerConfig{CA: &certmanagerv1.CAIssuer{SecretName: name + "-ca"}},
		},
	}
}

// selfSignedClusterIssuer is a self-signed *leaf* ClusterIssuer (NOT CA-capable).
func selfSignedClusterIssuer(name string) *certmanagerv1.ClusterIssuer {
	return &certmanagerv1.ClusterIssuer{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certmanagerv1.IssuerSpec{
			IssuerConfig: certmanagerv1.IssuerConfig{SelfSigned: &certmanagerv1.SelfSignedIssuer{}},
		},
	}
}

// cmSurfaceFor builds a cert-manager surface pointing at a named ClusterIssuer.
func cmSurfaceFor(issuer string) *ecv1alpha1.TLSSurface {
	return &ecv1alpha1.TLSSurface{
		Provider: "cert-manager",
		ProviderCfg: ecv1alpha1.ProviderConfig{
			CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
				IssuerKind: "ClusterIssuer",
				IssuerName: issuer,
			},
		},
	}
}

// clientSecretFor returns the operator's client secret (valid keypair + CA) for ec.
func clientSecretFor(t *testing.T, ec *ecv1alpha1.EtcdCluster) *corev1.Secret {
	t.Helper()
	certPEM, keyPEM, caPEM := genClientKeypair(t)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: getClientCertName(ec.Name), Namespace: ec.Namespace},
		Data: map[string][]byte{
			tlsCertFile: certPEM,
			tlsKeyFile:  keyPEM,
			tlsCAFile:   caPEM,
		},
	}
}

// TestEvaluateTLSReadiness drives the Recorder-free verdict through every state in
// the reason vocabulary: both-nil, fully-wired ready, and each failure reason.
func TestEvaluateTLSReadiness(t *testing.T) {
	scheme := schemeWithCertManager(t)

	t.Run("both surfaces nil => TLSNotConfigured, not configured", func(t *testing.T) {
		ec := clusterWithTLS("ec", nil)
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.False(t, got.configured)
		assert.Equal(t, reasonTLSNotConfigured, got.reason)
	})

	t.Run("client surface fully wired => TLSReady=True", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Client: cmSurfaceFor("ca-issuer")})
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(caClusterIssuer("ca-issuer"), clientSecretFor(t, ec)).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.True(t, got.configured)
		assert.True(t, got.ready)
		assert.Equal(t, reasonTLSReady, got.reason)
	})

	t.Run("peer+client fully wired => TLSReady=True", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{
			Peer:   cmSurfaceFor("ca-issuer"),
			Client: cmSurfaceFor("ca-issuer"),
		})
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(caClusterIssuer("ca-issuer"), clientSecretFor(t, ec)).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.True(t, got.ready)
		assert.Equal(t, reasonTLSReady, got.reason)
	})

	t.Run("client issuer missing => IssuerNotFound", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Client: cmSurfaceFor("nope")})
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.True(t, got.configured)
		assert.False(t, got.ready)
		assert.Equal(t, reasonIssuerNotFound, got.reason)
	})

	t.Run("peer issuer is self-signed leaf => PeerCANotShared", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurfaceFor("selfsigned")})
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(selfSignedClusterIssuer("selfsigned")).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.False(t, got.ready)
		assert.Equal(t, reasonPeerCANotShared, got.reason)
	})

	t.Run("peer issuer missing => IssuerNotFound (not PeerCANotShared)", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurfaceFor("nope")})
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.Equal(t, reasonIssuerNotFound, got.reason)
	})

	t.Run("client secret not yet materialized => SurfaceNotReady", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Client: cmSurfaceFor("ca-issuer")})
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(caClusterIssuer("ca-issuer")).Build() // issuer present, secret absent
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.False(t, got.ready)
		assert.Equal(t, reasonSurfaceNotReady, got.reason)
	})

	t.Run("client secret malformed => ClientCertificateError", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Client: cmSurfaceFor("ca-issuer")})
		badSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: getClientCertName(ec.Name), Namespace: ec.Namespace},
			Data: map[string][]byte{
				tlsCertFile: []byte("not a pem"),
				tlsKeyFile:  []byte("not a pem"),
				tlsCAFile:   []byte("not a pem"),
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(caClusterIssuer("ca-issuer"), badSecret).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.False(t, got.ready)
		assert.Equal(t, reasonClientCertificateError, got.reason)
	})

	t.Run("peer-only with auto provider => TLSReady (auto mints a shared CA)", func(t *testing.T) {
		ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{
			Peer: &ecv1alpha1.TLSSurface{Provider: "auto"},
		})
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		got := evaluateTLSReadiness(context.Background(), ec, c)
		assert.True(t, got.ready)
		assert.Equal(t, reasonTLSReady, got.reason)
	})
}

// TestIssuerIsCACapable unit-tests the peer-CA predicate against representative
// issuer inputs: a self-signed leaf is NOT CA-capable; a CA issuer is; a missing
// issuer is an error (folded into IssuerNotFound by the caller).
func TestIssuerIsCACapable(t *testing.T) {
	scheme := schemeWithCertManager(t)
	ctx := context.Background()

	t.Run("self-signed leaf is not CA-capable", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(selfSignedClusterIssuer("ss")).Build()
		ok, err := issuerIsCACapable(ctx, c, "ss", "ClusterIssuer", "ns")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("CA issuer is CA-capable", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(caClusterIssuer("ca")).Build()
		ok, err := issuerIsCACapable(ctx, c, "ca", "ClusterIssuer", "ns")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("namespaced self-signed Issuer is not CA-capable", func(t *testing.T) {
		iss := &certmanagerv1.Issuer{
			ObjectMeta: metav1.ObjectMeta{Name: "ss", Namespace: "ns"},
			Spec: certmanagerv1.IssuerSpec{
				IssuerConfig: certmanagerv1.IssuerConfig{SelfSigned: &certmanagerv1.SelfSignedIssuer{}},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(iss).Build()
		ok, err := issuerIsCACapable(ctx, c, "ss", "Issuer", "ns")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("missing issuer errors", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		_, err := issuerIsCACapable(ctx, c, "gone", "ClusterIssuer", "ns")
		assert.Error(t, err)
	})

	t.Run("unsupported kind errors", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		_, err := issuerIsCACapable(ctx, c, "x", "Bogus", "ns")
		assert.Error(t, err)
	})
}

// TestTLSReadyCondition drives updateConditions and asserts the TLSReady condition
// is set/omitted with the correct Status and Reason for each verdict.
func TestTLSReadyCondition(t *testing.T) {
	r := &EtcdClusterReconciler{}

	newStateWithTLS := func(t tlsReadiness) *reconcileState {
		return &reconcileState{
			cluster: &ecv1alpha1.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "ns", Generation: 7}},
			tls:     t,
		}
	}

	t.Run("not configured => no TLSReady condition", func(t *testing.T) {
		s := newStateWithTLS(tlsReadiness{configured: false, reason: reasonTLSNotConfigured})
		r.updateConditions(s)
		assert.Nil(t, meta.FindStatusCondition(s.cluster.Status.Conditions, tlsReadyConditionType))
	})

	t.Run("ready => TLSReady True", func(t *testing.T) {
		s := newStateWithTLS(tlsReadiness{configured: true, ready: true, reason: reasonTLSReady, message: "ok"})
		r.updateConditions(s)
		cond := meta.FindStatusCondition(s.cluster.Status.Conditions, tlsReadyConditionType)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, reasonTLSReady, cond.Reason)
		assert.Equal(t, int64(7), cond.ObservedGeneration)
	})

	failureReasons := []string{
		reasonIssuerNotFound,
		reasonPeerCANotShared,
		reasonClientServerCAMismatch,
		reasonClientCertificateError,
		reasonSurfaceNotReady,
	}
	for _, reason := range failureReasons {
		t.Run("failure "+reason+" => TLSReady False with reason", func(t *testing.T) {
			s := newStateWithTLS(tlsReadiness{configured: true, ready: false, reason: reason, message: "boom"})
			r.updateConditions(s)
			cond := meta.FindStatusCondition(s.cluster.Status.Conditions, tlsReadyConditionType)
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionFalse, cond.Status)
			assert.Equal(t, reason, cond.Reason)
		})
	}

	t.Run("flipping back to cleartext removes a stale TLSReady", func(t *testing.T) {
		s := newStateWithTLS(tlsReadiness{configured: true, ready: true, reason: reasonTLSReady})
		r.updateConditions(s)
		require.NotNil(t, meta.FindStatusCondition(s.cluster.Status.Conditions, tlsReadyConditionType))
		// Now reconcile the same cluster with no TLS surface.
		s.tls = tlsReadiness{configured: false, reason: reasonTLSNotConfigured}
		r.updateConditions(s)
		assert.Nil(t, meta.FindStatusCondition(s.cluster.Status.Conditions, tlsReadyConditionType))
	})
}

// drain collects all currently-buffered events from a FakeRecorder.
func drain(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func hasEventWith(recorded []string, eventType, reason string) bool {
	for _, e := range recorded {
		// FakeRecorder formats as "<eventtype> <reason> <note>".
		if len(e) >= len(eventType)+1+len(reason) &&
			e[:len(eventType)] == eventType &&
			e[len(eventType)+1:len(eventType)+1+len(reason)] == reason {
			return true
		}
	}
	return false
}

// TestRecordTLSEvent asserts the controller boundary emits the expected Event
// (type + reason) for each verdict, and that a steady-state ready cluster does NOT
// re-emit the Normal TLSReady event.
func TestRecordTLSEvent(t *testing.T) {
	ec := &ecv1alpha1.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "ns"}}

	t.Run("not configured => no event", func(t *testing.T) {
		rec := events.NewFakeRecorder(10)
		r := &EtcdClusterReconciler{Recorder: rec}
		r.recordTLSEvent(ec, tlsReadiness{configured: false, reason: reasonTLSNotConfigured})
		assert.Empty(t, drain(rec))
	})

	failureCases := []string{
		reasonIssuerNotFound,
		reasonPeerCANotShared,
		reasonClientCertificateError,
		reasonSurfaceNotReady,
		reasonClientServerCAMismatch,
	}
	for _, reason := range failureCases {
		t.Run("failure "+reason+" => Warning event with matching reason", func(t *testing.T) {
			rec := events.NewFakeRecorder(10)
			r := &EtcdClusterReconciler{Recorder: rec}
			r.recordTLSEvent(ec, tlsReadiness{configured: true, ready: false, reason: reason, message: "m"})
			got := drain(rec)
			assert.True(t, hasEventWith(got, corev1.EventTypeWarning, reason), "events=%v", got)
		})
	}

	t.Run("ready (fresh) => Normal TLSReady event", func(t *testing.T) {
		rec := events.NewFakeRecorder(10)
		r := &EtcdClusterReconciler{Recorder: rec}
		fresh := &ecv1alpha1.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "ns"}}
		r.recordTLSEvent(fresh, tlsReadiness{configured: true, ready: true, reason: reasonTLSReady, message: "m"})
		got := drain(rec)
		assert.True(t, hasEventWith(got, corev1.EventTypeNormal, reasonTLSReady), "events=%v", got)
	})

	t.Run("ready (already True) => no repeat event", func(t *testing.T) {
		rec := events.NewFakeRecorder(10)
		r := &EtcdClusterReconciler{Recorder: rec}
		steady := &ecv1alpha1.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "ns"}}
		meta.SetStatusCondition(&steady.Status.Conditions, metav1.Condition{
			Type: tlsReadyConditionType, Status: metav1.ConditionTrue, Reason: reasonTLSReady,
		})
		r.recordTLSEvent(steady, tlsReadiness{configured: true, ready: true, reason: reasonTLSReady, message: "m"})
		assert.Empty(t, drain(rec))
	})
}

// TestFetchAndValidateStateEmitsTLSEvents exercises the integration at the
// fetchAndValidateState boundary: a self-signed-leaf peer issuer must produce a
// PeerCANotShared Warning Event AND stash a False verdict on the returned state.
func TestFetchAndValidateStateEmitsTLSEvents(t *testing.T) {
	scheme := schemeWithCertManager(t)
	ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurfaceFor("selfsigned")})
	ec.Spec.Version = "v3.6.12"

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(ec, selfSignedClusterIssuer("selfsigned")).Build()
	rec := events.NewFakeRecorder(10)
	r := &EtcdClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}

	state, _, err := r.fetchAndValidateState(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "ec", Namespace: "ns"}})
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, reasonPeerCANotShared, state.tls.reason)
	assert.False(t, state.tls.ready)
	assert.True(t, hasEventWith(drain(rec), corev1.EventTypeWarning, reasonPeerCANotShared))
}
