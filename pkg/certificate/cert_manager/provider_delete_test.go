package cert_manager

import (
	"context"
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// finalizeCluster relies on DeleteCertificateSecret to be NotFound-tolerant, and
// on it removing a Certificate cert-manager never issued: otherwise the cluster
// cleanup finalizer stays and the EtcdCluster is never deleted.
func TestDeleteCertificateSecret(t *testing.T) {
	key := client.ObjectKey{Name: "etcd-client-tls", Namespace: "default"}
	certificate := func(conditionType certmanagerv1.CertificateConditionType,
		status cmmeta.ConditionStatus) *certmanagerv1.Certificate {
		return &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Status: certmanagerv1.CertificateStatus{Conditions: []certmanagerv1.CertificateCondition{
				{Type: conditionType, Status: status},
			}},
		}
	}
	ready := func() client.Object {
		return certificate(certmanagerv1.CertificateConditionReady, cmmeta.ConditionTrue)
	}
	notReady := func() client.Object {
		return certificate(certmanagerv1.CertificateConditionReady, cmmeta.ConditionFalse)
	}
	issuing := func() client.Object {
		return certificate(certmanagerv1.CertificateConditionIssuing, cmmeta.ConditionTrue)
	}
	secret := func() client.Object {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
	}

	for name, objects := range map[string][]client.Object{
		"certificate and secret missing": nil,
		"certificate missing":            {secret()},
		"secret missing":                 {ready()},
		"certificate ready":              {ready(), secret()},
		"certificate not ready":          {notReady(), secret()},
		"certificate issuing":            {issuing()},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, certmanagerv1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()

			require.NoError(t, New(c).DeleteCertificateSecret(context.Background(), key))

			err := c.Get(context.Background(), key, &certmanagerv1.Certificate{})
			assert.True(t, k8serrors.IsNotFound(err), "Certificate still exists: %v", err)
			err = c.Get(context.Background(), key, &corev1.Secret{})
			assert.True(t, k8serrors.IsNotFound(err), "Secret still exists: %v", err)
		})
	}
}
