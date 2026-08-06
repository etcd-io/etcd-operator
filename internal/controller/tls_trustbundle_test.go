package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// bundledSurface returns a cert-manager surface that also requests the trust
// bundle from the "extra-cas" ConfigMap.
func bundledSurface() *ecv1alpha1.TLSSurface {
	s := cmSurface(nil)
	s.TrustBundleConfigMapRef = &ecv1alpha1.TrustBundleConfigMapRef{Name: "extra-cas"}
	return s
}

func trustFixtures(t *testing.T) (issuedCA, bundleCA []byte) {
	t.Helper()
	_, _, issuedCA = genClientKeypair(t)
	_, _, bundleCA = genClientKeypair(t)
	return issuedCA, bundleCA
}

func TestValidateTrustBundlePEM(t *testing.T) {
	_, validCA := trustFixtures(t)

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"single CA accepted", string(validCA), false},
		{"two CAs accepted", string(validCA) + string(validCA), false},
		{"empty rejected", "", true},
		{"no certificates rejected", "just text\n", true},
		{"corrupt certificate block rejected",
			"-----BEGIN CERTIFICATE-----\nnotbase64!!!\n-----END CERTIFICATE-----\n", true},
		{"non-certificate PEM block rejected",
			"-----BEGIN PRIVATE KEY-----\nMIGH\n-----END PRIVATE KEY-----\n", true},
		{"valid cert with trailing garbage rejected", string(validCA) + "trailing", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTrustBundlePEM([]byte(tt.data))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestApplyTrustBundlesComposition(t *testing.T) {
	issuedCA, bundleCA := trustFixtures(t)

	ec := clusterWithTLS(&ecv1alpha1.EtcdClusterTLS{Client: bundledSurface()})
	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: getServerCertName(ec.Name), Namespace: ec.Namespace},
		Data:       map[string][]byte{tlsCAFile: issuedCA},
	}
	userCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "extra-cas", Namespace: ec.Namespace},
		Data:       map[string]string{trustBundleKey: string(bundleCA)},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(certSecret, userCM).Build()

	require.NoError(t, applyTrustBundles(t.Context(), ec, c))

	composed := &corev1.ConfigMap{}
	require.NoError(t, c.Get(t.Context(),
		client.ObjectKey{Name: getServerTrustName(ec.Name), Namespace: ec.Namespace}, composed))
	got := composed.Data[trustBundleKey]
	assert.True(t, strings.HasPrefix(got, strings.TrimRight(string(issuedCA), "\n")+"\n"),
		"composed bundle must start with the issued CA")
	assert.True(t, strings.HasSuffix(got, string(bundleCA)),
		"composed bundle must end with the user bundle")
	assert.NotEmpty(t, composed.OwnerReferences, "composed ConfigMap must be owned by the EtcdCluster")
}

func TestApplyTrustBundlesRecomposeOnRotation(t *testing.T) {
	issuedCA, bundleCA := trustFixtures(t)
	_, _, rotatedCA := genClientKeypair(t)

	ec := clusterWithTLS(&ecv1alpha1.EtcdClusterTLS{Client: bundledSurface()})
	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: getServerCertName(ec.Name), Namespace: ec.Namespace},
		Data:       map[string][]byte{tlsCAFile: issuedCA},
	}
	userCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "extra-cas", Namespace: ec.Namespace},
		Data:       map[string]string{trustBundleKey: string(bundleCA)},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(certSecret, userCM).Build()
	require.NoError(t, applyTrustBundles(t.Context(), ec, c))

	// Rotate the issued CA; the next reconcile must recompose.
	certSecret.Data[tlsCAFile] = rotatedCA
	require.NoError(t, c.Update(t.Context(), certSecret))
	require.NoError(t, applyTrustBundles(t.Context(), ec, c))

	composed := &corev1.ConfigMap{}
	require.NoError(t, c.Get(t.Context(),
		client.ObjectKey{Name: getServerTrustName(ec.Name), Namespace: ec.Namespace}, composed))
	assert.Contains(t, composed.Data[trustBundleKey], strings.TrimRight(string(rotatedCA), "\n"),
		"composed bundle must pick up the rotated issued CA")
	assert.NotContains(t, composed.Data[trustBundleKey], strings.TrimRight(string(issuedCA), "\n"),
		"composed bundle must not retain the pre-rotation CA")
}

func TestApplyTrustBundlesFailureModes(t *testing.T) {
	issuedCA, bundleCA := trustFixtures(t)

	newEC := func() *ecv1alpha1.EtcdCluster {
		return clusterWithTLS(&ecv1alpha1.EtcdClusterTLS{Client: bundledSurface()})
	}
	certSecret := func(ec *ecv1alpha1.EtcdCluster) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: getServerCertName(ec.Name), Namespace: ec.Namespace},
			Data:       map[string][]byte{tlsCAFile: issuedCA},
		}
	}

	t.Run("missing user ConfigMap is an explicit error", func(t *testing.T) {
		ec := newEC()
		c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(certSecret(ec)).Build()
		assert.Error(t, applyTrustBundles(t.Context(), ec, c))
	})

	t.Run("invalid bundle errors and preserves the last good composition", func(t *testing.T) {
		ec := newEC()
		lastGood := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: getServerTrustName(ec.Name), Namespace: ec.Namespace},
			Data:       map[string]string{trustBundleKey: string(issuedCA) + string(bundleCA)},
		}
		badUserCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "extra-cas", Namespace: ec.Namespace},
			Data:       map[string]string{trustBundleKey: "-----BEGIN CERTIFICATE-----\nbad!\n-----END CERTIFICATE-----\n"},
		}
		c := fake.NewClientBuilder().WithScheme(newScheme(t)).
			WithObjects(certSecret(ec), lastGood, badUserCM).Build()

		err := applyTrustBundles(t.Context(), ec, c)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "trust bundle invalid")

		preserved := &corev1.ConfigMap{}
		require.NoError(t, c.Get(t.Context(),
			client.ObjectKey{Name: getServerTrustName(ec.Name), Namespace: ec.Namespace}, preserved))
		assert.Equal(t, lastGood.Data, preserved.Data, "a bad bundle must not clobber the last good composition")
	})

	t.Run("bundle ConfigMap missing the ca.crt key is rejected", func(t *testing.T) {
		ec := newEC()
		userCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "extra-cas", Namespace: ec.Namespace},
			Data:       map[string]string{"wrong-key": string(bundleCA)},
		}
		c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(certSecret(ec), userCM).Build()
		assert.Error(t, applyTrustBundles(t.Context(), ec, c))
	})
}

// TestTrustBundleArgsAndMounts asserts the trust-bundle wiring flips exactly the
// --*trusted-ca-file flag of the requesting surface and that output without a
// bundle stays byte-identical to the bundle-free path.
func TestTrustBundleArgsAndMounts(t *testing.T) {
	t.Run("no bundle output is byte-identical", func(t *testing.T) {
		plain := createArgs("ec", nil, tlsArgs{peerEnabled: true, clientEnabled: true, peerCertAuth: true, clientCertAuth: true})
		viaSurfaces := createArgs("ec", nil, tlsArgsFor(clusterWithTLS(
			&ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil), Client: cmSurface(nil)})))
		assert.Equal(t, plain, viaSurfaces)
	})

	t.Run("client bundle flips only --trusted-ca-file", func(t *testing.T) {
		args := createArgs("ec", nil, tlsArgsFor(clusterWithTLS(
			&ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil), Client: bundledSurface()})))
		assert.Contains(t, args, "--trusted-ca-file="+serverTrustMountPath+"/"+tlsCAFile)
		assert.Contains(t, args, "--peer-trusted-ca-file="+peerCertMountPath+"/"+tlsCAFile)
		assert.Contains(t, args, "--cert-file="+serverCertMountPath+"/"+tlsCertFile,
			"the cert/key flags must keep pointing at the secret mount")
	})

	t.Run("peer bundle flips only --peer-trusted-ca-file", func(t *testing.T) {
		args := createArgs("ec", nil, tlsArgsFor(clusterWithTLS(
			&ecv1alpha1.EtcdClusterTLS{Peer: bundledSurface(), Client: cmSurface(nil)})))
		assert.Contains(t, args, "--peer-trusted-ca-file="+peerTrustMountPath+"/"+tlsCAFile)
		assert.Contains(t, args, "--trusted-ca-file="+serverCertMountPath+"/"+tlsCAFile)
	})

	t.Run("trust ConfigMap volume is mounted per requesting surface", func(t *testing.T) {
		ec := clusterWithTLS(&ecv1alpha1.EtcdClusterTLS{
			Peer:   cmSurface(nil),
			Client: bundledSurface(),
		})
		container, vols := stsContainer(t, ec)
		vNames := volumeNames(vols)
		mNames := mountNames(container.VolumeMounts)
		assert.Contains(t, vNames, serverTrustVolumeName, "server trust volume")
		assert.Contains(t, mNames, serverTrustVolumeName, "server trust mount")
		assert.NotContains(t, vNames, peerTrustVolumeName, "no peer trust volume without a peer bundle")
	})
}
