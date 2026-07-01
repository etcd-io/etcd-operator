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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// validSourceEndpoint and validTargetEndpoint are baseline endpoints that
// satisfy EtcdMirrorEndpoint's own oneOf rule, so each test case only needs
// to perturb the field(s) it's actually exercising.
func validSourceEndpoint() ecv1alpha1.EtcdMirrorEndpoint {
	return ecv1alpha1.EtcdMirrorEndpoint{
		EndpointList: []string{"https://etcd-source.example.com:2379"},
		Prefix:       "/registry/",
	}
}

func validTargetEndpoint() ecv1alpha1.EtcdMirrorEndpoint {
	return ecv1alpha1.EtcdMirrorEndpoint{
		ServiceRef: &ecv1alpha1.EtcdMirrorServiceRef{Name: "etcd-target-client"},
		Prefix:     "/mirrored/",
	}
}

// TestEtcdMirrorSyncPrefixCELValidation drives the sync.destPrefix /
// sync.noDestPrefix mutual-exclusion XValidation rule on EtcdMirrorSpec
// against the real envtest apiserver.
func TestEtcdMirrorSyncPrefixCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	tests := []struct {
		name      string
		sync      ecv1alpha1.EtcdMirrorSyncSpec
		wantApply bool
	}{
		{
			name:      "neither destPrefix nor noDestPrefix set accepted",
			sync:      ecv1alpha1.EtcdMirrorSyncSpec{},
			wantApply: true,
		},
		{
			name:      "destPrefix alone accepted",
			sync:      ecv1alpha1.EtcdMirrorSyncSpec{DestPrefix: "/other/"},
			wantApply: true,
		},
		{
			name:      "noDestPrefix alone accepted",
			sync:      ecv1alpha1.EtcdMirrorSyncSpec{NoDestPrefix: true},
			wantApply: true,
		},
		{
			name:      "destPrefix and noDestPrefix together rejected",
			sync:      ecv1alpha1.EtcdMirrorSyncSpec{DestPrefix: "/other/", NoDestPrefix: true},
			wantApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := &ecv1alpha1.EtcdMirror{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "cel-mirror-syncprefix-",
					Namespace:    "default",
				},
				Spec: ecv1alpha1.EtcdMirrorSpec{
					Source: validSourceEndpoint(),
					Target: validTargetEndpoint(),
					Sync:   tt.sync,
				},
			}
			err := k8sClient.Create(t.Context(), em)
			if tt.wantApply {
				require.NoError(t, err, "apiserver should accept a valid sync spec")
				_ = k8sClient.Delete(t.Context(), em, &client.DeleteOptions{})
			} else {
				assert.Error(t, err, "apiserver should reject destPrefix+noDestPrefix via CEL")
			}
		})
	}
}

// TestEtcdMirrorEndpointOneOfCELValidation drives the endpointList/serviceRef
// exactly-one-of XValidation rule on EtcdMirrorEndpoint, exercised on both
// Source and Target.
func TestEtcdMirrorEndpointOneOfCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	neither := ecv1alpha1.EtcdMirrorEndpoint{Prefix: "/x/"}
	both := ecv1alpha1.EtcdMirrorEndpoint{
		EndpointList: []string{"https://etcd.example.com:2379"},
		ServiceRef:   &ecv1alpha1.EtcdMirrorServiceRef{Name: "etcd-client"},
	}
	emptyList := ecv1alpha1.EtcdMirrorEndpoint{EndpointList: []string{}}

	tests := []struct {
		name      string
		source    ecv1alpha1.EtcdMirrorEndpoint
		target    ecv1alpha1.EtcdMirrorEndpoint
		wantApply bool
	}{
		{
			name:      "valid endpointList source, serviceRef target accepted",
			source:    validSourceEndpoint(),
			target:    validTargetEndpoint(),
			wantApply: true,
		},
		{
			name:      "source with neither endpointList nor serviceRef rejected",
			source:    neither,
			target:    validTargetEndpoint(),
			wantApply: false,
		},
		{
			name:      "source with both endpointList and serviceRef rejected",
			source:    both,
			target:    validTargetEndpoint(),
			wantApply: false,
		},
		{
			name:      "source with empty endpointList and no serviceRef rejected",
			source:    emptyList,
			target:    validTargetEndpoint(),
			wantApply: false,
		},
		{
			name:      "target with neither endpointList nor serviceRef rejected",
			source:    validSourceEndpoint(),
			target:    neither,
			wantApply: false,
		},
		{
			name:      "target with both endpointList and serviceRef rejected",
			source:    validSourceEndpoint(),
			target:    both,
			wantApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := &ecv1alpha1.EtcdMirror{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "cel-mirror-endpoint-",
					Namespace:    "default",
				},
				Spec: ecv1alpha1.EtcdMirrorSpec{
					Source: tt.source,
					Target: tt.target,
				},
			}
			err := k8sClient.Create(t.Context(), em)
			if tt.wantApply {
				require.NoError(t, err, "apiserver should accept exactly-one-of endpointList/serviceRef")
				_ = k8sClient.Delete(t.Context(), em, &client.DeleteOptions{})
			} else {
				assert.Error(t, err, "apiserver should reject endpointList/serviceRef oneOf violation via CEL")
			}
		})
	}
}

// TestEtcdMirrorTLSInsecureSkipVerifyCELValidation drives the
// insecureSkipVerify/insecureSkipVerifyAcknowledgeRisk companion-field
// XValidation rule on EtcdMirrorTLS.
func TestEtcdMirrorTLSInsecureSkipVerifyCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	secretRef := corev1.LocalObjectReference{Name: "etcd-mirror-tls"}

	tests := []struct {
		name      string
		tls       *ecv1alpha1.EtcdMirrorTLS
		wantApply bool
	}{
		{
			name:      "no TLS block accepted",
			tls:       nil,
			wantApply: true,
		},
		{
			name:      "TLS with verification enabled accepted",
			tls:       &ecv1alpha1.EtcdMirrorTLS{SecretRef: secretRef},
			wantApply: true,
		},
		{
			name: "insecureSkipVerify with acknowledgement accepted",
			tls: &ecv1alpha1.EtcdMirrorTLS{
				SecretRef:                         secretRef,
				InsecureSkipVerify:                true,
				InsecureSkipVerifyAcknowledgeRisk: true,
			},
			wantApply: true,
		},
		{
			name: "insecureSkipVerify without acknowledgement rejected",
			tls: &ecv1alpha1.EtcdMirrorTLS{
				SecretRef:          secretRef,
				InsecureSkipVerify: true,
			},
			wantApply: false,
		},
		{
			name: "acknowledgement without insecureSkipVerify accepted (not the risky case)",
			tls: &ecv1alpha1.EtcdMirrorTLS{
				SecretRef:                         secretRef,
				InsecureSkipVerifyAcknowledgeRisk: true,
			},
			wantApply: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := validSourceEndpoint()
			source.TLS = tt.tls
			em := &ecv1alpha1.EtcdMirror{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "cel-mirror-tls-",
					Namespace:    "default",
				},
				Spec: ecv1alpha1.EtcdMirrorSpec{
					Source: source,
					Target: validTargetEndpoint(),
				},
			}
			err := k8sClient.Create(t.Context(), em)
			if tt.wantApply {
				require.NoError(t, err, "apiserver should accept a valid TLS block")
				_ = k8sClient.Delete(t.Context(), em, &client.DeleteOptions{})
			} else {
				assert.Error(t, err, "apiserver should reject insecureSkipVerify without acknowledgement via CEL")
			}
		})
	}
}

// TestEtcdMirrorSecretRefNameRequiredCELValidation drives the
// "secretRef.name is required" XValidation rule shared by EtcdMirrorTLS and
// EtcdMirrorAuth, exercised on both the TLS and Auth blocks.
func TestEtcdMirrorSecretRefNameRequiredCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	tests := []struct {
		name      string
		tls       *ecv1alpha1.EtcdMirrorTLS
		auth      *ecv1alpha1.EtcdMirrorAuth
		wantApply bool
	}{
		{
			name:      "TLS secretRef with non-empty name accepted",
			tls:       &ecv1alpha1.EtcdMirrorTLS{SecretRef: corev1.LocalObjectReference{Name: "etcd-mirror-tls"}},
			wantApply: true,
		},
		{
			name:      "TLS secretRef with empty name rejected",
			tls:       &ecv1alpha1.EtcdMirrorTLS{SecretRef: corev1.LocalObjectReference{Name: ""}},
			wantApply: false,
		},
		{
			name:      "Auth secretRef with non-empty name accepted",
			auth:      &ecv1alpha1.EtcdMirrorAuth{SecretRef: corev1.LocalObjectReference{Name: "etcd-mirror-auth"}},
			wantApply: true,
		},
		{
			name:      "Auth secretRef with empty name rejected",
			auth:      &ecv1alpha1.EtcdMirrorAuth{SecretRef: corev1.LocalObjectReference{Name: ""}},
			wantApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := validSourceEndpoint()
			source.TLS = tt.tls
			source.Auth = tt.auth
			em := &ecv1alpha1.EtcdMirror{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "cel-mirror-secretref-",
					Namespace:    "default",
				},
				Spec: ecv1alpha1.EtcdMirrorSpec{
					Source: source,
					Target: validTargetEndpoint(),
				},
			}
			err := k8sClient.Create(t.Context(), em)
			if tt.wantApply {
				require.NoError(t, err, "apiserver should accept a non-empty secretRef.name")
				_ = k8sClient.Delete(t.Context(), em, &client.DeleteOptions{})
			} else {
				assert.Error(t, err, "apiserver should reject an empty secretRef.name via CEL")
			}
		})
	}
}
