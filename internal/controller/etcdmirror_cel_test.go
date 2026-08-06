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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// validSourceEndpoint and validTargetEndpoint are baseline endpoints that
// satisfy EtcdMirrorEndpoint's own CEL rules (oneOf, scheme-vs-TLS), so each
// test case only needs to perturb the field(s) it's actually exercising. The
// source endpoint is deliberately scheme-less so TLS blocks can be added or
// removed freely without tripping the scheme rules.
func validSourceEndpoint() ecv1alpha1.EtcdMirrorEndpoint {
	return ecv1alpha1.EtcdMirrorEndpoint{
		EndpointList: []string{"etcd-source.example.com:2379"},
		Prefix:       "/registry/",
	}
}

func validTargetEndpoint() ecv1alpha1.EtcdMirrorEndpoint {
	return ecv1alpha1.EtcdMirrorEndpoint{
		ServiceRef: &ecv1alpha1.EtcdMirrorServiceRef{Name: "etcd-target-client"},
		Prefix:     "/mirrored/",
	}
}

func newMirror(prefix string, spec ecv1alpha1.EtcdMirrorSpec) *ecv1alpha1.EtcdMirror {
	return &ecv1alpha1.EtcdMirror{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: prefix,
			Namespace:    "default",
		},
		Spec: spec,
	}
}

// createAndCheck applies the mirror and asserts admission matched wantApply,
// deleting it again on success.
func createAndCheck(t *testing.T, em *ecv1alpha1.EtcdMirror, wantApply bool, msg string) {
	t.Helper()
	err := k8sClient.Create(t.Context(), em)
	if wantApply {
		require.NoError(t, err, msg)
		_ = k8sClient.Delete(t.Context(), em, &client.DeleteOptions{})
	} else {
		assert.Error(t, err, msg)
	}
}

// TestEtcdMirrorEndpointOneOfCELValidation drives the endpointList/serviceRef
// exactly-one-of XValidation rule on EtcdMirrorEndpoint, exercised on both
// Source and Target. An empty endpointList is deliberately treated as unset
// (k8s list conventions), so [] alongside a serviceRef must be ACCEPTED.
func TestEtcdMirrorEndpointOneOfCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	neither := ecv1alpha1.EtcdMirrorEndpoint{Prefix: "/x/"}
	both := ecv1alpha1.EtcdMirrorEndpoint{
		EndpointList: []string{"etcd.example.com:2379"},
		ServiceRef:   &ecv1alpha1.EtcdMirrorServiceRef{Name: "etcd-client"},
	}
	emptyListOnly := ecv1alpha1.EtcdMirrorEndpoint{EndpointList: []string{}}
	emptyListWithServiceRef := ecv1alpha1.EtcdMirrorEndpoint{
		EndpointList: []string{},
		ServiceRef:   &ecv1alpha1.EtcdMirrorServiceRef{Name: "etcd-client"},
	}

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
			source:    emptyListOnly,
			target:    validTargetEndpoint(),
			wantApply: false,
		},
		{
			// Pinned deliberately: empty list == unset, so this is the
			// serviceRef-only case, not the both-set case.
			name:      "source with empty endpointList plus serviceRef accepted",
			source:    emptyListWithServiceRef,
			target:    validTargetEndpoint(),
			wantApply: true,
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
			em := newMirror("cel-mirror-endpoint-", ecv1alpha1.EtcdMirrorSpec{
				Source: tt.source,
				Target: tt.target,
			})
			createAndCheck(t, em, tt.wantApply, "endpointList/serviceRef oneOf")
		})
	}
}

// TestEtcdMirrorEndpointSchemeTLSCELValidation drives the two scheme-vs-TLS
// XValidation rules on EtcdMirrorEndpoint: http:// forbids a tls block,
// https:// requires one (an empty tls block means system trust roots).
func TestEtcdMirrorEndpointSchemeTLSCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	tlsBlock := &ecv1alpha1.EtcdMirrorTLS{
		SecretRef: &corev1.LocalObjectReference{Name: "etcd-mirror-tls"},
	}

	tests := []struct {
		name      string
		endpoints []string
		tls       *ecv1alpha1.EtcdMirrorTLS
		wantApply bool
	}{
		{
			name:      "https endpoint with tls block accepted",
			endpoints: []string{"https://etcd.example.com:2379"},
			tls:       tlsBlock,
			wantApply: true,
		},
		{
			name:      "https endpoint with empty tls block (system roots) accepted",
			endpoints: []string{"https://etcd.example.com:2379"},
			tls:       &ecv1alpha1.EtcdMirrorTLS{},
			wantApply: true,
		},
		{
			name:      "https endpoint without tls block rejected",
			endpoints: []string{"https://etcd.example.com:2379"},
			tls:       nil,
			wantApply: false,
		},
		{
			name:      "http endpoint without tls block accepted",
			endpoints: []string{"http://etcd.example.com:2379"},
			tls:       nil,
			wantApply: true,
		},
		{
			name:      "http endpoint with tls block rejected",
			endpoints: []string{"http://etcd.example.com:2379"},
			tls:       tlsBlock,
			wantApply: false,
		},
		{
			name:      "mixed http and https endpoints with tls block rejected",
			endpoints: []string{"https://a.example.com:2379", "http://b.example.com:2379"},
			tls:       tlsBlock,
			wantApply: false,
		},
		{
			name:      "scheme-less endpoint with tls block accepted",
			endpoints: []string{"etcd.example.com:2379"},
			tls:       tlsBlock,
			wantApply: true,
		},
		{
			name:      "scheme-less endpoint without tls block accepted",
			endpoints: []string{"etcd.example.com:2379"},
			tls:       nil,
			wantApply: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := ecv1alpha1.EtcdMirrorEndpoint{
				EndpointList: tt.endpoints,
				Prefix:       "/registry/",
				TLS:          tt.tls,
			}
			em := newMirror("cel-mirror-scheme-", ecv1alpha1.EtcdMirrorSpec{
				Source: source,
				Target: validTargetEndpoint(),
			})
			createAndCheck(t, em, tt.wantApply, "scheme-vs-TLS rule on source")
		})
	}

	t.Run("scheme rules also apply to target", func(t *testing.T) {
		em := newMirror("cel-mirror-scheme-", ecv1alpha1.EtcdMirrorSpec{
			Source: validSourceEndpoint(),
			Target: ecv1alpha1.EtcdMirrorEndpoint{
				EndpointList: []string{"https://etcd-target.example.com:2379"},
				Prefix:       "/mirrored/",
			},
		})
		createAndCheck(t, em, false, "https target without tls must be rejected")
	})
}

// TestEtcdMirrorTLSInsecureSkipVerifyCELValidation drives the
// insecureSkipVerify/insecureSkipVerifyAcknowledgeRisk companion-field
// XValidation rule on EtcdMirrorTLS.
func TestEtcdMirrorTLSInsecureSkipVerifyCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	secretRef := &corev1.LocalObjectReference{Name: "etcd-mirror-tls"}

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
			name: "insecureSkipVerify with acknowledgement and no secretRef accepted",
			tls: &ecv1alpha1.EtcdMirrorTLS{
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
			em := newMirror("cel-mirror-tls-", ecv1alpha1.EtcdMirrorSpec{
				Source: source,
				Target: validTargetEndpoint(),
			})
			createAndCheck(t, em, tt.wantApply, "insecureSkipVerify companion rule")
		})
	}
}

// TestEtcdMirrorSecretRefCELValidation drives the secretRef rules: on TLS the
// secretRef is optional (nil = system trust roots) but must have a non-empty
// name when present; on Auth it is required with a non-empty name.
func TestEtcdMirrorSecretRefCELValidation(t *testing.T) {
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
			tls:       &ecv1alpha1.EtcdMirrorTLS{SecretRef: &corev1.LocalObjectReference{Name: "etcd-mirror-tls"}},
			wantApply: true,
		},
		{
			name:      "TLS with nil secretRef (system trust roots) accepted",
			tls:       &ecv1alpha1.EtcdMirrorTLS{},
			wantApply: true,
		},
		{
			name:      "TLS secretRef with empty name rejected",
			tls:       &ecv1alpha1.EtcdMirrorTLS{SecretRef: &corev1.LocalObjectReference{Name: ""}},
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
			em := newMirror("cel-mirror-secretref-", ecv1alpha1.EtcdMirrorSpec{
				Source: source,
				Target: validTargetEndpoint(),
			})
			createAndCheck(t, em, tt.wantApply, "secretRef name rules")
		})
	}
}

// TestEtcdMirrorCABundleRefValidation drives EtcdMirrorCABundleRef's schema
// validation (kind enum, required name).
func TestEtcdMirrorCABundleRefValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	tests := []struct {
		name      string
		ref       *ecv1alpha1.EtcdMirrorCABundleRef
		wantApply bool
	}{
		{
			name:      "configMap caBundleRef accepted",
			ref:       &ecv1alpha1.EtcdMirrorCABundleRef{Kind: "ConfigMap", Name: "mirror-trust"},
			wantApply: true,
		},
		{
			name:      "secret caBundleRef with key accepted",
			ref:       &ecv1alpha1.EtcdMirrorCABundleRef{Kind: "Secret", Name: "mirror-trust", Key: "bundle.pem"},
			wantApply: true,
		},
		{
			name:      "caBundleRef with empty name rejected",
			ref:       &ecv1alpha1.EtcdMirrorCABundleRef{Kind: "ConfigMap", Name: ""},
			wantApply: false,
		},
		{
			name:      "caBundleRef with bogus kind rejected",
			ref:       &ecv1alpha1.EtcdMirrorCABundleRef{Kind: "DaemonSet", Name: "mirror-trust"},
			wantApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := validSourceEndpoint()
			source.TLS = &ecv1alpha1.EtcdMirrorTLS{CABundleRef: tt.ref}
			em := newMirror("cel-mirror-cabundle-", ecv1alpha1.EtcdMirrorSpec{
				Source: source,
				Target: validTargetEndpoint(),
			})
			createAndCheck(t, em, tt.wantApply, "caBundleRef schema rules")
		})
	}
}

// TestEtcdMirrorInitialSyncCELValidation drives the initialSync rules: the
// mode enum, and the startRevision-requires-Overwrite guard (a target seeded
// via snapshot restore cannot also be required empty).
func TestEtcdMirrorInitialSyncCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	tests := []struct {
		name        string
		initialSync *ecv1alpha1.EtcdMirrorInitialSyncSpec
		wantApply   bool
	}{
		{
			name:        "nil initialSync accepted",
			initialSync: nil,
			wantApply:   true,
		},
		{
			name:        "explicit RequireEmpty accepted",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{Mode: ecv1alpha1.EtcdMirrorInitialSyncRequireEmpty},
			wantApply:   true,
		},
		{
			name:        "Overwrite accepted",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{Mode: ecv1alpha1.EtcdMirrorInitialSyncOverwrite},
			wantApply:   true,
		},
		{
			name:        "OverwriteAndPrune accepted",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{Mode: ecv1alpha1.EtcdMirrorInitialSyncOverwriteAndPrune},
			wantApply:   true,
		},
		{
			name:        "bogus mode rejected by enum",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{Mode: "TruncateFirst"},
			wantApply:   false,
		},
		{
			name: "startRevision with Overwrite accepted",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{
				Mode:          ecv1alpha1.EtcdMirrorInitialSyncOverwrite,
				StartRevision: 42,
			},
			wantApply: true,
		},
		{
			name: "startRevision with OverwriteAndPrune accepted",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{
				Mode:          ecv1alpha1.EtcdMirrorInitialSyncOverwriteAndPrune,
				StartRevision: 42,
			},
			wantApply: true,
		},
		{
			name: "startRevision with explicit RequireEmpty rejected",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{
				Mode:          ecv1alpha1.EtcdMirrorInitialSyncRequireEmpty,
				StartRevision: 42,
			},
			wantApply: false,
		},
		{
			name: "startRevision with defaulted mode (RequireEmpty) rejected",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{
				StartRevision: 42,
			},
			wantApply: false,
		},
		{
			name: "startRevision zero with RequireEmpty accepted",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{
				Mode:          ecv1alpha1.EtcdMirrorInitialSyncRequireEmpty,
				StartRevision: 0,
			},
			wantApply: true,
		},
		{
			name: "negative startRevision rejected by minimum",
			initialSync: &ecv1alpha1.EtcdMirrorInitialSyncSpec{
				Mode:          ecv1alpha1.EtcdMirrorInitialSyncOverwrite,
				StartRevision: -1,
			},
			wantApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := newMirror("cel-mirror-initialsync-", ecv1alpha1.EtcdMirrorSpec{
				Source:      validSourceEndpoint(),
				Target:      validTargetEndpoint(),
				InitialSync: tt.initialSync,
			})
			createAndCheck(t, em, tt.wantApply, "initialSync rules")
		})
	}
}

// TestEtcdMirrorModeValidation drives the spec.mode enum.
func TestEtcdMirrorModeValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	tests := []struct {
		name      string
		mode      ecv1alpha1.EtcdMirrorMode
		wantApply bool
	}{
		{name: "mode unset accepted (defaults to Sync)", mode: "", wantApply: true},
		{name: "mode Sync accepted", mode: ecv1alpha1.EtcdMirrorModeSync, wantApply: true},
		{name: "mode Drain accepted", mode: ecv1alpha1.EtcdMirrorModeDrain, wantApply: true},
		{name: "bogus mode rejected", mode: "Bidirectional", wantApply: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := newMirror("cel-mirror-mode-", ecv1alpha1.EtcdMirrorSpec{
				Mode:   tt.mode,
				Source: validSourceEndpoint(),
				Target: validTargetEndpoint(),
			})
			createAndCheck(t, em, tt.wantApply, "spec.mode enum")
		})
	}
}

// TestEtcdMirrorImmutabilityCELValidation drives the CEL transition rules:
// source.prefix, target.prefix, sync.destPrefix, sync.excludePrefixes and
// checkpoint.key are immutable; endpoints (and ordinary fields) stay mutable
// — same-cluster endpoint rotation is routine and cross-cluster repoints are
// caught at runtime by the checkpoint's cluster-ID binding, not by spec
// validation.
func TestEtcdMirrorImmutabilityCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	baseSpec := func() ecv1alpha1.EtcdMirrorSpec {
		return ecv1alpha1.EtcdMirrorSpec{
			Source: validSourceEndpoint(),
			Target: validTargetEndpoint(),
			Sync: ecv1alpha1.EtcdMirrorSyncSpec{
				DestPrefix:      "registry/",
				ExcludePrefixes: []string{"/registry/events/"},
			},
			Checkpoint: &ecv1alpha1.EtcdMirrorCheckpointSpec{
				// Must live under the effective destination prefix
				// (target.prefix "/mirrored/" + destPrefix "registry/").
				Key: "/mirrored/registry/\x00etcdmirror-checkpoint",
			},
		}
	}

	tests := []struct {
		name       string
		mutate     func(em *ecv1alpha1.EtcdMirror)
		wantUpdate bool
	}{
		{
			name:       "changing source.prefix rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Source.Prefix = "/other/" },
			wantUpdate: false,
		},
		{
			name:       "unsetting source.prefix rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Source.Prefix = "" },
			wantUpdate: false,
		},
		{
			name:       "changing target.prefix rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Target.Prefix = "/elsewhere/" },
			wantUpdate: false,
		},
		{
			name:       "changing sync.destPrefix rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Sync.DestPrefix = "moved/" },
			wantUpdate: false,
		},
		{
			name:       "unsetting sync.destPrefix rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Sync.DestPrefix = "" },
			wantUpdate: false,
		},
		{
			name:       "changing checkpoint.key rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Checkpoint.Key = "/mirrored/registry/\x00other" },
			wantUpdate: false,
		},
		{
			name: "changing sync.excludePrefixes rejected",
			mutate: func(em *ecv1alpha1.EtcdMirror) {
				em.Spec.Sync.ExcludePrefixes = []string{"/registry/leases/"}
			},
			wantUpdate: false,
		},
		{
			name:       "removing sync.excludePrefixes rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Sync.ExcludePrefixes = nil },
			wantUpdate: false,
		},
		{
			name:       "removing checkpoint block (unsetting key) rejected",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Checkpoint = nil },
			wantUpdate: false,
		},
		{
			name: "changing source endpoints accepted (endpoints deliberately mutable)",
			mutate: func(em *ecv1alpha1.EtcdMirror) {
				em.Spec.Source.EndpointList = []string{"etcd-source-b.example.com:2379", "etcd-source-c.example.com:2379"}
			},
			wantUpdate: true,
		},
		{
			name:       "changing paused accepted",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Paused = true },
			wantUpdate: true,
		},
		{
			name:       "changing sync.maxOpsPerSecond accepted",
			mutate:     func(em *ecv1alpha1.EtcdMirror) { em.Spec.Sync.MaxOpsPerSecond = 250 },
			wantUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := newMirror("cel-mirror-immutable-", baseSpec())
			require.NoError(t, k8sClient.Create(t.Context(), em), "baseline mirror must be accepted")
			defer func() { _ = k8sClient.Delete(t.Context(), em, &client.DeleteOptions{}) }()

			tt.mutate(em)
			err := k8sClient.Update(t.Context(), em)
			if tt.wantUpdate {
				assert.NoError(t, err, "update should be accepted")
			} else {
				assert.Error(t, err, "update should be rejected by transition rule")
			}
		})
	}

	t.Run("setting sync.destPrefix from unset rejected", func(t *testing.T) {
		spec := baseSpec()
		spec.Sync = ecv1alpha1.EtcdMirrorSyncSpec{}
		spec.Checkpoint = nil
		em := newMirror("cel-mirror-immutable-", spec)
		require.NoError(t, k8sClient.Create(t.Context(), em))
		defer func() { _ = k8sClient.Delete(t.Context(), em, &client.DeleteOptions{}) }()

		em.Spec.Sync.DestPrefix = "late/"
		assert.Error(t, k8sClient.Update(t.Context(), em), "unset -> set is still a destPrefix mutation")
	})

	// Present-empty and absent are the same VALUE for these fields (and Go
	// typed clients drop "" through omitempty), so a CR created from YAML
	// with an explicit destPrefix: "" must stay updatable via typed clients
	// — presence-based transition rules would 422 every such update on a
	// field it never touched.
	t.Run("explicit empty destPrefix stays typed-client updatable", func(t *testing.T) {
		u := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "operator.etcd.io/v1alpha1",
			"kind":       "EtcdMirror",
			"metadata": map[string]interface{}{
				"generateName": "cel-mirror-emptydest-",
				"namespace":    "default",
			},
			"spec": map[string]interface{}{
				"source": map[string]interface{}{
					"endpointList": []interface{}{"etcd-source.example.com:2379"},
					"prefix":       "/registry/",
				},
				"target": map[string]interface{}{
					"serviceRef": map[string]interface{}{"name": "etcd-target-client"},
					"prefix":     "/mirrored/",
				},
				"sync": map[string]interface{}{
					"destPrefix": "", // stored present-but-empty
				},
			},
		}}
		require.NoError(t, k8sClient.Create(t.Context(), u), "explicit empty destPrefix must be accepted")
		defer func() { _ = k8sClient.Delete(t.Context(), u, &client.DeleteOptions{}) }()

		em := &ecv1alpha1.EtcdMirror{}
		require.NoError(t, k8sClient.Get(t.Context(),
			client.ObjectKey{Namespace: "default", Name: u.GetName()}, em))
		em.Spec.Paused = true // typed round-trip drops destPrefix to absent
		assert.NoError(t, k8sClient.Update(t.Context(), em),
			"a typed-client update must not be rejected for a field it never touched")
	})
}

// TestEtcdMirrorCheckpointKeyCELValidation drives the create-time rule that
// checkpoint.key must live under the effective destination prefix: the
// engine rejects anything else permanently at first start, and the key's own
// immutability would otherwise make the Failed CR unrepairable in place.
func TestEtcdMirrorCheckpointKeyCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	tests := []struct {
		name      string
		spec      ecv1alpha1.EtcdMirrorSpec
		wantApply bool
	}{
		{
			name: "key under effective destination prefix accepted",
			spec: ecv1alpha1.EtcdMirrorSpec{
				Source: validSourceEndpoint(),
				Target: validTargetEndpoint(),
				Checkpoint: &ecv1alpha1.EtcdMirrorCheckpointSpec{
					Key: "/mirrored/\x00my-checkpoint",
				},
			},
			wantApply: true,
		},
		{
			name: "key outside destination prefix rejected",
			spec: ecv1alpha1.EtcdMirrorSpec{
				Source: validSourceEndpoint(),
				Target: validTargetEndpoint(),
				Checkpoint: &ecv1alpha1.EtcdMirrorCheckpointSpec{
					Key: "/checkpoints/mirror-a",
				},
			},
			wantApply: false,
		},
		{
			name: "key under target.prefix but outside destPrefix rejected",
			spec: ecv1alpha1.EtcdMirrorSpec{
				Source: validSourceEndpoint(),
				Target: validTargetEndpoint(),
				Sync:   ecv1alpha1.EtcdMirrorSyncSpec{DestPrefix: "registry/"},
				Checkpoint: &ecv1alpha1.EtcdMirrorCheckpointSpec{
					Key: "/mirrored/\x00etcdmirror-checkpoint",
				},
			},
			wantApply: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := newMirror("cel-mirror-ckptkey-", tt.spec)
			createAndCheck(t, em, tt.wantApply, "checkpoint.key placement")
		})
	}
}
