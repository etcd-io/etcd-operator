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

package v1alpha1

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// causeMessages flattens the aggregated field errors carried by an
// apierrors.NewInvalid status into "<field>: <message>" strings so tests can
// assert on the exact user-facing rejection text.
func causeMessages(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		return nil
	}
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		t.Fatalf("expected an apierrors status error, got %T: %v", err, err)
	}
	var msgs []string
	if status.Status().Details != nil {
		for _, c := range status.Status().Details.Causes {
			msgs = append(msgs, c.Field+": "+c.Message)
		}
	}
	return msgs
}

func newCluster(size int, version string) *EtcdCluster {
	return &EtcdCluster{Spec: EtcdClusterSpec{Size: size, Version: version}}
}

func TestValidateCreate_Messages(t *testing.T) {
	v := &EtcdClusterCustomValidator{}

	tests := []struct {
		name        string
		cluster     *EtcdCluster
		wantOK      bool
		wantField   string
		wantMessage string // exact, fully-formed message text
	}{
		{
			name:    "valid odd size with semver version",
			cluster: newCluster(3, "3.6.1"),
			wantOK:  true,
		},
		{
			name:        "even size is rejected with quorum guidance",
			cluster:     newCluster(4, "3.6.1"),
			wantField:   "spec.size",
			wantMessage: "Invalid value: 4: size must be an odd number so the cluster can form a majority quorum; got 4. An even-sized etcd cluster tolerates no more failures than the next-smaller odd size while being more likely to lose quorum. Use 3 or 5 instead.",
		},
		{
			name:        "zero size is rejected",
			cluster:     newCluster(0, "3.6.1"),
			wantField:   "spec.size",
			wantMessage: "Invalid value: 0: size must be at least 1; got 0. Set spec.size to a positive odd number (e.g. 1, 3, or 5).",
		},
		{
			name:        "empty version is rejected as required",
			cluster:     newCluster(1, ""),
			wantField:   "spec.version",
			wantMessage: "Required value: version is required; set spec.version to a semver etcd image tag such as \"3.6.1\".",
		},
		{
			name:        "non-semver version is rejected",
			cluster:     newCluster(1, "latest"),
			wantField:   "spec.version",
			wantMessage: "Invalid value: \"latest\": version \"latest\" is not a valid semantic version (expected MAJOR.MINOR.PATCH, e.g. \"3.6.1\"): latest is not in dotted-tri format.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.ValidateCreate(context.Background(), tc.cluster)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected admission to succeed, got error: %v", err)
				}
				return
			}
			msgs := causeMessages(t, err)
			joined := strings.Join(msgs, " | ")
			found := false
			for _, m := range msgs {
				if m == tc.wantField+": "+tc.wantMessage {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("did not find expected cause.\n  want: %s: %s\n  got:  %s",
					tc.wantField, tc.wantMessage, joined)
			}
		})
	}
}

func TestValidateTLS_Messages(t *testing.T) {
	v := &EtcdClusterCustomValidator{}

	tests := []struct {
		name        string
		tls         *TLSCertificate
		wantOK      bool
		wantMessage string
	}{
		{
			name:   "auto provider with no config is valid",
			tls:    &TLSCertificate{Provider: "auto"},
			wantOK: true,
		},
		{
			name:   "empty provider is treated as auto and is valid",
			tls:    &TLSCertificate{},
			wantOK: true,
		},
		{
			name:        "unknown provider lists the supported choices",
			tls:         &TLSCertificate{Provider: "vault"},
			wantMessage: "spec.tls.provider: Unsupported value: \"vault\": supported values: \"auto\", \"cert-manager\"",
		},
		{
			name:        "cert-manager provider without config block",
			tls:         &TLSCertificate{Provider: "cert-manager"},
			wantMessage: "spec.tls.providerCfg.certManagerCfg: Required value: providerCfg.certManagerCfg is required when provider is \"cert-manager\"; supply issuerKind and issuerName.",
		},
		{
			name: "cert-manager provider missing issuerName",
			tls: &TLSCertificate{
				Provider:    "cert-manager",
				ProviderCfg: ProviderConfig{CertManagerCfg: &ProviderCertManagerConfig{IssuerKind: "Issuer"}},
			},
			wantMessage: "spec.tls.providerCfg.certManagerCfg.issuerName: Required value: issuerName is required for the cert-manager provider; set it to the name of an Issuer or ClusterIssuer.",
		},
		{
			name: "cert-manager provider with bad issuerKind",
			tls: &TLSCertificate{
				Provider:    "cert-manager",
				ProviderCfg: ProviderConfig{CertManagerCfg: &ProviderCertManagerConfig{IssuerKind: "Bogus", IssuerName: "ca"}},
			},
			wantMessage: "spec.tls.providerCfg.certManagerCfg.issuerKind: Unsupported value: \"Bogus\": supported values: \"Issuer\", \"ClusterIssuer\"",
		},
		{
			name: "auto provider must not carry cert-manager config",
			tls: &TLSCertificate{
				Provider:    "auto",
				ProviderCfg: ProviderConfig{CertManagerCfg: &ProviderCertManagerConfig{IssuerName: "ca"}},
			},
			wantMessage: "spec.tls.providerCfg.certManagerCfg: Invalid value: \"<set>\": providerCfg.certManagerCfg must not be set when provider is \"auto\"; either remove providerCfg.certManagerCfg or set provider to \"cert-manager\".",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(1, "3.6.1")
			c.Spec.TLS = tc.tls
			_, err := v.ValidateCreate(context.Background(), c)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected admission to succeed, got: %v", err)
				}
				return
			}
			msgs := causeMessages(t, err)
			found := false
			for _, m := range msgs {
				if m == tc.wantMessage {
					found = true
				}
			}
			if !found {
				t.Fatalf("did not find expected TLS cause.\n  want: %s\n  got:  %s",
					tc.wantMessage, strings.Join(msgs, " | "))
			}
		})
	}
}

func TestValidateUpdate_UpgradePathAndImmutability(t *testing.T) {
	v := &EtcdClusterCustomValidator{}

	tests := []struct {
		name        string
		old         *EtcdCluster
		updated     *EtcdCluster
		wantOK      bool
		wantMessage string
	}{
		{
			name:    "same version no-op is allowed",
			old:     newCluster(3, "3.6.1"),
			updated: newCluster(3, "3.6.2"),
			wantOK:  true,
		},
		{
			name:    "single minor upgrade is allowed",
			old:     newCluster(3, "3.5.1"),
			updated: newCluster(3, "3.6.1"),
			wantOK:  true,
		},
		{
			name:        "skip-minor upgrade is rejected",
			old:         newCluster(3, "3.5.1"),
			updated:     newCluster(3, "3.7.1"),
			wantMessage: "spec.version: Invalid value: \"3.7.1\": upgrading from version 3.5.1 to version 3.7.1 is not allowed (skips a minor version). etcd only supports sequential single-minor upgrades and forbids downgrades; upgrade one minor version at a time (current \"3.5.1\" -> target \"3.7.1\").",
		},
		{
			name:        "downgrade is rejected",
			old:         newCluster(3, "3.6.1"),
			updated:     newCluster(3, "3.5.1"),
			wantMessage: "spec.version: Invalid value: \"3.5.1\": downgrading from version 3.6.1 to version 3.5.1 is not allowed. etcd only supports sequential single-minor upgrades and forbids downgrades; upgrade one minor version at a time (current \"3.6.1\" -> target \"3.5.1\").",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.ValidateUpdate(context.Background(), tc.old, tc.updated)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected update to succeed, got: %v", err)
				}
				return
			}
			msgs := causeMessages(t, err)
			found := false
			for _, m := range msgs {
				if m == tc.wantMessage {
					found = true
				}
			}
			if !found {
				t.Fatalf("did not find expected upgrade cause.\n  want: %s\n  got:  %s",
					tc.wantMessage, strings.Join(msgs, " | "))
			}
		})
	}
}

func TestValidateUpdate_StorageSpecImmutable(t *testing.T) {
	v := &EtcdClusterCustomValidator{}

	withStorage := func() *EtcdCluster {
		c := newCluster(3, "3.6.1")
		c.Spec.StorageSpec = &StorageSpec{StorageClassName: "standard"}
		return c
	}

	t.Run("removing storageSpec is rejected", func(t *testing.T) {
		_, err := v.ValidateUpdate(context.Background(), withStorage(), newCluster(3, "3.6.1"))
		want := "spec.storageSpec: Invalid value: null: storageSpec is immutable and cannot be removed once set; restore the original storageSpec or recreate the cluster."
		assertCause(t, err, want)
	})

	t.Run("changing storageSpec is rejected", func(t *testing.T) {
		updated := withStorage()
		updated.Spec.StorageSpec.StorageClassName = "fast"
		_, err := v.ValidateUpdate(context.Background(), withStorage(), updated)
		msgs := causeMessages(t, err)
		ok := false
		for _, m := range msgs {
			if strings.HasPrefix(m, "spec.storageSpec: Invalid value:") &&
				strings.Contains(m, "storageSpec is immutable and cannot be changed once set") {
				ok = true
			}
		}
		if !ok {
			t.Fatalf("expected storageSpec immutability error, got: %s", strings.Join(msgs, " | "))
		}
	})

	t.Run("unchanged storageSpec is allowed", func(t *testing.T) {
		if _, err := v.ValidateUpdate(context.Background(), withStorage(), withStorage()); err != nil {
			t.Fatalf("expected unchanged storageSpec to be allowed, got: %v", err)
		}
	})
}

func assertCause(t *testing.T, err error, want string) {
	t.Helper()
	msgs := causeMessages(t, err)
	for _, m := range msgs {
		if m == want {
			return
		}
	}
	t.Fatalf("did not find expected cause.\n  want: %s\n  got:  %s", want, strings.Join(msgs, " | "))
}

func TestDefault_TLSProvider(t *testing.T) {
	d := &EtcdClusterCustomDefaulter{}

	t.Run("empty provider defaults to auto", func(t *testing.T) {
		c := newCluster(3, "3.6.1")
		c.Spec.TLS = &TLSCertificate{}
		if err := d.Default(context.Background(), c); err != nil {
			t.Fatalf("Default returned error: %v", err)
		}
		if c.Spec.TLS.Provider != "auto" {
			t.Fatalf("expected provider to default to \"auto\", got %q", c.Spec.TLS.Provider)
		}
	})

	t.Run("explicit provider is preserved", func(t *testing.T) {
		c := newCluster(3, "3.6.1")
		c.Spec.TLS = &TLSCertificate{Provider: "cert-manager"}
		if err := d.Default(context.Background(), c); err != nil {
			t.Fatalf("Default returned error: %v", err)
		}
		if c.Spec.TLS.Provider != "cert-manager" {
			t.Fatalf("expected provider preserved, got %q", c.Spec.TLS.Provider)
		}
	})

	t.Run("nil TLS is left untouched", func(t *testing.T) {
		c := newCluster(3, "3.6.1")
		if err := d.Default(context.Background(), c); err != nil {
			t.Fatalf("Default returned error: %v", err)
		}
		if c.Spec.TLS != nil {
			t.Fatalf("expected TLS to stay nil")
		}
	})
}
