package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.etcd.io/etcd-operator/api/v1alpha1"
)

func TestEtcdClusterHash(t *testing.T) {
	// Baseline configuration to compare mutations against
	baseCluster := &v1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-etcd", Namespace: "default"},
		Spec: v1alpha1.EtcdClusterSpec{
			Size:          3,
			ImageRegistry: "gcr.io/etcd-development/etcd",
			Version:       "v3.5.0",
			EtcdOptions:   []string{"--auto-compaction-retention=1"},
			PodTemplate: &v1alpha1.PodTemplate{
				Metadata: &v1alpha1.PodMetadata{
					Labels:      map[string]string{"app": "etcd"},
					Annotations: map[string]string{"prometheus.io/scrape": "true"},
				},
				Spec: &v1alpha1.PodSpec{
					NodeSelector: map[string]string{"hardware": "ssd"},
				},
			},
		},
	}

	// Helper inline functions to generate mutated clusters cleanly
	withSize := func(size int) *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.Size = size
		return c
	}
	withMetaChanged := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.PodTemplate.Metadata.Labels["canary"] = "true"
		c.Spec.PodTemplate.Metadata.Annotations["prometheus.io/port"] = "2379"
		return c
	}
	withOptionsChanged := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.EtcdOptions = []string{"--auto-compaction-retention=1", "--max-txn-ops=10000"}
		return c
	}
	withImageRegistryChanged := func() *v1alpha1.EtcdCluster {
		// ImageRegistry is a user-declarable spec field, so changing it must
		// trigger a pod roll via hash divergence.
		c := baseCluster.DeepCopy()
		c.Spec.ImageRegistry = "registry.example.com/etcd"
		return c
	}
	withNilTemplate := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.PodTemplate = nil
		return c
	}
	withVersionChanged := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.Version = "v8.8.8"
		return c
	}
	withPaused := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.Paused = true
		return c
	}
	withAllowVersionUpgradeOnRepair := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.AllowVersionUpgradeOnRepair = true
		return c
	}
	withStorageSpecChanged := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.StorageSpec = &v1alpha1.StorageSpec{
			AccessModes:       corev1.ReadWriteOnce,
			VolumeSizeRequest: resource.MustParse("10Gi"),
		}
		return c
	}
	withTLSChanged := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.TLS = &v1alpha1.TLSCertificate{Provider: "cert-manager"}
		return c
	}
	withPodSpecChanged := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.PodTemplate.Spec.Tolerations = []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpExists},
		}
		return c
	}
	withNilPodTemplate := func() *v1alpha1.EtcdCluster {
		c := baseCluster.DeepCopy()
		c.Spec.PodTemplate = nil
		return c
	}
	withPodTemplateMetadataChanged := func() *v1alpha1.EtcdCluster {
		c := withNilPodTemplate().DeepCopy()
		c.Spec.PodTemplate = &v1alpha1.PodTemplate{
			Metadata: &v1alpha1.PodMetadata{
				Labels: map[string]string{"foo": "bar"},
			},
		}
		return c
	}

	// Define our table-driven test cases
	tests := []struct {
		name         string
		baseCluster  *v1alpha1.EtcdCluster
		cluster      *v1alpha1.EtcdCluster
		expectEqual  bool
		expectedHash string // Used when checking explicit values, like base or exact match
		checkLength  bool
	}{
		{
			name:        "Deterministic - Identical configuration must return the identical hash",
			baseCluster: baseCluster,
			cluster:     baseCluster.DeepCopy(),
			expectEqual: true,
			checkLength: true,
		},
		{
			name:        "Size Bypass - Scaling the cluster size must NOT change the hash",
			baseCluster: baseCluster,
			cluster:     withSize(9),
			expectEqual: true,
		},
		{
			name:        "Metadata Bypass - Modifying pod metadata must NOT change the hash",
			baseCluster: baseCluster,
			cluster:     withMetaChanged(),
			expectEqual: true,
		},
		{
			name:        "Core Spec Change - Modifying EtcdOptions must force a brand new hash",
			baseCluster: baseCluster,
			cluster:     withOptionsChanged(),
			expectEqual: false,
		},
		{
			name:        "Nil Guard Check - A missing PodTemplate must not panic and must hash cleanly",
			baseCluster: baseCluster,
			cluster:     withNilTemplate(),
			expectEqual: false,
			checkLength: true,
		},
		{
			name:        "Version check - Modifying etcd version must NOT change the hash",
			baseCluster: baseCluster,
			cluster:     withVersionChanged(),
			expectEqual: true,
		},
		{
			name:        "Core Spec Change - Modifying ImageRegistry must force a brand new hash",
			baseCluster: baseCluster,
			cluster:     withImageRegistryChanged(),
			expectEqual: false,
		},
		{
			name:        "Core Spec Change - Modifying StorageSpec must force a brand new hash",
			baseCluster: baseCluster,
			cluster:     withStorageSpecChanged(),
			expectEqual: false,
		},
		{
			name:        "Core Spec Change - Modifying TLS configuration must force a brand new hash",
			baseCluster: baseCluster,
			cluster:     withTLSChanged(),
			expectEqual: false,
		},
		{
			name:        "Core Spec Change - Modifying PodTemplate scheduling spec must force a brand new hash",
			baseCluster: baseCluster,
			cluster:     withPodSpecChanged(),
			expectEqual: false,
		},
		{
			name:        "Paused Bypass - Toggling Paused must NOT change the hash",
			baseCluster: baseCluster,
			cluster:     withPaused(),
			expectEqual: true,
		},
		{
			name:        "AllowVersionUpgradeOnRepair Bypass - Toggling the repair-upgrade knob must NOT change the hash",
			baseCluster: baseCluster,
			cluster:     withAllowVersionUpgradeOnRepair(),
			expectEqual: true,
		},
		{
			name:        "PodTemplate.Metadata Bypass - Adding a metadata-only PodTemplate must NOT change the hash",
			baseCluster: withNilPodTemplate(),
			cluster:     withPodTemplateMetadataChanged(),
			expectEqual: true,
		},
	}

	// Loop through the table executing each row in isolation
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseHash := EtcdClusterHash(tt.baseCluster)
			currentHash := EtcdClusterHash(tt.cluster)

			if tt.checkLength {
				assert.Len(t, currentHash, hashLength, "Hash must respect the %d-character limit constraint", hashLength)
			}

			if tt.expectEqual {
				assert.Equal(t, baseHash, currentHash, "Hash should be identical to the baseline reference hash")
			} else {
				assert.NotEqual(t, baseHash, currentHash, "Hash should have changed compared to the baseline reference hash")
			}
		})
	}
}

// TestEtcdClusterHashPinned pins EtcdClusterHash's output for an empty
// EtcdClusterSpec, so that any future change to the EtcdClusterSpec schema
// fails this test. If it fails, revisit EtcdClusterHash to decide whether
// the schema change should affect the hash before updating
// expectedEmptySpecHash.
func TestEtcdClusterHashPinned(t *testing.T) {
	// Captured for an all-zero-value EtcdClusterSpec at commit
	// 5483df641bf23347d98a56b8d966bc2c495edcb5.
	const expectedEmptySpecHash = "f71305f40ce2"
	currentHash := EtcdClusterHash(&v1alpha1.EtcdCluster{Spec: v1alpha1.EtcdClusterSpec{}})
	assert.Equal(t, expectedEmptySpecHash, currentHash,
		"hash input changed, see comment above before updating this constant")
}
