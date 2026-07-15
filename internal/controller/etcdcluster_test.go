package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

	// Calculate base hash once to use as a benchmark in assertions
	baseHash := EtcdClusterHash(baseCluster)

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

	// Define our table-driven test cases
	tests := []struct {
		name         string
		cluster      *v1alpha1.EtcdCluster
		expectEqual  bool
		expectedHash string // Used when checking explicit values, like base or exact match
		checkLength  bool
	}{
		{
			name:        "Deterministic - Identical configuration must return the identical hash",
			cluster:     baseCluster.DeepCopy(),
			expectEqual: true,
			checkLength: true,
		},
		{
			name:        "Size Bypass - Scaling the cluster size must NOT change the hash",
			cluster:     withSize(9),
			expectEqual: true,
		},
		{
			name:        "Metadata Bypass - Modifying pod metadata must NOT change the hash",
			cluster:     withMetaChanged(),
			expectEqual: true,
		},
		{
			name:        "Core Spec Change - Modifying EtcdOptions must force a brand new hash",
			cluster:     withOptionsChanged(),
			expectEqual: false,
		},
		{
			name:        "Nil Guard Check - A missing PodTemplate must not panic and must hash cleanly",
			cluster:     withNilTemplate(),
			expectEqual: false,
			checkLength: true,
		},
		{
			name:        "Version check - Modifying etcd version must NOT change the hash",
			cluster:     withVersionChanged(),
			expectEqual: true,
		},
	}

	// Loop through the table executing each row in isolation
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
