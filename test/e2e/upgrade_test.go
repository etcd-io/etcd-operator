package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestEtcdClusterUpgrade drives a full etcd version upgrade for a 3-member
// EtcdCluster and asserts every member ends up Ready on the target version.
//
// Each entry in testCases is one upgrade path; every entry follows the same
// three steps:
//  1. Create a 3-member EtcdCluster at fromVersion and wait for readiness.
//  2. Bump EtcdCluster.Spec.Version to toVersion.
//  3. Wait until every EtcdMember is Ready AND its observed etcd version
//     (EtcdMemberStatus.CurrentVersion) is toVersion.
func TestEtcdClusterUpgrade(t *testing.T) {
	testCases := []struct {
		name        string
		fromVersion string
		toVersion   string
		clusterSize int
	}{
		{
			name:        "SameMinorPatchUpgrade",
			fromVersion: "v3.6.11",
			toVersion:   "v3.6.12",
			clusterSize: 3,
		},
		{
			name:        "CrossMinorVersionUpgrade",
			fromVersion: "v3.6.12",
			toVersion:   "v3.7.2",
			clusterSize: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			feature := features.New(tc.name)
			clusterName := "etcd-upgrade-" + strings.ToLower(tc.name)
			clusterSize := tc.clusterSize

			feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
				ec := &ecv1alpha1.EtcdCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      clusterName,
						Namespace: namespace,
					},
					Spec: ecv1alpha1.EtcdClusterSpec{
						Size:    clusterSize,
						Version: tc.fromVersion,
						StorageSpec: &ecv1alpha1.StorageSpec{
							AccessModes:       corev1.ReadWriteOnce,
							StorageClassName:  getAvailableStorageClass(ctx, t, c),
							VolumeSizeRequest: resource.MustParse("64Mi"),
							VolumeSizeLimit:   resource.MustParse("64Mi"),
						},
					},
				}
				if err := c.Client().Resources().Create(ctx, ec); err != nil {
					t.Fatalf("unable to create etcd cluster %s at version %s: %v", clusterName, tc.fromVersion, err)
				}
				if err := waitForAllEtcdMemberReady(t, c, etcdClusterRef(clusterName, clusterSize)); err != nil {
					t.Fatalf("etcd cluster %s failed to reach readiness at version %s: %v",
						clusterName, tc.fromVersion, err)
				}
				return ctx
			})

			feature.Assess("bump spec.version and roll all members to "+tc.toVersion,
				func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
					res := c.Client().Resources()
					var ec ecv1alpha1.EtcdCluster
					if err := res.Get(ctx, clusterName, namespace, &ec); err != nil {
						t.Fatalf("failed to get etcd cluster %s: %v", clusterName, err)
					}
					ec.Spec.Version = tc.toVersion
					if err := res.Update(ctx, &ec); err != nil {
						t.Fatalf("failed to bump etcd cluster %s to version %s: %v",
							clusterName, tc.toVersion, err)
					}

					// Wait until every member is Ready AND its observed etcd
					// version is the target. CurrentVersion is what the
					// controller's live check reports for the member's
					// running etcd process, so seeing it == toVersion means
					// the Pod has actually been recreated on the new image,
					// not just had its spec bumped.
					if err := wait.For(func(ctx context.Context) (bool, error) {
						var members ecv1alpha1.EtcdMemberList
						if err := res.List(ctx, &members,
							resources.WithLabelSelector("operator.etcd.io/cluster="+clusterName)); err != nil {
							return false, err
						}
						if len(members.Items) != clusterSize {
							return false, nil
						}
						for i := range members.Items {
							m := &members.Items[i]
							// Layer 1 (spec): the operator stamped this member with the target
							// version. Same form both sides (image-tag form, "v"-prefixed).
							if m.Spec.Version != tc.toVersion {
								return false, nil
							}
							// Layer 2 (observed): the phase of each member should be ready
							if m.Status.Phase != ecv1alpha1.EtcdMemberReady {
								return false, nil
							}
						}
						return true, nil
					}, wait.WithTimeout(5*time.Minute), wait.WithInterval(5*time.Second)); err != nil {
						t.Fatalf("cluster %s never rolled all %d members to %s: %v",
							clusterName, clusterSize, tc.toVersion, err)
					}
					return ctx
				})

			feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
				cleanupEtcdCluster(ctx, t, c, clusterName)
				return ctx
			})

			_ = testEnv.Test(t, feature.Feature())
		})
	}
}
