// Package status provides utilities for managing Kubernetes resource status,
// particularly focusing on Conditions according to standard practices.
package status

// Condition types used for EtcdCluster status.
// Adhering to Kubernetes API conventions as much as possible.
// See:
// github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md

const (
	// ConditionAvailable indicates that the etcd cluster has reached its desired state,
	// has quorum, and is ready to serve requests. All members are healthy.
	ConditionAvailable string = "Available"

	// ConditionProgressing indicates that the operator is actively working
	// to bring the etcd cluster towards the desired state (e.g., creating resources,
	// scaling, promoting learners). It's True when reconciliation is in progress
	// and False when the desired state is reached or a terminal state occurs.
	ConditionProgressing string = "Progressing"

	// ConditionDegraded indicates that the etcd cluster is functional but
	// operating with potential issues that might impact performance or fault tolerance
	// (e.g., some members unhealthy but quorum maintained, leader missing temporarily).
	// It requires attention but is not necessarily completely unavailable.
	ConditionDegraded string = "Degraded"
)

// Common reasons for EtcdCluster status conditions. Reasons should be CamelCase and concise.
const (
	// General Reasons
	ReasonReconciling      string = "Reconciling"
	ReasonReconcileSuccess string = "ReconcileSuccess"
	ReasonClusterHealthy   string = "ClusterHealthy"

	// Available Reasons
	ReasonClusterReady   string = "ClusterReady"
	ReasonLeaderNotFound string = "LeaderNotFound"
	ReasonSizeIsZero     string = "SizeIsZero"

	// Progressing Reasons
	ReasonInitializingCluster string = "InitializingCluster"
	ReasonCreatingResources   string = "CreatingResources" // STS, Service etc.
	ReasonScalingUp           string = "ScalingUp"
	ReasonScalingDown         string = "ScalingDown"
	ReasonPromotingLearner    string = "PromotingLearner"
	ReasonMembersMismatch     string = "MembersMismatch"

	// Degraded Reasons
	ReasonMembersUnhealthy   string = "MembersUnhealthy"
	ReasonHealthCheckError   string = "HealthCheckError"
	ReasonResourceCreateFail string = "ResourceCreateFail" // Non-fatal resource creation failure
	ReasonResourceUpdateFail string = "ResourceUpdateFail" // Non-fatal resource update failure
)
