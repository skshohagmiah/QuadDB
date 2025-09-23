package cluster

import "time"

// NodeState indicates the node's current state
type NodeState string

const (
	NodeStateActive   NodeState = "active"
	NodeStateInactive NodeState = "inactive"
	NodeStateJoining  NodeState = "joining"
	NodeStateLeaving  NodeState = "leaving"
)

// Node represents a cluster member
type Node struct {
	ID       string
	Address  string
	State    NodeState
	LastSeen time.Time
}

// ClusterInfo provides cluster topology information
type ClusterInfo struct {
	NodeID            string
	TotalPartitions   int32
	ReplicationFactor int32
	TotalNodes        int32
	ActiveNodes       int32
	Partitions        []*PartitionInfo
}

// PartitionInfo provides information about a partition
type PartitionInfo struct {
	ID       int32
	Primary  string
	Replicas []string
}

// PartitionStats provides partition distribution statistics
type PartitionStats struct {
	TotalPartitions      int32
	UnassignedPartitions int32
	NodeStats            map[string]*NodePartitionStats
}

// NodePartitionStats provides partition statistics for a node
type NodePartitionStats struct {
	TotalPartitions   int32
	PrimaryPartitions int32
	ReplicaPartitions int32
}

// ClusterHealth provides comprehensive cluster health information
type ClusterHealth struct {
	Status        string // "healthy", "no-quorum", "no-leader"
	TotalNodes    int32
	ActiveNodes   int32
	HasQuorum     bool
	IsLeader      bool
	LeaderID      string
	CanWrite      bool
	ReadOnlyMode  bool
}
