package cluster

import "time"

// Role indicates the node's cluster role.
type Role string

const (
	RoleLeader    Role = "leader"
	RoleFollower  Role = "follower"
)

// Node represents a cluster member.
type Node struct {
	ID       string
	Address  string
	Role     Role
	State    string
	LastSeen time.Time
}

// Config controls the in-process cluster manager.
type Config struct {
	// NodeID is this process's ID.
	NodeID string
	// Address is this process's advertised address.
	Address string
	// HeartbeatTTL marks a node unhealthy if not seen within this duration.
	HeartbeatTTL time.Duration
}
