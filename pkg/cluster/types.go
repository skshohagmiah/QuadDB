package cluster

import "time"

// Role indicates the node's cluster role.
type Role string

const (
	RoleLeader   Role = "leader"
	RoleFollower Role = "follower"
)

// Node represents a cluster member (view from Raft configuration).
type Node struct {
	ID       string
	Address  string
	Role     Role
	State    string
	LastSeen time.Time
}
