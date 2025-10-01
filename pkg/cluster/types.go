package cluster

import (
	"encoding/json"
	"time"
)

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
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	State    NodeState `json:"state"`
	LastSeen time.Time `json:"last_seen"`
}

// ClusterHealth provides cluster health information
type ClusterHealth struct {
	Status      string `json:"status"`       // "healthy", "no-quorum", "no-leader"
	TotalNodes  int32  `json:"total_nodes"`
	ActiveNodes int32  `json:"active_nodes"`
	HasQuorum   bool   `json:"has_quorum"`
	IsLeader    bool   `json:"is_leader"`
	LeaderID    string `json:"leader_id"`
	CanWrite    bool   `json:"can_write"`
}

// Config defines cluster configuration
type Config struct {
	NodeID    string   `json:"node_id"`
	BindAddr  string   `json:"bind_addr"`
	DataDir   string   `json:"data_dir"`
	Bootstrap bool     `json:"bootstrap"`
	SeedNodes []string `json:"seed_nodes"`
}

// CommandType describes the replicated operation type
type CommandType string

const (
	CmdKVSet    CommandType = "KV_SET"
	CmdKVDelete CommandType = "KV_DELETE"
	CmdNodeJoin CommandType = "NODE_JOIN"
	CmdNodeLeave CommandType = "NODE_LEAVE"
)

// Command is the envelope replicated via Raft
type Command struct {
	Version int             `json:"v"`
	Type    CommandType     `json:"t"`
	Payload json.RawMessage `json:"p"`
}
