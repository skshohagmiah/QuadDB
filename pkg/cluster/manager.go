package cluster

import (
	"sort"
	"sync"
	"time"
)

// Manager maintains in-memory cluster state and basic leader election.
type Manager struct {
	mu    sync.RWMutex
	cfg   Config
	nodes map[string]*Node
}

// NewManager creates a manager and registers the local node.
func NewManager(cfg Config) *Manager {
	m := &Manager{
		cfg:   cfg,
		nodes: make(map[string]*Node),
	}
	// Register self
	m.nodes[cfg.NodeID] = &Node{
		ID:       cfg.NodeID,
		Address:  cfg.Address,
		Role:     RoleFollower,
		State:    "active",
		LastSeen: time.Now(),
	}
	m.updateRolesLocked()
	return m
}

// Join registers or updates a node in the cluster.
func (m *Manager) Join(id, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[id]
	if !ok {
		n = &Node{ID: id}
		m.nodes[id] = n
	}
	n.Address = address
	n.State = "active"
	n.LastSeen = time.Now()
	m.updateRolesLocked()
}

// Leave marks a node as left/removed.
func (m *Manager) Leave(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, id)
	m.updateRolesLocked()
}

// Heartbeat updates a node's liveness timestamp.
func (m *Manager) Heartbeat(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[id]; ok {
		n.LastSeen = time.Now()
	}
	m.sweepLocked()
	m.updateRolesLocked()
}

// GetNodes returns a snapshot of current nodes.
func (m *Manager) GetNodes() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.sweepLocked()
	out := make([]Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, *n)
	}
	return out
}

// GetLeader returns the current leader if any.
func (m *Manager) GetLeader() (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.sweepLocked()
	leaderID := m.leaderIDLocked()
	if leaderID == "" {
		return Node{}, false
	}
	ln := m.nodes[leaderID]
	if ln == nil {
		return Node{}, false
	}
	return *ln, true
}

// sweepLocked removes/marks nodes that missed heartbeats.
func (m *Manager) sweepLocked() {
	ttl := m.cfg.HeartbeatTTL
	if ttl <= 0 {
		return
	}
	deadline := time.Now().Add(-ttl)
	for id, n := range m.nodes {
		if n.LastSeen.Before(deadline) {
			delete(m.nodes, id)
		}
	}
}

// updateRolesLocked elects the smallest ID as leader and sets roles.
func (m *Manager) updateRolesLocked() {
	ids := make([]string, 0, len(m.nodes))
	for id := range m.nodes {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return
	}
	sort.Strings(ids)
	leader := ids[0]
	for id, n := range m.nodes {
		if id == leader {
			n.Role = RoleLeader
		} else {
			n.Role = RoleFollower
		}
	}
}

func (m *Manager) leaderIDLocked() string {
	ids := make([]string, 0, len(m.nodes))
	for id := range m.nodes {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return ""
	}
	sort.Strings(ids)
	return ids[0]
}
