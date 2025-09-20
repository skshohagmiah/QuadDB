package cluster

import (
	"time"

	hraft "github.com/hashicorp/raft"
)

// Join adds a server to the Raft configuration as a voting member.
func (m *Manager) Join(id, address string) error {
	if m == nil || m.raft == nil {
		return nil
	}
	cfgFuture := m.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return err
	}
	for _, s := range cfgFuture.Configuration().Servers {
		if s.ID == hraft.ServerID(id) || s.Address == hraft.ServerAddress(address) {
			// already a member; treat as success
			return nil
		}
	}
	f := m.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(address), 0, 0)
	return f.Error()
}

// Leave removes a server from the Raft configuration.
func (m *Manager) Leave(id string) error {
	if m == nil || m.raft == nil {
		return nil
	}
	f := m.raft.RemoveServer(hraft.ServerID(id), 0, 0)
	return f.Error()
}

// GetNodes returns the current servers known to Raft.
func (m *Manager) GetNodes() []Node {
	if m == nil || m.raft == nil {
		return nil
	}
	future := m.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil
	}
	cfg := future.Configuration()
	leader := string(m.raft.Leader())
	nodes := make([]Node, 0, len(cfg.Servers))
	now := time.Now()
	for _, s := range cfg.Servers {
		n := Node{
			ID:       string(s.ID),
			Address:  string(s.Address),
			Role:     RoleFollower,
			State:    "active",
			LastSeen: now,
		}
		if string(s.Address) == leader {
			n.Role = RoleLeader
		}
		nodes = append(nodes, n)
	}
	return nodes
}

// GetLeader returns the current leader node, if known.
func (m *Manager) GetLeader() (Node, bool) {
	if m == nil || m.raft == nil {
		return Node{}, false
	}
	leaderAddr := string(m.raft.Leader())
	if leaderAddr == "" {
		return Node{}, false
	}
	future := m.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return Node{}, false
	}
	for _, s := range future.Configuration().Servers {
		if string(s.Address) == leaderAddr {
			return Node{ID: string(s.ID), Address: leaderAddr, Role: RoleLeader, State: "active", LastSeen: time.Now()}, true
		}
	}
	return Node{}, false
}
