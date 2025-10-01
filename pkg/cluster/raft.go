package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// RaftManager manages the Raft consensus engine
type RaftManager struct {
	raft   *hraft.Raft
	store  *raftboltdb.BoltStore
	stable *raftboltdb.BoltStore
	snap   *hraft.FileSnapshotStore
	trans  *hraft.NetworkTransport
}

// StartRaft starts a raft node with the given configuration
func StartRaft(ctx context.Context, cfg Config, fsm hraft.FSM) (*RaftManager, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// Create stores
	logPath := filepath.Join(cfg.DataDir, "raft-log.bolt")
	stablePath := filepath.Join(cfg.DataDir, "raft-stable.bolt")
	snapDir := filepath.Join(cfg.DataDir, "raft-snapshots")

	store, err := raftboltdb.NewBoltStore(logPath)
	if err != nil {
		return nil, fmt.Errorf("bolt log store: %w", err)
	}

	stable, err := raftboltdb.NewBoltStore(stablePath)
	if err != nil {
		return nil, fmt.Errorf("bolt stable store: %w", err)
	}

	snap, err := hraft.NewFileSnapshotStore(snapDir, 2, nil)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve bind addr: %w", err)
	}

	trans, err := hraft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, nil)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	// Configure Raft
	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID = hraft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 1000 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.CommitTimeout = 500 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond

	// Create Raft instance
	raft, err := hraft.NewRaft(raftConfig, fsm, store, store, snap, trans)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	manager := &RaftManager{
		raft:   raft,
		store:  store,
		stable: stable,
		snap:   snap,
		trans:  trans,
	}

	// Bootstrap if required
	if cfg.Bootstrap {
		configuration := hraft.Configuration{
			Servers: []hraft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: trans.LocalAddr(),
				},
			},
		}
		if err := raft.BootstrapCluster(configuration).Error(); err != nil {
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	return manager, nil
}

// IsLeader returns true if this node is the Raft leader
func (rm *RaftManager) IsLeader() bool {
	if rm.raft == nil {
		return false
	}
	return rm.raft.State() == hraft.Leader
}

// LeaderID returns the current leader's ID
func (rm *RaftManager) LeaderID() string {
	if rm.raft == nil {
		return ""
	}
	return string(rm.raft.Leader())
}

// State returns the current Raft state
func (rm *RaftManager) State() string {
	if rm.raft == nil {
		return "disabled"
	}
	switch rm.raft.State() {
	case hraft.Leader:
		return "leader"
	case hraft.Candidate:
		return "candidate"
	case hraft.Follower:
		return "follower"
	default:
		return "unknown"
	}
}

// Apply applies a command to the Raft log
func (rm *RaftManager) Apply(cmd Command, timeout time.Duration) error {
	if rm.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	future := rm.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("apply command: %w", err)
	}

	return nil
}

// AddVoter adds a new voting member to the cluster
func (rm *RaftManager) AddVoter(id, address string) error {
	if rm.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := rm.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(address), 0, 10*time.Second)
	return future.Error()
}

// RemoveServer removes a server from the cluster
func (rm *RaftManager) RemoveServer(id string) error {
	if rm.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := rm.raft.RemoveServer(hraft.ServerID(id), 0, 10*time.Second)
	return future.Error()
}

// Close shuts down the Raft manager
func (rm *RaftManager) Close() error {
	if rm.raft != nil {
		if err := rm.raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	if rm.trans != nil {
		rm.trans.Close()
	}
	if rm.store != nil {
		rm.store.Close()
	}
	if rm.stable != nil {
		rm.stable.Close()
	}
	return nil
}
