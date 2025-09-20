package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"gomsg/storage"
)

// RaftConfig defines how to start the local raft node
// All clustering code lives in this package; no subfolders are required.
type RaftConfig struct {
	NodeID    string
	BindAddr  string
	DataDir   string
	Bootstrap bool
}

// Manager owns the local raft node and provides helpers for cluster operations
// and data replication.
type Manager struct {
	raft   *hraft.Raft
	store  *raftboltdb.BoltStore
	stable *raftboltdb.BoltStore
	snap   *hraft.FileSnapshotStore
	trans  *hraft.NetworkTransport
	st     storage.Storage
}

// Start starts a raft node with a storage-backed FSM.
func Start(ctx context.Context, st storage.Storage, cfg RaftConfig) (*Manager, error) {
	// Stores
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

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil { return nil, err }
	trans, err := hraft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, nil)
	if err != nil { return nil, err }

	// Raft config
	rcfg := hraft.DefaultConfig()
	rcfg.LocalID = hraft.ServerID(cfg.NodeID)
	rcfg.HeartbeatTimeout = 200 * time.Millisecond
	rcfg.ElectionTimeout = 200 * time.Millisecond
	rcfg.LeaderLeaseTimeout = 200 * time.Millisecond
	rcfg.CommitTimeout = 50 * time.Millisecond

	// FSM
	fsm := NewFSM(st)
	ra, err := hraft.NewRaft(rcfg, fsm, store, store, snap, trans)
	if err != nil { return nil, err }

	m := &Manager{raft: ra, store: store, stable: stable, snap: snap, trans: trans, st: st}

	if cfg.Bootstrap {
		conf := hraft.Configuration{Servers: []hraft.Server{{
			ID:      rcfg.LocalID,
			Address: trans.LocalAddr(),
		}}}
		if err := ra.BootstrapCluster(conf).Error(); err != nil {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}
	return m, nil
}

// Raft returns the underlying raft instance
func (m *Manager) Raft() *hraft.Raft { if m == nil { return nil }; return m.raft }

// LeaderID returns the current leader ID if known
func (m *Manager) LeaderID() string { if m == nil || m.raft == nil { return "" }; return string(m.raft.Leader()) }

// IsLeader reports whether this node is the current leader
func (m *Manager) IsLeader() bool { if m == nil || m.raft == nil { return false }; return m.raft.State() == hraft.Leader }

// Close shuts down raft and closes stores
func (m *Manager) Close() {
	if m == nil { return }
	if m.raft != nil { m.raft.Shutdown() }
	if m.trans != nil { m.trans.Close() }
	if m.store != nil { _ = m.store.Close() }
	if m.stable != nil { _ = m.stable.Close() }
}
