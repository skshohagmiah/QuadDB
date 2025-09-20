package raft

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// Config contains the minimal settings to start a Raft node.
type Config struct {
	NodeID   string
	BindAddr string
	DataDir  string
	Bootstrap bool
	JoinAddrs []string
}

// Node wraps hashicorp/raft components.
type Node struct {
	raft   *hraft.Raft
	store  *raftboltdb.BoltStore
	stable *raftboltdb.BoltStore
	snap   *hraft.FileSnapshotStore
	trans  *hraft.NetworkTransport
}

// Start sets up a local raft node. Caller should handle Join via RPC separately.
func Start(cfg Config, fsm hraft.FSM) (*Node, error) {
	// Stores
	storePath := filepath.Join(cfg.DataDir, "raft-log.bolt")
	stablePath := filepath.Join(cfg.DataDir, "raft-stable.bolt")
	snapDir := filepath.Join(cfg.DataDir, "raft-snapshots")

	store, err := raftboltdb.NewBoltStore(storePath)
	if err != nil { return nil, fmt.Errorf("bolt log store: %w", err) }
	stable, err := raftboltdb.NewBoltStore(stablePath)
	if err != nil { return nil, fmt.Errorf("bolt stable store: %w", err) }
	snap, err := hraft.NewFileSnapshotStore(snapDir, 2, nil)
	if err != nil { return nil, fmt.Errorf("snapshot store: %w", err) }

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

	ra, err := hraft.NewRaft(rcfg, fsm, store, store, snap, trans)
	if err != nil { return nil, err }

	n := &Node{raft: ra, store: store, stable: stable, snap: snap, trans: trans}

	if cfg.Bootstrap {
		conf := hraft.Configuration{Servers: []hraft.Server{{
			ID:      rcfg.LocalID,
			Address: trans.LocalAddr(),
		}}}
		if err := ra.BootstrapCluster(conf).Error(); err != nil {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	return n, nil
}

// LeaderID returns the current leader id, if known.
func (n *Node) LeaderID() string {
	if n == nil || n.raft == nil { return "" }
	return string(n.raft.Leader())
}

// IsLeader reports whether this node is the current leader.
func (n *Node) IsLeader() bool {
	if n == nil || n.raft == nil { return false }
	return n.raft.State() == hraft.Leader
}

// LocalID returns the local server ID.
func (n *Node) LocalID() string {
	if n == nil || n.raft == nil { return "" }
	return string(n.raft.Config().LocalID)
}

// Shutdown stops raft and closes stores.
func (n *Node) Shutdown() error {
	if n == nil { return nil }
	n.raft.Shutdown()
	if n.trans != nil { n.trans.Close() }
	if n.store != nil { _ = n.store.Close() }
	if n.stable != nil { _ = n.stable.Close() }
	return nil
}
