package raft

import (
	"context"
	"encoding/json"
	"time"
)

// Submitter wraps a Raft node to submit replicated commands.
type Submitter struct {
	n *Node
	// default timeout for Apply
	DefaultTimeout time.Duration
}

func NewSubmitter(n *Node) *Submitter {
	return &Submitter{n: n, DefaultTimeout: 3 * time.Second}
}

// IsLeader reports whether the local node is leader.
func (s *Submitter) IsLeader() bool { return s != nil && s.n != nil && s.n.IsLeader() }

// LeaderID returns the current leader ID.
func (s *Submitter) LeaderID() string {
	if s == nil || s.n == nil { return "" }
	return s.n.LeaderID()
}

// Submit encodes and applies a Command via Raft and waits for completion.
func (s *Submitter) Submit(ctx context.Context, cmd Command, timeout time.Duration) error {
	if s == nil || s.n == nil || s.n.raft == nil {
		return nil // no-op if raft not initialized
	}
	if timeout <= 0 {
		timeout = s.DefaultTimeout
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	applyF := s.n.raft.Apply(data, timeout)
	return applyF.Error()
}
