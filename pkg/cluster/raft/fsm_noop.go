package raft

import (
	hraft "github.com/hashicorp/raft"
)

// noopFSM is a minimal FSM that accepts log entries but does nothing.
type noopFSM struct{}

func (n noopFSM) Apply(*hraft.Log) interface{} { return nil }
func (n noopFSM) Snapshot() (hraft.FSMSnapshot, error) { return noopSnapshot{}, nil }
func (n noopFSM) Restore(hraft.SnapshotRestoreReader) error { return nil }

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink hraft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                           {}

// NewNoopFSM returns a stub FSM useful for wiring until a real FSM is provided.
func NewNoopFSM() hraft.FSM { return noopFSM{} }
