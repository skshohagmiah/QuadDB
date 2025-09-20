//go:build raft_enabled
// +build raft_enabled

package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	hraft "github.com/hashicorp/raft"

	"gomsg/storage"
)

// CommandType describes the replicated operation type.
type CommandType string

const (
	CmdKVSet        CommandType = "KV_SET"
	CmdKVDelete     CommandType = "KV_DELETE"
	CmdStreamCreate CommandType = "STREAM_CREATE_TOPIC"
	CmdStreamDelete CommandType = "STREAM_DELETE_TOPIC"
)

// Command is the envelope replicated via Raft.
type Command struct {
	Version int            `json:"v"`
	Type    CommandType    `json:"t"`
	Payload json.RawMessage `json:"p"`
}

// FSM applies replicated commands onto storage.
type FSM struct {
	st storage.Storage
}

// NewFSM constructs a storage-backed FSM.
func NewFSM(st storage.Storage) hraft.FSM { return &FSM{st: st} }

// Apply decodes and executes a replicated command.
func (f *FSM) Apply(log *hraft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("fsm decode: %w", err)
	}
	ctx := context.TODO()
	switch cmd.Type {
	case CmdKVSet:
		var req struct {
			Key        string `json:"k"`
			Value      []byte `json:"v"`
			TTLSeconds int64  `json:"ttl"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		var ttl time.Duration
		if req.TTLSeconds > 0 {
			ttl = time.Duration(req.TTLSeconds) * time.Second
		}
		return f.st.Set(ctx, req.Key, req.Value, ttl)
	case CmdKVDelete:
		var req struct{ Keys []string `json:"ks"` }
		if err := json.Unmarshal(cmd.Payload, &req); err != nil { return err }
		_, err := f.st.Delete(ctx, req.Keys...)
		return err
	case CmdStreamCreate:
		var req struct{ Topic string `json:"topic"`; Partitions int32 `json:"parts"` }
		if err := json.Unmarshal(cmd.Payload, &req); err != nil { return err }
		return f.st.StreamCreateTopic(ctx, req.Topic, req.Partitions)
	case CmdStreamDelete:
		var req struct{ Topic string `json:"topic"` }
		if err := json.Unmarshal(cmd.Payload, &req); err != nil { return err }
		return f.st.StreamDeleteTopic(ctx, req.Topic)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// Snapshot implements hraft.FSM.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	// TODO: implement durable snapshot using storage.Backup
	return noopSnapshot{}, nil
}

// Restore implements hraft.FSM.
func (f *FSM) Restore(io.ReadCloser) error {
	// TODO: implement restore with storage.Restore
	return nil
}
