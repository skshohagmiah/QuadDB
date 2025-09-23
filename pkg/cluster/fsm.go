package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/skshohagmiah/fluxdl/storage"
)

// fsm implements hashicorp/raft.FSM and applies replicated commands to storage.
type fsm struct{ st storage.Storage }

// NewFSM constructs the storage-backed FSM.
func NewFSM(st storage.Storage) *fsm { return &fsm{st: st} }

func (f *fsm) Apply(l *hraft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
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
		var req struct {
			Keys []string `json:"ks"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		_, err := f.st.Delete(ctx, req.Keys...)
		return err
	case CmdStreamCreate:
		var req struct {
			Topic      string `json:"topic"`
			Partitions int32  `json:"parts"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return f.st.StreamCreateTopic(ctx, req.Topic, req.Partitions)
	case CmdStreamDelete:
		var req struct {
			Topic string `json:"topic"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return f.st.StreamDeleteTopic(ctx, req.Topic)
	case CmdPartitionAssign:
		var req struct {
			Partition int32    `json:"partition"`
			Owners    []string `json:"owners"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		// Apply partition assignment - this would update the partition map
		// For now, we'll store it as a special key in storage
		assignmentKey := fmt.Sprintf("__partition_assignment_%d", req.Partition)
		assignmentData, _ := json.Marshal(req.Owners)
		return f.st.Set(ctx, assignmentKey, assignmentData, 0)
	case CmdPartitionMigrate:
		var req struct {
			Partition int32  `json:"partition"`
			FromNode  string `json:"from"`
			ToNode    string `json:"to"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		// Apply partition migration - this would trigger data movement
		// For now, we'll store the migration record
		migrationKey := fmt.Sprintf("__partition_migration_%d", req.Partition)
		migrationData, _ := json.Marshal(req)
		return f.st.Set(ctx, migrationKey, migrationData, 0)
	default:
		return nil
	}
}

func (f *fsm) Snapshot() (hraft.FSMSnapshot, error) { return noopSnapshot{}, nil }
func (f *fsm) Restore(r io.ReadCloser) error        { return r.Close() }

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink hraft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                              {}
