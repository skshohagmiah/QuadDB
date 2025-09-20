package cluster

import (
	"context"
	"encoding/json"
	"io"
	"time"

	hraft "github.com/hashicorp/raft"
	"gomsg/storage"
)

// fsm implements hashicorp/raft.FSM and applies replicated commands to storage.
type fsm struct{ st storage.Storage }

// NewFSM constructs the storage-backed FSM.
func NewFSM(st storage.Storage) *fsm { return &fsm{st: st} }

func (f *fsm) Apply(l *hraft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil { return err }
	ctx := context.TODO()
	switch cmd.Type {
	case CmdKVSet:
		var req struct{ Key string `json:"k"`; Value []byte `json:"v"`; TTLSeconds int64 `json:"ttl"` }
		if err := json.Unmarshal(cmd.Payload, &req); err != nil { return err }
		var ttl time.Duration
		if req.TTLSeconds > 0 { ttl = time.Duration(req.TTLSeconds) * time.Second }
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
		return nil
	}
}

func (f *fsm) Snapshot() (hraft.FSMSnapshot, error) { return noopSnapshot{}, nil }
func (f *fsm) Restore(r io.ReadCloser) error { return r.Close() }

type noopSnapshot struct{}
func (noopSnapshot) Persist(sink hraft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                           {}
