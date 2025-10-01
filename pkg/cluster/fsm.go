package cluster

import (
	"context"
	"encoding/json"
	"io"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/skshohagmiah/gomsg/storage"
)

// FSM implements hashicorp/raft.FSM and applies replicated commands to storage
type FSM struct {
	storage storage.Storage
	cluster *Cluster
}

// NewFSM constructs the storage-backed FSM
func NewFSM(storage storage.Storage, cluster *Cluster) *FSM {
	return &FSM{
		storage: storage,
		cluster: cluster,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(l *hraft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}

	ctx := context.TODO()
	switch cmd.Type {
	case CmdKVSet:
		var req struct {
			Key   string `json:"k"`
			Value []byte `json:"v"`
			TTL   int64  `json:"ttl"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		var ttl time.Duration
		if req.TTL > 0 {
			ttl = time.Duration(req.TTL) * time.Second
		}
		return f.storage.Set(ctx, req.Key, req.Value, ttl)

	case CmdKVDelete:
		var req struct {
			Keys []string `json:"ks"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		_, err := f.storage.Delete(ctx, req.Keys...)
		return err

	case CmdNodeJoin:
		var node Node
		if err := json.Unmarshal(cmd.Payload, &node); err != nil {
			return err
		}
		return f.cluster.addNode(&node)

	case CmdNodeLeave:
		var req struct {
			NodeID string `json:"node_id"`
		}
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return f.cluster.removeNode(req.NodeID)

	default:
		return nil
	}
}

// Snapshot returns a snapshot of the FSM state
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

// Restore restores the FSM state from a snapshot
func (f *FSM) Restore(r io.ReadCloser) error {
	return r.Close()
}

// snapshot implements hraft.FSMSnapshot
type snapshot struct{}

func (s *snapshot) Persist(sink hraft.SnapshotSink) error {
	return sink.Close()
}

func (s *snapshot) Release() {}
