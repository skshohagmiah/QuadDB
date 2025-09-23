package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/skshohagmiah/fluxdl/storage"

	hraft "github.com/hashicorp/raft"
)

// PartitionedFSM implements hashicorp/raft.FSM with partition-aware storage.
// Only stores data for partitions that this node owns.
type PartitionedFSM struct {
	st          storage.Storage
	partitioner *Partitioner

	// Partition ownership tracking
	mu              sync.RWMutex
	ownedPartitions map[int32]bool // partitionID -> owned
	nodeID          string
	totalPartitions int32
}

// PartitionAssignment represents which node owns which partitions
type PartitionAssignment struct {
	NodeID     string  `json:"node_id"`
	Partitions []int32 `json:"partitions"`
}

// NewPartitionedFSM constructs a partition-aware FSM
func NewPartitionedFSM(st storage.Storage, nodeID string, totalPartitions int32) *PartitionedFSM {
	if totalPartitions <= 0 {
		totalPartitions = 16 // default
	}

	return &PartitionedFSM{
		st:              st,
		partitioner:     NewPartitioner(totalPartitions, 16), // 16 virtual nodes
		ownedPartitions: make(map[int32]bool),
		nodeID:          nodeID,
		totalPartitions: totalPartitions,
	}
}

// SetOwnedPartitions updates which partitions this node should own
func (f *PartitionedFSM) SetOwnedPartitions(partitions []int32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clear existing ownership
	f.ownedPartitions = make(map[int32]bool)

	// Set new ownership
	for _, p := range partitions {
		f.ownedPartitions[p] = true
	}
}

// GetOwnedPartitions returns the partitions this node currently owns
func (f *PartitionedFSM) GetOwnedPartitions() []int32 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	partitions := make([]int32, 0, len(f.ownedPartitions))
	for p := range f.ownedPartitions {
		partitions = append(partitions, p)
	}
	return partitions
}

// OwnsPartition checks if this node owns the given partition
func (f *PartitionedFSM) OwnsPartition(partition int32) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.ownedPartitions[partition]
}

// OwnsKey checks if this node owns the partition for the given key
func (f *PartitionedFSM) OwnsKey(key string) bool {
	partition := f.partitioner.Partition(key)
	return f.OwnsPartition(partition)
}

// Apply processes replicated commands, but only stores data for owned partitions
func (f *PartitionedFSM) Apply(l *hraft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}

	ctx := context.TODO()

	switch cmd.Type {
	case CmdKVSet:
		return f.applyKVSet(ctx, cmd.Payload)
	case CmdKVDelete:
		return f.applyKVDelete(ctx, cmd.Payload)
	case CmdStreamCreate:
		return f.applyStreamCreate(ctx, cmd.Payload)
	case CmdStreamDelete:
		return f.applyStreamDelete(ctx, cmd.Payload)
	case CmdPartitionAssign:
		return f.applyPartitionAssign(ctx, cmd.Payload)
	case CmdPartitionMigrate:
		return f.applyPartitionMigrate(ctx, cmd.Payload)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (f *PartitionedFSM) applyKVSet(ctx context.Context, payload json.RawMessage) interface{} {
	var req struct {
		Key        string `json:"k"`
		Value      []byte `json:"v"`
		TTLSeconds int64  `json:"ttl"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}

	// Only store if we own this key's partition
	if !f.OwnsKey(req.Key) {
		return nil // Not an error, just not our responsibility
	}

	var ttl time.Duration
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}

	return f.st.Set(ctx, req.Key, req.Value, ttl)
}

func (f *PartitionedFSM) applyKVDelete(ctx context.Context, payload json.RawMessage) interface{} {
	var req struct {
		Keys []string `json:"ks"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}

	// Filter keys to only those we own
	ownedKeys := make([]string, 0, len(req.Keys))
	for _, key := range req.Keys {
		if f.OwnsKey(key) {
			ownedKeys = append(ownedKeys, key)
		}
	}

	if len(ownedKeys) == 0 {
		return nil // No keys to delete
	}

	_, err := f.st.Delete(ctx, ownedKeys...)
	return err
}

func (f *PartitionedFSM) applyStreamCreate(ctx context.Context, payload json.RawMessage) interface{} {
	var req struct {
		Topic      string `json:"topic"`
		Partitions int32  `json:"parts"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}

	// For streams, we need to check if we own the topic's partition
	// Topics are partitioned by topic name
	if !f.OwnsKey(req.Topic) {
		return nil // Not our responsibility
	}

	return f.st.StreamCreateTopic(ctx, req.Topic, req.Partitions)
}

func (f *PartitionedFSM) applyStreamDelete(ctx context.Context, payload json.RawMessage) interface{} {
	var req struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}

	// For streams, check if we own the topic's partition
	if !f.OwnsKey(req.Topic) {
		return nil // Not our responsibility
	}

	return f.st.StreamDeleteTopic(ctx, req.Topic)
}

func (f *PartitionedFSM) applyPartitionAssign(ctx context.Context, payload json.RawMessage) interface{} {
	var req struct {
		Assignments []PartitionAssignment `json:"assignments"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}

	// Find our assignment
	for _, assignment := range req.Assignments {
		if assignment.NodeID == f.nodeID {
			f.SetOwnedPartitions(assignment.Partitions)
			break
		}
	}

	return nil
}

func (f *PartitionedFSM) applyPartitionMigrate(ctx context.Context, payload json.RawMessage) interface{} {
	var req struct {
		Partition int32  `json:"partition"`
		FromNode  string `json:"from_node"`
		ToNode    string `json:"to_node"`
		Data      []byte `json:"data,omitempty"` // Serialized partition data
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}

	// If we're the target node, accept the partition
	if req.ToNode == f.nodeID {
		f.mu.Lock()
		f.ownedPartitions[req.Partition] = true
		f.mu.Unlock()

		// TODO: Deserialize and restore partition data if provided
		// This would involve restoring keys that belong to this partition
	}

	// If we're the source node, release the partition
	if req.FromNode == f.nodeID {
		f.mu.Lock()
		delete(f.ownedPartitions, req.Partition)
		f.mu.Unlock()

		// TODO: Clean up keys that belong to this partition
		// This would involve deleting keys that hash to this partition
	}

	return nil
}

// Snapshot creates a snapshot of the current state
func (f *PartitionedFSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Create a snapshot that includes partition ownership
	snapshot := &PartitionedSnapshot{
		nodeID:          f.nodeID,
		ownedPartitions: make(map[int32]bool),
		totalPartitions: f.totalPartitions,
	}

	// Copy owned partitions
	for p, owned := range f.ownedPartitions {
		snapshot.ownedPartitions[p] = owned
	}

	return snapshot, nil
}

// Restore restores the FSM state from a snapshot
func (f *PartitionedFSM) Restore(r io.ReadCloser) error {
	defer r.Close()

	var snapshot struct {
		NodeID          string         `json:"node_id"`
		OwnedPartitions map[int32]bool `json:"owned_partitions"`
		TotalPartitions int32          `json:"total_partitions"`
	}

	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&snapshot); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.nodeID = snapshot.NodeID
	f.ownedPartitions = snapshot.OwnedPartitions
	f.totalPartitions = snapshot.TotalPartitions

	// Recreate partitioner with correct settings
	f.partitioner = NewPartitioner(f.totalPartitions, 16)

	return nil
}

// PartitionedSnapshot implements hraft.FSMSnapshot for partitioned state
type PartitionedSnapshot struct {
	nodeID          string
	ownedPartitions map[int32]bool
	totalPartitions int32
}

// Persist saves the snapshot to the sink
func (s *PartitionedSnapshot) Persist(sink hraft.SnapshotSink) error {
	data := struct {
		NodeID          string         `json:"node_id"`
		OwnedPartitions map[int32]bool `json:"owned_partitions"`
		TotalPartitions int32          `json:"total_partitions"`
	}{
		NodeID:          s.nodeID,
		OwnedPartitions: s.ownedPartitions,
		TotalPartitions: s.totalPartitions,
	}

	encoder := json.NewEncoder(sink)
	if err := encoder.Encode(data); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release is called when the snapshot is no longer needed
func (s *PartitionedSnapshot) Release() {
	// Nothing to clean up
}
