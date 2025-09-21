package cluster

import (
	"context"
	"fmt"
	"time"

	"gomsg/storage"
)

// PartitionedStorage wraps a storage interface with partition awareness
type PartitionedStorage struct {
	storage storage.Storage
	cluster *Cluster
}

// NewPartitionedStorage creates a partition-aware storage wrapper
func NewPartitionedStorage(storage storage.Storage, cluster *Cluster) *PartitionedStorage {
	return &PartitionedStorage{
		storage: storage,
		cluster: cluster,
	}
}

// Set stores a key-value pair if this node owns the key's partition
func (ps *PartitionedStorage) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if !ps.cluster.OwnsKey(key) {
		return fmt.Errorf("key %s not owned by this node", key)
	}
	
	return ps.storage.Set(ctx, key, value, ttl)
}

// Get retrieves a value if this node owns the key's partition
func (ps *PartitionedStorage) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if !ps.cluster.OwnsKey(key) {
		return nil, false, fmt.Errorf("key %s not owned by this node", key)
	}
	
	value, found, err := ps.storage.Get(ctx, key)
	if err != nil {
		return nil, false, err
	}
	
	return value, found, nil
}

// Delete removes keys if this node owns their partitions
func (ps *PartitionedStorage) Delete(ctx context.Context, keys ...string) (int, error) {
	// Filter keys to only those we own
	ownedKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if ps.cluster.OwnsKey(key) {
			ownedKeys = append(ownedKeys, key)
		}
	}
	
	if len(ownedKeys) == 0 {
		return 0, nil
	}
	
	return ps.storage.Delete(ctx, ownedKeys...)
}

// Exists checks if a key exists and this node owns it
func (ps *PartitionedStorage) Exists(ctx context.Context, key string) (bool, error) {
	if !ps.cluster.OwnsKey(key) {
		return false, fmt.Errorf("key %s not owned by this node", key)
	}
	
	return ps.storage.Exists(ctx, key)
}

// StreamCreateTopic creates a topic if this node owns the topic's partition
func (ps *PartitionedStorage) StreamCreateTopic(ctx context.Context, topic string, partitions int32) error {
	if !ps.cluster.OwnsKey(topic) {
		return fmt.Errorf("topic %s not owned by this node", topic)
	}
	
	return ps.storage.StreamCreateTopic(ctx, topic, partitions)
}

// StreamDeleteTopic deletes a topic if this node owns the topic's partition
func (ps *PartitionedStorage) StreamDeleteTopic(ctx context.Context, topic string) error {
	if !ps.cluster.OwnsKey(topic) {
		return fmt.Errorf("topic %s not owned by this node", topic)
	}
	
	return ps.storage.StreamDeleteTopic(ctx, topic)
}

// GetPartitionKeys returns all keys owned by a specific partition
func (ps *PartitionedStorage) GetPartitionKeys(ctx context.Context, partition int32) ([]string, error) {
	// This would require the underlying storage to support key enumeration
	// For now, return an error indicating this needs to be implemented
	return nil, fmt.Errorf("partition key enumeration not implemented")
}

// MigratePartition moves all data for a partition to another node
func (ps *PartitionedStorage) MigratePartition(ctx context.Context, partition int32, targetNode string) error {
	// This would involve:
	// 1. Get all keys for the partition
	// 2. Send them to the target node
	// 3. Delete them locally
	// 4. Update partition ownership
	
	return fmt.Errorf("partition migration not implemented")
}

// ReplicateKey replicates a key to replica nodes
func (ps *PartitionedStorage) ReplicateKey(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Get replica nodes for this key
	owners := ps.cluster.GetKeyOwners(key)
	if len(owners) <= 1 {
		return nil // No replicas needed
	}
	
	// Skip the primary (ourselves) and replicate to others
	for i := 1; i < len(owners); i++ {
		replicaNode := owners[i]
		if replicaNode == ps.cluster.nodeID {
			continue // Skip if we're also a replica
		}
		
		// TODO: Send replication request to replica node
		// This would involve making a gRPC call to the replica
		_ = replicaNode // Placeholder
	}
	
	return nil
}

// HandleReplication handles incoming replication requests
func (ps *PartitionedStorage) HandleReplication(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Verify we should be a replica for this key
	owners := ps.cluster.GetKeyOwners(key)
	isReplica := false
	for _, owner := range owners {
		if owner == ps.cluster.nodeID {
			isReplica = true
			break
		}
	}
	
	if !isReplica {
		return fmt.Errorf("this node is not a replica for key %s", key)
	}
	
	// Store the replicated data
	return ps.storage.Set(ctx, key, value, ttl)
}
