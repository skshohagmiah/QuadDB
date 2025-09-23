package cluster

import (
	"context"
	"fmt"
	"time"

	"gomsg/storage"
	// TODO: Uncomment after generating gRPC code with: protoc --go_out=. --go-grpc_out=. api/proto/*.proto
	// clusterpb "gomsg/api/generated/cluster"
	// "google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
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

// QueuePush pushes a message to a queue if this node owns the queue's partition
func (ps *PartitionedStorage) QueuePush(ctx context.Context, queue string, message []byte, delay time.Duration) (string, error) {
	if !ps.cluster.OwnsKey(queue) {
		return "", fmt.Errorf("queue %s not owned by this node", queue)
	}

	return ps.storage.QueuePush(ctx, queue, message)
}

// QueuePop pops a message from a queue if this node owns the queue's partition
func (ps *PartitionedStorage) QueuePop(ctx context.Context, queue string, timeout time.Duration) (storage.QueueMessage, error) {
	if !ps.cluster.OwnsKey(queue) {
		return storage.QueueMessage{}, fmt.Errorf("queue %s not owned by this node", queue)
	}

	return ps.storage.QueuePop(ctx, queue, timeout)
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

// StreamPublish publishes a message to a stream, routing by partition key
func (ps *PartitionedStorage) StreamPublish(ctx context.Context, topic string, partitionKey string, data []byte, headers map[string]string) (storage.StreamMessage, error) {
	// For streams, we use the partition key to determine routing
	// If no partition key, use the topic name
	routingKey := partitionKey
	if routingKey == "" {
		routingKey = topic
	}

	if !ps.cluster.OwnsKey(routingKey) {
		return storage.StreamMessage{}, fmt.Errorf("stream partition for key %s not owned by this node", routingKey)
	}

	return ps.storage.StreamPublish(ctx, topic, partitionKey, data, headers)
}

// StreamRead reads messages from a stream if this node owns the partition
func (ps *PartitionedStorage) StreamRead(ctx context.Context, topic string, partition int32, offset int64, limit int32) ([]storage.StreamMessage, error) {
	// Check if we own this specific stream partition
	streamPartitionKey := fmt.Sprintf("%s:%d", topic, partition)
	if !ps.cluster.OwnsKey(streamPartitionKey) {
		return nil, fmt.Errorf("stream partition %s:%d not owned by this node", topic, partition)
	}

	return ps.storage.StreamRead(ctx, topic, partition, offset, limit)
}

// GetPartitionKeys returns all keys owned by a specific partition
func (ps *PartitionedStorage) GetPartitionKeys(ctx context.Context, partition int32) ([]string, error) {
	// Get all keys and filter by partition
	allKeys, err := ps.storage.Keys(ctx, "*", 0) // Get all keys
	if err != nil {
		return nil, fmt.Errorf("failed to get keys: %w", err)
	}

	var partitionKeys []string
	for _, key := range allKeys {
		keyPartition := ps.cluster.GetKeyPartition(key)
		if keyPartition == partition {
			partitionKeys = append(partitionKeys, key)
		}
	}

	return partitionKeys, nil
}

// MigratePartition moves all data for a partition to another node
func (ps *PartitionedStorage) MigratePartition(ctx context.Context, partition int32, targetNode string) error {
	// 1. Get all keys for the partition
	keys, err := ps.GetPartitionKeys(ctx, partition)
	if err != nil {
		return fmt.Errorf("failed to get partition keys: %w", err)
	}

	if len(keys) == 0 {
		return nil // Nothing to migrate
	}

	// 2. Get all key-value pairs
	data := make(map[string][]byte)
	for _, key := range keys {
		value, found, err := ps.storage.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get key %s: %w", key, err)
		}
		if found {
			data[key] = value
		}
	}

	// 3. Send data to target node (placeholder for gRPC call)
	if err := ps.sendDataToNode(ctx, targetNode, data); err != nil {
		return fmt.Errorf("failed to send data to target node: %w", err)
	}

	// 4. Delete keys locally after successful transfer
	if _, err := ps.storage.Delete(ctx, keys...); err != nil {
		return fmt.Errorf("failed to delete migrated keys: %w", err)
	}

	return nil
}

// sendDataToNode sends key-value data to another node
func (ps *PartitionedStorage) sendDataToNode(ctx context.Context, targetNode string, data map[string][]byte) error {
	// Get target node address from cluster
	nodeAddr := ps.getNodeAddress(targetNode)
	if nodeAddr == "" {
		return fmt.Errorf("target node %s address not found", targetNode)
	}

	// TODO: Implement actual gRPC call after generating protobuf code
	// conn, err := grpc.Dial(nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	//     return fmt.Errorf("failed to dial target node %s: %w", nodeAddr, err)
	// }
	// defer conn.Close()
	//
	// client := clusterpb.NewClusterServiceClient(conn)
	//
	// // Convert data to protobuf format
	// var keyValuePairs []*clusterpb.KeyValuePair
	// for key, value := range data {
	//     keyValuePairs = append(keyValuePairs, &clusterpb.KeyValuePair{
	//         Key:   key,
	//         Value: value,
	//         Ttl:   0, // No TTL for migrated data
	//     })
	// }
	//
	// req := &clusterpb.MigrateDataRequest{
	//     Partition: 0, // Will be set by caller
	//     Data:      keyValuePairs,
	// }
	//
	// resp, err := client.MigrateData(ctx, req)
	// if err != nil {
	//     return fmt.Errorf("failed to migrate data: %w", err)
	// }
	//
	// if !resp.Status.Success {
	//     return fmt.Errorf("migrate data failed: %s", resp.Status.Message)
	// }

	// Placeholder - assume successful transfer for now
	_ = targetNode
	_ = data

	return nil
}

// ReceiveMigratedData receives migrated data from another node
func (ps *PartitionedStorage) ReceiveMigratedData(ctx context.Context, data map[string][]byte) error {
	// Store all received data
	for key, value := range data {
		if err := ps.storage.Set(ctx, key, value, 0); err != nil {
			return fmt.Errorf("failed to store migrated key %s: %w", key, err)
		}
	}
	return nil
}

// ReplicateKey replicates a key to replica nodes
func (ps *PartitionedStorage) ReplicateKey(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Get replica nodes for this key
	owners := ps.cluster.GetKeyOwners(key)
	if len(owners) <= 1 {
		return nil // No replicas needed
	}

	// Skip the primary (ourselves) and replicate to others
	var replicationErrors []error
	for i := 1; i < len(owners); i++ {
		replicaNode := owners[i]
		if replicaNode == ps.cluster.nodeID {
			continue // Skip if we're also a replica
		}

		// Send replication request to replica
		if err := ps.sendReplicationToNode(ctx, replicaNode, key, value, ttl); err != nil {
			replicationErrors = append(replicationErrors, fmt.Errorf("failed to replicate to %s: %w", replicaNode, err))
			continue
		}
	}

	// Return error if all replications failed
	if len(replicationErrors) > 0 && len(replicationErrors) == len(owners)-1 {
		return fmt.Errorf("all replications failed: %v", replicationErrors)
	}

	return nil
}

// sendReplicationToNode sends a replication request to a specific node
func (ps *PartitionedStorage) sendReplicationToNode(ctx context.Context, nodeID, key string, value []byte, ttl time.Duration) error {
	// Get target node address from cluster
	nodeAddr := ps.getNodeAddress(nodeID)
	if nodeAddr == "" {
		return fmt.Errorf("target node %s address not found", nodeID)
	}

	// TODO: Implement actual gRPC call after generating protobuf code
	// conn, err := grpc.Dial(nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	//     return fmt.Errorf("failed to dial target node %s: %w", nodeAddr, err)
	// }
	// defer conn.Close()
	//
	// client := clusterpb.NewClusterServiceClient(conn)
	//
	// req := &clusterpb.ReplicateDataRequest{
	//     Key:   key,
	//     Value: value,
	//     Ttl:   int64(ttl.Seconds()),
	// }
	//
	// resp, err := client.ReplicateData(ctx, req)
	// if err != nil {
	//     return fmt.Errorf("failed to replicate data: %w", err)
	// }
	//
	// if !resp.Status.Success {
	//     return fmt.Errorf("replicate data failed: %s", resp.Status.Message)
	// }

	// Placeholder - assume successful replication for now
	_ = nodeID
	_ = key
	_ = value
	_ = ttl

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

// GetPartitionForQueue returns the partition that owns a queue
func (ps *PartitionedStorage) GetPartitionForQueue(queue string) int32 {
	return ps.cluster.GetKeyPartition(queue)
}

// GetPartitionForStream returns the partition that owns a stream topic
func (ps *PartitionedStorage) GetPartitionForStream(topic string) int32 {
	return ps.cluster.GetKeyPartition(topic)
}

// GetPartitionForStreamPartition returns the partition for a specific stream partition
func (ps *PartitionedStorage) GetPartitionForStreamPartition(topic string, partition int32) int32 {
	streamPartitionKey := fmt.Sprintf("%s:%d", topic, partition)
	return ps.cluster.GetKeyPartition(streamPartitionKey)
}

// getNodeAddress returns the address for a given node ID
func (ps *PartitionedStorage) getNodeAddress(nodeID string) string {
	// This would look up the node address from cluster topology
	// For now, assume node ID format includes address
	// In production, this would query the cluster for node addresses
	return nodeID // Placeholder - assume nodeID is the address
}
