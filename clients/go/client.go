package client

import (
	"context"
	"fmt"
	"time"

	clusterpb "gomsg/api/generated/cluster"
	commonpb "gomsg/api/generated/common"
	kvpb "gomsg/api/generated/kv"
	queuepb "gomsg/api/generated/queue"
)

// GomsgClient is the main client that provides the clean API matching the README
type GomsgClient struct {
	partitionedClient *PartitionedClient
	KV                *KVClient
	Queue             *QueueClient
	Cluster           *ClusterClient
}

// Config holds optional configuration for the client
type Config struct {
	// Partitions is the number of partitions for key distribution (default: 16)
	Partitions int32
	// RefreshInterval for cluster topology updates (default: 30s)
	RefreshInterval time.Duration
	// DialTimeout for connection establishment (default: 5s)
	DialTimeout time.Duration
	// Insecure skips TLS (default: true for development)
	Insecure bool
}

// Connect creates a new gomsg client that automatically handles clustering, partitioning, and replication
// Usage: client := gomsg.Connect("server1:9000", "server2:9001", "server3:9002")
func Connect(servers ...string) (*GomsgClient, error) {
	return ConnectWithConfig(nil, servers...)
}

// ConnectWithConfig creates a new gomsg client with custom configuration
func ConnectWithConfig(config *Config, servers ...string) (*GomsgClient, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("at least one server address must be provided")
	}

	// Set defaults
	if config == nil {
		config = &Config{}
	}
	if config.Partitions <= 0 {
		config.Partitions = 16
	}
	if config.RefreshInterval <= 0 {
		config.RefreshInterval = 30 * time.Second
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = 5 * time.Second
	}
	// Default to insecure for development
	if !config.Insecure {
		config.Insecure = true
	}

	// Create partitioned client options
	opts := &PartitionedOptions{
		Options: &Options{
			DialTimeout: config.DialTimeout,
			Insecure:    config.Insecure,
		},
		Partitions:      config.Partitions,
		RefreshInterval: config.RefreshInterval,
		InitialNodes:    servers,
	}

	// Create the underlying partitioned client
	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout)
	defer cancel()

	partitionedClient, err := NewPartitioned(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create partitioned client: %w", err)
	}

	// Create the main client with clean API wrappers
	client := &GomsgClient{
		partitionedClient: partitionedClient,
	}

	// Initialize the clean API wrappers
	client.KV = &KVClient{client: client}
	client.Queue = &QueueClient{client: client}
	client.Cluster = &ClusterClient{client: client}

	return client, nil
}

// Close closes the client and all underlying connections
func (c *GomsgClient) Close() error {
	return c.partitionedClient.Close()
}

// KVClient provides the clean Key/Value API as shown in README
type KVClient struct {
	client *GomsgClient
}

// Set stores a key-value pair
func (kv *KVClient) Set(ctx context.Context, key, value string) error {
	return kv.client.partitionedClient.Set(ctx, key, []byte(value), 0)
}

// SetTTL stores a key-value pair with TTL
func (kv *KVClient) SetTTL(ctx context.Context, key, value string, ttl time.Duration) error {
	return kv.client.partitionedClient.Set(ctx, key, []byte(value), ttl)
}

// Get retrieves a value by key
func (kv *KVClient) Get(ctx context.Context, key string) (string, error) {
	value, found, err := kv.client.partitionedClient.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("key not found")
	}
	return string(value), nil
}

// GetBytes retrieves raw bytes by key
func (kv *KVClient) GetBytes(ctx context.Context, key string) ([]byte, bool, error) {
	return kv.client.partitionedClient.Get(ctx, key)
}

// Exists checks if a key exists
func (kv *KVClient) Exists(ctx context.Context, key string) (bool, error) {
	return kv.client.partitionedClient.Exists(ctx, key)
}

// Del deletes one or more keys
func (kv *KVClient) Del(ctx context.Context, keys ...string) (int32, error) {
	return kv.client.partitionedClient.Del(ctx, keys...)
}

// Incr increments a counter by 1
func (kv *KVClient) Incr(ctx context.Context, key string) (int64, error) {
	return kv.IncrBy(ctx, key, 1)
}

// IncrBy increments a counter by delta
func (kv *KVClient) IncrBy(ctx context.Context, key string, delta int64) (int64, error) {
	// Route to the correct partition
	partition := kv.client.partitionedClient.partitioner.Partition(key)
	targetNode, err := kv.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get target node for key %s: %w", key, err)
	}

	req := &kvpb.IncrRequest{Key: key, By: delta}
	resp, err := targetNode.KV.Incr(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to increment key %s: %w", key, err)
	}

	if !resp.Status.Success {
		return 0, fmt.Errorf("increment failed: %s", resp.Status.Message)
	}

	return resp.Value, nil
}

// Decr decrements a counter by 1
func (kv *KVClient) Decr(ctx context.Context, key string) (int64, error) {
	return kv.DecrBy(ctx, key, 1)
}

// DecrBy decrements a counter by delta
func (kv *KVClient) DecrBy(ctx context.Context, key string, delta int64) (int64, error) {
	// Route to the correct partition
	partition := kv.client.partitionedClient.partitioner.Partition(key)
	targetNode, err := kv.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get target node for key %s: %w", key, err)
	}

	req := &kvpb.DecrRequest{Key: key, By: delta}
	resp, err := targetNode.KV.Decr(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to decrement key %s: %w", key, err)
	}

	if !resp.Status.Success {
		return 0, fmt.Errorf("decrement failed: %s", resp.Status.Message)
	}

	return resp.Value, nil
}

// MGet retrieves multiple values
func (kv *KVClient) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	// Group keys by target node to batch requests
	nodeKeys := make(map[string][]string)
	for _, key := range keys {
		partition := kv.client.partitionedClient.partitioner.Partition(key)
		targetNode, err := kv.client.partitionedClient.getNodeForPartition(partition)
		if err != nil {
			return nil, fmt.Errorf("failed to get target node for key %s: %w", key, err)
		}
		nodeKeys[targetNode.nodeID] = append(nodeKeys[targetNode.nodeID], key)
	}

	// Execute MGet on each node and combine results
	result := make(map[string]string)
	for nodeID, nodeKeyList := range nodeKeys {
		kv.client.partitionedClient.mu.RLock()
		node, exists := kv.client.partitionedClient.nodes[nodeID]
		kv.client.partitionedClient.mu.RUnlock()

		if !exists {
			return nil, fmt.Errorf("node %s not found", nodeID)
		}

		req := &kvpb.MGetRequest{Keys: nodeKeyList}
		resp, err := node.KV.MGet(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to mget from node %s: %w", nodeID, err)
		}

		if !resp.Status.Success {
			return nil, fmt.Errorf("mget failed on node %s: %s", nodeID, resp.Status.Message)
		}

		// Add results to the combined map
		for _, kv := range resp.Values {
			result[kv.Key] = string(kv.Value)
		}
	}

	return result, nil
}

// MSet sets multiple key-value pairs
func (kv *KVClient) MSet(ctx context.Context, pairs map[string]string) error {
	if len(pairs) == 0 {
		return nil
	}

	// Group pairs by target node
	nodePairs := make(map[string]map[string]string)
	for key, value := range pairs {
		partition := kv.client.partitionedClient.partitioner.Partition(key)
		targetNode, err := kv.client.partitionedClient.getNodeForPartition(partition)
		if err != nil {
			return fmt.Errorf("failed to get target node for key %s: %w", key, err)
		}

		if nodePairs[targetNode.nodeID] == nil {
			nodePairs[targetNode.nodeID] = make(map[string]string)
		}
		nodePairs[targetNode.nodeID][key] = value
	}

	// Execute MSet on each node
	for nodeID, nodePairMap := range nodePairs {
		kv.client.partitionedClient.mu.RLock()
		node, exists := kv.client.partitionedClient.nodes[nodeID]
		kv.client.partitionedClient.mu.RUnlock()

		if !exists {
			return fmt.Errorf("node %s not found", nodeID)
		}

		// Convert to proto format
		var protoValues []*commonpb.KeyValue
		for k, v := range nodePairMap {
			protoValues = append(protoValues, &commonpb.KeyValue{
				Key:   k,
				Value: []byte(v),
			})
		}

		req := &kvpb.MSetRequest{Values: protoValues}
		resp, err := node.KV.MSet(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to mset on node %s: %w", nodeID, err)
		}

		if !resp.Status.Success {
			return fmt.Errorf("mset failed on node %s: %s", nodeID, resp.Status.Message)
		}
	}

	return nil
}

// Keys returns keys matching a pattern
func (kv *KVClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	// This needs to query all nodes since keys are distributed
	kv.client.partitionedClient.mu.RLock()
	defer kv.client.partitionedClient.mu.RUnlock()

	var allKeys []string
	for _, node := range kv.client.partitionedClient.nodes {
		req := &kvpb.KeysRequest{Pattern: pattern, Limit: 1000}
		resp, err := node.KV.Keys(ctx, req)
		if err != nil {
			continue // Skip nodes that fail
		}

		if resp.Status.Success {
			allKeys = append(allKeys, resp.Keys...)
		}
	}

	return allKeys, nil
}

// DelPattern deletes keys matching a pattern
func (kv *KVClient) DelPattern(ctx context.Context, pattern string) (int32, error) {
	keys, err := kv.Keys(ctx, pattern)
	if err != nil {
		return 0, err
	}
	return kv.Del(ctx, keys...)
}

// Expire sets TTL on a key
func (kv *KVClient) Expire(ctx context.Context, key string, ttl time.Duration) error {
	partition := kv.client.partitionedClient.partitioner.Partition(key)
	targetNode, err := kv.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return fmt.Errorf("failed to get target node for key %s: %w", key, err)
	}

	req := &kvpb.ExpireRequest{Key: key, Ttl: int64(ttl.Seconds())}
	resp, err := targetNode.KV.Expire(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to set expire on key %s: %w", key, err)
	}

	if !resp.Status.Success {
		return fmt.Errorf("expire failed: %s", resp.Status.Message)
	}

	return nil
}

// TTL gets the TTL of a key
func (kv *KVClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	partition := kv.client.partitionedClient.partitioner.Partition(key)
	targetNode, err := kv.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get target node for key %s: %w", key, err)
	}

	req := &kvpb.TTLRequest{Key: key}
	resp, err := targetNode.KV.TTL(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to get TTL for key %s: %w", key, err)
	}

	if !resp.Status.Success {
		return 0, fmt.Errorf("TTL failed: %s", resp.Status.Message)
	}

	return time.Duration(resp.Ttl) * time.Second, nil
}

// QueueClient provides the clean Queue API as shown in README
type QueueClient struct {
	client *GomsgClient
}

// QueueMessage represents a queue message
type QueueMessage struct {
	ID         string
	Queue      string
	Data       []byte
	CreatedAt  time.Time
	DelayUntil time.Time
	RetryCount int32
	ConsumerID string
}

// Push adds a message to a queue
func (q *QueueClient) Push(ctx context.Context, queue string, data string) (string, error) {
	return q.PushDelayed(ctx, queue, data, 0)
}

// PushBytes adds raw bytes to a queue
func (q *QueueClient) PushBytes(ctx context.Context, queue string, data []byte) (string, error) {
	return q.PushDelayedBytes(ctx, queue, data, 0)
}

// PushDelayed adds a message to a queue with delay
func (q *QueueClient) PushDelayed(ctx context.Context, queue string, data string, delay time.Duration) (string, error) {
	return q.PushDelayedBytes(ctx, queue, []byte(data), delay)
}

// PushDelayedBytes adds raw bytes to a queue with delay
func (q *QueueClient) PushDelayedBytes(ctx context.Context, queue string, data []byte, delay time.Duration) (string, error) {
	// Route based on queue name
	partition := q.client.partitionedClient.partitioner.Partition(queue)
	targetNode, err := q.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return "", fmt.Errorf("failed to get target node for queue %s: %w", queue, err)
	}

	req := &queuepb.PushRequest{
		Queue: queue,
		Data:  data,
		Delay: int64(delay.Seconds()),
	}

	resp, err := targetNode.Queue.Push(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to push to queue %s: %w", queue, err)
	}

	if !resp.Status.Success {
		return "", fmt.Errorf("push failed: %s", resp.Status.Message)
	}

	return resp.MessageId, nil
}

// Pop retrieves a message from a queue
func (q *QueueClient) Pop(ctx context.Context, queue string) (*QueueMessage, error) {
	return q.PopTimeout(ctx, queue, 0)
}

// PopTimeout retrieves a message from a queue with timeout
func (q *QueueClient) PopTimeout(ctx context.Context, queue string, timeout time.Duration) (*QueueMessage, error) {
	// Route based on queue name
	partition := q.client.partitionedClient.partitioner.Partition(queue)
	targetNode, err := q.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get target node for queue %s: %w", queue, err)
	}

	req := &queuepb.PopRequest{
		Queue:   queue,
		Timeout: int32(timeout.Seconds()),
	}

	resp, err := targetNode.Queue.Pop(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to pop from queue %s: %w", queue, err)
	}

	if !resp.Status.Success {
		return nil, fmt.Errorf("pop failed: %s", resp.Status.Message)
	}

	if resp.Message == nil {
		return nil, nil // No message available
	}

	return &QueueMessage{
		ID:         resp.Message.Id,
		Queue:      resp.Message.Queue,
		Data:       resp.Message.Data,
		CreatedAt:  time.Unix(resp.Message.CreatedAt, 0),
		DelayUntil: time.Unix(resp.Message.DelayUntil, 0),
		RetryCount: resp.Message.RetryCount,
		ConsumerID: resp.Message.ConsumerId,
	}, nil
}

// Ack acknowledges a message
func (q *QueueClient) Ack(ctx context.Context, messageID string) error {
	// For now, try all nodes (in production, we'd track which node has the message)
	q.client.partitionedClient.mu.RLock()
	defer q.client.partitionedClient.mu.RUnlock()

	var lastErr error
	for _, node := range q.client.partitionedClient.nodes {
		req := &queuepb.AckRequest{MessageId: messageID}
		resp, err := node.Queue.Ack(ctx, req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.Status.Success {
			return nil // Success on any node
		}
		lastErr = fmt.Errorf("ack failed: %s", resp.Status.Message)
	}

	return fmt.Errorf("failed to ack message %s: %w", messageID, lastErr)
}

// Nack negatively acknowledges a message (requeue for retry)
func (q *QueueClient) Nack(ctx context.Context, messageID string) error {
	// For now, try all nodes (in production, we'd track which node has the message)
	q.client.partitionedClient.mu.RLock()
	defer q.client.partitionedClient.mu.RUnlock()

	var lastErr error
	for _, node := range q.client.partitionedClient.nodes {
		req := &queuepb.NackRequest{MessageId: messageID}
		resp, err := node.Queue.Nack(ctx, req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.Status.Success {
			return nil // Success on any node
		}
		lastErr = fmt.Errorf("nack failed: %s", resp.Status.Message)
	}

	return fmt.Errorf("failed to nack message %s: %w", messageID, lastErr)
}

// Peek looks at messages without removing them
func (q *QueueClient) Peek(ctx context.Context, queue string) ([]*QueueMessage, error) {
	return q.PeekLimit(ctx, queue, 10)
}

// PeekLimit looks at up to limit messages without removing them
func (q *QueueClient) PeekLimit(ctx context.Context, queue string, limit int) ([]*QueueMessage, error) {
	// Route based on queue name
	partition := q.client.partitionedClient.partitioner.Partition(queue)
	targetNode, err := q.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get target node for queue %s: %w", queue, err)
	}

	req := &queuepb.PeekRequest{
		Queue: queue,
		Limit: int32(limit),
	}

	resp, err := targetNode.Queue.Peek(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to peek queue %s: %w", queue, err)
	}

	if !resp.Status.Success {
		return nil, fmt.Errorf("peek failed: %s", resp.Status.Message)
	}

	var messages []*QueueMessage
	for _, msg := range resp.Messages {
		messages = append(messages, &QueueMessage{
			ID:         msg.Id,
			Queue:      msg.Queue,
			Data:       msg.Data,
			CreatedAt:  time.Unix(msg.CreatedAt, 0),
			DelayUntil: time.Unix(msg.DelayUntil, 0),
			RetryCount: msg.RetryCount,
			ConsumerID: msg.ConsumerId,
		})
	}

	return messages, nil
}

// QueueStats represents queue statistics
type QueueStats struct {
	Name      string
	Size      int64
	Consumers int64
	Pending   int64
	Processed int64
	Failed    int64
}

// Stats gets queue statistics
func (q *QueueClient) Stats(ctx context.Context, queue string) (*QueueStats, error) {
	// Route based on queue name
	partition := q.client.partitionedClient.partitioner.Partition(queue)
	targetNode, err := q.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get target node for queue %s: %w", queue, err)
	}

	req := &queuepb.StatsRequest{Queue: queue}
	resp, err := targetNode.Queue.Stats(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats for queue %s: %w", queue, err)
	}

	if !resp.Status.Success {
		return nil, fmt.Errorf("stats failed: %s", resp.Status.Message)
	}

	return &QueueStats{
		Name:      resp.Stats.Name,
		Size:      resp.Stats.Size,
		Consumers: resp.Stats.Consumers,
		Pending:   resp.Stats.Pending,
		Processed: resp.Stats.Processed,
		Failed:    resp.Stats.Failed,
	}, nil
}

// Purge clears all messages from a queue
func (q *QueueClient) Purge(ctx context.Context, queue string) (int64, error) {
	// Route based on queue name
	partition := q.client.partitionedClient.partitioner.Partition(queue)
	targetNode, err := q.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get target node for queue %s: %w", queue, err)
	}

	req := &queuepb.PurgeRequest{Queue: queue}
	resp, err := targetNode.Queue.Purge(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to purge queue %s: %w", queue, err)
	}

	if !resp.Status.Success {
		return 0, fmt.Errorf("purge failed: %s", resp.Status.Message)
	}

	return resp.PurgedCount, nil
}

// Delete deletes an entire queue
func (q *QueueClient) Delete(ctx context.Context, queue string) error {
	// Route based on queue name
	partition := q.client.partitionedClient.partitioner.Partition(queue)
	targetNode, err := q.client.partitionedClient.getNodeForPartition(partition)
	if err != nil {
		return fmt.Errorf("failed to get target node for queue %s: %w", queue, err)
	}

	req := &queuepb.DeleteRequest{Queue: queue}
	resp, err := targetNode.Queue.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete queue %s: %w", queue, err)
	}

	if !resp.Status.Success {
		return fmt.Errorf("delete failed: %s", resp.Status.Message)
	}

	return nil
}

// List returns all queue names
func (q *QueueClient) List(ctx context.Context) ([]string, error) {
	// Query all nodes and combine results
	q.client.partitionedClient.mu.RLock()
	defer q.client.partitionedClient.mu.RUnlock()

	queueSet := make(map[string]bool)
	for _, node := range q.client.partitionedClient.nodes {
		req := &queuepb.ListRequest{}
		resp, err := node.Queue.List(ctx, req)
		if err != nil {
			continue // Skip nodes that fail
		}

		if resp.Status.Success {
			for _, queue := range resp.Queues {
				queueSet[queue] = true
			}
		}
	}

	var queues []string
	for queue := range queueSet {
		queues = append(queues, queue)
	}

	return queues, nil
}

// ClusterClient provides cluster management API
type ClusterClient struct {
	client *GomsgClient
}

// Nodes returns information about all cluster nodes
func (c *ClusterClient) Nodes(ctx context.Context) ([]*Node, error) {
	nodes, err := c.client.partitionedClient.GetNodes(ctx)
	if err != nil {
		return nil, err
	}

	var result []*Node
	for _, node := range nodes {
		result = append(result, &Node{
			ID:      node.Id,
			Address: node.Address,
			State:   node.State,
		})
	}

	return result, nil
}

// Node represents a cluster node
type Node struct {
	ID      string
	Address string
	State   string
}

// Status returns cluster health status
func (c *ClusterClient) Status(ctx context.Context) (string, error) {
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return "", err
	}

	activeNodes := 0
	for _, node := range nodes {
		if node.State == "active" {
			activeNodes++
		}
	}

	return fmt.Sprintf("Cluster: %d/%d nodes active", activeNodes, len(nodes)), nil
}

// Leader returns the current leader node
func (c *ClusterClient) Leader(ctx context.Context) (*Node, error) {
	c.client.partitionedClient.mu.RLock()
	defer c.client.partitionedClient.mu.RUnlock()

	if len(c.client.partitionedClient.nodes) == 0 {
		return nil, fmt.Errorf("no active nodes available")
	}

	// Use any node to get leader info
	var client *nodeClient
	for _, nc := range c.client.partitionedClient.nodes {
		client = nc
		break
	}

	resp, err := client.Cluster.GetLeader(ctx, &clusterpb.GetLeaderRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get leader: %w", err)
	}

	if !resp.Status.Success {
		return nil, fmt.Errorf("get leader failed: %s", resp.Status.Message)
	}

	return &Node{
		ID:      resp.Leader.Id,
		Address: resp.Leader.Address,
		State:   resp.Leader.State,
	}, nil
}
