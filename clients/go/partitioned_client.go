package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	clusterpb "gomsg/api/generated/cluster"
	kvpb "gomsg/api/generated/kv"
	queuepb "gomsg/api/generated/queue"
	"gomsg/pkg/cluster"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Options holds basic connection options
type Options struct {
	DialTimeout time.Duration
	Insecure    bool
}

// PartitionedOptions holds configuration for the partitioned client
type PartitionedOptions struct {
	*Options
	Partitions      int32
	RefreshInterval time.Duration
	InitialNodes    []string
}

// nodeClient represents a connection to a single node
type nodeClient struct {
	nodeID string
	addr   string
	conn   *grpc.ClientConn
	KV     kvpb.KVServiceClient
	Queue  queuepb.QueueServiceClient
	Cluster clusterpb.ClusterServiceClient
}

// PartitionedClient manages connections to multiple nodes and handles partitioning
type PartitionedClient struct {
	mu          sync.RWMutex
	nodes       map[string]*nodeClient
	partitioner *cluster.Partitioner
	options     *PartitionedOptions
	
	// Cluster topology
	clusterNodes map[string]*clusterpb.Node
	
	// Background refresh
	ctx    context.Context
	cancel context.CancelFunc
}

// NewPartitioned creates a new partitioned client
func NewPartitioned(ctx context.Context, opts *PartitionedOptions) (*PartitionedClient, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}
	if len(opts.InitialNodes) == 0 {
		return nil, fmt.Errorf("at least one initial node must be provided")
	}
	if opts.Partitions <= 0 {
		opts.Partitions = 16
	}
	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = 30 * time.Second
	}

	clientCtx, cancel := context.WithCancel(context.Background())
	
	pc := &PartitionedClient{
		nodes:        make(map[string]*nodeClient),
		partitioner:  cluster.NewPartitioner(opts.Partitions, 16),
		options:      opts,
		clusterNodes: make(map[string]*clusterpb.Node),
		ctx:          clientCtx,
		cancel:       cancel,
	}

	// Connect to initial nodes
	if err := pc.connectToInitialNodes(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to initial nodes: %w", err)
	}

	// Start background refresh
	go pc.refreshLoop()

	return pc, nil
}

// connectToInitialNodes establishes connections to the initial set of nodes
func (pc *PartitionedClient) connectToInitialNodes(ctx context.Context) error {
	var lastErr error
	connected := 0

	for _, addr := range pc.options.InitialNodes {
		if err := pc.connectToNode(ctx, addr); err != nil {
			lastErr = err
			continue
		}
		connected++
	}

	if connected == 0 {
		return fmt.Errorf("failed to connect to any initial nodes: %w", lastErr)
	}

	// Refresh cluster topology
	return pc.refreshClusterTopology(ctx)
}

// connectToNode establishes a connection to a single node
func (pc *PartitionedClient) connectToNode(ctx context.Context, addr string) error {
	var opts []grpc.DialOption
	if pc.options.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	dialCtx, cancel := context.WithTimeout(ctx, pc.options.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	// Create a temporary node ID (will be updated from cluster info)
	nodeID := fmt.Sprintf("node-%s", addr)
	
	node := &nodeClient{
		nodeID:  nodeID,
		addr:    addr,
		conn:    conn,
		KV:      kvpb.NewKVServiceClient(conn),
		Queue:   queuepb.NewQueueServiceClient(conn),
		Cluster: clusterpb.NewClusterServiceClient(conn),
	}

	pc.mu.Lock()
	pc.nodes[nodeID] = node
	pc.mu.Unlock()

	return nil
}

// refreshClusterTopology updates the cluster topology information
func (pc *PartitionedClient) refreshClusterTopology(ctx context.Context) error {
	pc.mu.RLock()
	var anyNode *nodeClient
	for _, node := range pc.nodes {
		anyNode = node
		break
	}
	pc.mu.RUnlock()

	if anyNode == nil {
		return fmt.Errorf("no nodes available")
	}

	resp, err := anyNode.Cluster.GetNodes(ctx, &clusterpb.GetNodesRequest{})
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes: %w", err)
	}

	if !resp.Status.Success {
		return fmt.Errorf("get nodes failed: %s", resp.Status.Message)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Update cluster nodes map
	pc.clusterNodes = make(map[string]*clusterpb.Node)
	for _, node := range resp.Nodes {
		pc.clusterNodes[node.Id] = node
		
		// Update node ID if we have a connection to this address
		for _, clientNode := range pc.nodes {
			if clientNode.addr == node.Address {
				// Update the node ID
				delete(pc.nodes, clientNode.nodeID)
				clientNode.nodeID = node.Id
				pc.nodes[node.Id] = clientNode
				break
			}
		}
	}

	return nil
}

// refreshLoop periodically refreshes cluster topology
func (pc *PartitionedClient) refreshLoop() {
	ticker := time.NewTicker(pc.options.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pc.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(pc.ctx, 10*time.Second)
			_ = pc.refreshClusterTopology(ctx)
			cancel()
		}
	}
}

// getNodeForPartition returns the node responsible for a given partition
func (pc *PartitionedClient) getNodeForPartition(partition int32) (*nodeClient, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if len(pc.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	// For now, use simple round-robin based on partition
	// In a real implementation, this would use consistent hashing
	nodeIDs := make([]string, 0, len(pc.nodes))
	for nodeID := range pc.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}

	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no active nodes")
	}

	targetNodeID := nodeIDs[int(partition)%len(nodeIDs)]
	node, exists := pc.nodes[targetNodeID]
	if !exists {
		return nil, fmt.Errorf("target node %s not found", targetNodeID)
	}

	return node, nil
}

// Set stores a key-value pair
func (pc *PartitionedClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	partition := pc.partitioner.Partition(key)
	node, err := pc.getNodeForPartition(partition)
	if err != nil {
		return err
	}

	req := &kvpb.SetRequest{
		Key:   key,
		Value: value,
		Ttl:   int64(ttl.Seconds()),
	}

	resp, err := node.KV.Set(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Status.Success {
		return fmt.Errorf("set failed: %s", resp.Status.Message)
	}

	return nil
}

// Get retrieves a value by key
func (pc *PartitionedClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	partition := pc.partitioner.Partition(key)
	node, err := pc.getNodeForPartition(partition)
	if err != nil {
		return nil, false, err
	}

	req := &kvpb.GetRequest{Key: key}
	resp, err := node.KV.Get(ctx, req)
	if err != nil {
		return nil, false, err
	}

	if !resp.Status.Success {
		return nil, false, fmt.Errorf("get failed: %s", resp.Status.Message)
	}

	return resp.Value, resp.Found, nil
}

// Exists checks if a key exists
func (pc *PartitionedClient) Exists(ctx context.Context, key string) (bool, error) {
	partition := pc.partitioner.Partition(key)
	node, err := pc.getNodeForPartition(partition)
	if err != nil {
		return false, err
	}

	req := &kvpb.ExistsRequest{Key: key}
	resp, err := node.KV.Exists(ctx, req)
	if err != nil {
		return false, err
	}

	if !resp.Status.Success {
		return false, fmt.Errorf("exists failed: %s", resp.Status.Message)
	}

	return resp.Exists, nil
}

// Del deletes one or more keys
func (pc *PartitionedClient) Del(ctx context.Context, keys ...string) (int32, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Group keys by partition/node
	nodeKeys := make(map[string][]string)
	for _, key := range keys {
		partition := pc.partitioner.Partition(key)
		node, err := pc.getNodeForPartition(partition)
		if err != nil {
			return 0, err
		}
		nodeKeys[node.nodeID] = append(nodeKeys[node.nodeID], key)
	}

	var totalDeleted int32
	for nodeID, keyList := range nodeKeys {
		pc.mu.RLock()
		node, exists := pc.nodes[nodeID]
		pc.mu.RUnlock()

		if !exists {
			continue
		}

		req := &kvpb.DelRequest{Keys: keyList}
		resp, err := node.KV.Del(ctx, req)
		if err != nil {
			return totalDeleted, err
		}

		if resp.Status.Success {
			totalDeleted += resp.Deleted
		}
	}

	return totalDeleted, nil
}

// GetNodes returns cluster node information
func (pc *PartitionedClient) GetNodes(ctx context.Context) ([]*clusterpb.Node, error) {
	pc.mu.RLock()
	nodes := make([]*clusterpb.Node, 0, len(pc.clusterNodes))
	for _, node := range pc.clusterNodes {
		nodes = append(nodes, node)
	}
	pc.mu.RUnlock()

	return nodes, nil
}

// Close closes all connections
func (pc *PartitionedClient) Close() error {
	pc.cancel()

	pc.mu.Lock()
	defer pc.mu.Unlock()

	var lastErr error
	for _, node := range pc.nodes {
		if err := node.conn.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}
