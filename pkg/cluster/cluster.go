package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/skshohagmiah/fluxdl/storage"
	// TODO: Uncomment after generating gRPC code with: protoc --go_out=. --go-grpc_out=. api/proto/*.proto
	// clusterpb "github.com/skshohagmiah/fluxdl/api/generated/cluster"
	// "google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
)

// Cluster manages a partitioned, replicated fluxdl cluster
type Cluster struct {
	mu sync.RWMutex

	// Node identity
	nodeID   string
	bindAddr string

	// Cluster configuration
	partitions        int32
	replicationFactor int32

	// Partition management
	partitionMap *PartitionMap
	storage      storage.Storage

	// Raft consensus
	raftManager *Manager

	// Metrics and monitoring
	metrics *Metrics

	// Node discovery and health
	nodes      map[string]*Node
	nodeHealth map[string]time.Time

	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Config defines cluster configuration
type Config struct {
	NodeID            string
	BindAddr          string
	Partitions        int32 // Total number of partitions in cluster
	ReplicationFactor int32 // Number of replicas per partition
	DataDir           string
	Bootstrap         bool     // True if this is the first node
	SeedNodes         []string // Other nodes to join
}

// New creates a new cluster instance
func New(ctx context.Context, storage storage.Storage, cfg Config) (*Cluster, error) {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 32 // default
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 3 // default
	}

	ctx, cancel := context.WithCancel(ctx)

	c := &Cluster{
		nodeID:            cfg.NodeID,
		bindAddr:          cfg.BindAddr,
		partitions:        cfg.Partitions,
		replicationFactor: cfg.ReplicationFactor,
		storage:           storage,
		nodes:             make(map[string]*Node),
		nodeHealth:        make(map[string]time.Time),
		ctx:               ctx,
		cancel:            cancel,
	}

	// Initialize partition map
	c.partitionMap = NewPartitionMap(cfg.Partitions, cfg.ReplicationFactor)

	// Initialize metrics
	c.metrics = NewMetrics()
	c.metrics.UpdateHealthStatus("starting")

	// Initialize Raft for consensus
	raftConfig := RaftConfig{
		NodeID:    cfg.NodeID,
		BindAddr:  fmt.Sprintf("%s:7000", cfg.BindAddr), // Use different port for Raft
		DataDir:   cfg.DataDir,
		Bootstrap: cfg.Bootstrap,
	}

	raftManager, err := Start(ctx, c.storage, raftConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start Raft: %w", err)
	}
	c.raftManager = raftManager

	// Start cluster
	if err := c.start(cfg); err != nil {
		cancel()
		return nil, err
	}

	return c, nil
}

// start initializes the cluster
func (c *Cluster) start(cfg Config) error {
	// Add self as a node
	c.addNode(&Node{
		ID:       c.nodeID,
		Address:  c.bindAddr,
		State:    NodeStateActive,
		LastSeen: time.Now(),
	})

	if cfg.Bootstrap {
		// Bootstrap: assign all partitions to self initially
		c.partitionMap.Bootstrap(c.nodeID)
	} else {
		// Join existing cluster
		if err := c.joinCluster(cfg.SeedNodes); err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
	}

	// Start background tasks
	c.startBackgroundTasks()

	// Update metrics status
	c.metrics.UpdateHealthStatus("active")

	return nil
}

// joinCluster attempts to join an existing cluster
func (c *Cluster) joinCluster(seedNodes []string) error {
	// Try to contact seed nodes to get cluster topology
	var clusterInfo *ClusterInfo
	var seedNode string

	for _, seed := range seedNodes {
		info, err := c.contactSeedNode(seed)
		if err != nil {
			continue // Try next seed
		}
		clusterInfo = info
		seedNode = seed
		break
	}

	if clusterInfo == nil {
		return fmt.Errorf("failed to contact any seed nodes")
	}

	// Register this node with the cluster
	if err := c.registerWithCluster(seedNode); err != nil {
		return fmt.Errorf("failed to register with cluster: %w", err)
	}

	// Import existing cluster topology
	c.importClusterTopology(clusterInfo)

	// Trigger partition rebalancing to include this node
	go c.rebalancePartitions()

	return nil
}

// contactSeedNode contacts a seed node to get cluster information
func (c *Cluster) contactSeedNode(seedAddr string) (*ClusterInfo, error) {
	// TODO: Implement actual gRPC call after generating protobuf code
	// conn, err := grpc.Dial(seedAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	//     return nil, fmt.Errorf("failed to dial seed node %s: %w", seedAddr, err)
	// }
	// defer conn.Close()
	//
	// client := clusterpb.NewClusterServiceClient(conn)
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	//
	// resp, err := client.GetClusterInfo(ctx, &clusterpb.GetClusterInfoRequest{})

	// Placeholder implementation for now
	resp := struct {
		Status struct {
			Success bool
			Message string
		}
		ClusterInfo struct {
			NodeId            string
			TotalPartitions   int32
			ReplicationFactor int32
			TotalNodes        int32
			ActiveNodes       int32
			Partitions        []struct {
				Id       int32
				Primary  string
				Replicas []string
			}
		}
	}{
		Status: struct {
			Success bool
			Message string
		}{Success: true, Message: "OK"},
		ClusterInfo: struct {
			NodeId            string
			TotalPartitions   int32
			ReplicationFactor int32
			TotalNodes        int32
			ActiveNodes       int32
			Partitions        []struct {
				Id       int32
				Primary  string
				Replicas []string
			}
		}{
			NodeId:            "seed-node",
			TotalPartitions:   c.partitions,
			ReplicationFactor: c.replicationFactor,
			TotalNodes:        1,
			ActiveNodes:       1,
			Partitions: []struct {
				Id       int32
				Primary  string
				Replicas []string
			}{},
		},
	}
	var err error
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}

	if !resp.Status.Success {
		return nil, fmt.Errorf("get cluster info failed: %s", resp.Status.Message)
	}

	// Convert protobuf to internal type
	info := &ClusterInfo{
		NodeID:            resp.ClusterInfo.NodeId,
		TotalPartitions:   resp.ClusterInfo.TotalPartitions,
		ReplicationFactor: resp.ClusterInfo.ReplicationFactor,
		TotalNodes:        resp.ClusterInfo.TotalNodes,
		ActiveNodes:       resp.ClusterInfo.ActiveNodes,
		Partitions:        make([]*PartitionInfo, len(resp.ClusterInfo.Partitions)),
	}

	for i, p := range resp.ClusterInfo.Partitions {
		info.Partitions[i] = &PartitionInfo{
			ID:       p.Id,
			Primary:  p.Primary,
			Replicas: p.Replicas,
		}
	}

	return info, nil
}

// registerWithCluster registers this node with an existing cluster
func (c *Cluster) registerWithCluster(seedAddr string) error {
	// TODO: Implement actual gRPC call after generating protobuf code
	// conn, err := grpc.Dial(seedAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	//     return fmt.Errorf("failed to dial seed node %s: %w", seedAddr, err)
	// }
	// defer conn.Close()
	//
	// client := clusterpb.NewClusterServiceClient(conn)
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	//
	// resp, err := client.Join(ctx, &clusterpb.JoinRequest{
	//     NodeId:  c.nodeID,
	//     Address: c.bindAddr,
	// })

	// Placeholder implementation for now
	resp := struct {
		Status struct {
			Success bool
			Message string
		}
	}{
		Status: struct {
			Success bool
			Message string
		}{Success: true, Message: "OK"},
	}
	var err error
	if err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}

	if !resp.Status.Success {
		return fmt.Errorf("join cluster failed: %s", resp.Status.Message)
	}

	// Add ourselves to the local node list after successful join
	c.addNode(&Node{
		ID:       c.nodeID,
		Address:  c.bindAddr,
		State:    NodeStateActive,
		LastSeen: time.Now(),
	})

	return nil
}

// importClusterTopology imports existing cluster topology
func (c *Cluster) importClusterTopology(info *ClusterInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Import partition assignments from cluster info
	for _, partInfo := range info.Partitions {
		if len(partInfo.Replicas) > 0 {
			c.partitionMap.AssignPartition(partInfo.ID, partInfo.Replicas)
		}
	}
}

// startBackgroundTasks starts health checking and rebalancing
func (c *Cluster) startBackgroundTasks() {
	// Health checker
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				c.checkNodeHealth()
			}
		}
	}()

	// Partition rebalancer
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				c.rebalancePartitions()
			}
		}
	}()

	// Start metrics collection
	c.startMetricsCollection()
}

// addNode adds or updates a node in the cluster
func (c *Cluster) addNode(node *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodes[node.ID] = node
	c.nodeHealth[node.ID] = time.Now()
}

// removeNode removes a node from the cluster
func (c *Cluster) removeNode(nodeID string) {
	c.mu.Lock()
	node, exists := c.nodes[nodeID]
	if !exists {
		c.mu.Unlock()
		return
	}

	// Mark node as leaving
	node.State = NodeStateLeaving
	c.nodes[nodeID] = node
	c.mu.Unlock()

	// Record metrics
	c.metrics.RecordClusterEvent("node_leave")

	// Start graceful removal process
	go c.gracefulNodeRemoval(nodeID)
}

// gracefulNodeRemoval handles the graceful removal of a node
func (c *Cluster) gracefulNodeRemoval(nodeID string) {
	// Get partitions owned by this node
	partitions := c.partitionMap.GetPartitionsForNode(nodeID)

	// Migrate data from this node to other nodes
	for _, partition := range partitions {
		if err := c.migratePartitionData(partition, nodeID); err != nil {
			// Log error but continue with other partitions
			continue
		}
	}

	// Remove node from partition map
	c.partitionMap.RemoveNode(nodeID)

	// Remove from cluster
	c.mu.Lock()
	delete(c.nodes, nodeID)
	delete(c.nodeHealth, nodeID)
	c.mu.Unlock()

	// Trigger rebalancing to redistribute partitions
	c.rebalancePartitions()
}

// migratePartitionData migrates data for a partition from one node to others
func (c *Cluster) migratePartitionData(partition int32, fromNodeID string) error {
	// Get new owners for this partition (excluding the leaving node)
	c.mu.RLock()
	activeNodes := make([]string, 0)
	for nodeID, node := range c.nodes {
		if nodeID != fromNodeID && node.State == NodeStateActive {
			activeNodes = append(activeNodes, nodeID)
		}
	}
	c.mu.RUnlock()

	if len(activeNodes) == 0 {
		return fmt.Errorf("no active nodes available for migration")
	}

	// For now, this is a placeholder for data migration
	// In a real implementation, this would:
	// 1. Get all keys for this partition from the leaving node
	// 2. Transfer them to the new owner nodes
	// 3. Verify the transfer completed successfully
	// 4. Remove the data from the leaving node

	// TODO: Implement actual data migration logic
	// This would involve gRPC calls to transfer data between nodes

	return nil
}

// JoinNode handles a new node joining the cluster
func (c *Cluster) JoinNode(nodeID, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if node already exists
	if _, exists := c.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists in cluster", nodeID)
	}

	// Add new node
	newNode := &Node{
		ID:       nodeID,
		Address:  address,
		State:    NodeStateJoining,
		LastSeen: time.Now(),
	}

	c.nodes[nodeID] = newNode
	c.nodeHealth[nodeID] = time.Now()

	// Mark as active after successful join
	newNode.State = NodeStateActive

	// Record metrics
	c.metrics.RecordClusterEvent("node_join")

	// Trigger rebalancing to include new node
	go c.rebalancePartitions()

	return nil
}

// LeaveNode handles a node gracefully leaving the cluster
func (c *Cluster) LeaveNode(nodeID string) error {
	c.mu.RLock()
	node, exists := c.nodes[nodeID]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node %s not found in cluster", nodeID)
	}

	if node.State != NodeStateActive {
		return fmt.Errorf("node %s is not active", nodeID)
	}

	// Start graceful removal
	c.removeNode(nodeID)
	return nil
}

// checkNodeHealth checks if nodes are still alive
func (c *Cluster) checkNodeHealth() {
	c.mu.RLock()
	now := time.Now()
	deadNodes := make([]string, 0)

	for nodeID, lastSeen := range c.nodeHealth {
		if now.Sub(lastSeen) > 30*time.Second { // 30s timeout
			deadNodes = append(deadNodes, nodeID)
		}
	}
	c.mu.RUnlock()

	// Remove dead nodes
	for _, nodeID := range deadNodes {
		if nodeID != c.nodeID { // Don't remove self
			c.removeNode(nodeID)
		}
	}
}

// rebalancePartitions redistributes partitions across available nodes using Raft consensus
func (c *Cluster) rebalancePartitions() {
	c.mu.RLock()
	activeNodes := make([]string, 0, len(c.nodes))
	for nodeID, node := range c.nodes {
		if node.State == NodeStateActive {
			activeNodes = append(activeNodes, nodeID)
		}
	}
	c.mu.RUnlock()

	if len(activeNodes) == 0 {
		return
	}

	// Only leader can initiate rebalancing
	if !c.raftManager.IsLeader() {
		return
	}

	// Check if we have quorum before making changes
	if !c.hasQuorum() {
		// Log warning: cluster doesn't have quorum, cannot rebalance
		return
	}

	// Calculate new partition assignments
	newAssignments := c.calculatePartitionAssignments(activeNodes)

	// Apply assignments through Raft consensus
	for partition, owners := range newAssignments {
		if err := c.proposePartitionAssignment(partition, owners); err != nil {
			// Log error but continue with other partitions
			continue
		}
	}
}

// calculatePartitionAssignments calculates optimal partition distribution
func (c *Cluster) calculatePartitionAssignments(activeNodes []string) map[int32][]string {
	assignments := make(map[int32][]string)

	// Use consistent hashing for deterministic assignment
	for partition := int32(0); partition < c.partitions; partition++ {
		owners := make([]string, 0, c.replicationFactor)

		// Calculate replicas needed (min of replicationFactor and available nodes)
		replicas := int(c.replicationFactor)
		if replicas > len(activeNodes) {
			replicas = len(activeNodes)
		}

		// Assign nodes using consistent hashing
		for i := 0; i < replicas; i++ {
			nodeIndex := (int(partition) + i) % len(activeNodes)
			owners = append(owners, activeNodes[nodeIndex])
		}

		assignments[partition] = owners
	}

	return assignments
}

// proposePartitionAssignment proposes a partition assignment through Raft
func (c *Cluster) proposePartitionAssignment(partition int32, owners []string) error {
	// Create partition assignment command
	payload := struct {
		Partition int32    `json:"partition"`
		Owners    []string `json:"owners"`
	}{
		Partition: partition,
		Owners:    owners,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := Command{
		Version: 1,
		Type:    CmdPartitionAssign,
		Payload: payloadBytes,
	}

	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Apply through Raft consensus
	future := c.raftManager.Raft().Apply(cmdBytes, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// Update local partition map after consensus
	c.partitionMap.AssignPartition(partition, owners)

	return nil
}

// GetPartitionOwners returns the nodes that own a partition (primary + replicas)
func (c *Cluster) GetPartitionOwners(partition int32) []string {
	return c.partitionMap.GetOwners(partition)
}

// GetPartitionPrimary returns the primary owner of a partition
func (c *Cluster) GetPartitionPrimary(partition int32) string {
	owners := c.partitionMap.GetOwners(partition)
	if len(owners) > 0 {
		return owners[0] // First owner is primary
	}
	return ""
}

// GetKeyPartition returns the partition for a given key
func (c *Cluster) GetKeyPartition(key string) int32 {
	return c.partitionMap.GetPartition(key)
}

// GetKeyOwners returns the nodes that own a key (primary + replicas)
func (c *Cluster) GetKeyOwners(key string) []string {
	partition := c.GetKeyPartition(key)
	return c.GetPartitionOwners(partition)
}

// GetKeyPrimary returns the primary owner of a key
func (c *Cluster) GetKeyPrimary(key string) string {
	partition := c.GetKeyPartition(key)
	return c.GetPartitionPrimary(partition)
}

// OwnsKey returns true if this node owns the key (primary or replica)
func (c *Cluster) OwnsKey(key string) bool {
	owners := c.GetKeyOwners(key)
	for _, owner := range owners {
		if owner == c.nodeID {
			return true
		}
	}
	return false
}

// IsPrimaryFor returns true if this node is the primary owner of the key
func (c *Cluster) IsPrimaryFor(key string) bool {
	return c.GetKeyPrimary(key) == c.nodeID
}

// GetNodes returns all nodes in the cluster
func (c *Cluster) GetNodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetActiveNodes returns only active nodes
func (c *Cluster) GetActiveNodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		if node.State == NodeStateActive {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetClusterInfo returns cluster topology information
func (c *Cluster) GetClusterInfo() *ClusterInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info := &ClusterInfo{
		NodeID:            c.nodeID,
		TotalPartitions:   c.partitions,
		ReplicationFactor: c.replicationFactor,
		TotalNodes:        int32(len(c.nodes)),
		ActiveNodes:       0,
		Partitions:        make([]*PartitionInfo, c.partitions),
	}

	// Count active nodes
	for _, node := range c.nodes {
		if node.State == NodeStateActive {
			info.ActiveNodes++
		}
	}

	// Get partition information
	for i := int32(0); i < c.partitions; i++ {
		owners := c.partitionMap.GetOwners(i)
		primary := ""
		if len(owners) > 0 {
			primary = owners[0]
		}

		info.Partitions[i] = &PartitionInfo{
			ID:       i,
			Primary:  primary,
			Replicas: owners,
		}
	}

	return info
}

// Close shuts down the cluster
func (c *Cluster) Close() error {
	c.cancel()
	c.wg.Wait()
	if c.raftManager != nil {
		c.raftManager.Close()
	}
	return nil
}

// hasQuorum checks if the cluster has a majority of nodes active
func (c *Cluster) hasQuorum() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalNodes := len(c.nodes)
	activeNodes := 0

	for _, node := range c.nodes {
		if node.State == NodeStateActive {
			activeNodes++
		}
	}

	// Need majority (more than half) for quorum
	quorumSize := (totalNodes / 2) + 1
	return activeNodes >= quorumSize
}

// isReadOnlyMode checks if cluster should be in read-only mode
func (c *Cluster) isReadOnlyMode() bool {
	// If we don't have quorum, we should be read-only
	return !c.hasQuorum()
}

// canAcceptWrites checks if this node can accept write operations
func (c *Cluster) canAcceptWrites() bool {
	// Must be leader and have quorum
	return c.raftManager.IsLeader() && c.hasQuorum()
}

// startMetricsCollection starts background metrics collection
func (c *Cluster) startMetricsCollection() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(30 * time.Second) // Collect metrics every 30 seconds
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				c.collectResourceMetrics()
			}
		}
	}()
}

// collectResourceMetrics collects system resource metrics
func (c *Cluster) collectResourceMetrics() {
	// In a real implementation, this would collect actual system metrics
	// For now, we'll use placeholder values

	// Memory usage (would use runtime.MemStats or system calls)
	memoryUsage := uint64(50 * 1024 * 1024) // 50MB placeholder

	// Disk usage (would check actual disk usage)
	diskUsage := uint64(1024 * 1024 * 1024) // 1GB placeholder

	// Connection count (would count actual connections)
	connectionCount := uint64(len(c.nodes))

	c.metrics.UpdateResourceUsage(memoryUsage, diskUsage, connectionCount)

	// Update health status based on cluster state
	health := c.GetClusterHealth()
	c.metrics.UpdateHealthStatus(health.Status)
}

// GetMetrics returns the current metrics snapshot
func (c *Cluster) GetMetrics() *MetricsSnapshot {
	c.mu.RLock()
	nodeCount := int32(len(c.nodes))
	activeNodes := int32(0)
	for _, node := range c.nodes {
		if node.State == NodeStateActive {
			activeNodes++
		}
	}
	c.mu.RUnlock()

	return c.metrics.GetSnapshot(nodeCount, activeNodes)
}

// GetClusterHealth returns the current cluster health status (moved from earlier incomplete implementation)
func (c *Cluster) GetClusterHealth() *ClusterHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalNodes := len(c.nodes)
	activeNodes := 0
	for _, node := range c.nodes {
		if node.State == NodeStateActive {
			activeNodes++
		}
	}

	hasQuorum := c.hasQuorum()
	isLeader := c.raftManager.IsLeader()
	leaderID := c.raftManager.LeaderID()

	status := "healthy"
	if !hasQuorum {
		status = "no-quorum"
	} else if !isLeader && leaderID == "" {
		status = "no-leader"
	}

	return &ClusterHealth{
		Status:       status,
		TotalNodes:   int32(totalNodes),
		ActiveNodes:  int32(activeNodes),
		HasQuorum:    hasQuorum,
		IsLeader:     isLeader,
		LeaderID:     leaderID,
		CanWrite:     c.canAcceptWrites(),
		ReadOnlyMode: c.isReadOnlyMode(),
	}
}

// GetLeaderID returns the current leader ID
func (c *Cluster) GetLeaderID() string {
	if c.raftManager == nil {
		return ""
	}
	return c.raftManager.LeaderID()
}
