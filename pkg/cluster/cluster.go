package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gomsg/storage"
)

// Cluster manages a partitioned, replicated GoMsg cluster
type Cluster struct {
	mu sync.RWMutex
	
	// Node identity
	nodeID   string
	bindAddr string
	
	// Cluster configuration
	partitions      int32
	replicationFactor int32
	
	// Partition management
	partitionMap    *PartitionMap
	storage         storage.Storage
	
	// Node discovery and health
	nodes           map[string]*Node
	nodeHealth      map[string]time.Time
	
	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Config defines cluster configuration
type Config struct {
	NodeID            string
	BindAddr          string
	Partitions        int32  // Total number of partitions in cluster
	ReplicationFactor int32  // Number of replicas per partition
	DataDir           string
	Bootstrap         bool   // True if this is the first node
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
	
	return nil
}

// joinCluster attempts to join an existing cluster
func (c *Cluster) joinCluster(seedNodes []string) error {
	// TODO: Implement cluster join logic
	// This would involve:
	// 1. Contact seed nodes to get cluster topology
	// 2. Register this node with the cluster
	// 3. Trigger partition rebalancing
	
	return fmt.Errorf("cluster join not implemented yet")
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
	defer c.mu.Unlock()
	
	delete(c.nodes, nodeID)
	delete(c.nodeHealth, nodeID)
	
	// Trigger partition rebalancing
	go c.rebalancePartitions()
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

// rebalancePartitions redistributes partitions across available nodes
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
	
	// Rebalance partition assignments
	c.partitionMap.Rebalance(activeNodes, c.replicationFactor)
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
	return nil
}
