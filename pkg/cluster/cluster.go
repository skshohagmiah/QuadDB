package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	clusterpb "github.com/skshohagmiah/gomsg/api/generated/cluster"
	"github.com/skshohagmiah/gomsg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Cluster manages a distributed GoMsg cluster
type Cluster struct {
	mu sync.RWMutex

	// Configuration
	nodeID   string
	bindAddr string

	// Cluster state
	nodes      map[string]*Node
	nodeHealth map[string]time.Time

	// Raft consensus
	raftManager *RaftManager
	fsm         *FSM

	// Storage
	storage storage.Storage

	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new cluster instance
func New(ctx context.Context, storage storage.Storage, cfg Config) (*Cluster, error) {
	ctx, cancel := context.WithCancel(ctx)

	cluster := &Cluster{
		nodeID:     cfg.NodeID,
		bindAddr:   cfg.BindAddr,
		storage:    storage,
		nodes:      make(map[string]*Node),
		nodeHealth: make(map[string]time.Time),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Create FSM
	cluster.fsm = NewFSM(storage, cluster)

	// Start Raft
	raftManager, err := StartRaft(ctx, cfg, cluster.fsm)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("start raft: %w", err)
	}
	cluster.raftManager = raftManager

	// Add self as a node
	cluster.addNode(&Node{
		ID:       cfg.NodeID,
		Address:  cfg.BindAddr,
		State:    NodeStateActive,
		LastSeen: time.Now(),
	})

	// Join existing cluster if not bootstrapping (do this asynchronously but more aggressively)
	if !cfg.Bootstrap && len(cfg.SeedNodes) > 0 {
		go func() {
			fmt.Printf("Attempting to join cluster with seed nodes: %v\n", cfg.SeedNodes)
			
			// Wait for Raft transport to be fully ready
			time.Sleep(3 * time.Second)
			
			// Retry join with exponential backoff
			maxRetries := 15
			for i := 0; i < maxRetries; i++ {
				if err := cluster.joinCluster(cfg.SeedNodes); err != nil {
					fmt.Printf("Join attempt %d/%d failed: %v\n", i+1, maxRetries, err)
					if i < maxRetries-1 {
						backoff := time.Duration(1<<uint(i)) * time.Second
						if backoff > 30*time.Second {
							backoff = 30 * time.Second
						}
						time.Sleep(backoff)
					}
				} else {
					fmt.Printf("Successfully joined cluster!\n")
					return
				}
			}
			fmt.Printf("Failed to join cluster after %d attempts\n", maxRetries)
		}()
	}

	// Start background tasks
	cluster.startBackgroundTasks()

	return cluster, nil
}

// addNode adds a node to the cluster (internal method)
func (c *Cluster) addNode(node *Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodes[node.ID] = node
	c.nodeHealth[node.ID] = time.Now()
	return nil
}

// removeNode removes a node from the cluster (internal method)
func (c *Cluster) removeNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.nodes, nodeID)
	delete(c.nodeHealth, nodeID)
	return nil
}

// JoinNode adds a new node to the cluster
func (c *Cluster) JoinNode(nodeID, address string) error {
	if !c.raftManager.IsLeader() {
		return fmt.Errorf("only leader can add nodes")
	}

	fmt.Printf("Adding node %s at %s to cluster\n", nodeID, address)

	// Add to Raft cluster with retry logic
	var raftErr error
	for i := 0; i < 3; i++ {
		raftErr = c.raftManager.AddVoter(nodeID, address)
		if raftErr == nil {
			break
		}
		fmt.Printf("Raft AddVoter attempt %d failed: %v\n", i+1, raftErr)
		time.Sleep(time.Second * time.Duration(i+1))
	}
	
	if raftErr != nil {
		return fmt.Errorf("add voter to raft after retries: %w", raftErr)
	}

	// Apply node join command
	cmd := Command{
		Version: 1,
		Type:    CmdNodeJoin,
	}

	node := Node{
		ID:       nodeID,
		Address:  address,
		State:    NodeStateJoining,
		LastSeen: time.Now(),
	}

	payload, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("marshal node: %w", err)
	}
	cmd.Payload = payload

	if err := c.raftManager.Apply(cmd, 10*time.Second); err != nil {
		return fmt.Errorf("apply join command: %w", err)
	}

	fmt.Printf("Successfully added node %s to cluster\n", nodeID)
	return nil
}

// LeaveNode removes a node from the cluster
func (c *Cluster) LeaveNode(nodeID string) error {
	if !c.raftManager.IsLeader() {
		return fmt.Errorf("only leader can remove nodes")
	}

	// Remove from Raft cluster
	if err := c.raftManager.RemoveServer(nodeID); err != nil {
		return fmt.Errorf("remove server from raft: %w", err)
	}

	// Apply node leave command
	cmd := Command{
		Version: 1,
		Type:    CmdNodeLeave,
	}

	req := struct {
		NodeID string `json:"node_id"`
	}{
		NodeID: nodeID,
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal leave request: %w", err)
	}
	cmd.Payload = payload

	return c.raftManager.Apply(cmd, 5*time.Second)
}

// GetNodes returns all cluster nodes
func (c *Cluster) GetNodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetLeaderID returns the current leader ID
func (c *Cluster) GetLeaderID() string {
	return c.raftManager.LeaderID()
}

// GetClusterHealth returns cluster health information
func (c *Cluster) GetClusterHealth() *ClusterHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	isLeader := c.raftManager.IsLeader()
	leaderID := c.raftManager.LeaderID()
	totalNodes := int32(len(c.nodes))
	activeNodes := int32(0)

	// Count active nodes
	now := time.Now()
	for _, lastSeen := range c.nodeHealth {
		if now.Sub(lastSeen) < 30*time.Second {
			activeNodes++
		}
	}

	status := "healthy"
	if leaderID == "" {
		status = "no-leader"
	} else if activeNodes < (totalNodes/2)+1 {
		status = "no-quorum"
	}

	return &ClusterHealth{
		Status:      status,
		TotalNodes:  totalNodes,
		ActiveNodes: activeNodes,
		HasQuorum:   activeNodes >= (totalNodes/2)+1,
		IsLeader:    isLeader,
		LeaderID:    leaderID,
		CanWrite:    isLeader && status == "healthy",
	}
}

// joinCluster attempts to join an existing cluster
func (c *Cluster) joinCluster(seedNodes []string) error {
	for _, seed := range seedNodes {
		if err := c.contactSeedNode(seed); err != nil {
			continue // Try next seed
		}
		return nil
	}
	return fmt.Errorf("failed to contact any seed nodes")
}

// contactSeedNode contacts a seed node to join the cluster
func (c *Cluster) contactSeedNode(seedAddr string) error {
	fmt.Printf("Attempting to contact seed node at %s\n", seedAddr)

	// Connect to seed node
	conn, err := grpc.Dial(seedAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial seed node: %w", err)
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Request to join the cluster
	req := &clusterpb.JoinRequest{
		NodeId:  c.nodeID,
		Address: c.bindAddr,
	}

	fmt.Printf("Sending join request for node %s at %s\n", c.nodeID, c.bindAddr)
	resp, err := client.Join(ctx, req)
	if err != nil {
		return fmt.Errorf("join request: %w", err)
	}

	if !resp.Status.Success {
		return fmt.Errorf("join rejected: %s", resp.Status.Message)
	}

	fmt.Printf("Join request accepted by seed node\n")
	return nil
}

// isRaftReady checks if the Raft transport is ready to accept connections
func (c *Cluster) isRaftReady() bool {
	if c.raftManager == nil || c.raftManager.raft == nil {
		return false
	}
	
	// For joining nodes, just check that Raft is not shutdown
	// The transport should be ready even if the node hasn't joined yet
	state := c.raftManager.raft.State()
	return state != hraft.Shutdown
}

// startBackgroundTasks starts background maintenance tasks
func (c *Cluster) startBackgroundTasks() {
	// Health monitoring
	c.wg.Add(1)
	go c.healthMonitorLoop()
}

// healthMonitorLoop monitors node health
func (c *Cluster) healthMonitorLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.updateNodeHealth()
		}
	}
}

// updateNodeHealth updates the health status of all nodes
func (c *Cluster) updateNodeHealth() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update self
	c.nodeHealth[c.nodeID] = time.Now()

	// TODO: Ping other nodes to check health
}

// Close shuts down the cluster
func (c *Cluster) Close() error {
	c.cancel()
	c.wg.Wait()

	if c.raftManager != nil {
		return c.raftManager.Close()
	}
	return nil
}
