package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clusterpb "github.com/skshohagmiah/fluxdl/api/generated/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ReplicationTask represents a single replication operation
type ReplicationTask struct {
	Key       string
	Value     []byte
	TTL       time.Duration
	Replicas  []string
	Timestamp time.Time
	Retries   int
}

// ReplicationBatch groups multiple tasks for efficient processing
type ReplicationBatch struct {
	NodeID string
	Tasks  []ReplicationTask
}

// ConsistencyLevel defines replication consistency requirements
type ConsistencyLevel int

const (
	EventualConsistency ConsistencyLevel = iota
	StrongConsistency
)

// ReplicationManager handles high-performance async replication
type ReplicationManager struct {
	mu sync.RWMutex

	// Configuration
	workerCount    int
	batchSize      int
	batchTimeout   time.Duration
	queueSize      int
	maxRetries     int

	// Worker management
	taskQueue   chan ReplicationTask
	batchQueues map[string]chan ReplicationTask // Per-node queues
	workers     []ReplicationWorker
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	// Connection pooling
	connPool map[string]*grpc.ClientConn
	connMu   sync.RWMutex

	// Metrics
	totalTasks     uint64
	completedTasks uint64
	failedTasks    uint64
	batchesSent    uint64

	// Write-ahead log
	wal *WriteAheadLog

	// Cluster reference
	cluster *Cluster
}

// ReplicationWorker processes replication tasks for a specific node
type ReplicationWorker struct {
	id       int
	nodeID   string
	manager  *ReplicationManager
	batch    []ReplicationTask
	lastSent time.Time
}

// NewReplicationManager creates a high-performance replication manager
func NewReplicationManager(cluster *Cluster, config ReplicationConfig) *ReplicationManager {
	ctx, cancel := context.WithCancel(context.Background())

	rm := &ReplicationManager{
		workerCount:  config.WorkerCount,
		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,
		queueSize:    config.QueueSize,
		maxRetries:   config.MaxRetries,

		taskQueue:   make(chan ReplicationTask, config.QueueSize),
		batchQueues: make(map[string]chan ReplicationTask),
		connPool:    make(map[string]*grpc.ClientConn),
		ctx:         ctx,
		cancel:      cancel,
		cluster:     cluster,
	}

	// Initialize write-ahead log
	rm.wal = NewWriteAheadLog(config.WALPath)

	return rm
}

// ReplicationConfig holds configuration for the replication manager
type ReplicationConfig struct {
	WorkerCount  int           // Number of worker goroutines per node
	BatchSize    int           // Max tasks per batch
	BatchTimeout time.Duration // Max time to wait for batch
	QueueSize    int           // Size of task queue
	MaxRetries   int           // Max retry attempts
	WALPath      string        // Write-ahead log directory
}

// DefaultReplicationConfig returns optimized defaults for high throughput
func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		WorkerCount:  8,  // 8 workers per replica node
		BatchSize:    100, // Batch up to 100 operations
		BatchTimeout: 10 * time.Millisecond, // 10ms max batch wait
		QueueSize:    10000, // 10k pending tasks
		MaxRetries:   3,
		WALPath:      "./data/replication_wal",
	}
}

// Start initializes the replication manager and workers
func (rm *ReplicationManager) Start() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Start write-ahead log
	if err := rm.wal.Start(); err != nil {
		return fmt.Errorf("failed to start WAL: %w", err)
	}

	// Start main task distributor
	rm.wg.Add(1)
	go rm.taskDistributor()

	// Start per-node batch processors
	rm.startNodeWorkers()

	return nil
}

// Stop gracefully shuts down the replication manager
func (rm *ReplicationManager) Stop() error {
	rm.cancel()
	rm.wg.Wait()

	// Close all connections
	rm.connMu.Lock()
	for _, conn := range rm.connPool {
		conn.Close()
	}
	rm.connMu.Unlock()

	// Stop WAL
	return rm.wal.Stop()
}

// ReplicateAsync adds a replication task to the async queue
func (rm *ReplicationManager) ReplicateAsync(key string, value []byte, ttl time.Duration, replicas []string) error {
	task := ReplicationTask{
		Key:       key,
		Value:     value,
		TTL:       ttl,
		Replicas:  replicas,
		Timestamp: time.Now(),
		Retries:   0,
	}

	// Write to WAL first for durability
	if err := rm.wal.WriteTask(task); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Add to async queue (non-blocking)
	select {
	case rm.taskQueue <- task:
		atomic.AddUint64(&rm.totalTasks, 1)
		return nil
	default:
		// Queue full - could implement backpressure here
		return fmt.Errorf("replication queue full")
	}
}

// ReplicateSync performs synchronous replication with consistency guarantee
func (rm *ReplicationManager) ReplicateSync(ctx context.Context, key string, value []byte, ttl time.Duration, replicas []string) error {
	// For strong consistency, send directly and wait
	var wg sync.WaitGroup
	errChan := make(chan error, len(replicas))

	for _, nodeID := range replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := rm.sendSingleReplication(ctx, nodeID, key, value, ttl); err != nil {
				errChan <- err
			}
		}(nodeID)
	}

	wg.Wait()
	close(errChan)

	// Check if any replications failed
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("replication failed to %d replicas: %v", len(errors), errors)
	}

	return nil
}

// taskDistributor distributes tasks to per-node queues
func (rm *ReplicationManager) taskDistributor() {
	defer rm.wg.Done()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case task := <-rm.taskQueue:
			// Distribute to per-node queues
			for _, nodeID := range task.Replicas {
				rm.getNodeQueue(nodeID) <- task
			}
		}
	}
}

// getNodeQueue gets or creates a queue for a specific node
func (rm *ReplicationManager) getNodeQueue(nodeID string) chan ReplicationTask {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if queue, exists := rm.batchQueues[nodeID]; exists {
		return queue
	}

	// Create new queue and workers for this node
	queue := make(chan ReplicationTask, rm.queueSize)
	rm.batchQueues[nodeID] = queue

	// Start workers for this node
	for i := 0; i < rm.workerCount; i++ {
		worker := ReplicationWorker{
			id:      i,
			nodeID:  nodeID,
			manager: rm,
			batch:   make([]ReplicationTask, 0, rm.batchSize),
		}
		rm.workers = append(rm.workers, worker)

		rm.wg.Add(1)
		go worker.run()
	}

	return queue
}

// startNodeWorkers starts workers for existing nodes
func (rm *ReplicationManager) startNodeWorkers() {
	// Get all active nodes from cluster
	nodes := rm.cluster.GetActiveNodes()
	
	for _, node := range nodes {
		rm.getNodeQueue(node.ID) // This will create workers
	}
}

// run is the main worker loop for processing batches
func (w *ReplicationWorker) run() {
	defer w.manager.wg.Done()

	ticker := time.NewTicker(w.manager.batchTimeout)
	defer ticker.Stop()

	queue := w.manager.batchQueues[w.nodeID]

	for {
		select {
		case <-w.manager.ctx.Done():
			// Send any remaining batch before shutdown
			if len(w.batch) > 0 {
				w.sendBatch()
			}
			return

		case task := <-queue:
			w.batch = append(w.batch, task)

			// Send batch if full
			if len(w.batch) >= w.manager.batchSize {
				w.sendBatch()
			}

		case <-ticker.C:
			// Send batch on timeout if not empty
			if len(w.batch) > 0 && time.Since(w.lastSent) >= w.manager.batchTimeout {
				w.sendBatch()
			}
		}
	}
}

// sendBatch sends accumulated tasks as a batch to the target node
func (w *ReplicationWorker) sendBatch() {
	if len(w.batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.manager.sendBatchToNode(ctx, w.nodeID, w.batch); err != nil {
		// Handle batch failure - could retry individual tasks
		w.handleBatchFailure(w.batch, err)
	} else {
		atomic.AddUint64(&w.manager.completedTasks, uint64(len(w.batch)))
		atomic.AddUint64(&w.manager.batchesSent, 1)
	}

	// Clear batch and update timestamp
	w.batch = w.batch[:0]
	w.lastSent = time.Now()
}

// sendBatchToNode sends a batch of replication tasks to a specific node
func (rm *ReplicationManager) sendBatchToNode(ctx context.Context, nodeID string, tasks []ReplicationTask) error {
	conn, err := rm.getConnection(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get connection to %s: %w", nodeID, err)
	}

	client := clusterpb.NewClusterServiceClient(conn)

	// Send each task individually for now (can be optimized with batch API later)
	for _, task := range tasks {
		req := &clusterpb.ReplicateDataRequest{
			Key:   task.Key,
			Value: task.Value,
			Ttl:   int64(task.TTL.Seconds()),
		}

		resp, err := client.ReplicateData(ctx, req)
		if err != nil {
			return fmt.Errorf("replication failed for key %s: %w", task.Key, err)
		}

		if resp.Status.Code != 0 {
			return fmt.Errorf("replication rejected for key %s: %s", task.Key, resp.Status.Message)
		}
	}

	return nil
}

// sendSingleReplication sends a single replication (for sync mode)
func (rm *ReplicationManager) sendSingleReplication(ctx context.Context, nodeID, key string, value []byte, ttl time.Duration) error {
	conn, err := rm.getConnection(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get connection to %s: %w", nodeID, err)
	}

	client := clusterpb.NewClusterServiceClient(conn)

	req := &clusterpb.ReplicateDataRequest{
		Key:   key,
		Value: value,
		Ttl:   int64(ttl.Seconds()),
	}

	resp, err := client.ReplicateData(ctx, req)
	if err != nil {
		return fmt.Errorf("replication failed: %w", err)
	}

	if resp.Status.Code != 0 {
		return fmt.Errorf("replication rejected: %s", resp.Status.Message)
	}

	return nil
}

// getConnection gets or creates a pooled connection to a node
func (rm *ReplicationManager) getConnection(nodeID string) (*grpc.ClientConn, error) {
	rm.connMu.RLock()
	if conn, exists := rm.connPool[nodeID]; exists {
		rm.connMu.RUnlock()
		return conn, nil
	}
	rm.connMu.RUnlock()

	// Create new connection
	rm.connMu.Lock()
	defer rm.connMu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := rm.connPool[nodeID]; exists {
		return conn, nil
	}

	// Get node address from cluster
	node := rm.cluster.getNodeByID(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	conn, err := grpc.Dial(node.Address, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", node.Address, err)
	}

	rm.connPool[nodeID] = conn
	return conn, nil
}

// handleBatchFailure handles failed batch operations
func (w *ReplicationWorker) handleBatchFailure(tasks []ReplicationTask, err error) {
	atomic.AddUint64(&w.manager.failedTasks, uint64(len(tasks)))

	// Retry individual tasks with exponential backoff
	for _, task := range tasks {
		if task.Retries < w.manager.maxRetries {
			task.Retries++
			
			// Exponential backoff
			delay := time.Duration(1<<task.Retries) * time.Second
			time.AfterFunc(delay, func() {
				select {
				case w.manager.taskQueue <- task:
				default:
					// Queue full, drop task
				}
			})
		}
	}
}

// GetMetrics returns replication performance metrics
func (rm *ReplicationManager) GetMetrics() ReplicationMetrics {
	return ReplicationMetrics{
		TotalTasks:     atomic.LoadUint64(&rm.totalTasks),
		CompletedTasks: atomic.LoadUint64(&rm.completedTasks),
		FailedTasks:    atomic.LoadUint64(&rm.failedTasks),
		BatchesSent:    atomic.LoadUint64(&rm.batchesSent),
		QueueLength:    len(rm.taskQueue),
		ActiveWorkers:  len(rm.workers),
	}
}

// ReplicationMetrics provides performance insights
type ReplicationMetrics struct {
	TotalTasks     uint64
	CompletedTasks uint64
	FailedTasks    uint64
	BatchesSent    uint64
	QueueLength    int
	ActiveWorkers  int
}
