package cluster

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics provides comprehensive monitoring for fluxdl cluster
type Metrics struct {
	mu sync.RWMutex

	// Operation counters
	KVOperations     uint64 // atomic
	QueueOperations  uint64 // atomic
	StreamOperations uint64 // atomic

	// Performance metrics
	KVLatencySum       uint64 // atomic - in nanoseconds
	KVLatencyCount     uint64 // atomic
	QueueLatencySum    uint64 // atomic
	QueueLatencyCount  uint64 // atomic
	StreamLatencySum   uint64 // atomic
	StreamLatencyCount uint64 // atomic

	// Error counters
	KVErrors      uint64 // atomic
	QueueErrors   uint64 // atomic
	StreamErrors  uint64 // atomic
	ClusterErrors uint64 // atomic

	// Cluster metrics
	NodeJoins           uint64 // atomic
	NodeLeaves          uint64 // atomic
	PartitionMigrations uint64 // atomic
	ReplicationErrors   uint64 // atomic

	// Resource usage
	MemoryUsage     uint64 // atomic - in bytes
	DiskUsage       uint64 // atomic - in bytes
	ConnectionCount uint64 // atomic

	// Detailed metrics with timestamps
	recentOperations []OperationMetric
	maxRecentOps     int

	// Health status
	lastHealthCheck time.Time
	healthStatus    string
}

// OperationMetric represents a single operation measurement
type OperationMetric struct {
	Type      string // "kv", "queue", "stream"
	Operation string // "get", "set", "push", "pop", etc.
	Latency   time.Duration
	Success   bool
	Timestamp time.Time
	NodeID    string
}

// MetricsSnapshot provides a point-in-time view of all metrics
type MetricsSnapshot struct {
	Timestamp time.Time

	// Operation counts
	KVOps     uint64
	QueueOps  uint64
	StreamOps uint64

	// Average latencies (in milliseconds)
	KVLatencyMs     float64
	QueueLatencyMs  float64
	StreamLatencyMs float64

	// Error rates (errors per second)
	KVErrorRate      float64
	QueueErrorRate   float64
	StreamErrorRate  float64
	ClusterErrorRate float64

	// Cluster health
	NodeCount           int32
	ActiveNodes         int32
	PartitionMigrations uint64
	ReplicationErrors   uint64

	// Resource usage
	MemoryUsageMB   uint64
	DiskUsageGB     uint64
	ConnectionCount uint64

	// Health status
	HealthStatus    string
	LastHealthCheck time.Time
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		maxRecentOps:     1000, // Keep last 1000 operations
		recentOperations: make([]OperationMetric, 0, 1000),
		healthStatus:     "initializing",
		lastHealthCheck:  time.Now(),
	}
}

// RecordKVOperation records a KV operation metric
func (m *Metrics) RecordKVOperation(operation string, latency time.Duration, success bool, nodeID string) {
	atomic.AddUint64(&m.KVOperations, 1)
	atomic.AddUint64(&m.KVLatencySum, uint64(latency.Nanoseconds()))
	atomic.AddUint64(&m.KVLatencyCount, 1)

	if !success {
		atomic.AddUint64(&m.KVErrors, 1)
	}

	m.recordRecentOperation("kv", operation, latency, success, nodeID)
}

// RecordQueueOperation records a Queue operation metric
func (m *Metrics) RecordQueueOperation(operation string, latency time.Duration, success bool, nodeID string) {
	atomic.AddUint64(&m.QueueOperations, 1)
	atomic.AddUint64(&m.QueueLatencySum, uint64(latency.Nanoseconds()))
	atomic.AddUint64(&m.QueueLatencyCount, 1)

	if !success {
		atomic.AddUint64(&m.QueueErrors, 1)
	}

	m.recordRecentOperation("queue", operation, latency, success, nodeID)
}

// RecordStreamOperation records a Stream operation metric
func (m *Metrics) RecordStreamOperation(operation string, latency time.Duration, success bool, nodeID string) {
	atomic.AddUint64(&m.StreamOperations, 1)
	atomic.AddUint64(&m.StreamLatencySum, uint64(latency.Nanoseconds()))
	atomic.AddUint64(&m.StreamLatencyCount, 1)

	if !success {
		atomic.AddUint64(&m.StreamErrors, 1)
	}

	m.recordRecentOperation("stream", operation, latency, success, nodeID)
}

// RecordClusterEvent records cluster-related events
func (m *Metrics) RecordClusterEvent(eventType string) {
	switch eventType {
	case "node_join":
		atomic.AddUint64(&m.NodeJoins, 1)
	case "node_leave":
		atomic.AddUint64(&m.NodeLeaves, 1)
	case "partition_migration":
		atomic.AddUint64(&m.PartitionMigrations, 1)
	case "replication_error":
		atomic.AddUint64(&m.ReplicationErrors, 1)
	case "cluster_error":
		atomic.AddUint64(&m.ClusterErrors, 1)
	}
}

// UpdateResourceUsage updates resource usage metrics
func (m *Metrics) UpdateResourceUsage(memoryBytes, diskBytes, connections uint64) {
	atomic.StoreUint64(&m.MemoryUsage, memoryBytes)
	atomic.StoreUint64(&m.DiskUsage, diskBytes)
	atomic.StoreUint64(&m.ConnectionCount, connections)
}

// UpdateHealthStatus updates the cluster health status
func (m *Metrics) UpdateHealthStatus(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.healthStatus = status
	m.lastHealthCheck = time.Now()
}

// recordRecentOperation adds an operation to the recent operations buffer
func (m *Metrics) recordRecentOperation(opType, operation string, latency time.Duration, success bool, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	metric := OperationMetric{
		Type:      opType,
		Operation: operation,
		Latency:   latency,
		Success:   success,
		Timestamp: time.Now(),
		NodeID:    nodeID,
	}

	// Add to recent operations (circular buffer)
	if len(m.recentOperations) >= m.maxRecentOps {
		// Remove oldest operation
		m.recentOperations = m.recentOperations[1:]
	}
	m.recentOperations = append(m.recentOperations, metric)
}

// GetSnapshot returns a snapshot of current metrics
func (m *Metrics) GetSnapshot(nodeCount, activeNodes int32) *MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Calculate average latencies
	kvLatency := float64(0)
	if kvCount := atomic.LoadUint64(&m.KVLatencyCount); kvCount > 0 {
		kvLatency = float64(atomic.LoadUint64(&m.KVLatencySum)) / float64(kvCount) / 1e6 // Convert to ms
	}

	queueLatency := float64(0)
	if queueCount := atomic.LoadUint64(&m.QueueLatencyCount); queueCount > 0 {
		queueLatency = float64(atomic.LoadUint64(&m.QueueLatencySum)) / float64(queueCount) / 1e6
	}

	streamLatency := float64(0)
	if streamCount := atomic.LoadUint64(&m.StreamLatencyCount); streamCount > 0 {
		streamLatency = float64(atomic.LoadUint64(&m.StreamLatencySum)) / float64(streamCount) / 1e6
	}

	return &MetricsSnapshot{
		Timestamp: time.Now(),

		// Operation counts
		KVOps:     atomic.LoadUint64(&m.KVOperations),
		QueueOps:  atomic.LoadUint64(&m.QueueOperations),
		StreamOps: atomic.LoadUint64(&m.StreamOperations),

		// Latencies
		KVLatencyMs:     kvLatency,
		QueueLatencyMs:  queueLatency,
		StreamLatencyMs: streamLatency,

		// Error rates (simplified - would need time-based calculation for true rates)
		KVErrorRate:      float64(atomic.LoadUint64(&m.KVErrors)),
		QueueErrorRate:   float64(atomic.LoadUint64(&m.QueueErrors)),
		StreamErrorRate:  float64(atomic.LoadUint64(&m.StreamErrors)),
		ClusterErrorRate: float64(atomic.LoadUint64(&m.ClusterErrors)),

		// Cluster metrics
		NodeCount:           nodeCount,
		ActiveNodes:         activeNodes,
		PartitionMigrations: atomic.LoadUint64(&m.PartitionMigrations),
		ReplicationErrors:   atomic.LoadUint64(&m.ReplicationErrors),

		// Resource usage
		MemoryUsageMB:   atomic.LoadUint64(&m.MemoryUsage) / (1024 * 1024),
		DiskUsageGB:     atomic.LoadUint64(&m.DiskUsage) / (1024 * 1024 * 1024),
		ConnectionCount: atomic.LoadUint64(&m.ConnectionCount),

		// Health
		HealthStatus:    m.healthStatus,
		LastHealthCheck: m.lastHealthCheck,
	}
}

// GetRecentOperations returns recent operations for debugging
func (m *Metrics) GetRecentOperations(limit int) []OperationMetric {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > len(m.recentOperations) {
		limit = len(m.recentOperations)
	}

	// Return last N operations
	start := len(m.recentOperations) - limit
	result := make([]OperationMetric, limit)
	copy(result, m.recentOperations[start:])

	return result
}

// Reset resets all metrics (useful for testing)
func (m *Metrics) Reset() {
	// Reset atomic counters
	atomic.StoreUint64(&m.KVOperations, 0)
	atomic.StoreUint64(&m.QueueOperations, 0)
	atomic.StoreUint64(&m.StreamOperations, 0)
	atomic.StoreUint64(&m.KVLatencySum, 0)
	atomic.StoreUint64(&m.KVLatencyCount, 0)
	atomic.StoreUint64(&m.QueueLatencySum, 0)
	atomic.StoreUint64(&m.QueueLatencyCount, 0)
	atomic.StoreUint64(&m.StreamLatencySum, 0)
	atomic.StoreUint64(&m.StreamLatencyCount, 0)
	atomic.StoreUint64(&m.KVErrors, 0)
	atomic.StoreUint64(&m.QueueErrors, 0)
	atomic.StoreUint64(&m.StreamErrors, 0)
	atomic.StoreUint64(&m.ClusterErrors, 0)
	atomic.StoreUint64(&m.NodeJoins, 0)
	atomic.StoreUint64(&m.NodeLeaves, 0)
	atomic.StoreUint64(&m.PartitionMigrations, 0)
	atomic.StoreUint64(&m.ReplicationErrors, 0)
	atomic.StoreUint64(&m.MemoryUsage, 0)
	atomic.StoreUint64(&m.DiskUsage, 0)
	atomic.StoreUint64(&m.ConnectionCount, 0)

	// Reset recent operations
	m.mu.Lock()
	m.recentOperations = m.recentOperations[:0]
	m.healthStatus = "reset"
	m.lastHealthCheck = time.Now()
	m.mu.Unlock()
}
