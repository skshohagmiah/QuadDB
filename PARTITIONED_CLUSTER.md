# GoMsg Partitioned Cluster Architecture

This document explains the new partitioned cluster architecture for GoMsg, which enables horizontal scaling and load distribution.

## 🎯 Overview

The new GoMsg cluster architecture provides:

- **Partitioned Storage**: Data is distributed across nodes based on key hashing
- **Replication**: Each partition has configurable replicas for fault tolerance
- **Client-Side Routing**: SDKs automatically route requests to the correct nodes
- **Horizontal Scaling**: Add more nodes to increase capacity and throughput
- **Multi-Language Support**: Go, Node.js, and Python client SDKs

## 🏗️ Architecture

### Before: Raft-Based Full Replication
```
Client -> Leader Node -> Raft Log -> All Followers
                      -> Storage on ALL nodes (full replication)
```

**Issues:**
- All nodes store all data (no horizontal scaling)
- Write bottleneck at leader
- Storage limited by single node capacity

### After: Partitioned Distribution with Replication
```
Client SDK -> Determines Partition -> Routes to Primary Node
           -> Node 1 (partitions 0,3,6...) -> Replicates to Node 2,3
           -> Node 2 (partitions 1,4,7...) -> Replicates to Node 3,1  
           -> Node 3 (partitions 2,5,8...) -> Replicates to Node 1,2
```

**Benefits:**
- ✅ Horizontal scaling (storage capacity grows with nodes)
- ✅ Load distribution (parallel reads/writes)
- ✅ Fault tolerance (configurable replication)
- ✅ Better performance (no single write bottleneck)

## 📦 Components

### 1. Cluster Manager (`pkg/cluster/cluster.go`)
- Manages node membership and health
- Handles partition assignment and rebalancing
- Provides cluster topology information
- Supports dynamic node addition/removal

### 2. Partition Map (`pkg/cluster/partition_map.go`)
- Maps keys to partitions using consistent hashing
- Assigns partitions to nodes with replication
- Handles rebalancing when nodes join/leave
- Provides partition statistics

### 3. Partitioned Storage (`pkg/cluster/partitioned_storage.go`)
- Wraps storage interface with partition awareness
- Only stores data for owned partitions
- Handles replication to replica nodes
- Supports partition migration

### 4. Client SDK (`clients/go/simple_partitioned_client.go`)
- Discovers cluster topology
- Routes requests to correct nodes based on key partitioning
- Handles multi-key operations efficiently
- Provides transparent failover

## 🚀 Usage

### Starting a Cluster

#### Single Node (Development)
```go
config := cluster.Config{
    NodeID:            "node1",
    BindAddr:          "localhost:8080",
    Partitions:        32,
    ReplicationFactor: 1, // No replication for single node
    Bootstrap:         true,
}

cluster, err := cluster.New(ctx, storage, config)
```

#### Multi-Node Cluster (Production)
```go
// First node (bootstrap)
config1 := cluster.Config{
    NodeID:            "node1",
    BindAddr:          "localhost:8080",
    Partitions:        32,
    ReplicationFactor: 3,
    Bootstrap:         true,
}
node1, _ := cluster.New(ctx, storage1, config1)

// Additional nodes
config2 := cluster.Config{
    NodeID:            "node2", 
    BindAddr:          "localhost:8081",
    Partitions:        32,
    ReplicationFactor: 3,
    SeedNodes:         []string{"localhost:8080"},
}
node2, _ := cluster.New(ctx, storage2, config2)
```

### Using Client SDK

#### Go Client
```go
client, err := client.NewSimplePartitioned(ctx, &client.SimplePartitionedOptions{
    Partitions: 32,
    InitialNodes: []client.NodeInfo{
        {NodeID: "node1", Address: "http://localhost:8080"},
        {NodeID: "node2", Address: "http://localhost:8081"},
        {NodeID: "node3", Address: "http://localhost:8082"},
    },
})

// Operations are automatically routed to correct nodes
client.Set(ctx, "user:123", []byte("user data"), 0)
value, found, err := client.Get(ctx, "user:123")
```

#### Node.js Client (Planned)
```javascript
const client = new PartitionedClient({
    partitions: 32,
    initialNodes: [
        {nodeId: 'node1', address: 'http://localhost:8080'},
        {nodeId: 'node2', address: 'http://localhost:8081'},
        {nodeId: 'node3', address: 'http://localhost:8082'}
    ]
});

await client.set('user:123', 'user data');
const value = await client.get('user:123');
```

#### Python Client (Planned)
```python
client = PartitionedClient(
    partitions=32,
    initial_nodes=[
        {'node_id': 'node1', 'address': 'http://localhost:8080'},
        {'node_id': 'node2', 'address': 'http://localhost:8081'},
        {'node_id': 'node3', 'address': 'http://localhost:8082'}
    ]
)

await client.set('user:123', b'user data')
value = await client.get('user:123')
```

## 🔧 Configuration

### Cluster Configuration
| Parameter | Description | Default | Recommendation |
|-----------|-------------|---------|----------------|
| `Partitions` | Number of partitions | 32 | 32-128 for most use cases |
| `ReplicationFactor` | Replicas per partition | 3 | 3 for production, 1 for dev |
| `NodeID` | Unique node identifier | Required | Use hostname or UUID |
| `BindAddr` | Node bind address | Required | Use actual IP for multi-host |

### Client Configuration
| Parameter | Description | Default |
|-----------|-------------|---------|
| `Partitions` | Must match cluster | Required |
| `InitialNodes` | Bootstrap node list | Required |
| `Timeout` | Request timeout | 10s |

## 📊 Partition Distribution

### Key-to-Partition Mapping
```go
// Uses FNV-1a hash for consistent distribution
partition = hash(key) % total_partitions
```

### Partition-to-Node Assignment
```go
// Round-robin assignment with replication
primary = nodes[partition % len(nodes)]
replica1 = nodes[(partition + 1) % len(nodes)]
replica2 = nodes[(partition + 2) % len(nodes)]
```

### Example Distribution (3 nodes, 9 partitions, replication=2)
```
Partition 0: Primary=Node1, Replica=Node2
Partition 1: Primary=Node2, Replica=Node3  
Partition 2: Primary=Node3, Replica=Node1
Partition 3: Primary=Node1, Replica=Node2
Partition 4: Primary=Node2, Replica=Node3
Partition 5: Primary=Node3, Replica=Node1
Partition 6: Primary=Node1, Replica=Node2
Partition 7: Primary=Node2, Replica=Node3
Partition 8: Primary=Node3, Replica=Node1
```

## 🔄 Operations

### Write Operations
1. Client determines partition for key
2. Client sends request to partition's primary node
3. Primary node stores data locally
4. Primary node replicates to replica nodes
5. Primary node responds to client

### Read Operations
1. Client determines partition for key
2. Client sends request to partition's primary node
3. Primary node returns data from local storage

### Multi-Key Operations
1. Client groups keys by target node
2. Client sends batched requests to each node
3. Client aggregates responses

## 🚨 Failure Handling

### Node Failures
- Dead nodes are detected via health checks (30s timeout)
- Partitions are automatically rebalanced to remaining nodes
- Clients discover topology changes and reroute requests

### Partition Recovery
- When a node rejoins, it receives its assigned partitions
- Data migration occurs in the background
- Replication factor is restored

### Split-Brain Prevention
- Cluster requires majority of nodes to be active
- Minority partitions become read-only
- Manual intervention required for recovery

## 🔮 Future Enhancements

### Planned Features
- [ ] **Consistent Hashing**: Better load distribution with virtual nodes
- [ ] **Partition Migration**: Live migration of partitions between nodes
- [ ] **Cross-Partition Transactions**: ACID transactions across partitions
- [ ] **Compression**: Reduce network and storage overhead
- [ ] **Metrics & Monitoring**: Detailed cluster health metrics
- [ ] **Auto-Scaling**: Automatic node addition based on load

### Client SDK Roadmap
- [ ] **Node.js SDK**: Full-featured JavaScript/TypeScript client
- [ ] **Python SDK**: AsyncIO-based Python client
- [ ] **Connection Pooling**: Efficient connection management
- [ ] **Circuit Breakers**: Automatic failure detection and recovery
- [ ] **Load Balancing**: Smart routing based on node health

## 📈 Performance Characteristics

### Scalability
- **Storage**: Linear scaling with number of nodes
- **Throughput**: Near-linear scaling for read/write operations
- **Latency**: Consistent low latency (single-hop routing)

### Resource Usage
- **Memory**: Reduced per-node memory usage (only owned partitions)
- **Network**: Efficient replication (only to replica nodes)
- **CPU**: Distributed load across all nodes

### Benchmarks (Estimated)
| Nodes | Partitions | Throughput | Latency |
|-------|------------|------------|---------|
| 1 | 32 | 10K ops/sec | 1ms |
| 3 | 32 | 25K ops/sec | 1.5ms |
| 6 | 64 | 45K ops/sec | 2ms |
| 12 | 128 | 80K ops/sec | 2.5ms |

## 🛠️ Migration Guide

### From Old Raft-Based Cluster
1. **Backup Data**: Export all data from existing cluster
2. **Deploy New Cluster**: Start partitioned cluster nodes
3. **Import Data**: Restore data (will be automatically partitioned)
4. **Update Clients**: Switch to partitioned client SDKs
5. **Verify**: Test all operations work correctly

### Compatibility
- **Storage Interface**: Fully compatible with existing storage backends
- **API Endpoints**: Same HTTP/gRPC APIs (transparent to external clients)
- **Data Format**: No changes to data serialization

This new architecture provides a solid foundation for horizontal scaling while maintaining the simplicity and reliability that GoMsg is known for.
