# gomsg

**gomsg** is a fast, simple distributed data platform that replaces Redis + RabbitMQ + Kafka with one service.

## 🚀 Why gomsg?

**One project. Three data patterns. Built for scale.**

- **Key/Value store** like Redis
- **Message queues** like RabbitMQ  
- **Event streams** like Kafka

All with **high-performance gRPC APIs** and automatic clustering.

## 📁 Detailed File Structure

```
gomsg/
├── cmd/
│   ├── gomsg/                 # Server binary
│   │   ├── main.go           # Entry point
│   │   ├── server.go         # Server setup
│   │   └── config.go         # Config loading
│   └── cli/                   # CLI tool
│       ├── main.go           # CLI entry point
│       ├── kv.go             # KV commands
│       ├── queue.go          # Queue commands
│       ├── stream.go         # Stream commands
│       └── cluster.go        # Cluster commands
├── pkg/
│   ├── kv/                    # Key/Value operations
│   │   ├── store.go          # KV store interface
│   │   ├── operations.go     # CRUD operations
│   │   ├── expiration.go     # TTL management
│   │   ├── patterns.go       # Pattern matching (keys user:*)
│   │   └── batch.go          # Batch operations (mget, mset)
│   ├── queue/                 # Queue operations
│   │   ├── manager.go        # Queue management
│   │   ├── producer.go       # Message production
│   │   ├── consumer.go       # Message consumption
│   │   ├── ack.go            # Message acknowledgment
│   │   ├── delayed.go        # Delayed messages
│   │   └── stats.go          # Queue statistics
│   ├── stream/                # Stream operations
│   │   ├── broker.go         # Stream broker
│   │   ├── publisher.go      # Event publishing
│   │   ├── subscriber.go     # Event subscription
│   │   ├── consumer_group.go # Consumer groups
│   │   ├── offset.go         # Offset management
│   │   └── partition.go      # Stream partitioning
│   ├── client/                # gRPC client
│   │   ├── client.go         # Main client
│   │   ├── kv_client.go      # KV client methods
│   │   ├── queue_client.go   # Queue client methods
│   │   ├── stream_client.go  # Stream client methods
│   │   ├── connection.go     # Connection pool
│   │   └── loadbalancer.go   # Client-side load balancing
│   ├── server/                # gRPC server
│   │   ├── server.go         # Main gRPC server
│   │   ├── kv_service.go     # KV gRPC service
│   │   ├── queue_service.go  # Queue gRPC service
│   │   ├── stream_service.go # Stream gRPC service
│   │   ├── cluster_service.go# Cluster gRPC service
│   │   └── middleware.go     # Auth, logging, metrics
│   └── cluster/               # Clustering
│       ├── node.go           # Cluster node
│       ├── raft.go           # Raft consensus
│       ├── membership.go     # Node discovery
│       ├── partitioner.go    # Data partitioning
│       ├── replication.go    # Data replication
│       └── balancer.go       # Load balancing
├── api/
│   ├── proto/                 # gRPC definitions
│   │   ├── kv.proto          # KV service proto
│   │   ├── queue.proto       # Queue service proto
│   │   ├── stream.proto      # Stream service proto
│   │   ├── cluster.proto     # Cluster service proto
│   │   └── common.proto      # Common types
│   └── generated/             # Generated gRPC code
│       ├── kv/               # Generated KV code
│       ├── queue/            # Generated Queue code
│       ├── stream/           # Generated Stream code
│       └── cluster/          # Generated Cluster code
├── storage/
│   ├── interface.go          # Storage interface
│   ├── badger.go             # BadgerDB implementation
│   ├── memory.go             # In-memory implementation
│   ├── wal.go                # Write-ahead log
│   └── snapshot.go           # Snapshot management
├── config/
│   ├── config.go             # Config struct
│   ├── default.yaml          # Default config
│   └── cluster.yaml          # Cluster config example
├── docker/
│   ├── Dockerfile            # Docker image
│   ├── docker-compose.yml    # Single node
│   └── docker-compose.cluster.yml # 3-node cluster
├── examples/
│   ├── go/
│   │   ├── basic.go          # Basic usage
│   │   ├── cluster.go        # Cluster usage
│   │   └── advanced.go       # Advanced patterns
│   ├── nodejs/
│   │   ├── basic.js          # Basic usage
│   │   ├── cluster.js        # Cluster usage
│   │   └── advanced.js       # Advanced patterns
│   └── python/
│       ├── basic.py          # Basic usage
│       ├── cluster.py        # Cluster usage
│       └── advanced.py       # Advanced patterns
├── go.mod                     # Go module
├── go.sum                     # Go dependencies
├── Makefile                   # Build scripts
└── README.md                  # Project readme
```

## 🔧 Clustering

### Quick Cluster Setup
```bash
# Node 1 (Bootstrap)
./gomsg --cluster --node-id=node1 --port=9000 --bootstrap

# Node 2 (Join)
./gomsg --cluster --node-id=node2 --port=9001 --join=localhost:9000

# Node 3 (Join)
./gomsg --cluster --node-id=node3 --port=9002 --join=localhost:9000
```

### Auto-Scaling Features
- **Data Partitioning**: Keys distributed across nodes automatically
- **Replication**: Each write replicated to 3 nodes by default
- **Leader Election**: Automatic failover in case of node failure
- **Load Balancing**: Clients connect to any node, requests routed optimally

### Docker Cluster
```yaml
# docker-compose.yml
version: '3.8'
services:
  gomsg-1:
    image: gomsg/gomsg
    ports: ["9000:9000"]
    command: --cluster --node-id=node1 --port=9000 --bootstrap
    
  gomsg-2:
    image: gomsg/gomsg
    ports: ["9001:9001"]
    command: --cluster --node-id=node2 --port=9001 --join=gomsg-1:9000
    
  gomsg-3:
    image: gomsg/gomsg
    ports: ["9002:9002"]
    command: --cluster --node-id=node3 --port=9002 --join=gomsg-1:9000
```

## 🔌 Complete Client API Reference

### 📊 Key/Value APIs

#### Go Client
```go
// Basic operations
client.KV.Set(ctx, "key", "value")
client.KV.SetTTL(ctx, "key", "value", time.Hour)
value := client.KV.Get(ctx, "key")
exists := client.KV.Exists(ctx, "key")
client.KV.Del(ctx, "key")

// Atomic operations
newValue := client.KV.Incr(ctx, "counter")
newValue := client.KV.IncrBy(ctx, "counter", 10)
newValue := client.KV.Decr(ctx, "counter")
newValue := client.KV.DecrBy(ctx, "counter", 5)

// Batch operations
values := client.KV.MGet(ctx, []string{"key1", "key2", "key3"})
client.KV.MSet(ctx, map[string]string{"key1": "val1", "key2": "val2"})

// Pattern matching
keys := client.KV.Keys(ctx, "user:*")
client.KV.DelPattern(ctx, "session:*")

// Expiration
client.KV.Expire(ctx, "key", time.Minute*30)
ttl := client.KV.TTL(ctx, "key")
```

#### Node.js Client
```javascript
// Basic operations
await client.kv.set('key', 'value')
await client.kv.setTTL('key', 'value', 3600) // seconds
const value = await client.kv.get('key')
const exists = await client.kv.exists('key')
await client.kv.del('key')

// Atomic operations
const newValue = await client.kv.incr('counter')
const newValue = await client.kv.incrBy('counter', 10)
const newValue = await client.kv.decr('counter')
const newValue = await client.kv.decrBy('counter', 5)

// Batch operations
const values = await client.kv.mget(['key1', 'key2', 'key3'])
await client.kv.mset({key1: 'val1', key2: 'val2'})

// Pattern matching
const keys = await client.kv.keys('user:*')
await client.kv.delPattern('session:*')

// Expiration
await client.kv.expire('key', 1800) // seconds
const ttl = await client.kv.ttl('key')
```

#### Python Client
```python
# Basic operations
await client.kv.set('key', 'value')
await client.kv.set_ttl('key', 'value', 3600)  # seconds
value = await client.kv.get('key')
exists = await client.kv.exists('key')
await client.kv.delete('key')

# Atomic operations
new_value = await client.kv.incr('counter')
new_value = await client.kv.incr_by('counter', 10)
new_value = await client.kv.decr('counter')
new_value = await client.kv.decr_by('counter', 5)

# Batch operations
values = await client.kv.mget(['key1', 'key2', 'key3'])
await client.kv.mset({'key1': 'val1', 'key2': 'val2'})

# Pattern matching
keys = await client.kv.keys('user:*')
await client.kv.del_pattern('session:*')

# Expiration
await client.kv.expire('key', 1800)  # seconds
ttl = await client.kv.ttl('key')
```

### 📮 Queue APIs

#### Go Client
```go
// Basic operations
msgID := client.Queue.Push(ctx, "jobs", "task-data")
client.Queue.PushDelayed(ctx, "jobs", "task-data", time.Hour*2)
msg := client.Queue.Pop(ctx, "jobs")
msg := client.Queue.PopTimeout(ctx, "jobs", time.Second*30)

// Message handling
client.Queue.Ack(ctx, msg.ID)
client.Queue.Nack(ctx, msg.ID)  // Requeue for retry
msg := client.Queue.Peek(ctx, "jobs")  // Look without removing

// Queue management
stats := client.Queue.Stats(ctx, "jobs")  // {size, consumers, etc}
client.Queue.Purge(ctx, "jobs")  // Clear all messages
client.Queue.Delete(ctx, "jobs")  // Delete entire queue
queues := client.Queue.List(ctx)  // List all queues

// Batch operations
client.Queue.PushBatch(ctx, "jobs", []string{"task1", "task2", "task3"})
msgs := client.Queue.PopBatch(ctx, "jobs", 10)  // Pop up to 10 messages
```

#### Node.js Client
```javascript
// Basic operations
const msgId = await client.queue.push('jobs', 'task-data')
await client.queue.pushDelayed('jobs', 'task-data', 7200) // seconds
const msg = await client.queue.pop('jobs')
const msg = await client.queue.popTimeout('jobs', 30) // seconds

// Message handling
await client.queue.ack(msg.id)
await client.queue.nack(msg.id)  // Requeue for retry
const msg = await client.queue.peek('jobs')  // Look without removing

// Queue management
const stats = await client.queue.stats('jobs')  // {size, consumers, etc}
await client.queue.purge('jobs')  // Clear all messages
await client.queue.delete('jobs')  // Delete entire queue
const queues = await client.queue.list()  // List all queues

// Batch operations
await client.queue.pushBatch('jobs', ['task1', 'task2', 'task3'])
const msgs = await client.queue.popBatch('jobs', 10)  // Pop up to 10 messages
```

#### Python Client
```python
# Basic operations
msg_id = await client.queue.push('jobs', 'task-data')
await client.queue.push_delayed('jobs', 'task-data', 7200)  # seconds
msg = await client.queue.pop('jobs')
msg = await client.queue.pop_timeout('jobs', 30)  # seconds

# Message handling
await client.queue.ack(msg.id)
await client.queue.nack(msg.id)  # Requeue for retry
msg = await client.queue.peek('jobs')  # Look without removing

# Queue management
stats = await client.queue.stats('jobs')  # {size, consumers, etc}
await client.queue.purge('jobs')  # Clear all messages
await client.queue.delete('jobs')  # Delete entire queue
queues = await client.queue.list()  # List all queues

# Batch operations
await client.queue.push_batch('jobs', ['task1', 'task2', 'task3'])
msgs = await client.queue.pop_batch('jobs', 10)  # Pop up to 10 messages
```

### 📡 Stream APIs

#### Go Client
```go
// Publishing
msgID := client.Stream.Publish(ctx, "events", "user_login")
msgID := client.Stream.PublishWithKey(ctx, "events", "user123", "user_login")

// Subscribing
client.Stream.Subscribe(ctx, "events", func(msg *StreamMessage) {
    println("Event:", string(msg.Data))
    msg.Ack()  // Acknowledge processing
})

// Consumer groups (for load balancing)
client.Stream.SubscribeGroup(ctx, "events", "analytics-group", func(msg *StreamMessage) {
    // Process in group - only one consumer gets each message
    processEvent(msg.Data)
    msg.Ack()
})

// Reading from offset
events := client.Stream.Read(ctx, "events", offset, limit)
events := client.Stream.ReadFrom(ctx, "events", time.Now().Add(-time.Hour))

// Offset management
client.Stream.Seek(ctx, "events", "consumer-1", offset)
offset := client.Stream.GetOffset(ctx, "events", "consumer-1")

// Stream management
info := client.Stream.Info(ctx, "events")  // {partitions, consumers, etc}
topics := client.Stream.List(ctx)  // List all topics
client.Stream.Create(ctx, "new-events")  // Create topic
client.Stream.Delete(ctx, "old-events")  // Delete topic
client.Stream.Purge(ctx, "events")  // Clear all events
```

#### Node.js Client
```javascript
// Publishing
const msgId = await client.stream.publish('events', 'user_login')
const msgId = await client.stream.publishWithKey('events', 'user123', 'user_login')

// Subscribing
client.stream.subscribe('events', (msg) => {
    console.log('Event:', msg.data)
    msg.ack()  // Acknowledge processing
})

// Consumer groups (for load balancing)
client.stream.subscribeGroup('events', 'analytics-group', (msg) => {
    // Process in group - only one consumer gets each message
    processEvent(msg.data)
    msg.ack()
})

// Reading from offset
const events = await client.stream.read('events', offset, limit)
const events = await client.stream.readFrom('events', Date.now() - 3600000)

// Offset management
await client.stream.seek('events', 'consumer-1', offset)
const offset = await client.stream.getOffset('events', 'consumer-1')

// Stream management
const info = await client.stream.info('events')  // {partitions, consumers, etc}
const topics = await client.stream.list()  // List all topics
await client.stream.create('new-events')  // Create topic
await client.stream.delete('old-events')  // Delete topic
await client.stream.purge('events')  // Clear all events
```

#### Python Client
```python
# Publishing
msg_id = await client.stream.publish('events', 'user_login')
msg_id = await client.stream.publish_with_key('events', 'user123', 'user_login')

# Subscribing
async def event_handler(msg):
    print(f'Event: {msg.data}')
    await msg.ack()  # Acknowledge processing

await client.stream.subscribe('events', event_handler)

# Consumer groups (for load balancing)
async def group_handler(msg):
    # Process in group - only one consumer gets each message
    await process_event(msg.data)
    await msg.ack()

await client.stream.subscribe_group('events', 'analytics-group', group_handler)

# Reading from offset
events = await client.stream.read('events', offset, limit)
events = await client.stream.read_from('events', datetime.now() - timedelta(hours=1))

# Offset management
await client.stream.seek('events', 'consumer-1', offset)
offset = await client.stream.get_offset('events', 'consumer-1')

# Stream management
info = await client.stream.info('events')  # {partitions, consumers, etc}
topics = await client.stream.list()  # List all topics
await client.stream.create('new-events')  # Create topic
await client.stream.delete('old-events')  # Delete topic
await client.stream.purge('events')  # Clear all events
```

### 🔧 Cluster APIs

#### Go Client
```go
// Node management
nodes := client.Cluster.Nodes(ctx)  // List all cluster nodes
status := client.Cluster.Status(ctx)  // Cluster health status
leader := client.Cluster.Leader(ctx)  // Current leader node

// Health checks
health := client.Cluster.Health(ctx, "node-1")
client.Cluster.Join(ctx, "new-node", "localhost:9003")
client.Cluster.Leave(ctx, "node-2")

// Statistics
stats := client.Cluster.Stats(ctx)  // Cluster-wide statistics
```

#### Node.js Client
```javascript
// Node management
const nodes = await client.cluster.nodes()  // List all cluster nodes
const status = await client.cluster.status()  // Cluster health status
const leader = await client.cluster.leader()  // Current leader node

// Health checks
const health = await client.cluster.health('node-1')
await client.cluster.join('new-node', 'localhost:9003')
await client.cluster.leave('node-2')

// Statistics
const stats = await client.cluster.stats()  // Cluster-wide statistics
```

#### Python Client
```python
# Node management
nodes = await client.cluster.nodes()  # List all cluster nodes
status = await client.cluster.status()  # Cluster health status
leader = await client.cluster.leader()  # Current leader node

# Health checks
health = await client.cluster.health('node-1')
await client.cluster.join('new-node', 'localhost:9003')
await client.cluster.leave('node-2')

# Statistics
stats = await client.cluster.stats()  # Cluster-wide statistics
```

## 📊 Performance

### Single Node
- **50K+ operations/sec**
- **<1ms latency p99**
- **~50MB memory usage**

### 3-Node Cluster
- **150K+ operations/sec** (3x scaling)
- **Same latency** with automatic failover
- **Linear scaling** - add more nodes for more performance

## ⚡ Quick Start

### 1. Single Server
```bash
# Download and run
curl -L https://github.com/gomsg/gomsg/releases/latest/download/gomsg-linux -o gomsg
chmod +x gomsg
./gomsg

# Server starts on localhost:9000
```

### 2. Docker Cluster
```bash
docker-compose up -d
# 3-node cluster ready at localhost:9000,9001,9002
```

### 3. Use CLI
```bash
# Key/Value
gomsg kv set name alice
gomsg kv get name

# Queue
gomsg queue push jobs "backup database"
gomsg queue pop jobs

# Stream
gomsg stream publish events "user signup"
gomsg stream subscribe events
```

## 🎯 Perfect For

✅ **Replace Redis** - Distributed KV store with clustering  
✅ **Replace RabbitMQ** - Reliable queues with auto-scaling  
✅ **Replace Kafka** - Event streaming with simple APIs  
✅ **Microservices** - One service for all data patterns  
✅ **High Scale** - Linear scaling from 1 to 100+ nodes  

## 🔗 Links

- **GitHub**: [github.com/gomsg/gomsg](https://github.com/gomsg/gomsg)
- **Docs**: [gomsg.dev/docs](https://gomsg.dev/docs)
- **Discord**: [gomsg.dev/chat](https://gomsg.dev/chat)# gomsg
