# GoMsg

**GoMsg** is a fast, distributed data platform that replaces Redis + RabbitMQ + Kafka with one unified service.

## 🚀 Why GoMsg?

**One service. Three data patterns. Built for scale.**

- **🔑 Key/Value store** like Redis - Fast caching and data storage
- **📬 Message queues** like RabbitMQ - Reliable pub/sub messaging  
- **🌊 Event streams** like Kafka - High-throughput event processing

All with **high-performance gRPC APIs**, **automatic clustering**, and **multi-language SDKs**.

## 📖 Quick Links

- **🚀 [Installation Guide](INSTALLATION.md)** - Get started in 2 minutes
- **📚 [SDK Documentation](sdks/README.md)** - Go, Node.js, Python clients
- **🐳 [Docker Guide](DOCKER_SIMPLE.md)** - Container deployment
- **🔧 [API Reference](#-api-examples)** - Full API docs

## ⚡ Quick Start

### 1. Run GoMsg (Docker - Recommended)
```bash
docker run -d --name gomsg -p 9000:9000 -v gomsg-data:/data shohag2100/gomsg:latest
```

### 2. Install SDK (Choose Your Language)
```bash
# Go
go get github.com/shohag2100/gomsg-go-sdk

# Node.js
npm install @shohag2100/gomsg-nodejs-sdk

# Python
pip install gomsg-python-sdk
```

### 3. Connect and Use
```go
// Go
client, _ := gomsg.NewClient(&gomsg.Config{Address: "localhost:9000"})
client.KV.Set(ctx, "user:1", "John Doe")
```

```javascript
// Node.js
const client = await GoMsgClient.connect({address: 'localhost:9000'});
await client.kv.set('user:1', 'John Doe');
```

```python
# Python
client = await GoMsgClient.create(address="localhost:9000")
await client.kv.set("user:1", "John Doe")
```

## 🎯 Use Cases

✅ **Replace Redis** - Distributed KV store with clustering  
✅ **Replace RabbitMQ** - Reliable queues with auto-scaling  
✅ **Replace Kafka** - Event streaming with simple APIs  
✅ **Microservices** - One service for all data patterns  
✅ **High Scale** - Linear scaling from 1 to 100+ nodes

## 📊 Performance

### Single Node
- **50K+ operations/sec**
- **<1ms latency p99**
- **~50MB memory usage**

### 3-Node Cluster
- **150K+ operations/sec** (3x scaling)
- **Same latency** with automatic failover
- **Linear scaling** - add more nodes for more performance

## 🏗️ Architecture

```
GoMsg Server (Docker Container)
    ↕ gRPC (port 9000)
┌─────────────────┬─────────────────┬─────────────────┐
│   Go SDK        │   Node.js SDK   │   Python SDK    │
├─────────────────┼─────────────────┼─────────────────┤
│ • KV Client     │ • KV Client     │ • KV Client     │
│ • Queue Client  │ • Queue Client  │ • Queue Client  │
│ • Stream Client │ • Stream Client │ • Stream Client │
└─────────────────┴─────────────────┴─────────────────┘
```

## 📁 Project Structure

```
gomsg/
├── INSTALLATION.md            # 📖 Complete setup guide
├── DOCKER_SIMPLE.md          # 🐳 Docker deployment
├── sdks/                     # 📚 Multi-language SDKs
│   ├── README.md            # SDK overview
│   ├── go/                  # 🐹 Go SDK
│   │   ├── README.md        # Go documentation
│   │   ├── client.go        # Main client
│   │   ├── kv.go           # Key-Value operations
│   │   ├── queue.go        # Queue operations
│   │   ├── stream.go       # Stream operations
│   │   └── example/        # Working examples
│   ├── nodejs/             # 🟢 Node.js/TypeScript SDK
│   │   ├── README.md       # Node.js documentation
│   │   ├── src/           # Source code
│   │   └── examples/      # Working examples
│   └── python/            # 🐍 Python SDK
│       ├── README.md      # Python documentation
│       ├── gomsg_sdk/     # Package source
│       └── examples/      # Working examples
├── cmd/                   # 🔧 Binaries
│   ├── gomsg/            # Server binary
│   └── cli/              # CLI tool
├── pkg/                  # 📦 Core packages
│   ├── kv/              # Key-Value store
│   ├── queue/           # Message queues
│   ├── stream/          # Event streams
│   ├── server/          # gRPC server
│   └── cluster/         # Clustering logic
├── api/                 # 🔌 gRPC definitions
│   ├── proto/          # Protocol buffers
│   └── generated/      # Generated code
├── storage/            # 💾 Storage backends
├── tests/              # 🧪 Test suites
├── scripts/            # 🔨 Build scripts
├── Dockerfile          # 🐳 Container image
├── docker-compose.simple.yml # 🐳 Simple deployment
├── Makefile           # 🔨 Build commands
└── go.mod            # 📦 Go dependencies
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
    image: shohag2100/gomsg:latest
    ports: ["9000:9000"]
    volumes: ["gomsg-data-1:/data"]
    command: --cluster --node-id=node1 --port=9000 --bootstrap
    
  gomsg-2:
    image: shohag2100/gomsg:latest
    ports: ["9001:9001"]
    volumes: ["gomsg-data-2:/data"]
    command: --cluster --node-id=node2 --port=9001 --join=gomsg-1:9000
    
  gomsg-3:
    image: shohag2100/gomsg:latest
    ports: ["9002:9002"]
    volumes: ["gomsg-data-3:/data"]
    command: --cluster --node-id=node3 --port=9002 --join=gomsg-1:9000

volumes:
  gomsg-data-1:
  gomsg-data-2:
  gomsg-data-3:
```

Or use the simple deployment:
```bash
make docker-compose  # Uses docker-compose.simple.yml
```

## 📚 Multi-Language SDKs

GoMsg provides **production-ready SDKs** for multiple programming languages with **consistent APIs**:

| Language | Package | Status | Documentation |
|----------|---------|---------|---------------|
| **🐹 Go** | `github.com/shohag2100/gomsg-go-sdk` | ✅ Ready | [Go SDK](sdks/go/README.md) |
| **🟢 Node.js** | `@shohag2100/gomsg-nodejs-sdk` | ✅ Ready | [Node.js SDK](sdks/nodejs/README.md) |
| **🐍 Python** | `gomsg-python-sdk` | ✅ Ready | [Python SDK](sdks/python/README.md) |

### SDK Features
- **🔑 Key-Value Operations** - Redis-like caching and storage
- **📬 Queue Operations** - RabbitMQ-like pub/sub messaging
- **🌊 Stream Operations** - Kafka-like event processing
- **🔌 Connection Management** - Automatic reconnection and pooling
- **🛡️ Type Safety** - Full type definitions and error handling
- **🐳 Docker Ready** - Works seamlessly with GoMsg containers

## 🔌 API Examples

### Key-Value Operations (Redis-like)

```go
// Go
client.KV.Set(ctx, "user:1", "John Doe")
value, _ := client.KV.Get(ctx, "user:1")
client.KV.Delete(ctx, "user:1")
```

```javascript
// Node.js
await client.kv.set('user:1', 'John Doe');
const value = await client.kv.get('user:1');
await client.kv.delete('user:1');
```

```python
# Python
await client.kv.set("user:1", "John Doe")
value = await client.kv.get("user:1")
await client.kv.delete("user:1")
```

### Queue Operations (RabbitMQ-like)

```go
// Go
client.Queue.Push(ctx, "tasks", "process-payment")
message, _ := client.Queue.Pop(ctx, "tasks")
```

```javascript
// Node.js
await client.queue.push('tasks', 'process-payment');
const message = await client.queue.pop('tasks');
```

```python
# Python
await client.queue.push("tasks", "process-payment")
message = await client.queue.pop("tasks")
```

### Stream Operations (Kafka-like)

```go
// Go
client.Stream.CreateStream(ctx, "events", 3)
client.Stream.Publish(ctx, "events", "user-login")
```

```javascript
// Node.js
await client.stream.createStream('events', 3);
await client.stream.publish('events', 'user-login');
```

```python
# Python
await client.stream.create_stream("events", partitions=3)
await client.stream.publish("events", "user-login")
```

## 🚀 Getting Started

1. **📖 [Read the Installation Guide](INSTALLATION.md)** - Complete setup instructions
2. **🐳 Run GoMsg**: `docker run -d --name gomsg -p 9000:9000 shohag2100/gomsg:latest`
3. **📚 Choose your SDK**: [Go](sdks/go/README.md) | [Node.js](sdks/nodejs/README.md) | [Python](sdks/python/README.md)
4. **🔧 Start building**: Replace Redis, RabbitMQ, and Kafka with one service!

## 🛠️ Development

### Build from Source
```bash
git clone https://github.com/skshohagmiah/gomsg.git
cd gomsg
make build
./bin/gomsg server
```

### Run Tests
```bash
make test
```

### Docker Build
```bash
make docker
make docker-run
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🔗 Links

- **📖 Documentation**: [Installation Guide](INSTALLATION.md) | [SDK Docs](sdks/README.md)
- **🐳 Docker**: [Docker Hub](https://hub.docker.com/r/shohag2100/gomsg)
- **🐛 Issues**: [GitHub Issues](https://github.com/skshohagmiah/gomsg/issues)
- **💬 Discussions**: [GitHub Discussions](https://github.com/skshohagmiah/gomsg/discussions)

---

**GoMsg**: One service to replace Redis + RabbitMQ + Kafka 🚀
