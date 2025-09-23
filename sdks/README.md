# fluxdl SDKs

Client libraries for connecting to fluxdl Docker containers in multiple programming languages.

📖 **For complete installation instructions, see [INSTALLATION.md](../INSTALLATION.md)**

## Available SDKs

### 🐹 Go SDK
**Location**: `./go/`

```go
import "github.com/skshohagmiah/fluxdl-go-sdk"

client, _ := fluxdl.NewClient(&fluxdl.Config{Address: "localhost:9000"})
client.KV.Set(ctx, "key", "value")
```

### 🟢 Node.js/TypeScript SDK  
**Location**: `./nodejs/`

```typescript
import { fluxdlClient } from '@skshohagmiah/fluxdl-nodejs-sdk';

const client = await fluxdlClient.connect({ address: 'localhost:9000' });
await client.kv.set('key', 'value');
```

### 🐍 Python SDK
**Location**: `./python/`

```python
from fluxdl_sdk import fluxdlClient

client = await fluxdlClient.create(address="localhost:9000")
await client.kv.set("key", "value")
```

## Quick Start with Docker

1. **Run fluxdl Container**:
```bash
docker run -d -p 9000:9000 -v fluxdl-data:/data --name fluxdl shohag2100/fluxdl:latest
```

2. **Choose Your SDK**:
   - **Go**: `cd go && go mod tidy`
   - **Node.js**: `cd nodejs && npm install`
   - **Python**: `cd python && pip install -e .`

3. **Connect and Use**:
   - All SDKs connect to `localhost:9000` by default
   - Support Key-Value, Queue, and Stream operations
   - Redis + RabbitMQ + Kafka functionality in one service

## Features Across All SDKs

| Feature | Go SDK | Node.js SDK | Python SDK | Status |
|---------|--------|-------------|------------|---------|
| Key-Value Store | ✅ | ✅ | ✅ | Interface Ready |
| Message Queues | ✅ | ✅ | ✅ | Interface Ready |
| Event Streams | ✅ | ✅ | ✅ | Interface Ready |
| Connection Management | ✅ | ✅ | ✅ | Interface Ready |
| Type Safety | ✅ | ✅ | ✅ | Full Types |
| Error Handling | ✅ | ✅ | ✅ | Custom Errors |
| Docker Integration | ✅ | ✅ | ✅ | Port 9000 |
| Async Support | ✅ | ✅ | ✅ | Full Async |

## Development Status

🚧 **Current Status**: Interface Complete, Implementation Pending

All SDKs provide complete, type-safe interfaces for all fluxdl operations. To make them fully functional:

1. **Copy Protobuf Files**: Copy `.proto` files from `../api/proto/` to each SDK
2. **Generate gRPC Clients**: Use protoc to generate language-specific clients
3. **Implement gRPC Calls**: Replace placeholder implementations with actual gRPC calls

## Architecture

```
fluxdl Docker Container (port 9000)
    ↕ gRPC
┌─────────────────┬─────────────────┬─────────────────┐
│   Go SDK        │   Node.js SDK   │   Python SDK    │
├─────────────────┼─────────────────┼─────────────────┤
│ • KV Client     │ • KV Client     │ • KV Client     │
│ • Queue Client  │ • Queue Client  │ • Queue Client  │
│ • Stream Client │ • Stream Client │ • Stream Client │
└─────────────────┴─────────────────┴─────────────────┘
```

## Example Usage

### Key-Value Operations (Redis-like)
```bash
# Go
client.KV.Set(ctx, "user:1", "John")
value, _ := client.KV.Get(ctx, "user:1")

# Node.js
await client.kv.set("user:1", "John")
const value = await client.kv.get("user:1")

# Python
await client.kv.set("user:1", "John")
value = await client.kv.get("user:1")
```

### Queue Operations (RabbitMQ-like)
```bash
# Go
client.Queue.Push(ctx, "tasks", "process-payment")
message, _ := client.Queue.Pop(ctx, "tasks")

# Node.js
await client.queue.push("tasks", "process-payment")
const message = await client.queue.pop("tasks")

# Python
await client.queue.push("tasks", "process-payment")
message = await client.queue.pop("tasks")
```

### Stream Operations (Kafka-like)
```bash
# Go
client.Stream.CreateStream(ctx, "events", 3)
client.Stream.Publish(ctx, "events", "user-login")

# Node.js
await client.stream.createStream("events", 3)
await client.stream.publish("events", "user-login")

# Python
await client.stream.create_stream("events", partitions=3)
await client.stream.publish("events", "user-login")
```

## Testing SDKs

### Test Go SDK
```bash
cd go
go run example/main.go
```

### Test Node.js SDK
```bash
cd nodejs
npm install
npm run example
```

### Test Python SDK
```bash
cd python
pip install -e .
python examples/basic_example.py
```

## Contributing

1. Fork the repository
2. Choose an SDK to work on
3. Implement gRPC integration
4. Add tests and examples
5. Submit pull request

## Roadmap

- [ ] **Phase 1**: Complete gRPC integration
- [x] **Phase 2**: Add Python SDK ✅
- [ ] **Phase 3**: Add Java SDK
- [ ] **Phase 4**: Add Rust SDK
- [ ] **Phase 5**: Add C# SDK

## Support

- 📖 [Main Documentation](../README.md)
- 🐳 [Docker Guide](../DOCKER_SIMPLE.md)
- 🐛 [Issues](https://github.com/skshohagmiah/fluxdl/issues)

---

**fluxdl**: One service to replace Redis + RabbitMQ + Kafka 🚀
