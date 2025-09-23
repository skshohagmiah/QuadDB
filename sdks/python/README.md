# fluxdl Python SDK

Python client library for connecting to fluxdl Docker containers.

## Installation

```bash
pip install fluxdl-python-sdk
# or
pip install -e .  # for development
```

## Quick Start

```python
import asyncio
from fluxdl_sdk import fluxdlClient

async def main():
    # Connect to fluxdl Docker container
    client = await fluxdlClient.create(
        address="localhost:9000",
        timeout=10.0
    )
    # Key-Value operations (Redis-like)
    await client.kv.set("user:1", "John Doe")
    user = await client.kv.get("user:1")
    
    # Queue operations (RabbitMQ-like)
    await client.queue.push("tasks", "process-payment")
    task = await client.queue.pop("tasks")
    
    # Stream operations (Kafka-like)
    await client.stream.create_stream("events", partitions=3)
    await client.stream.publish("events", "user-login")
    
    await client.disconnect()

asyncio.run(main())
```

## Features

- **🔑 Key-Value Store**: Redis-like operations with TTL support
- **📬 Message Queues**: RabbitMQ-like pub/sub messaging
- **🌊 Event Streams**: Kafka-like streaming with partitions
- **🔌 Async/Await**: Full async support with asyncio
- **🐳 Docker Ready**: Works seamlessly with fluxdl containers
- **🛡️ Type Safety**: Full type hints and dataclasses
- **⚡ High Performance**: gRPC-based communication

## Docker Integration

Perfect for fluxdl Docker containers:

```bash
# Run fluxdl container
docker run -d -p 9000:9000 -v fluxdl-data:/data --name fluxdl shohag2100/fluxdl:latest

# Your Python app connects automatically
client = await fluxdlClient.create(address="localhost:9000")
```

## API Reference

### Client Connection

```python
# Create and connect
client = await fluxdlClient.create(address="localhost:9000", timeout=30.0)

# Or create then connect
client = fluxdlClient(address="localhost:9000")
await client.connect()

# Test connection
is_connected = await client.ping()

# Disconnect
await client.disconnect()

# Use as context manager
async with fluxdlClient.create() as client:
    await client.kv.set("key", "value")
```

### Key-Value Operations

```python
# Basic operations
await client.kv.set("key", "value")
await client.kv.set("key", "value", ttl=3600)  # With TTL
value = await client.kv.get("key")
deleted = await client.kv.delete("key")

# Advanced operations
exists = await client.kv.exists("key")
keys = await client.kv.keys("user:*")
count = await client.kv.increment("counter", 5)
count = await client.kv.decrement("counter", 2)

# Batch operations
await client.kv.mset({"key1": "value1", "key2": "value2"})
values = await client.kv.mget(["key1", "key2"])

# TTL operations
ttl = await client.kv.ttl("key")
await client.kv.expire("key", 3600)
await client.kv.persist("key")
```

### Queue Operations

```python
# Basic queue operations
await client.queue.push("queue-name", "message")
await client.queue.push("queue-name", "priority-message", priority=10)
message = await client.queue.pop("queue-name")
message = await client.queue.pop("queue-name", timeout=5.0)
peeked = await client.queue.peek("queue-name")

# Queue management
queues = await client.queue.list()
stats = await client.queue.stats("queue-name")
purged_count = await client.queue.purge("queue-name")
size = await client.queue.size("queue-name")

# Batch operations
await client.queue.push_batch("queue", ["msg1", "msg2", "msg3"])
messages = await client.queue.pop_batch("queue", count=10)

# Queue lifecycle
await client.queue.create("new-queue", max_size=1000)
await client.queue.delete("old-queue")
```

### Stream Operations

```python
# Stream management
await client.stream.create_stream("stream-name", partitions=3)
streams = await client.stream.list_streams()
info = await client.stream.get_stream_info("stream-name")
await client.stream.delete_stream("stream-name")

# Publishing
offset = await client.stream.publish("stream-name", "message")
offset = await client.stream.publish("stream-name", "message", key="user:123")
offsets = await client.stream.publish_batch("stream-name", [
    {"key": "key1", "value": "message1"},
    {"key": "key2", "value": "message2"}
])

# Subscribing
def message_handler(message):
    print(f"Received: {message.value} (offset: {message.offset})")

await client.stream.subscribe("stream-name", message_handler)

# Advanced subscription
from fluxdl_sdk.stream import SubscribeOptions

options = SubscribeOptions(group="my-group", partition=0, offset=100)
await client.stream.subscribe("stream-name", message_handler, options)

# Reading messages
messages = await client.stream.get_messages("stream-name", partition=0, offset=0, limit=10)

# Offset management
await client.stream.seek("stream-name", partition=0, offset=500)
await client.stream.commit_offset("stream-name", partition=0, offset=600, group="my-group")
offset = await client.stream.get_committed_offset("stream-name", partition=0, group="my-group")
```

## Data Types

### Queue Statistics
```python
@dataclass
class QueueStats:
    name: str
    messages: int
    size: int
    consumers: int
```

### Stream Message
```python
@dataclass
class StreamMessage:
    stream: str
    partition: int
    offset: int
    key: Optional[str]
    value: str
    timestamp: int
    headers: Dict[str, str]
```

### Stream Information
```python
@dataclass
class StreamInfo:
    name: str
    partitions: int
    messages: int
    size: int
    created_at: int
```

## Error Handling

```python
from fluxdl_sdk import fluxdlClient, fluxdlError, ConnectionError, TimeoutError

try:
    await client.kv.set("key", "value")
except ConnectionError:
    print("Connection failed")
except TimeoutError:
    print("Request timed out")
except fluxdlError as e:
    print(f"fluxdl error: {e.message} (code: {e.code})")
```

## Configuration

```python
client = fluxdlClient(
    address="localhost:9000",      # Server address
    timeout=30.0,                  # Request timeout in seconds
    credentials=None,              # gRPC credentials (None = insecure)
    options={                      # gRPC channel options
        'grpc.keepalive_time_ms': 30000,
        'grpc.keepalive_timeout_ms': 5000,
    }
)
```

## Examples

Run the examples:

```bash
# Basic example
python examples/basic_example.py

# Key-value example
python examples/kv_example.py

# Queue example
python examples/queue_example.py

# Stream example
python examples/stream_example.py
```

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black fluxdl_sdk/
isort fluxdl_sdk/

# Type checking
mypy fluxdl_sdk/
```

## Development Status

🚧 **Work in Progress**: This SDK provides a complete async Python interface. For full functionality:

1. Copy protobuf files from main fluxdl project
2. Generate Python gRPC clients using `grpcio-tools`
3. Replace placeholder implementations with actual gRPC calls

## Requirements

- Python 3.8+
- asyncio support
- grpcio >= 1.60.0

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - see LICENSE file for details.

## Support

- 📖 [Documentation](https://github.com/shohag2100/fluxdl-python-sdk/wiki)
- 🐛 [Issues](https://github.com/shohag2100/fluxdl-python-sdk/issues)
- 💬 [Discussions](https://github.com/shohag2100/fluxdl-python-sdk/discussions)
