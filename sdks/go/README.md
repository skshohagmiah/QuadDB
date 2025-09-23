# fluxdl Go SDK

Go client library for connecting to fluxdl Docker containers.

## Installation

```bash
go mod init your-project
go get github.com/skshohagmiah/fluxdl-go-sdk
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"
    
    fluxdl "github.com/skshohagmiah/fluxdl-go-sdk"
)

func main() {
    // Connect to fluxdl Docker container
    config := &fluxdl.Config{
        Address: "localhost:9000",
        Timeout: 30 * time.Second,
    }
    
    client, err := fluxdl.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    ctx := context.Background()
    
    // Key-Value operations (Redis-like)
    client.KV.Set(ctx, "user:1", "John Doe")
    value, _ := client.KV.Get(ctx, "user:1")
    
    // Queue operations (RabbitMQ-like)
    client.Queue.Push(ctx, "tasks", "process-payment")
    message, _ := client.Queue.Pop(ctx, "tasks")
    
    // Stream operations (Kafka-like)
    client.Stream.CreateStream(ctx, "events", 3)
    client.Stream.Publish(ctx, "events", "user-login")
}
```

## Features

- **Key-Value Store**: Redis-like operations (get, set, delete, etc.)
- **Message Queues**: RabbitMQ-like pub/sub messaging
- **Event Streams**: Kafka-like streaming with partitions
- **Connection Management**: Automatic reconnection and timeouts
- **Type Safety**: Full Go type safety and error handling

## Docker Integration

Works seamlessly with fluxdl Docker containers:

```bash
# Run fluxdl container
docker run -d -p 9000:9000 -v fluxdl-data:/data --name fluxdl shohag2100/fluxdl:latest

# Your Go app connects automatically
client, _ := fluxdl.NewClient(&fluxdl.Config{Address: "localhost:9000"})
```

## API Reference

### Client

- `NewClient(config *Config) (*Client, error)` - Create new client
- `Close() error` - Close connection
- `Ping(ctx context.Context) error` - Test connection

### Key-Value Operations

- `Set(ctx, key, value string) (string, error)` - Store key-value
- `Get(ctx, key string) (string, error)` - Retrieve value
- `Delete(ctx, key string) error` - Remove key
- `Exists(ctx, key string) (bool, error)` - Check existence
- `Keys(ctx, pattern string) ([]string, error)` - Find keys
- `Increment(ctx, key string) (int64, error)` - Increment counter
- `Decrement(ctx, key string) (int64, error)` - Decrement counter

### Queue Operations

- `Push(ctx, queue, message string) error` - Add message
- `Pop(ctx, queue string) (string, error)` - Remove message
- `Peek(ctx, queue string) (string, error)` - View without removing
- `List(ctx) ([]string, error)` - List all queues
- `Stats(ctx, queue string) (*QueueStats, error)` - Get statistics
- `Purge(ctx, queue string) error` - Clear queue

### Stream Operations

- `Publish(ctx, stream, message string) error` - Send message
- `Subscribe(ctx, stream string, handler MessageHandler) error` - Listen
- `CreateStream(ctx, stream string, partitions int) error` - Create stream
- `ListStreams(ctx) ([]string, error)` - List streams
- `GetStreamInfo(ctx, stream string) (*StreamInfo, error)` - Get info

## Development Status

🚧 **Work in Progress**: This SDK currently provides the interface structure. Full implementation requires:

1. Copy protobuf files from main fluxdl project
2. Generate Go gRPC clients
3. Implement actual gRPC calls

See `example/main.go` for usage examples.

## Contributing

1. Fork the repository
2. Create feature branch
3. Add tests
4. Submit pull request

## License

MIT License - see LICENSE file for details.
