# fluxdl Node.js SDK

Node.js/TypeScript client library for connecting to fluxdl Docker containers.

## Installation

```bash
npm install @skshohagmiah/fluxdl-nodejs-sdk
# or
yarn add @skshohagmiah/fluxdl-nodejs-sdk
```

## Quick Start

```typescript
import { fluxdlClient } from '@skshohagmiah/fluxdl-nodejs-sdk';

async function main() {
  // Connect to fluxdl Docker container
  const client = await fluxdlClient.connect({
    address: 'localhost:9000',
    timeout: 10000
  });

  // Key-Value operations (Redis-like)
  await client.kv.set('user:1', 'John Doe');
  const user = await client.kv.get('user:1');
  
  // Queue operations (RabbitMQ-like)
  await client.queue.push('tasks', 'process-payment');
  const task = await client.queue.pop('tasks');
  
  // Stream operations (Kafka-like)
  await client.stream.createStream('events', 3);
  await client.stream.publish('events', 'user-login');
  
  await client.disconnect();
}

main().catch(console.error);
```

## Features

- **🔑 Key-Value Store**: Redis-like operations with TTL support
- **📬 Message Queues**: RabbitMQ-like pub/sub messaging
- **🌊 Event Streams**: Kafka-like streaming with partitions
- **🔌 Easy Connection**: Simple connection management
- **📝 TypeScript**: Full TypeScript support with type definitions
- **🐳 Docker Ready**: Works seamlessly with fluxdl containers

## Docker Integration

Perfect for fluxdl Docker containers:

```bash
# Run fluxdl container
docker run -d -p 9000:9000 -v fluxdl-data:/data --name fluxdl shohag2100/fluxdl:latest

# Your Node.js app connects automatically
const client = await fluxdlClient.connect({ address: 'localhost:9000' });
```

## API Reference

### Client Connection

```typescript
// Create client
const client = new fluxdlClient({ address: 'localhost:9000' });
await client.connect();

// Or connect in one step
const client = await fluxdlClient.connect({ address: 'localhost:9000' });

// Test connection
const isConnected = await client.ping();

// Disconnect
await client.disconnect();
```

### Key-Value Operations

```typescript
// Basic operations
await client.kv.set('key', 'value');
const value = await client.kv.get('key');
await client.kv.delete('key');

// Advanced operations
const exists = await client.kv.exists('key');
const keys = await client.kv.keys('user:*');
const count = await client.kv.increment('counter', 5);

// Batch operations
await client.kv.mset({ 'key1': 'value1', 'key2': 'value2' });
const values = await client.kv.mget(['key1', 'key2']);
```

### Queue Operations

```typescript
// Basic queue operations
await client.queue.push('queue-name', 'message');
const message = await client.queue.pop('queue-name');
const peeked = await client.queue.peek('queue-name');

// Queue management
const queues = await client.queue.list();
const stats = await client.queue.stats('queue-name');
const purged = await client.queue.purge('queue-name');

// Batch operations
await client.queue.pushBatch('queue', ['msg1', 'msg2', 'msg3']);
const messages = await client.queue.popBatch('queue', 10);
```

### Stream Operations

```typescript
// Stream management
await client.stream.createStream('stream-name', 3); // 3 partitions
const streams = await client.stream.listStreams();
const info = await client.stream.getStreamInfo('stream-name');

// Publishing
await client.stream.publish('stream-name', 'message', 'optional-key');
await client.stream.publishBatch('stream-name', [
  { key: 'key1', value: 'message1' },
  { key: 'key2', value: 'message2' }
]);

// Subscribing
await client.stream.subscribe('stream-name', async (message) => {
  console.log(`Received: ${message.value}`);
  console.log(`Offset: ${message.offset}`);
  console.log(`Partition: ${message.partition}`);
});

// Reading messages
const messages = await client.stream.getMessages('stream-name', 0, 0, 10);
```

## Configuration Options

```typescript
interface fluxdlConfig {
  address?: string;     // Server address (default: 'localhost:9000')
  timeout?: number;     // Request timeout in ms (default: 30000)
  credentials?: any;    // gRPC credentials (default: insecure)
}
```

## Error Handling

```typescript
import { fluxdlError, ConnectionError, TimeoutError } from '@skshohagmiah/fluxdl-nodejs-sdk';

try {
  await client.kv.set('key', 'value');
} catch (error) {
  if (error instanceof ConnectionError) {
    console.log('Connection failed');
  } else if (error instanceof TimeoutError) {
    console.log('Request timed out');
  } else if (error instanceof fluxdlError) {
    console.log('fluxdl error:', error.message);
  }
}
```

## Examples

See the `examples/` directory for complete examples:

- `basic.ts` - Basic usage of all features
- `kv-example.ts` - Key-value operations
- `queue-example.ts` - Queue operations  
- `stream-example.ts` - Stream operations

Run examples:

```bash
npm run example
```

## Development Status

🚧🚀 **Work in Progress**: This SDK provides a complete TypeScript interface. For full functionality:

1. Copy protobuf files from main fluxdl project
2. Generate Node.js gRPC clients
3. Replace placeholder implementations with actual gRPC calls

## Building from Source

```bash
git clone https://github.com/skshohagmiah/fluxdl-nodejs-sdk.git
cd fluxdl-nodejs-sdk
npm install
npm run build
```

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - see LICENSE file for details.

## Support

- 📖 [Documentation](https://github.com/shohag2100/fluxdl-nodejs-sdk/wiki)
- 🐛 [Issues](https://github.com/shohag2100/fluxdl-nodejs-sdk/issues)
- 💬 [Discussions](https://github.com/shohag2100/fluxdl-nodejs-sdk/discussions)
