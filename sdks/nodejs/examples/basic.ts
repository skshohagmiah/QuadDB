import { fluxdlClient } from '../src';

async function main() {
  console.log('🚀 fluxdl Node.js SDK Example\n');

  // Create and connect to fluxdl Docker container
  const client = await fluxdlClient.connect({
    address: 'localhost:9000',
    timeout: 10000
  });

  try {
    // Test connection
    console.log('📡 Testing connection...');
    const isConnected = await client.ping();
    console.log(`✅ Connection: ${isConnected ? 'OK' : 'Failed'}\n`);

    // Key-Value operations (Redis-like)
    console.log('🔑 Key-Value Operations:');
    await client.kv.set('user:1', 'John Doe');
    const user = await client.kv.get('user:1');
    console.log(`   Set user:1 = John Doe`);
    console.log(`   Get user:1 = ${user}`);
    
    await client.kv.increment('counter', 5);
    console.log(`   Incremented counter by 5`);
    
    const keys = await client.kv.keys('*');
    console.log(`   Found keys: ${keys.join(', ')}\n`);

    // Queue operations (RabbitMQ-like)
    console.log('📬 Queue Operations:');
    await client.queue.push('notifications', 'Welcome to fluxdl!');
    console.log(`   Pushed message to notifications queue`);
    
    const message = await client.queue.pop('notifications');
    console.log(`   Popped message: ${message}`);
    
    const stats = await client.queue.stats('notifications');
    console.log(`   Queue stats: ${stats.messages} messages, ${stats.size} bytes\n`);

    // Stream operations (Kafka-like)
    console.log('🌊 Stream Operations:');
    await client.stream.createStream('events', 3);
    console.log(`   Created stream 'events' with 3 partitions`);
    
    await client.stream.publish('events', 'User logged in', 'user:123');
    console.log(`   Published message to events stream`);
    
    const streamInfo = await client.stream.getStreamInfo('events');
    console.log(`   Stream info: ${streamInfo.partitions} partitions, ${streamInfo.messages} messages`);

    // Subscribe to stream messages
    console.log(`   Subscribing to events stream...`);
    await client.stream.subscribe('events', async (message) => {
      console.log(`   📨 Received: ${message.value} (offset: ${message.offset})`);
    });

    // Wait a bit to receive messages
    await new Promise(resolve => setTimeout(resolve, 1000));

  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    // Disconnect
    await client.disconnect();
    console.log('\n🎉 Example completed!');
    console.log('💡 Note: This is a demo implementation. Full functionality requires gRPC integration.');
  }
}

// Run the example
main().catch(console.error);
