const { GoMsgClient } = require('../dist/index');

async function main() {
  // Connect to GoMsg cluster (Docker containers)
  const client = new GoMsgClient({
    nodes: [
      'localhost:8080', // GoMsg node 1
      'localhost:8081', // GoMsg node 2
      'localhost:8082', // GoMsg node 3
    ],
    connectTimeout: 10000,
    requestTimeout: 30000,
    retryAttempts: 3,
  });

  try {
    await client.connect();
    console.log('✅ Connected to GoMsg cluster');

    // Test KV operations
    console.log('\n=== KV Operations ===');

    // Set a key
    await client.kv.set('user:123', Buffer.from('John Doe'), 300); // 5 minutes TTL
    console.log('✅ Set user:123 = "John Doe"');

    // Get the key
    const result = await client.kv.get('user:123');
    if (result.found) {
      console.log(`✅ Get user:123 = "${result.value.toString()}"`);
    } else {
      console.log('❌ Key not found');
    }

    // Increment a counter
    const count = await client.kv.increment('page_views', 1);
    console.log(`✅ Incremented page_views to ${count}`);

    // Test Queue operations
    console.log('\n=== Queue Operations ===');

    // Push a message
    const msgId = await client.queue.push('notifications', Buffer.from('Hello'));
    console.log(`✅ Pushed message to notifications queue: ${msgId}`);

    // Pop a message
    const message = await client.queue.pop('notifications', 5);
    if (message) {
      console.log(`✅ Popped message: ${message.id} (data: ${message.data.toString()})`);
    } else {
      console.log('❌ No messages in queue');
    }

    // Check queue size
    const size = await client.queue.size('notifications');
    console.log(`✅ Queue size: ${size}`);

    // Test Stream operations
    console.log('\n=== Stream Operations ===');

    // Create a topic
    await client.stream.createTopic('user-events', 4);
    console.log('✅ Created topic "user-events" with 4 partitions');

    // Publish a message
    const headers = { source: 'web', version: '1.0' };
    const msg = await client.stream.publish(
      'user-events',
      'user:123',
      Buffer.from(JSON.stringify({ action: 'login', user: '123' })),
      headers
    );
    console.log(`✅ Published to stream: offset=${msg.offset}, partition=${msg.partition}`);

    // Read messages
    const messages = await client.stream.read('user-events', 0, 0, 10);
    console.log(`✅ Read ${messages.length} messages from stream`);
    messages.forEach(msg => {
      console.log(`   Message: offset=${msg.offset}, data=${msg.data.toString()}`);
    });

    // Check cluster health
    console.log('\n=== Cluster Health ===');
    const health = await client.health();
    Object.entries(health).forEach(([node, healthy]) => {
      const status = healthy ? '✅ Healthy' : '❌ Unhealthy';
      console.log(`Node ${node}: ${status}`);
    });

    console.log('\n🎉 GoMsg Node.js client demo completed!');

  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await client.close();
    console.log('👋 Disconnected from GoMsg');
  }
}

main().catch(console.error);
