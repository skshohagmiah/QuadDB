#!/usr/bin/env python3
"""
GoMsg Python SDK Basic Example

This example demonstrates how to use the GoMsg Python SDK to connect to a GoMsg Docker container
and perform key-value, queue, and stream operations.
"""

import asyncio
from gomsg_sdk import GoMsgClient


async def main():
    print("🐍 GoMsg Python SDK Example\n")

    # Connect to GoMsg Docker container
    client = await GoMsgClient.create(
        address="localhost:9000",
        timeout=10.0
    )

    try:
        # Test connection
        print("📡 Testing connection...")
        is_connected = await client.ping()
        print(f"✅ Connection: {'OK' if is_connected else 'Failed'}\n")

        # Key-Value operations (Redis-like)
        print("🔑 Key-Value Operations:")
        await client.kv.set("user:1", "John Doe")
        user = await client.kv.get("user:1")
        print(f"   Set user:1 = John Doe")
        print(f"   Get user:1 = {user}")
        
        await client.kv.increment("counter", 5)
        print(f"   Incremented counter by 5")
        
        keys = await client.kv.keys("*")
        print(f"   Found keys: {', '.join(keys)}")
        
        # Multiple operations
        await client.kv.mset({"name": "Alice", "age": "30", "city": "NYC"})
        values = await client.kv.mget(["name", "age", "city"])
        print(f"   Batch set/get: {values}\n")

        # Queue operations (RabbitMQ-like)
        print("📬 Queue Operations:")
        await client.queue.push("notifications", "Welcome to GoMsg!")
        print(f"   Pushed message to notifications queue")
        
        message = await client.queue.pop("notifications")
        print(f"   Popped message: {message}")
        
        stats = await client.queue.stats("notifications")
        print(f"   Queue stats: {stats.messages} messages, {stats.size} bytes")
        
        # Batch operations
        await client.queue.push_batch("tasks", ["task1", "task2", "task3"])
        batch_messages = await client.queue.pop_batch("tasks", count=2)
        print(f"   Batch operations: pushed 3, popped {len(batch_messages)}\n")

        # Stream operations (Kafka-like)
        print("🌊 Stream Operations:")
        await client.stream.create_stream("events", partitions=3)
        print(f"   Created stream 'events' with 3 partitions")
        
        offset = await client.stream.publish("events", "User logged in", key="user:123")
        print(f"   Published message to events stream (offset: {offset})")
        
        stream_info = await client.stream.get_stream_info("events")
        print(f"   Stream info: {stream_info.partitions} partitions, {stream_info.messages} messages")

        # Batch publish
        batch_messages = [
            {"key": "user:1", "value": "login"},
            {"key": "user:2", "value": "logout"},
            {"key": "user:3", "value": "signup"}
        ]
        offsets = await client.stream.publish_batch("events", batch_messages)
        print(f"   Batch published {len(batch_messages)} messages")

        # Subscribe to stream messages
        print(f"   Subscribing to events stream...")
        
        def message_handler(message):
            print(f"   📨 Received: {message.value} (key: {message.key}, offset: {message.offset})")
        
        await client.stream.subscribe("events", message_handler)

        # Get historical messages
        messages = await client.stream.get_messages("events", partition=0, offset=0, limit=3)
        print(f"   Retrieved {len(messages)} historical messages")

    except Exception as error:
        print(f"❌ Error: {error}")
    finally:
        # Disconnect
        await client.disconnect()
        print("\n🎉 Example completed!")
        print("💡 Note: This is a demo implementation. Full functionality requires gRPC integration.")


if __name__ == "__main__":
    asyncio.run(main())
