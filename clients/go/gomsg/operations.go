// This file has been refactored into service-specific clients:
// - kv_client.go: KV operations
// - queue_client.go: Queue operations  
// - stream_client.go: Stream operations
//
// Use the new API structure:
// client.KV.Set(ctx, key, value, ttl)
// client.Queue.Push(ctx, queue, data, delay)
// client.Stream.Publish(ctx, topic, partitionKey, data, headers)
