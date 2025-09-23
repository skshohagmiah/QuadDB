package performance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvpb "github.com/skshohagmiah/fluxdl/api/generated/kv"
	queuepb "github.com/skshohagmiah/fluxdl/api/generated/queue"
	streampb "github.com/skshohagmiah/fluxdl/api/generated/stream"
)

func setupBenchmarkClients(b *testing.B) (kvpb.KVServiceClient, queuepb.QueueServiceClient, streampb.StreamServiceClient) {
	conn, err := grpc.Dial("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("Failed to connect to server: %v", err)
	}
	b.Cleanup(func() { conn.Close() })

	return kvpb.NewKVServiceClient(conn),
		queuepb.NewQueueServiceClient(conn),
		streampb.NewStreamServiceClient(conn)
}

func BenchmarkKVSet(b *testing.B) {
	kvClient, _, _ := setupBenchmarkClients(b)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench_set_%d", i)
			value := []byte(fmt.Sprintf("value_%d", i))

			_, err := kvClient.Set(ctx, &kvpb.SetRequest{
				Key:   key,
				Value: value,
				Ttl:   300,
			})
			if err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkKVGet(b *testing.B) {
	kvClient, _, _ := setupBenchmarkClients(b)
	ctx := context.Background()

	// Pre-populate keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		kvClient.Set(ctx, &kvpb.SetRequest{
			Key:   key,
			Value: value,
			Ttl:   300,
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench_get_%d", i%numKeys)

			_, err := kvClient.Get(ctx, &kvpb.GetRequest{Key: key})
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkKVIncrement(b *testing.B) {
	kvClient, _, _ := setupBenchmarkClients(b)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench_incr_%d", i%100) // Use 100 different counters

			_, err := kvClient.Incr(ctx, &kvpb.IncrRequest{
				Key: key,
				By:  1,
			})
			if err != nil {
				b.Fatalf("Increment failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkQueuePush(b *testing.B) {
	_, queueClient, _ := setupBenchmarkClients(b)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			queue := fmt.Sprintf("bench_queue_%d", i%10) // Use 10 different queues
			data := []byte(fmt.Sprintf("message_%d", i))

			_, err := queueClient.Push(ctx, &queuepb.PushRequest{
				Queue: queue,
				Data:  data,
				Delay: 0,
			})
			if err != nil {
				b.Fatalf("Push failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkQueuePop(b *testing.B) {
	_, queueClient, _ := setupBenchmarkClients(b)
	ctx := context.Background()

	// Pre-populate queue
	queueName := "bench_pop_queue"
	for i := 0; i < b.N*2; i++ { // Populate more than we'll consume
		queueClient.Push(ctx, &queuepb.PushRequest{
			Queue: queueName,
			Data:  []byte(fmt.Sprintf("message_%d", i)),
			Delay: 0,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := queueClient.Pop(ctx, &queuepb.PopRequest{
			Queue:   queueName,
			Timeout: 1,
		})
		if err != nil {
			b.Fatalf("Pop failed: %v", err)
		}
	}
}

func BenchmarkStreamPublish(b *testing.B) {
	_, _, streamClient := setupBenchmarkClients(b)
	ctx := context.Background()

	// Create topic
	topicName := "bench_publish_topic"
	streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 4,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			partitionKey := fmt.Sprintf("key_%d", i%100)
			data := []byte(fmt.Sprintf(`{"event":"test","id":%d}`, i))

			_, err := streamClient.Publish(ctx, &streampb.PublishRequest{
				Topic:        topicName,
				PartitionKey: partitionKey,
				Data:         data,
			})
			if err != nil {
				b.Fatalf("Publish failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkStreamRead(b *testing.B) {
	_, _, streamClient := setupBenchmarkClients(b)
	ctx := context.Background()

	// Create topic and populate with messages
	topicName := "bench_read_topic"
	streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})

	// Pre-populate messages
	for i := 0; i < 1000; i++ {
		streamClient.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: "test",
			Data:         []byte(fmt.Sprintf(`{"message":%d}`, i)),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i % 900) // Read from different offsets
		_, err := streamClient.Read(ctx, &streampb.ReadRequest{
			Topic:      topicName,
			Partition:  0,
			FromOffset: offset,
			Limit:      10,
		})
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}

func BenchmarkMixedWorkload(b *testing.B) {
	kvClient, queueClient, streamClient := setupBenchmarkClients(b)
	ctx := context.Background()

	// Setup
	streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       "mixed_workload",
		Partitions: 2,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Mix of operations (33% each)
			switch i % 3 {
			case 0: // KV operation
				key := fmt.Sprintf("mixed_key_%d", i)
				_, err := kvClient.Set(ctx, &kvpb.SetRequest{
					Key:   key,
					Value: []byte(fmt.Sprintf("value_%d", i)),
					Ttl:   300,
				})
				if err != nil {
					b.Fatalf("KV Set failed: %v", err)
				}

			case 1: // Queue operation
				_, err := queueClient.Push(ctx, &queuepb.PushRequest{
					Queue: "mixed_queue",
					Data:  []byte(fmt.Sprintf("job_%d", i)),
					Delay: 0,
				})
				if err != nil {
					b.Fatalf("Queue Push failed: %v", err)
				}

			case 2: // Stream operation
				_, err := streamClient.Publish(ctx, &streampb.PublishRequest{
					Topic:        "mixed_workload",
					PartitionKey: fmt.Sprintf("key_%d", i%10),
					Data:         []byte(fmt.Sprintf(`{"event":%d}`, i)),
				})
				if err != nil {
					b.Fatalf("Stream Publish failed: %v", err)
				}
			}
			i++
		}
	})
}

// Throughput test - measures operations per second
func BenchmarkThroughput(b *testing.B) {
	kvClient, queueClient, streamClient := setupBenchmarkClients(b)
	ctx := context.Background()

	// Setup
	streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       "throughput_test",
		Partitions: 4,
	})

	const duration = 10 * time.Second
	const numWorkers = 50

	var (
		kvOps     int64
		queueOps  int64
		streamOps int64
		wg        sync.WaitGroup
		mu        sync.Mutex
	)

	start := time.Now()
	deadline := start.Add(duration)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(3) // 3 types of operations

		// KV worker
		go func(workerID int) {
			defer wg.Done()
			ops := 0
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("throughput_kv_%d_%d", workerID, ops)
				_, err := kvClient.Set(ctx, &kvpb.SetRequest{
					Key:   key,
					Value: []byte("value"),
					Ttl:   300,
				})
				if err == nil {
					ops++
				}
			}
			mu.Lock()
			kvOps += int64(ops)
			mu.Unlock()
		}(i)

		// Queue worker
		go func(workerID int) {
			defer wg.Done()
			ops := 0
			for time.Now().Before(deadline) {
				_, err := queueClient.Push(ctx, &queuepb.PushRequest{
					Queue: fmt.Sprintf("throughput_queue_%d", workerID%5),
					Data:  []byte(fmt.Sprintf("message_%d", ops)),
					Delay: 0,
				})
				if err == nil {
					ops++
				}
			}
			mu.Lock()
			queueOps += int64(ops)
			mu.Unlock()
		}(i)

		// Stream worker
		go func(workerID int) {
			defer wg.Done()
			ops := 0
			for time.Now().Before(deadline) {
				_, err := streamClient.Publish(ctx, &streampb.PublishRequest{
					Topic:        "throughput_test",
					PartitionKey: fmt.Sprintf("worker_%d", workerID),
					Data:         []byte(fmt.Sprintf(`{"op":%d}`, ops)),
				})
				if err == nil {
					ops++
				}
			}
			mu.Lock()
			streamOps += int64(ops)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := kvOps + queueOps + streamOps
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	b.Logf("Throughput Results:")
	b.Logf("  Duration: %v", elapsed)
	b.Logf("  KV Operations: %d (%.0f ops/sec)", kvOps, float64(kvOps)/elapsed.Seconds())
	b.Logf("  Queue Operations: %d (%.0f ops/sec)", queueOps, float64(queueOps)/elapsed.Seconds())
	b.Logf("  Stream Operations: %d (%.0f ops/sec)", streamOps, float64(streamOps)/elapsed.Seconds())
	b.Logf("  Total Operations: %d", totalOps)
	b.Logf("  Overall Throughput: %.0f ops/sec", opsPerSec)

	// Set a custom metric for the benchmark
	b.ReportMetric(opsPerSec, "ops/sec")
}

// Latency test - measures response times
func BenchmarkLatency(b *testing.B) {
	kvClient, queueClient, streamClient := setupBenchmarkClients(b)
	ctx := context.Background()

	// Setup
	streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       "latency_test",
		Partitions: 1,
	})

	const numSamples = 1000
	kvLatencies := make([]time.Duration, numSamples)
	queueLatencies := make([]time.Duration, numSamples)
	streamLatencies := make([]time.Duration, numSamples)

	// Measure KV latencies
	for i := 0; i < numSamples; i++ {
		start := time.Now()
		_, err := kvClient.Set(ctx, &kvpb.SetRequest{
			Key:   fmt.Sprintf("latency_test_%d", i),
			Value: []byte("test_value"),
			Ttl:   300,
		})
		if err != nil {
			b.Fatalf("KV Set failed: %v", err)
		}
		kvLatencies[i] = time.Since(start)
	}

	// Measure Queue latencies
	for i := 0; i < numSamples; i++ {
		start := time.Now()
		_, err := queueClient.Push(ctx, &queuepb.PushRequest{
			Queue: "latency_queue",
			Data:  []byte(fmt.Sprintf("message_%d", i)),
			Delay: 0,
		})
		if err != nil {
			b.Fatalf("Queue Push failed: %v", err)
		}
		queueLatencies[i] = time.Since(start)
	}

	// Measure Stream latencies
	for i := 0; i < numSamples; i++ {
		start := time.Now()
		_, err := streamClient.Publish(ctx, &streampb.PublishRequest{
			Topic:        "latency_test",
			PartitionKey: "test",
			Data:         []byte(fmt.Sprintf(`{"test":%d}`, i)),
		})
		if err != nil {
			b.Fatalf("Stream Publish failed: %v", err)
		}
		streamLatencies[i] = time.Since(start)
	}

	// Calculate statistics
	kvAvg, kvP95, kvP99 := calculateLatencyStats(kvLatencies)
	queueAvg, queueP95, queueP99 := calculateLatencyStats(queueLatencies)
	streamAvg, streamP95, streamP99 := calculateLatencyStats(streamLatencies)

	b.Logf("Latency Results (based on %d samples):", numSamples)
	b.Logf("  KV Operations:")
	b.Logf("    Average: %v", kvAvg)
	b.Logf("    P95: %v", kvP95)
	b.Logf("    P99: %v", kvP99)
	b.Logf("  Queue Operations:")
	b.Logf("    Average: %v", queueAvg)
	b.Logf("    P95: %v", queueP95)
	b.Logf("    P99: %v", queueP99)
	b.Logf("  Stream Operations:")
	b.Logf("    Average: %v", streamAvg)
	b.Logf("    P95: %v", streamP95)
	b.Logf("    P99: %v", streamP99)

	// Report metrics
	b.ReportMetric(float64(kvAvg.Nanoseconds()), "kv_avg_ns")
	b.ReportMetric(float64(queueAvg.Nanoseconds()), "queue_avg_ns")
	b.ReportMetric(float64(streamAvg.Nanoseconds()), "stream_avg_ns")
}

func calculateLatencyStats(latencies []time.Duration) (avg, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}

	// Sort latencies
	for i := 0; i < len(latencies)-1; i++ {
		for j := i + 1; j < len(latencies); j++ {
			if latencies[i] > latencies[j] {
				latencies[i], latencies[j] = latencies[j], latencies[i]
			}
		}
	}

	// Calculate average
	var total time.Duration
	for _, lat := range latencies {
		total += lat
	}
	avg = total / time.Duration(len(latencies))

	// Calculate percentiles
	p95Index := int(float64(len(latencies)) * 0.95)
	p99Index := int(float64(len(latencies)) * 0.99)

	if p95Index >= len(latencies) {
		p95Index = len(latencies) - 1
	}
	if p99Index >= len(latencies) {
		p99Index = len(latencies) - 1
	}

	p95 = latencies[p95Index]
	p99 = latencies[p99Index]

	return avg, p95, p99
}
