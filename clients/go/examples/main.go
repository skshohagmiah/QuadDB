package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gomsg/client-go/gomsg"
)

func main() {
	ctx := context.Background()
	
	// Connect to GoMsg cluster (Docker containers)
	config := &gomsg.Config{
		Nodes: []string{
			"localhost:8080", // GoMsg node 1
			"localhost:8081", // GoMsg node 2
			"localhost:8082", // GoMsg node 3
		},
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 30 * time.Second,
		RetryAttempts:  3,
	}
	
	client, err := gomsg.NewClient(ctx, config)
	if err != nil {
		log.Fatalf("Failed to connect to GoMsg: %v", err)
	}
	defer client.Close()
	
	// Test KV operations
	fmt.Println("=== KV Operations ===")
	
	// KV Operations
	err = client.KV.Set(ctx, "user:123", []byte("John Doe"), 5*time.Minute)
	if err != nil {
		log.Printf("Set failed: %v", err)
	} else {
		fmt.Println("✅ Set user:123 = 'John Doe'")
	}
	
	value, found, err := client.KV.Get(ctx, "user:123")
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else if found {
		fmt.Printf("✅ Get user:123 = '%s'\n", string(value))
	} else {
		fmt.Println("❌ Key not found")
	}
	
	count, err := client.KV.Increment(ctx, "page_views", 1)
	if err != nil {
		log.Printf("Increment failed: %v", err)
	} else {
		fmt.Printf("✅ Incremented page_views to %d\n", count)
	}
	
	// Test Queue operations
	fmt.Println("\n=== Queue Operations ===")
	
	// Push a message
	messageID, err := client.Queue.Push(ctx, "notifications", []byte("Welcome email"), 0)
	if err != nil {
		log.Printf("Queue push failed: %v", err)
	} else {
		fmt.Printf("✅ Pushed message to notifications queue: %s\n", messageID)
	}
	
	// Pop a message
	message, err := client.Queue.Pop(ctx, "notifications", 5*time.Second)
	if err != nil {
		log.Printf("Queue pop failed: %v", err)
	} else if message != nil {
		fmt.Printf("✅ Popped message: %s (data: %s)\n", message.ID, string(message.Data))
	} else {
		fmt.Println("❌ No messages in queue")
	}
	
	// Check queue size
	size, err := client.Queue.Size(ctx, "notifications")
	if err != nil {
		log.Printf("Queue size failed: %v", err)
	} else {
		fmt.Printf("✅ Queue size: %d\n", size)
	}
	
	// Test Stream operations
	fmt.Println("\n=== Stream Operations ===")
	
	// Create a topic
	err = client.Stream.CreateTopic(ctx, "user-events", 4)
	if err != nil {
		log.Printf("Create topic failed: %v", err)
	} else {
		fmt.Println("✅ Created topic 'user-events' with 4 partitions")
	}
	
	// Publish a message
	headers := map[string]string{"source": "web", "version": "1.0"}
	streamMsg, err := client.Stream.Publish(ctx, "user-events", "user:123", []byte(`{"action":"login","user":"123"}`), headers)
	if err != nil {
		log.Printf("Stream publish failed: %v", err)
	} else {
		fmt.Printf("✅ Published to stream: offset=%d, partition=%d\n", streamMsg.Offset, streamMsg.Partition)
	}
	
	// Read messages
	messages, err := client.Stream.Read(ctx, "user-events", 0, 0, 10)
	if err != nil {
		log.Printf("Stream read failed: %v", err)
	} else {
		fmt.Printf("✅ Read %d messages from stream\n", len(messages))
		for _, msg := range messages {
			fmt.Printf("   Message: offset=%d, data=%s\n", msg.Offset, string(msg.Data))
		}
	}
	
	// Test real-time streaming
	fmt.Println("\n=== Real-time Streaming ===")
	
	// Start a consumer group subscription
	msgChan, errChan, err := client.Stream.SubscribeGroup(ctx, "user-events", "analytics-group", "consumer-1")
	if err != nil {
		log.Printf("Stream subscribe failed: %v", err)
	} else {
		fmt.Println("✅ Started real-time stream subscription")
		
		// Listen for messages for 10 seconds
		timeout := time.After(10 * time.Second)
		
	streamLoop:
		for {
			select {
			case msg := <-msgChan:
				if msg != nil {
					fmt.Printf("📨 Received real-time message: %s\n", string(msg.Data))
				}
			case err := <-errChan:
				if err != nil {
					log.Printf("Stream error: %v", err)
					break streamLoop
				}
			case <-timeout:
				fmt.Println("⏰ Stream subscription timeout")
				break streamLoop
			}
		}
	}
	
	// Check cluster health
	fmt.Println("\n=== Cluster Health ===")
	health, err := client.Health(ctx)
	if err != nil {
		log.Printf("Health check failed: %v", err)
	} else {
		for node, healthy := range health {
			status := "❌ Unhealthy"
			if healthy {
				status = "✅ Healthy"
			}
			fmt.Printf("Node %s: %s\n", node, status)
		}
	}
	
	fmt.Println("\n🎉 GoMsg client demo completed!")
}
