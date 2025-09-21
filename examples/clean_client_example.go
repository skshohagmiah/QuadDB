package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"gomsg/clients/go"
)

// This example shows the clean API design that matches the README
func main() {
	// Simple connection - exactly as promised in README
	client, err := client.Connect("localhost:9000", "localhost:9001", "localhost:9002")
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Key/Value operations - exactly as shown in README
	fmt.Println("=== Key/Value Operations ===")
	
	// Basic operations
	err = client.KV.Set(ctx, "user:123", "john_doe")
	if err != nil {
		log.Printf("Set failed: %v", err)
	}

	err = client.KV.SetTTL(ctx, "session:abc", "active", time.Hour)
	if err != nil {
		log.Printf("SetTTL failed: %v", err)
	}

	value, err := client.KV.Get(ctx, "user:123")
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else {
		fmt.Printf("user:123 = %s\n", value)
	}

	exists, err := client.KV.Exists(ctx, "user:123")
	if err != nil {
		log.Printf("Exists failed: %v", err)
	} else {
		fmt.Printf("user:123 exists: %v\n", exists)
	}

	// Atomic operations
	newValue, err := client.KV.Incr(ctx, "counter")
	if err != nil {
		log.Printf("Incr failed: %v", err)
	} else {
		fmt.Printf("counter incremented to: %d\n", newValue)
	}

	newValue, err = client.KV.IncrBy(ctx, "counter", 10)
	if err != nil {
		log.Printf("IncrBy failed: %v", err)
	} else {
		fmt.Printf("counter incremented by 10 to: %d\n", newValue)
	}

	// Batch operations
	values, err := client.KV.MGet(ctx, []string{"user:123", "session:abc", "counter"})
	if err != nil {
		log.Printf("MGet failed: %v", err)
	} else {
		fmt.Printf("Batch get results: %+v\n", values)
	}

	err = client.KV.MSet(ctx, map[string]string{
		"user:456": "jane_doe",
		"user:789": "bob_smith",
	})
	if err != nil {
		log.Printf("MSet failed: %v", err)
	}

	// Pattern matching
	keys, err := client.KV.Keys(ctx, "user:*")
	if err != nil {
		log.Printf("Keys failed: %v", err)
	} else {
		fmt.Printf("Keys matching 'user:*': %v\n", keys)
	}

	// Expiration
	err = client.KV.Expire(ctx, "user:456", time.Minute*30)
	if err != nil {
		log.Printf("Expire failed: %v", err)
	}

	ttl, err := client.KV.TTL(ctx, "user:456")
	if err != nil {
		log.Printf("TTL failed: %v", err)
	} else {
		fmt.Printf("user:456 TTL: %v\n", ttl)
	}

	// Queue operations - exactly as shown in README
	fmt.Println("\n=== Queue Operations ===")

	// Basic operations
	msgID, err := client.Queue.Push(ctx, "jobs", "process_payment")
	if err != nil {
		log.Printf("Push failed: %v", err)
	} else {
		fmt.Printf("Pushed message with ID: %s\n", msgID)
	}

	msgID, err = client.Queue.PushDelayed(ctx, "jobs", "send_reminder", time.Hour*2)
	if err != nil {
		log.Printf("PushDelayed failed: %v", err)
	} else {
		fmt.Printf("Pushed delayed message with ID: %s\n", msgID)
	}

	msg, err := client.Queue.Pop(ctx, "jobs")
	if err != nil {
		log.Printf("Pop failed: %v", err)
	} else if msg != nil {
		fmt.Printf("Popped message: ID=%s, Data=%s\n", msg.ID, string(msg.Data))
		
		// Acknowledge the message
		err = client.Queue.Ack(ctx, msg.ID)
		if err != nil {
			log.Printf("Ack failed: %v", err)
		}
	}

	// Message handling with timeout
	msg, err = client.Queue.PopTimeout(ctx, "jobs", time.Second*30)
	if err != nil {
		log.Printf("PopTimeout failed: %v", err)
	} else if msg != nil {
		fmt.Printf("Popped with timeout: ID=%s, Data=%s\n", msg.ID, string(msg.Data))
		
		// Negative acknowledge (requeue for retry)
		err = client.Queue.Nack(ctx, msg.ID)
		if err != nil {
			log.Printf("Nack failed: %v", err)
		}
	}

	// Look without removing
	messages, err := client.Queue.Peek(ctx, "jobs")
	if err != nil {
		log.Printf("Peek failed: %v", err)
	} else {
		fmt.Printf("Peeked %d messages\n", len(messages))
	}

	// Queue management
	stats, err := client.Queue.Stats(ctx, "jobs")
	if err != nil {
		log.Printf("Stats failed: %v", err)
	} else {
		fmt.Printf("Queue stats: Size=%d, Consumers=%d, Pending=%d\n", 
			stats.Size, stats.Consumers, stats.Pending)
	}

	queues, err := client.Queue.List(ctx)
	if err != nil {
		log.Printf("List failed: %v", err)
	} else {
		fmt.Printf("All queues: %v\n", queues)
	}

	// Cluster operations - exactly as shown in README
	fmt.Println("\n=== Cluster Operations ===")

	// Node management
	nodes, err := client.Cluster.Nodes(ctx)
	if err != nil {
		log.Printf("Nodes failed: %v", err)
	} else {
		fmt.Printf("Cluster nodes:\n")
		for _, node := range nodes {
			fmt.Printf("  - %s (%s) - %s\n", node.ID, node.Address, node.State)
		}
	}

	status, err := client.Cluster.Status(ctx)
	if err != nil {
		log.Printf("Status failed: %v", err)
	} else {
		fmt.Printf("Cluster status: %s\n", status)
	}

	leader, err := client.Cluster.Leader(ctx)
	if err != nil {
		log.Printf("Leader failed: %v", err)
	} else {
		fmt.Printf("Current leader: %s (%s)\n", leader.ID, leader.Address)
	}

	fmt.Println("\n=== All operations completed! ===")
	fmt.Println("This demonstrates the clean API that matches your README design.")
	fmt.Println("Behind the scenes, the client automatically handles:")
	fmt.Println("  - Clustering and node discovery")
	fmt.Println("  - Partitioning and key routing")
	fmt.Println("  - Replication and failover")
	fmt.Println("  - Load balancing")
}
