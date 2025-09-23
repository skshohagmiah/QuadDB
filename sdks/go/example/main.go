package main

import (
	"context"
	"fmt"
	"log"
	"time"

	fluxdl "github.com/skshohagmiah/fluxdl-go-sdk"
)

func main() {
	// Create client configuration
	config := &fluxdl.Config{
		Address: "localhost:9000", // fluxdl Docker container
		Timeout: 10 * time.Second,
	}

	// Connect to fluxdl
	client, err := fluxdl.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Test connection
	fmt.Println("Testing connection to fluxdl...")
	if err := client.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping server: %v", err)
	}
	fmt.Println("✅ Connected to fluxdl successfully!")

	// Key-Value operations
	fmt.Println("\n🔑 Testing Key-Value operations...")

	// Set a key
	result, err := client.KV.Set(ctx, "user:1", "John Doe")
	if err != nil {
		fmt.Printf("❌ Set failed: %v\n", err)
	} else {
		fmt.Printf("✅ Set result: %s\n", result)
	}

	// Get the key
	value, err := client.KV.Get(ctx, "user:1")
	if err != nil {
		fmt.Printf("❌ Get failed: %v\n", err)
	} else {
		fmt.Printf("✅ Get result: %s\n", value)
	}

	// Queue operations
	fmt.Println("\n📬 Testing Queue operations...")

	// Push to queue
	err = client.Queue.Push(ctx, "notifications", "Welcome message")
	if err != nil {
		fmt.Printf("❌ Queue push failed: %v\n", err)
	} else {
		fmt.Println("✅ Message pushed to queue")
	}

	// Pop from queue
	message, err := client.Queue.Pop(ctx, "notifications")
	if err != nil {
		fmt.Printf("❌ Queue pop failed: %v\n", err)
	} else {
		fmt.Printf("✅ Popped message: %s\n", message)
	}

	// Stream operations
	fmt.Println("\n🌊 Testing Stream operations...")

	// Create stream
	err = client.Stream.CreateStream(ctx, "events", 3)
	if err != nil {
		fmt.Printf("❌ Create stream failed: %v\n", err)
	} else {
		fmt.Println("✅ Stream created")
	}

	// Publish to stream
	err = client.Stream.Publish(ctx, "events", "User logged in")
	if err != nil {
		fmt.Printf("❌ Stream publish failed: %v\n", err)
	} else {
		fmt.Println("✅ Message published to stream")
	}

	fmt.Println("\n🎉 fluxdl Go SDK example completed!")
	fmt.Println("Note: Full implementation requires protobuf client generation")
}
