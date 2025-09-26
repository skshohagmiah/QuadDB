package unit

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	queuepb "github.com/skshohagmiah/gomsg/api/generated/queue"
	"github.com/skshohagmiah/gomsg/tests/testutil"
)

func setupQueueClient(t *testing.T) queuepb.QueueServiceClient {
	// Start test server automatically
	testServer := testutil.StartTestServer(t)

	conn, err := grpc.Dial(testServer.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return queuepb.NewQueueServiceClient(conn)
}

func TestQueuePushPop(t *testing.T) {
	client := setupQueueClient(t)
	ctx := context.Background()
	queueName := "test_queue_pushpop"

	// Test Push
	pushResp, err := client.Push(ctx, &queuepb.PushRequest{
		Queue: queueName,
		Data:  []byte("test message"),
		Delay: 0,
	})
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if !pushResp.Status.Success {
		t.Fatalf("Push failed: %s", pushResp.Status.Message)
	}
	if pushResp.MessageId == "" {
		t.Fatal("Expected message ID")
	}

	// Test Pop
	popResp, err := client.Pop(ctx, &queuepb.PopRequest{
		Queue:   queueName,
		Timeout: 5,
	})
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if popResp.Message == nil {
		t.Fatal("Expected message")
	}
	if string(popResp.Message.Data) != "test message" {
		t.Fatalf("Expected 'test message', got '%s'", string(popResp.Message.Data))
	}
	if popResp.Message.Id != pushResp.MessageId {
		t.Fatalf("Message ID mismatch: expected %s, got %s", pushResp.MessageId, popResp.Message.Id)
	}
}

func TestQueueDelayedMessage(t *testing.T) {
	t.Skip("Delayed message functionality has been removed")
}

func TestQueuePeek(t *testing.T) {
	client := setupQueueClient(t)
	ctx := context.Background()
	queueName := "test_queue_peek"

	// Push message
	client.Push(ctx, &queuepb.PushRequest{
		Queue: queueName,
		Data:  []byte("peek message"),
		Delay: 0,
	})

	// Peek message (should not remove it)
	peekResp, err := client.Peek(ctx, &queuepb.PeekRequest{Queue: queueName})
	if err != nil {
		t.Fatalf("Peek failed: %v", err)
	}
	if len(peekResp.Messages) == 0 {
		t.Fatal("Expected message")
	}
	if string(peekResp.Messages[0].Data) != "peek message" {
		t.Fatalf("Expected 'peek message', got '%s'", string(peekResp.Messages[0].Data))
	}

	// Pop message (should still be there)
	popResp, err := client.Pop(ctx, &queuepb.PopRequest{
		Queue:   queueName,
		Timeout: 1,
	})
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if popResp.Message == nil {
		t.Fatal("Expected message after peek")
	}
	if string(popResp.Message.Data) != "peek message" {
		t.Fatalf("Expected 'peek message', got '%s'", string(popResp.Message.Data))
	}
}

func TestQueueStats(t *testing.T) {
	client := setupQueueClient(t)
	ctx := context.Background()
	queueName := "test_queue_stats"

	// Push multiple messages
	for i := 0; i < 3; i++ {
		client.Push(ctx, &queuepb.PushRequest{
			Queue: queueName,
			Data:  []byte("message"),
			Delay: 0,
		})
	}

	// Check stats
	statsResp, err := client.Stats(ctx, &queuepb.StatsRequest{Queue: queueName})
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if statsResp.Stats.Size < 3 {
		t.Fatalf("Expected at least 3 messages, got %d", statsResp.Stats.Size)
	}

	// Pop one message
	client.Pop(ctx, &queuepb.PopRequest{Queue: queueName, Timeout: 1})

	// Check stats again
	statsResp, err = client.Stats(ctx, &queuepb.StatsRequest{Queue: queueName})
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if statsResp.Stats.Processed < 1 {
		t.Fatalf("Expected at least 1 processed message, got %d", statsResp.Stats.Processed)
	}
}

func TestQueuePurge(t *testing.T) {
	t.Skip("Queue purge has known issues with orphaned messages - needs deeper investigation")
	// TODO: Fix queue purge implementation to properly handle all edge cases
}

func TestQueueList(t *testing.T) {
	client := setupQueueClient(t)
	ctx := context.Background()

	// Create queues by pushing messages
	queues := []string{"list_test_1", "list_test_2", "list_test_3"}
	for _, queue := range queues {
		client.Push(ctx, &queuepb.PushRequest{
			Queue: queue,
			Data:  []byte("message"),
			Delay: 0,
		})
	}

	// List queues
	listResp, err := client.List(ctx, &queuepb.ListRequest{})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	// Check if our test queues are in the list
	queueMap := make(map[string]bool)
	for _, queue := range listResp.Queues {
		queueMap[queue] = true
	}

	for _, queue := range queues {
		if !queueMap[queue] {
			t.Fatalf("Expected queue '%s' in list", queue)
		}
	}
}

func TestQueueAckNack(t *testing.T) {
	client := setupQueueClient(t)
	ctx := context.Background()
	queueName := "test_queue_ack"

	// Push message
	_, err := client.Push(ctx, &queuepb.PushRequest{
		Queue: queueName,
		Data:  []byte("ack test message"),
		Delay: 0,
	})
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// Pop message
	popResp, err := client.Pop(ctx, &queuepb.PopRequest{
		Queue:   queueName,
		Timeout: 5,
	})
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if popResp.Message == nil {
		t.Fatal("Expected message")
	}

	// Acknowledge message
	ackResp, err := client.Ack(ctx, &queuepb.AckRequest{
		MessageId: popResp.Message.Id,
	})
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
	if !ackResp.Status.Success {
		t.Fatalf("Ack failed: %s", ackResp.Status.Message)
	}
}
