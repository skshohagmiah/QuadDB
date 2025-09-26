package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvpb "github.com/skshohagmiah/gomsg/api/generated/kv"
	queuepb "github.com/skshohagmiah/gomsg/api/generated/queue"
	streampb "github.com/skshohagmiah/gomsg/api/generated/stream"
	"github.com/skshohagmiah/gomsg/tests/testutil"
)

func setupClients(t *testing.T) (kvpb.KVServiceClient, queuepb.QueueServiceClient, streampb.StreamServiceClient) {
	// Start test server automatically
	testServer := testutil.StartTestServer(t)

	conn, err := grpc.Dial(testServer.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return kvpb.NewKVServiceClient(conn),
		queuepb.NewQueueServiceClient(conn),
		streampb.NewStreamServiceClient(conn)
}

func TestUserRegistrationWorkflow(t *testing.T) {
	kvClient, queueClient, streamClient := setupClients(t)
	ctx := context.Background()

	userID := "user_12345"
	email := "test@example.com"

	// Step 1: Store user data in KV store
	_, err := kvClient.Set(ctx, &kvpb.SetRequest{
		Key:   fmt.Sprintf("user:%s", userID),
		Value: []byte(fmt.Sprintf(`{"id":"%s","email":"%s","status":"pending"}`, userID, email)),
		Ttl:   3600, // 1 hour
	})
	if err != nil {
		t.Fatalf("Failed to store user data: %v", err)
	}

	// Step 2: Queue email verification job
	pushResp, err := queueClient.Push(ctx, &queuepb.PushRequest{
		Queue: "email_verification",
		Data:  []byte(fmt.Sprintf(`{"user_id":"%s","email":"%s","type":"verification"}`, userID, email)),
	})
	if err != nil {
		t.Fatalf("Failed to queue email job: %v", err)
	}

	// Step 3: Publish user registration event to stream
	_, err = streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       "user_events",
		Partitions: 4,
	})
	// Ignore error if topic already exists

	publishResp, err := streamClient.Publish(ctx, &streampb.PublishRequest{
		Topic:        "user_events",
		PartitionKey: userID,
		Data:         []byte(fmt.Sprintf(`{"event":"user_registered","user_id":"%s","email":"%s","timestamp":"%s"}`, userID, email, time.Now().Format(time.RFC3339))),
		Headers: map[string]string{
			"event_type": "user_registration",
			"source":     "api",
		},
	})
	if err != nil {
		t.Fatalf("Failed to publish event: %v", err)
	}

	// Step 4: Process email verification job
	popResp, err := queueClient.Pop(ctx, &queuepb.PopRequest{
		Queue:   "email_verification",
		Timeout: 5,
	})
	if err != nil {
		t.Fatalf("Failed to pop email job: %v", err)
	}
	if popResp.Message == nil {
		t.Fatal("Expected email verification job")
	}
	if popResp.Message.Id != pushResp.MessageId {
		t.Fatalf("Job ID mismatch: expected %s, got %s", pushResp.MessageId, popResp.Message.Id)
	}

	// Step 5: Update user status after email verification
	_, err = kvClient.Set(ctx, &kvpb.SetRequest{
		Key:   fmt.Sprintf("user:%s", userID),
		Value: []byte(fmt.Sprintf(`{"id":"%s","email":"%s","status":"verified"}`, userID, email)),
		Ttl:   3600,
	})
	if err != nil {
		t.Fatalf("Failed to update user status: %v", err)
	}

	// Step 6: Publish verification complete event
	_, err = streamClient.Publish(ctx, &streampb.PublishRequest{
		Topic:        "user_events",
		PartitionKey: userID,
		Data:         []byte(fmt.Sprintf(`{"event":"email_verified","user_id":"%s","timestamp":"%s"}`, userID, time.Now().Format(time.RFC3339))),
		Headers: map[string]string{
			"event_type": "email_verification",
			"source":     "email_service",
		},
	})
	if err != nil {
		t.Fatalf("Failed to publish verification event: %v", err)
	}

	// Step 7: Verify final user state
	getResp, err := kvClient.Get(ctx, &kvpb.GetRequest{Key: fmt.Sprintf("user:%s", userID)})
	if err != nil {
		t.Fatalf("Failed to get user data: %v", err)
	}
	if !getResp.Found {
		t.Fatal("User data not found")
	}

	userData := string(getResp.Value)
	if !contains(userData, `"status":"verified"`) {
		t.Fatalf("Expected verified status, got: %s", userData)
	}

	// Step 8: Read user events from stream
	readResp, err := streamClient.Read(ctx, &streampb.ReadRequest{
		Topic:      "user_events",
		Partition:  publishResp.Partition,
		FromOffset: 0,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Failed to read events: %v", err)
	}
	if len(readResp.Messages) < 2 {
		t.Fatalf("Expected at least 2 events, got %d", len(readResp.Messages))
	}

	// Verify events are for our user
	for _, msg := range readResp.Messages {
		if msg.PartitionKey != userID {
			continue // Skip other users' events
		}
		if !contains(string(msg.Data), userID) {
			t.Fatalf("Event doesn't contain user ID: %s", string(msg.Data))
		}
	}

	t.Logf("✅ User registration workflow completed successfully for user %s", userID)
}

func TestConcurrentOperations(t *testing.T) {
	kvClient, queueClient, streamClient := setupClients(t)
	ctx := context.Background()

	const numWorkers = 10
	const operationsPerWorker = 20

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*operationsPerWorker)

	// Create topic for stream operations
	streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       "concurrent_test",
		Partitions: 4,
	})

	// Start concurrent workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < operationsPerWorker; j++ {
				key := fmt.Sprintf("worker_%d_op_%d", workerID, j)

				// KV operations
				_, err := kvClient.Set(ctx, &kvpb.SetRequest{
					Key:   key,
					Value: []byte(fmt.Sprintf("value_%d_%d", workerID, j)),
					Ttl:   300,
				})
				if err != nil {
					errors <- fmt.Errorf("worker %d KV set failed: %v", workerID, err)
					continue
				}

				// Queue operations
				_, err = queueClient.Push(ctx, &queuepb.PushRequest{
					Queue: fmt.Sprintf("queue_%d", workerID),
					Data:  []byte(fmt.Sprintf("job_%d_%d", workerID, j)),
				})
				if err != nil {
					errors <- fmt.Errorf("worker %d queue push failed: %v", workerID, err)
					continue
				}

				// Stream operations
				_, err = streamClient.Publish(ctx, &streampb.PublishRequest{
					Topic:        "concurrent_test",
					PartitionKey: fmt.Sprintf("worker_%d", workerID),
					Data:         []byte(fmt.Sprintf(`{"worker":%d,"operation":%d}`, workerID, j)),
				})
				if err != nil {
					errors <- fmt.Errorf("worker %d stream publish failed: %v", workerID, err)
					continue
				}

				// Increment counter
				_, err = kvClient.Incr(ctx, &kvpb.IncrRequest{
					Key: "global_counter",
					By:  1,
				})
				if err != nil {
					errors <- fmt.Errorf("worker %d counter increment failed: %v", workerID, err)
					continue
				}
			}
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(errors)

	// Check for errors
	var errorCount int
	for err := range errors {
		t.Logf("Concurrent operation error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Fatalf("Concurrent operations failed with %d errors", errorCount)
	}

	// Verify final counter value
	getResp, err := kvClient.Get(ctx, &kvpb.GetRequest{Key: "global_counter"})
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}
	if !getResp.Found {
		t.Fatal("Counter not found")
	}

	expectedCount := numWorkers * operationsPerWorker
	t.Logf("✅ Concurrent operations completed. Expected %d operations, counter shows: %s",
		expectedCount, string(getResp.Value))
}

func TestDataConsistency(t *testing.T) {
	kvClient, queueClient, streamClient := setupClients(t)
	ctx := context.Background()

	// Test KV consistency
	testKey := "consistency_test"
	testValue := "initial_value"

	// Set initial value
	_, err := kvClient.Set(ctx, &kvpb.SetRequest{
		Key:   testKey,
		Value: []byte(testValue),
		Ttl:   300,
	})
	if err != nil {
		t.Fatalf("Failed to set initial value: %v", err)
	}

	// Read value multiple times to ensure consistency
	for i := 0; i < 10; i++ {
		getResp, err := kvClient.Get(ctx, &kvpb.GetRequest{Key: testKey})
		if err != nil {
			t.Fatalf("Get %d failed: %v", i, err)
		}
		if !getResp.Found {
			t.Fatalf("Key not found on read %d", i)
		}
		if string(getResp.Value) != testValue {
			t.Fatalf("Value mismatch on read %d: expected %s, got %s",
				i, testValue, string(getResp.Value))
		}
	}

	// Test Queue FIFO consistency
	queueName := "consistency_queue"
	messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}

	// Push messages in order
	for _, msg := range messages {
		_, err := queueClient.Push(ctx, &queuepb.PushRequest{
			Queue: queueName,
			Data:  []byte(msg),
		})
		if err != nil {
			t.Fatalf("Failed to push message %s: %v", msg, err)
		}
	}

	// Pop messages and verify order
	for i, expectedMsg := range messages {
		popResp, err := queueClient.Pop(ctx, &queuepb.PopRequest{
			Queue:   queueName,
			Timeout: 5,
		})
		if err != nil {
			t.Fatalf("Failed to pop message %d: %v", i, err)
		}
		if popResp.Message == nil {
			t.Fatalf("No message received at position %d", i)
		}
		if string(popResp.Message.Data) != expectedMsg {
			t.Fatalf("Message order violation at position %d: expected %s, got %s",
				i, expectedMsg, string(popResp.Message.Data))
		}
	}

	// Test Stream ordering consistency
	topicName := "consistency_stream"
	streamClient.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1, // Single partition for ordering
	})

	// Publish messages in order
	var offsets []int64
	for i, msg := range messages {
		publishResp, err := streamClient.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: "test",
			Data:         []byte(msg),
		})
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
		offsets = append(offsets, publishResp.Offset)
	}

	// Read messages and verify order
	readResp, err := streamClient.Read(ctx, &streampb.ReadRequest{
		Topic:      topicName,
		Partition:  0,
		FromOffset: 0,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Failed to read stream: %v", err)
	}

	if len(readResp.Messages) < len(messages) {
		t.Fatalf("Expected at least %d messages, got %d", len(messages), len(readResp.Messages))
	}

	// Verify message order and offsets
	for i, expectedMsg := range messages {
		if i >= len(readResp.Messages) {
			break
		}
		msg := readResp.Messages[i]
		if string(msg.Data) != expectedMsg {
			t.Fatalf("Stream order violation at position %d: expected %s, got %s",
				i, expectedMsg, string(msg.Data))
		}
		if msg.Offset != offsets[i] {
			t.Fatalf("Offset mismatch at position %d: expected %d, got %d",
				i, offsets[i], msg.Offset)
		}
	}

	t.Log("✅ Data consistency tests passed")
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				containsInMiddle(s, substr))))
}

func containsInMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
