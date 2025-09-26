package unit

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streampb "github.com/skshohagmiah/gomsg/api/generated/stream"
	"github.com/skshohagmiah/gomsg/tests/testutil"
)

func setupStreamClient(t *testing.T) streampb.StreamServiceClient {
	// Start test server automatically
	testServer := testutil.StartTestServer(t)

	conn, err := grpc.Dial(testServer.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return streampb.NewStreamServiceClient(conn)
}

func TestStreamCreateTopic(t *testing.T) {
	client := setupStreamClient(t)
	ctx := context.Background()
	topicName := "test_topic_create"

	// Create topic
	createResp, err := client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 4,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	if !createResp.Status.Success {
		t.Fatalf("CreateTopic failed: %s", createResp.Status.Message)
	}

	// Verify topic exists by getting info
	infoResp, err := client.GetTopicInfo(ctx, &streampb.GetTopicInfoRequest{
		Topic: topicName,
	})
	if err != nil {
		t.Fatalf("GetTopicInfo failed: %v", err)
	}
	if infoResp.Info.Partitions != 4 {
		t.Fatalf("Expected 4 partitions, got %d", infoResp.Info.Partitions)
	}
}

func TestStreamPublishRead(t *testing.T) {
	client := setupStreamClient(t)
	ctx := context.Background()
	topicName := "test_topic_pubread"

	// Create topic first
	client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 2,
	})

	// Publish message
	publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
		Topic:        topicName,
		PartitionKey: "user123",
		Data:         []byte(`{"action":"login","user":"123"}`),
		Headers: map[string]string{
			"source":    "web",
			"timestamp": "2023-01-01T00:00:00Z",
		},
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if !publishResp.Status.Success {
		t.Fatalf("Publish failed: %s", publishResp.Status.Message)
	}
	if publishResp.Offset < 0 {
		t.Fatalf("Expected valid offset, got %d", publishResp.Offset)
	}

	// Read messages
	readResp, err := client.Read(ctx, &streampb.ReadRequest{
		Topic:      topicName,
		Partition:  publishResp.Partition,
		FromOffset: 0,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readResp.Messages) == 0 {
		t.Fatal("Expected at least one message")
	}

	// Verify message content
	msg := readResp.Messages[0]
	if string(msg.Data) != `{"action":"login","user":"123"}` {
		t.Fatalf("Expected JSON data, got '%s'", string(msg.Data))
	}
	if msg.PartitionKey != "user123" {
		t.Fatalf("Expected partition key 'user123', got '%s'", msg.PartitionKey)
	}
	if msg.Headers["source"] != "web" {
		t.Fatalf("Expected header source='web', got '%s'", msg.Headers["source"])
	}
}

func TestStreamMultipleMessages(t *testing.T) {
	client := setupStreamClient(t)
	ctx := context.Background()
	topicName := "test_topic_multiple"

	// Create topic
	client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})

	// Publish multiple messages
	messages := []string{
		`{"event":"user_login","user":"alice"}`,
		`{"event":"user_logout","user":"alice"}`,
		`{"event":"user_login","user":"bob"}`,
	}

	var offsets []int64
	for i, data := range messages {
		publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: "events",
			Data:         []byte(data),
			Headers: map[string]string{
				"sequence": string(rune('0' + i)),
			},
		})
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
		offsets = append(offsets, publishResp.Offset)
	}

	// Read all messages
	readResp, err := client.Read(ctx, &streampb.ReadRequest{
		Topic:      topicName,
		Partition:  0,
		FromOffset: 0,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readResp.Messages) < 3 {
		t.Fatalf("Expected at least 3 messages, got %d", len(readResp.Messages))
	}

	// Verify message order and content
	for i, msg := range readResp.Messages[:3] {
		if string(msg.Data) != messages[i] {
			t.Fatalf("Message %d mismatch: expected '%s', got '%s'",
				i, messages[i], string(msg.Data))
		}
		if msg.Offset != offsets[i] {
			t.Fatalf("Offset %d mismatch: expected %d, got %d",
				i, offsets[i], msg.Offset)
		}
	}
}

func TestStreamSeekOffset(t *testing.T) {
	client := setupStreamClient(t)
	ctx := context.Background()
	topicName := "test_topic_seek"

	// Create topic and publish messages
	client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: "test",
			Data:         []byte("message " + string(rune('0'+i))),
		})
	}

	// Seek to offset 2
	seekResp, err := client.Seek(ctx, &streampb.SeekRequest{
		Topic:      topicName,
		ConsumerId: "test_consumer",
		Partition:  0,
		Offset:     2,
	})
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if !seekResp.Status.Success {
		t.Fatalf("Seek failed: %s", seekResp.Status.Message)
	}

	// Get current offset
	offsetResp, err := client.GetOffset(ctx, &streampb.GetOffsetRequest{
		Topic:      topicName,
		ConsumerId: "test_consumer",
		Partition:  0,
	})
	if err != nil {
		t.Fatalf("GetOffset failed: %v", err)
	}
	if offsetResp.Offset != 2 {
		t.Fatalf("Expected offset 2, got %d", offsetResp.Offset)
	}
}

func TestStreamListTopics(t *testing.T) {
	client := setupStreamClient(t)
	ctx := context.Background()

	// Create test topics
	topics := []string{"list_test_1", "list_test_2", "list_test_3"}
	for _, topic := range topics {
		client.CreateTopic(ctx, &streampb.CreateTopicRequest{
			Name:       topic,
			Partitions: 1,
		})
	}

	// List topics
	listResp, err := client.ListTopics(ctx, &streampb.ListTopicsRequest{})
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}

	// Check if our test topics are in the list
	topicMap := make(map[string]bool)
	for _, topic := range listResp.Topics {
		topicMap[topic] = true
	}

	for _, topic := range topics {
		if !topicMap[topic] {
			t.Fatalf("Expected topic '%s' in list", topic)
		}
	}
}

func TestStreamPurge(t *testing.T) {
	client := setupStreamClient(t)
	ctx := context.Background()
	topicName := "test_topic_purge"

	// Create topic and publish messages
	client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})

	// Publish messages
	for i := 0; i < 5; i++ {
		client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: "test",
			Data:         []byte("message"),
		})
	}

	// Purge topic
	purgeResp, err := client.Purge(ctx, &streampb.PurgeRequest{
		Topic: topicName,
	})
	if err != nil {
		t.Fatalf("Purge failed: %v", err)
	}
	if !purgeResp.Status.Success {
		t.Fatalf("Purge failed: %s", purgeResp.Status.Message)
	}

	// Verify topic is empty (new messages start from offset 0)
	publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
		Topic:        topicName,
		PartitionKey: "test",
		Data:         []byte("new message"),
	})
	if err != nil {
		t.Fatalf("Publish after purge failed: %v", err)
	}
	if publishResp.Offset != 0 {
		t.Fatalf("Expected offset 0 after purge, got %d", publishResp.Offset)
	}
}

func TestStreamDeleteTopic(t *testing.T) {
	client := setupStreamClient(t)
	ctx := context.Background()
	topicName := "test_topic_delete"

	// Create topic
	client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})

	// Verify topic exists
	_, err := client.GetTopicInfo(ctx, &streampb.GetTopicInfoRequest{
		Topic: topicName,
	})
	if err != nil {
		t.Fatalf("Topic should exist before delete: %v", err)
	}

	// Delete topic
	deleteResp, err := client.DeleteTopic(ctx, &streampb.DeleteTopicRequest{
		Name: topicName,
	})
	if err != nil {
		t.Fatalf("DeleteTopic failed: %v", err)
	}
	if !deleteResp.Status.Success {
		t.Fatalf("DeleteTopic failed: %s", deleteResp.Status.Message)
	}

	// Verify topic is deleted (should fail to get info)
	_, err = client.GetTopicInfo(ctx, &streampb.GetTopicInfoRequest{
		Topic: topicName,
	})
	if err == nil {
		t.Fatal("Expected error when getting info for deleted topic")
	}
}
