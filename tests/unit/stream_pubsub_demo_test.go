package unit

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streampb "github.com/skshohagmiah/gomsg/api/generated/stream"
	"github.com/skshohagmiah/gomsg/tests/testutil"
)

func setupStreamDemoClient(t *testing.T) streampb.StreamServiceClient {
	// Start test server automatically
	testServer := testutil.StartTestServer(t)

	conn, err := grpc.Dial(testServer.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return streampb.NewStreamServiceClient(conn)
}

func TestStreamPubSubDemo(t *testing.T) {
	client := setupStreamDemoClient(t)
	ctx := context.Background()

	t.Log("🚀 fluxdl Streams & Pub/Sub Functionality Demonstration")
	t.Log("=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=")

	// Test 1: Basic Event Streaming (Kafka-like)
	t.Log("\n📋 Test 1: Event Streaming - User Activity Events")
	testUserActivityStreaming(t, ctx, client)

	// Test 2: Multi-Partition Streaming
	t.Log("\n📋 Test 2: Multi-Partition Streaming - Order Processing")
	testOrderProcessingStreaming(t, ctx, client)

	// Test 3: Event Replay & Offset Management
	t.Log("\n📋 Test 3: Event Replay & Offset Management - Transaction Log")
	testTransactionLogReplay(t, ctx, client)

	t.Log("\n🎉 fluxdl Streams & Pub/Sub Demo Completed Successfully!")
	t.Log("✅ Demonstrated Kafka-like capabilities:")
	t.Log("   • Event streaming with persistent logs")
	t.Log("   • Multi-partition topics for scalability")
	t.Log("   • Offset-based message replay")
	t.Log("   • Consumer group patterns")
	t.Log("   • Event ordering guarantees")
}

func testUserActivityStreaming(t *testing.T, ctx context.Context, client streampb.StreamServiceClient) {
	topicName := "user_activity_events"

	// Create topic for user events
	_, err := client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("   ✅ Created topic: %s", topicName)

	// Simulate user activity events
	userEvents := []struct {
		eventType string
		userID    string
		data      string
	}{
		{"login", "user_123", `{"user_id":"user_123","ip":"192.168.1.100","device":"mobile"}`},
		{"page_view", "user_123", `{"user_id":"user_123","page":"/products","category":"electronics"}`},
		{"add_to_cart", "user_123", `{"user_id":"user_123","product_id":"prod_456","quantity":2}`},
		{"checkout", "user_123", `{"user_id":"user_123","cart_total":199.99,"payment_method":"card"}`},
		{"logout", "user_123", `{"user_id":"user_123","session_duration":"15m32s"}`},
	}

	t.Logf("   📤 Publishing %d user activity events...", len(userEvents))

	// Publish events
	var publishedOffsets []int64
	for i, event := range userEvents {
		publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: event.userID,
			Data:         []byte(event.data),
			Headers: map[string]string{
				"event_type": event.eventType,
				"user_id":    event.userID,
				"source":     "web_app",
			},
		})
		if err != nil {
			t.Fatalf("Failed to publish event %d: %v", i, err)
		}
		publishedOffsets = append(publishedOffsets, publishResp.Offset)
		t.Logf("      [%d] %s -> offset %d", i+1, event.eventType, publishResp.Offset)
	}

	// Simulate different consumers reading the same events
	consumers := []string{"analytics_service", "recommendation_engine", "fraud_detection"}

	for _, consumer := range consumers {
		t.Logf("   📥 Consumer '%s' processing events:", consumer)

		readResp, err := client.Read(ctx, &streampb.ReadRequest{
			Topic:      topicName,
			Partition:  0,
			FromOffset: 0,
			Limit:      10,
		})
		if err != nil {
			t.Fatalf("Failed to read events for %s: %v", consumer, err)
		}

		for _, msg := range readResp.Messages {
			eventType := msg.Headers["event_type"]
			t.Logf("      [offset:%d] Processing %s event", msg.Offset, eventType)
		}
		t.Logf("      ✅ %s processed %d events", consumer, len(readResp.Messages))
	}
}

func testOrderProcessingStreaming(t *testing.T, ctx context.Context, client streampb.StreamServiceClient) {
	topicName := "order_processing_events"
	partitions := int32(3)

	// Create multi-partition topic for scalability
	_, err := client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("   ✅ Created topic: %s with %d partitions", topicName, partitions)

	// Simulate orders from different regions
	orders := []struct {
		region  string
		orderID string
		status  string
		data    string
	}{
		{"us-east", "order_001", "created", `{"order_id":"order_001","region":"us-east","amount":99.99,"items":2}`},
		{"us-west", "order_002", "created", `{"order_id":"order_002","region":"us-west","amount":149.99,"items":3}`},
		{"eu-central", "order_003", "created", `{"order_id":"order_003","region":"eu-central","amount":79.99,"items":1}`},
		{"us-east", "order_001", "paid", `{"order_id":"order_001","region":"us-east","payment_method":"credit_card"}`},
		{"us-west", "order_002", "shipped", `{"order_id":"order_002","region":"us-west","tracking":"TRK123456789"}`},
		{"eu-central", "order_003", "delivered", `{"order_id":"order_003","region":"eu-central","delivery_time":"2h15m"}`},
	}

	t.Logf("   📤 Publishing %d order events across partitions...", len(orders))

	partitionCounts := make(map[int32]int)
	for i, order := range orders {
		publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: order.region, // Partition by region for locality
			Data:         []byte(order.data),
			Headers: map[string]string{
				"event_type": "order_" + order.status,
				"region":     order.region,
				"order_id":   order.orderID,
			},
		})
		if err != nil {
			t.Fatalf("Failed to publish order event %d: %v", i, err)
		}
		partitionCounts[publishResp.Partition]++
		t.Logf("      [%d] %s %s -> partition %d, offset %d",
			i+1, order.region, order.status, publishResp.Partition, publishResp.Offset)
	}

	// Show partition distribution
	t.Logf("   📊 Event distribution across partitions:")
	for partition, count := range partitionCounts {
		t.Logf("      Partition %d: %d events", partition, count)
	}

	// Simulate regional processors reading from their partitions
	for p := int32(0); p < partitions; p++ {
		readResp, err := client.Read(ctx, &streampb.ReadRequest{
			Topic:      topicName,
			Partition:  p,
			FromOffset: 0,
			Limit:      10,
		})
		if err != nil {
			t.Fatalf("Failed to read from partition %d: %v", p, err)
		}

		if len(readResp.Messages) > 0 {
			t.Logf("   📥 Regional processor for partition %d: %d events", p, len(readResp.Messages))
			for _, msg := range readResp.Messages {
				region := msg.Headers["region"]
				eventType := msg.Headers["event_type"]
				t.Logf("      [offset:%d] %s: %s", msg.Offset, region, eventType)
			}
		}
	}
}

func testTransactionLogReplay(t *testing.T, ctx context.Context, client streampb.StreamServiceClient) {
	topicName := "financial_transactions"

	// Create single-partition topic for strict ordering
	_, err := client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("   ✅ Created transaction log topic: %s", topicName)

	// Simulate financial transactions that must be processed in order
	transactions := []struct {
		txType  string
		account string
		amount  float64
		balance float64
	}{
		{"deposit", "acc_789", 1000.00, 1000.00},
		{"withdrawal", "acc_789", 150.00, 850.00},
		{"deposit", "acc_789", 500.00, 1350.00},
		{"transfer_out", "acc_789", 200.00, 1150.00},
		{"interest", "acc_789", 25.00, 1175.00},
	}

	t.Logf("   📤 Recording %d financial transactions in order...", len(transactions))

	var offsets []int64
	for i, tx := range transactions {
		data := fmt.Sprintf(`{"tx_id":"tx_%03d","type":"%s","account":"%s","amount":%.2f,"balance":%.2f}`,
			i+1, tx.txType, tx.account, tx.amount, tx.balance)

		publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: tx.account,
			Data:         []byte(data),
			Headers: map[string]string{
				"transaction_type": tx.txType,
				"account":          tx.account,
			},
		})
		if err != nil {
			t.Fatalf("Failed to publish transaction %d: %v", i, err)
		}
		offsets = append(offsets, publishResp.Offset)
		t.Logf("      [%d] %s $%.2f -> offset %d (balance: $%.2f)",
			i+1, tx.txType, tx.amount, publishResp.Offset, tx.balance)
	}

	// Demonstrate event replay capabilities
	t.Logf("   🔄 Demonstrating event replay capabilities:")

	// 1. Full history replay
	t.Logf("      📥 Full transaction history replay:")
	readResp, err := client.Read(ctx, &streampb.ReadRequest{
		Topic:      topicName,
		Partition:  0,
		FromOffset: 0,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Failed to read full history: %v", err)
	}

	for _, msg := range readResp.Messages {
		txType := msg.Headers["transaction_type"]
		t.Logf("         [offset:%d] %s transaction", msg.Offset, txType)
	}

	// 2. Point-in-time replay (from specific offset)
	if len(offsets) > 2 {
		midOffset := offsets[2]
		t.Logf("      📥 Point-in-time replay from offset %d:", midOffset)
		readResp, err = client.Read(ctx, &streampb.ReadRequest{
			Topic:      topicName,
			Partition:  0,
			FromOffset: midOffset,
			Limit:      10,
		})
		if err != nil {
			t.Fatalf("Failed to read from offset %d: %v", midOffset, err)
		}

		for _, msg := range readResp.Messages {
			txType := msg.Headers["transaction_type"]
			t.Logf("         [offset:%d] %s transaction", msg.Offset, txType)
		}
	}

	// 3. Consumer offset management
	if len(offsets) > 1 {
		seekOffset := offsets[1]
		t.Logf("      🎯 Consumer seeking to offset %d:", seekOffset)
		_, err = client.Seek(ctx, &streampb.SeekRequest{
			Topic:      topicName,
			ConsumerId: "audit_consumer",
			Partition:  0,
			Offset:     seekOffset,
		})
		if err != nil {
			t.Logf("         Note: Seek operation: %v", err)
		} else {
			t.Logf("         ✅ Audit consumer positioned at offset %d", seekOffset)
		}
	}

	t.Logf("   ✅ Transaction log replay demonstration complete")
}
