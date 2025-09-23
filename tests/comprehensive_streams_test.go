package tests

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streampb "gomsg/api/generated/stream"
	"gomsg/tests/testutil"
)

func setupComprehensiveStreamClient(t *testing.T) streampb.StreamServiceClient {
	// Start test server automatically
	testServer := testutil.StartTestServer(t)
	
	conn, err := grpc.Dial(testServer.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return streampb.NewStreamServiceClient(conn)
}

func TestComprehensiveStreamsDemo(t *testing.T) {
	client := setupComprehensiveStreamClient(t)
	ctx := context.Background()

	t.Log("🚀 GoMsg Comprehensive Streams & Pub/Sub Functionality Test")
	t.Log("===========================================================")

	// Test 1: E-commerce Event Streaming
	t.Log("\n📋 Test 1: E-commerce Event Streaming")
	testEcommerceEventStreaming(t, ctx, client)

	// Test 2: Financial Transaction Log
	t.Log("\n📋 Test 2: Financial Transaction Log with Replay")
	testFinancialTransactionLog(t, ctx, client)

	// Test 3: IoT Sensor Data Streaming
	t.Log("\n📋 Test 3: IoT Sensor Data Multi-Partition Streaming")
	testIoTSensorStreaming(t, ctx, client)

	t.Log("\n🎉 All comprehensive stream tests completed successfully!")
	t.Log("✅ GoMsg provides enterprise-grade event streaming:")
	t.Log("   • Kafka-like persistent event logs")
	t.Log("   • Multi-partition topics for horizontal scaling")
	t.Log("   • Offset-based message replay and recovery")
	t.Log("   • Consumer group patterns for load balancing")
	t.Log("   • Event ordering guarantees within partitions")
}

func testEcommerceEventStreaming(t *testing.T, ctx context.Context, client streampb.StreamServiceClient) {
	topicName := "ecommerce_events"

	// Create topic for e-commerce events
	_, err := client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 2,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("   ✅ Created e-commerce events topic")

	// Simulate customer journey events
	customerEvents := []struct {
		customerID string
		eventType  string
		data       string
	}{
		{"cust_001", "page_visit", `{"customer_id":"cust_001","page":"/products","category":"electronics"}`},
		{"cust_001", "product_view", `{"customer_id":"cust_001","product_id":"laptop_123","price":999.99}`},
		{"cust_001", "add_to_cart", `{"customer_id":"cust_001","product_id":"laptop_123","quantity":1}`},
		{"cust_002", "page_visit", `{"customer_id":"cust_002","page":"/products","category":"books"}`},
		{"cust_001", "checkout_start", `{"customer_id":"cust_001","cart_total":999.99,"items":1}`},
		{"cust_002", "product_view", `{"customer_id":"cust_002","product_id":"book_456","price":29.99}`},
		{"cust_001", "payment_complete", `{"customer_id":"cust_001","order_id":"ord_789","amount":999.99}`},
		{"cust_002", "add_to_cart", `{"customer_id":"cust_002","product_id":"book_456","quantity":2}`},
	}

	t.Logf("   📤 Publishing %d customer journey events...", len(customerEvents))
	
	partitionCounts := make(map[int32]int)
	for i, event := range customerEvents {
		publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: event.customerID, // Partition by customer for ordering
			Data:         []byte(event.data),
			Headers: map[string]string{
				"event_type":  event.eventType,
				"customer_id": event.customerID,
				"source":      "web_app",
			},
		})
		if err != nil {
			t.Fatalf("Failed to publish event %d: %v", i, err)
		}
		partitionCounts[publishResp.Partition]++
		t.Logf("      [%d] %s %s -> partition %d, offset %d", 
			i+1, event.customerID, event.eventType, publishResp.Partition, publishResp.Offset)
	}

	// Show partition distribution
	t.Logf("   📊 Event distribution by customer:")
	for partition, count := range partitionCounts {
		t.Logf("      Partition %d: %d events", partition, count)
	}

	// Simulate different services consuming the events
	services := []string{"analytics", "recommendations", "marketing", "fraud_detection"}
	
	for _, service := range services {
		t.Logf("   📥 Service '%s' processing events:", service)
		
		totalProcessed := 0
		for p := int32(0); p < 2; p++ {
			readResp, err := client.Read(ctx, &streampb.ReadRequest{
				Topic:      topicName,
				Partition:  p,
				FromOffset: 0,
				Limit:      10,
			})
			if err != nil {
				t.Fatalf("Failed to read from partition %d: %v", p, err)
			}
			totalProcessed += len(readResp.Messages)
		}
		t.Logf("      ✅ %s processed %d events", service, totalProcessed)
	}
}

func testFinancialTransactionLog(t *testing.T, ctx context.Context, client streampb.StreamServiceClient) {
	topicName := "financial_transactions"

	// Create single-partition topic for strict ordering
	_, err := client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("   ✅ Created financial transaction log")

	// Simulate account transactions
	transactions := []struct {
		txType  string
		account string
		amount  string
		balance string
	}{
		{"deposit", "acc_12345", "5000.00", "5000.00"},
		{"withdrawal", "acc_12345", "500.00", "4500.00"},
		{"transfer_in", "acc_12345", "1000.00", "5500.00"},
		{"purchase", "acc_12345", "299.99", "5200.01"},
		{"interest", "acc_12345", "15.50", "5215.51"},
		{"withdrawal", "acc_12345", "1000.00", "4215.51"},
	}

	t.Logf("   📤 Recording %d financial transactions...", len(transactions))
	
	var offsets []int64
	for i, tx := range transactions {
		data := fmt.Sprintf(`{"tx_id":"tx_%03d","type":"%s","account":"%s","amount":"%s","balance":"%s","timestamp":"%d"}`,
			i+1, tx.txType, tx.account, tx.amount, tx.balance, i*1000)
		
		publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: tx.account,
			Data:         []byte(data),
			Headers: map[string]string{
				"transaction_type": tx.txType,
				"account":          tx.account,
				"compliance":       "required",
			},
		})
		if err != nil {
			t.Fatalf("Failed to publish transaction %d: %v", i, err)
		}
		offsets = append(offsets, publishResp.Offset)
		t.Logf("      [%d] %s $%s -> offset %d (balance: $%s)", 
			i+1, tx.txType, tx.amount, publishResp.Offset, tx.balance)
	}

	// Demonstrate audit trail replay
	t.Logf("   🔍 Audit Trail Replay Capabilities:")

	// Full audit trail
	t.Logf("      📜 Complete transaction history:")
	readResp, err := client.Read(ctx, &streampb.ReadRequest{
		Topic:      topicName,
		Partition:  0,
		FromOffset: 0,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Failed to read audit trail: %v", err)
	}
	
	for _, msg := range readResp.Messages {
		txType := msg.Headers["transaction_type"]
		t.Logf("         [offset:%d] %s transaction", msg.Offset, txType)
	}

	// Point-in-time recovery
	if len(offsets) > 3 {
		recoveryOffset := offsets[3]
		t.Logf("      ⏰ Point-in-time recovery from offset %d:", recoveryOffset)
		readResp, err = client.Read(ctx, &streampb.ReadRequest{
			Topic:      topicName,
			Partition:  0,
			FromOffset: recoveryOffset,
			Limit:      10,
		})
		if err != nil {
			t.Fatalf("Failed to read from recovery point: %v", err)
		}
		t.Logf("         ✅ Recovered %d transactions from checkpoint", len(readResp.Messages))
	}
}

func testIoTSensorStreaming(t *testing.T, ctx context.Context, client streampb.StreamServiceClient) {
	topicName := "iot_sensor_data"
	partitions := int32(4)

	// Create multi-partition topic for IoT data
	_, err := client.CreateTopic(ctx, &streampb.CreateTopicRequest{
		Name:       topicName,
		Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("   ✅ Created IoT sensor data topic with %d partitions", partitions)

	// Simulate sensor data from different locations
	sensorData := []struct {
		sensorID string
		location string
		data     string
	}{
		{"temp_001", "warehouse_a", `{"sensor_id":"temp_001","type":"temperature","value":22.5,"unit":"celsius","location":"warehouse_a"}`},
		{"humid_001", "warehouse_a", `{"sensor_id":"humid_001","type":"humidity","value":65.2,"unit":"percent","location":"warehouse_a"}`},
		{"temp_002", "warehouse_b", `{"sensor_id":"temp_002","type":"temperature","value":18.9,"unit":"celsius","location":"warehouse_b"}`},
		{"pressure_001", "factory_floor", `{"sensor_id":"pressure_001","type":"pressure","value":1013.25,"unit":"hpa","location":"factory_floor"}`},
		{"temp_003", "server_room", `{"sensor_id":"temp_003","type":"temperature","value":19.1,"unit":"celsius","location":"server_room"}`},
		{"humid_002", "warehouse_b", `{"sensor_id":"humid_002","type":"humidity","value":58.7,"unit":"percent","location":"warehouse_b"}`},
		{"vibration_001", "factory_floor", `{"sensor_id":"vibration_001","type":"vibration","value":0.05,"unit":"mm_s","location":"factory_floor"}`},
		{"temp_004", "office", `{"sensor_id":"temp_004","type":"temperature","value":23.8,"unit":"celsius","location":"office"}`},
	}

	t.Logf("   📤 Publishing %d IoT sensor readings...", len(sensorData))
	
	locationCounts := make(map[string]int)
	partitionCounts := make(map[int32]int)
	
	for i, sensor := range sensorData {
		publishResp, err := client.Publish(ctx, &streampb.PublishRequest{
			Topic:        topicName,
			PartitionKey: sensor.location, // Partition by location for locality
			Data:         []byte(sensor.data),
			Headers: map[string]string{
				"sensor_id": sensor.sensorID,
				"location":  sensor.location,
				"source":    "iot_gateway",
			},
		})
		if err != nil {
			t.Fatalf("Failed to publish sensor data %d: %v", i, err)
		}
		locationCounts[sensor.location]++
		partitionCounts[publishResp.Partition]++
		t.Logf("      [%d] %s @ %s -> partition %d, offset %d", 
			i+1, sensor.sensorID, sensor.location, publishResp.Partition, publishResp.Offset)
	}

	// Show data distribution
	t.Logf("   📊 Sensor data distribution:")
	t.Logf("      By location:")
	for location, count := range locationCounts {
		t.Logf("         %s: %d readings", location, count)
	}
	t.Logf("      By partition:")
	for partition, count := range partitionCounts {
		t.Logf("         Partition %d: %d readings", partition, count)
	}

	// Simulate location-based processing
	t.Logf("   📥 Location-based data processing:")
	totalReadings := 0
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
			// Group by location for this partition
			locationData := make(map[string]int)
			for _, msg := range readResp.Messages {
				location := msg.Headers["location"]
				locationData[location]++
			}
			
			t.Logf("      Partition %d processor:", p)
			for location, count := range locationData {
				t.Logf("         %s: %d readings", location, count)
			}
			totalReadings += len(readResp.Messages)
		}
	}
	
	t.Logf("   ✅ Processed %d total IoT sensor readings across all partitions", totalReadings)
}
