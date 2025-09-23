package unit

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonpb "github.com/skshohagmiah/fluxdl/api/generated/common"
	kvpb "github.com/skshohagmiah/fluxdl/api/generated/kv"
	"github.com/skshohagmiah/fluxdl/tests/testutil"
)

func setupKVClient(t *testing.T) kvpb.KVServiceClient {
	// Start test server automatically
	testServer := testutil.StartTestServer(t)

	conn, err := grpc.Dial(testServer.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return kvpb.NewKVServiceClient(conn)
}

func TestKVSetGet(t *testing.T) {
	client := setupKVClient(t)
	ctx := context.Background()

	// Test Set
	setResp, err := client.Set(ctx, &kvpb.SetRequest{
		Key:   "test_key",
		Value: []byte("test_value"),
		Ttl:   300,
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if !setResp.Status.Success {
		t.Fatalf("Set failed: %s", setResp.Status.Message)
	}

	// Test Get
	getResp, err := client.Get(ctx, &kvpb.GetRequest{Key: "test_key"})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !getResp.Found {
		t.Fatal("Key not found")
	}
	if string(getResp.Value) != "test_value" {
		t.Fatalf("Expected 'test_value', got '%s'", string(getResp.Value))
	}
}

func TestKVIncrement(t *testing.T) {
	client := setupKVClient(t)
	ctx := context.Background()

	// Test Increment
	incrResp, err := client.Incr(ctx, &kvpb.IncrRequest{
		Key: "counter_test",
		By:  5,
	})
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if incrResp.Value != 5 {
		t.Fatalf("Expected 5, got %d", incrResp.Value)
	}

	// Test Increment again
	incrResp, err = client.Incr(ctx, &kvpb.IncrRequest{
		Key: "counter_test",
		By:  3,
	})
	if err != nil {
		t.Fatalf("Second increment failed: %v", err)
	}
	if incrResp.Value != 8 {
		t.Fatalf("Expected 8, got %d", incrResp.Value)
	}
}

func TestKVDecrement(t *testing.T) {
	client := setupKVClient(t)
	ctx := context.Background()

	// Set initial value
	client.Incr(ctx, &kvpb.IncrRequest{Key: "decr_test", By: 10})

	// Test Decrement
	decrResp, err := client.Decr(ctx, &kvpb.DecrRequest{
		Key: "decr_test",
		By:  3,
	})
	if err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}
	if decrResp.Value != 7 {
		t.Fatalf("Expected 7, got %d", decrResp.Value)
	}
}

func TestKVExists(t *testing.T) {
	client := setupKVClient(t)
	ctx := context.Background()

	// Test non-existent key
	existsResp, err := client.Exists(ctx, &kvpb.ExistsRequest{Key: "non_existent"})
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if existsResp.Exists {
		t.Fatal("Expected key to not exist")
	}

	// Set a key
	client.Set(ctx, &kvpb.SetRequest{
		Key:   "exists_test",
		Value: []byte("value"),
	})

	// Test existing key
	existsResp, err = client.Exists(ctx, &kvpb.ExistsRequest{Key: "exists_test"})
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !existsResp.Exists {
		t.Fatal("Expected key to exist")
	}
}

func TestKVDelete(t *testing.T) {
	client := setupKVClient(t)
	ctx := context.Background()

	// Set keys
	client.Set(ctx, &kvpb.SetRequest{Key: "del_test1", Value: []byte("value1")})
	client.Set(ctx, &kvpb.SetRequest{Key: "del_test2", Value: []byte("value2")})

	// Delete keys
	delResp, err := client.Del(ctx, &kvpb.DelRequest{
		Keys: []string{"del_test1", "del_test2"},
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if delResp.DeletedCount != 2 {
		t.Fatalf("Expected 2 deleted, got %d", delResp.DeletedCount)
	}

	// Verify keys are deleted
	getResp, _ := client.Get(ctx, &kvpb.GetRequest{Key: "del_test1"})
	if getResp.Found {
		t.Fatal("Expected key to be deleted")
	}
}

func TestKVTTL(t *testing.T) {
	client := setupKVClient(t)
	ctx := context.Background()

	// Set key with TTL
	client.Set(ctx, &kvpb.SetRequest{
		Key:   "ttl_test",
		Value: []byte("value"),
		Ttl:   60, // 1 minute
	})

	// Check TTL
	ttlResp, err := client.TTL(ctx, &kvpb.TTLRequest{Key: "ttl_test"})
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	if ttlResp.Ttl <= 0 || ttlResp.Ttl > 60 {
		t.Fatalf("Expected TTL between 0 and 60, got %d", ttlResp.Ttl)
	}
}

func TestKVMSetMGet(t *testing.T) {
	client := setupKVClient(t)
	ctx := context.Background()

	// Test MSet
	msetResp, err := client.MSet(ctx, &kvpb.MSetRequest{
		Values: []*commonpb.KeyValue{
			{Key: "mset_test1", Value: []byte("value1")},
			{Key: "mset_test2", Value: []byte("value2")},
			{Key: "mset_test3", Value: []byte("value3")},
		},
	})
	if err != nil {
		t.Fatalf("MSet failed: %v", err)
	}
	if !msetResp.Status.Success {
		t.Fatalf("MSet failed: %s", msetResp.Status.Message)
	}

	// Test MGet
	mgetResp, err := client.MGet(ctx, &kvpb.MGetRequest{
		Keys: []string{"mset_test1", "mset_test2", "mset_test3"},
	})
	if err != nil {
		t.Fatalf("MGet failed: %v", err)
	}
	if len(mgetResp.Values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(mgetResp.Values))
	}

	// Verify values
	expected := map[string]string{
		"mset_test1": "value1",
		"mset_test2": "value2",
		"mset_test3": "value3",
	}
	for _, kv := range mgetResp.Values {
		if expected[kv.Key] != string(kv.Value) {
			t.Fatalf("Expected %s, got %s for key %s", expected[kv.Key], string(kv.Value), kv.Key)
		}
	}
}
