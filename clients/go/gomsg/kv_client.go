package gomsg

import (
	"context"
	"fmt"
	"time"

	kvpb "gomsg/api/generated/kv"
)

// KVClient provides KV operations with automatic partitioning and failover
type KVClient struct {
	client *Client
}

// Set stores a key-value pair with optional TTL
func (kv *KVClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		req := &kvpb.SetRequest{
			Key:   key,
			Value: value,
		}
		
		if ttl > 0 {
			req.TtlSeconds = int64(ttl.Seconds())
		}
		
		resp, err := client.Set(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("set failed: %s", resp.Status.Message)
		}
		
		return nil
	})
}

// Get retrieves a value by key
func (kv *KVClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var value []byte
	var found bool
	
	err := kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("get failed: %s", resp.Status.Message)
		}
		
		value = resp.Value
		found = resp.Found
		return nil
	})
	
	return value, found, err
}

// MSet sets multiple key-value pairs in a batch
func (kv *KVClient) MSet(ctx context.Context, pairs map[string][]byte) error {
	return kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		resp, err := client.MSet(ctx, &kvpb.MSetRequest{Pairs: pairs})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("mset failed: %s", resp.Status.Message)
		}
		
		return nil
	})
}

// MGet retrieves multiple values by keys
func (kv *KVClient) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	var results map[string][]byte
	
	err := kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		resp, err := client.MGet(ctx, &kvpb.MGetRequest{Keys: keys})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("mget failed: %s", resp.Status.Message)
		}
		
		results = resp.Values
		return nil
	})
	
	return results, err
}

// Delete removes one or more keys
func (kv *KVClient) Delete(ctx context.Context, keys ...string) (int, error) {
	var deletedCount int
	
	err := kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		resp, err := client.Delete(ctx, &kvpb.DeleteRequest{Keys: keys})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("delete failed: %s", resp.Status.Message)
		}
		
		deletedCount = int(resp.DeletedCount)
		return nil
	})
	
	return deletedCount, err
}

// Exists checks if a key exists
func (kv *KVClient) Exists(ctx context.Context, key string) (bool, error) {
	var exists bool
	
	err := kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		resp, err := client.Exists(ctx, &kvpb.ExistsRequest{Key: key})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("exists failed: %s", resp.Status.Message)
		}
		
		exists = resp.Exists
		return nil
	})
	
	return exists, err
}

// Increment atomically increments a counter
func (kv *KVClient) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	var newValue int64
	
	err := kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		resp, err := client.Increment(ctx, &kvpb.IncrementRequest{
			Key:   key,
			Delta: delta,
		})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("increment failed: %s", resp.Status.Message)
		}
		
		newValue = resp.Value
		return nil
	})
	
	return newValue, err
}

// Keys returns keys matching a pattern
func (kv *KVClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	var allKeys []string
	
	// Keys operation needs to query all nodes since keys are distributed
	for node := range kv.client.kvClients {
		err := kv.client.executeWithRetry(func(node string) error {
			client := kv.client.kvClients[node]
			
			resp, err := client.Keys(ctx, &kvpb.KeysRequest{Pattern: pattern})
			if err != nil {
				return err
			}
			
			if !resp.Status.Success {
				return fmt.Errorf("keys failed: %s", resp.Status.Message)
			}
			
			allKeys = append(allKeys, resp.Keys...)
			return nil
		})
		
		if err != nil {
			return nil, err
		}
	}
	
	return allKeys, nil
}

// TTL returns the time-to-live for a key
func (kv *KVClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	var ttl time.Duration
	
	err := kv.client.executeWithRetry(func(node string) error {
		client := kv.client.kvClients[node]
		
		resp, err := client.TTL(ctx, &kvpb.TTLRequest{Key: key})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("ttl failed: %s", resp.Status.Message)
		}
		
		ttl = time.Duration(resp.TtlSeconds) * time.Second
		return nil
	})
	
	return ttl, err
}
