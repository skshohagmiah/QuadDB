package storage

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// MemoryKV provides a Redis-like high-performance in-memory KV store with sharding
type MemoryKV struct {
	// Sharded maps to reduce lock contention (Redis-like approach)
	shards    []*kvShard
	shardMask uint64
	
	// Performance counters
	ops   uint64 // atomic counter for operations
	hits  uint64 // atomic counter for cache hits
	misses uint64 // atomic counter for cache misses
	
	stop chan struct{}
}

type kvShard struct {
	mu   sync.RWMutex
	data map[string]memEntry
}

const numShards = 256 // Use 256 shards for better concurrency

type memEntry struct {
	val       []byte
	expiresAt time.Time // zero means no expiry
}

func NewMemoryKV() *MemoryKV {
	m := &MemoryKV{
		shards:    make([]*kvShard, numShards),
		shardMask: numShards - 1,
		stop:      make(chan struct{}),
	}
	
	// Initialize shards
	for i := 0; i < numShards; i++ {
		m.shards[i] = &kvShard{
			data: make(map[string]memEntry),
		}
	}
	
	go m.janitor()
	return m
}

// Fast hash function (FNV-1a) for sharding - similar to Redis
func (m *MemoryKV) hash(key string) uint64 {
	h := uint64(14695981039346656037) // FNV offset basis
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211 // FNV prime
	}
	return h
}

func (m *MemoryKV) getShard(key string) *kvShard {
	return m.shards[m.hash(key)&m.shardMask]
}

func (m *MemoryKV) Close() error {
	close(m.stop)
	return nil
}

func (m *MemoryKV) janitor() {
	// Optimized janitor with adaptive frequency and batch processing
	t := time.NewTicker(10 * time.Second) // Less frequent for better performance
	defer t.Stop()
	
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			now := time.Now()
			// Process shards in parallel for better performance
			var wg sync.WaitGroup
			for i, shard := range m.shards {
				wg.Add(1)
				go func(s *kvShard, shardID int) {
					defer wg.Done()
					m.cleanShard(s, now, shardID)
				}(shard, i)
			}
			wg.Wait()
		}
	}
}

// cleanShard optimizes expiry cleanup for a single shard
func (m *MemoryKV) cleanShard(shard *kvShard, now time.Time, shardID int) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	// Batch collect expired keys to minimize map operations
	var expiredKeys []string
	for k, e := range shard.data {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			expiredKeys = append(expiredKeys, k)
			if len(expiredKeys) >= 100 { // Process in batches of 100
				break
			}
		}
	}
	
	// Delete expired keys in batch
	for _, k := range expiredKeys {
		delete(shard.data, k)
	}
	
	// Update metrics
	if len(expiredKeys) > 0 {
		atomic.AddUint64(&m.ops, uint64(len(expiredKeys)))
	}
}

// KV operations

func (m *MemoryKV) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	shard := m.getShard(key)
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	
	shard.mu.Lock()
	// Avoid unnecessary memory copy - store reference directly
	shard.data[key] = memEntry{val: value, expiresAt: exp}
	shard.mu.Unlock()
	return nil
}

func (m *MemoryKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	shard := m.getShard(key)
	shard.mu.RLock()
	e, ok := shard.data[key]
	shard.mu.RUnlock()
	
	if !ok {
		atomic.AddUint64(&m.misses, 1)
		return nil, false, nil
	}
	
	// Lazy expiry check - Redis approach
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		shard.mu.Lock()
		delete(shard.data, key)
		shard.mu.Unlock()
		atomic.AddUint64(&m.misses, 1)
		return nil, false, nil
	}
	
	atomic.AddUint64(&m.hits, 1)
	// Return copy to prevent external modification
	return append([]byte(nil), e.val...), true, nil
}

func (m *MemoryKV) Delete(ctx context.Context, keys ...string) (int, error) {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	// Group keys by shard to minimize lock acquisitions
	shardKeys := make(map[*kvShard][]string)
	for _, key := range keys {
		shard := m.getShard(key)
		shardKeys[shard] = append(shardKeys[shard], key)
	}
	
	cnt := 0
	for shard, keyList := range shardKeys {
		shard.mu.Lock()
		for _, key := range keyList {
			if _, ok := shard.data[key]; ok {
				delete(shard.data, key)
				cnt++
			}
		}
		shard.mu.Unlock()
	}
	return cnt, nil
}

func (m *MemoryKV) Exists(ctx context.Context, key string) (bool, error) {
	_, ok, _ := m.Get(ctx, key)
	return ok, nil
}

func (m *MemoryKV) Keys(ctx context.Context, pattern string, limit int) ([]string, error) {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	var res []string
	count := 0
	
	// Optimize for common patterns
	if pattern == "" || pattern == "*" {
		// Return all keys from all shards
		for _, shard := range m.shards {
			shard.mu.RLock()
			for k := range shard.data {
				if limit > 0 && count >= limit {
					shard.mu.RUnlock()
					return res, nil
				}
				res = append(res, k)
				count++
			}
			shard.mu.RUnlock()
		}
		return res, nil
	}
	
	// Pattern matching across all shards
	for _, shard := range m.shards {
		shard.mu.RLock()
		for k := range shard.data {
			if limit > 0 && count >= limit {
				shard.mu.RUnlock()
				return res, nil
			}
			if matchesPattern(k, pattern) {
				res = append(res, k)
				count++
			}
		}
		shard.mu.RUnlock()
	}
	return res, nil
}


func (m *MemoryKV) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	shard := m.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	e, ok := shard.data[key]
	if !ok {
		return nil
	}
	if ttl <= 0 {
		e.expiresAt = time.Time{}
	} else {
		e.expiresAt = time.Now().Add(ttl)
	}
	shard.data[key] = e
	return nil
}

func (m *MemoryKV) TTL(ctx context.Context, key string) (time.Duration, error) {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	shard := m.getShard(key)
	shard.mu.RLock()
	e, ok := shard.data[key]
	shard.mu.RUnlock()
	
	if !ok || e.expiresAt.IsZero() {
		return 0, nil
	}
	if time.Now().After(e.expiresAt) {
		return 0, nil
	}
	return time.Until(e.expiresAt), nil
}

func (m *MemoryKV) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	shard := m.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	e := shard.data[key]
	cur := int64(0)
	if len(e.val) > 0 {
		// Use unsafe string conversion for better performance
		if v, err := strconv.ParseInt(*(*string)(unsafe.Pointer(&e.val)), 10, 64); err == nil {
			cur = v
		}
	}
	cur += delta
	e.val = []byte(strconv.FormatInt(cur, 10))
	shard.data[key] = e
	return cur, nil
}

func (m *MemoryKV) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return m.Increment(ctx, key, -delta)
}

func (m *MemoryKV) MSet(ctx context.Context, pairs map[string][]byte) error {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	// Group pairs by shard to minimize lock acquisitions
	shardPairs := make(map[*kvShard]map[string][]byte)
	for key, value := range pairs {
		shard := m.getShard(key)
		if shardPairs[shard] == nil {
			shardPairs[shard] = make(map[string][]byte)
		}
		shardPairs[shard][key] = value
	}
	
	// Set in each shard with single lock acquisition
	for shard, pairMap := range shardPairs {
		shard.mu.Lock()
		for key, value := range pairMap {
			shard.data[key] = memEntry{val: value}
		}
		shard.mu.Unlock()
	}
	return nil
}

func (m *MemoryKV) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	_ = ctx
	atomic.AddUint64(&m.ops, 1)
	
	res := make(map[string][]byte, len(keys))
	now := time.Now()
	
	// Group keys by shard to minimize lock acquisitions
	shardKeys := make(map[*kvShard][]string)
	for _, key := range keys {
		shard := m.getShard(key)
		shardKeys[shard] = append(shardKeys[shard], key)
	}
	
	// Get from each shard with single lock acquisition
	for shard, keyList := range shardKeys {
		shard.mu.RLock()
		for _, key := range keyList {
			if e, ok := shard.data[key]; ok {
				if e.expiresAt.IsZero() || now.Before(e.expiresAt) {
					res[key] = append([]byte(nil), e.val...)
				}
			}
		}
		shard.mu.RUnlock()
	}
	return res, nil
}

func min(a, b int) int { if a == 0 { return b }; if b == 0 { return a }; if a < b { return a }; return b }
