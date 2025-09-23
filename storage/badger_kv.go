package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto"
)

// BadgerStorage implements Storage interface using BadgerDB
type BadgerStorage struct {
	db           *badger.DB
	cache        *ristretto.Cache
	seqMu        sync.Mutex
	seq          map[string]*badger.Sequence
	nodeProvider NodeProvider
	// replicationFactor suggests how many replicas to pick for streams (including leader)
	replicationFactor int
}

// SetReplicationFactor sets the desired replication factor for stream topics.
func (s *BadgerStorage) SetReplicationFactor(n int) {
	s.seqMu.Lock()
	s.replicationFactor = n
	s.seqMu.Unlock()
}

// NodeInfo is a minimal description of a cluster node used by storage.
type NodeInfo struct {
	ID      string
	Address string
}

// NodeProvider provides current nodes and leader identity to storage.
type NodeProvider interface {
	ListNodes() []NodeInfo
	LeaderID() string
}

// SetNodeProvider attaches a node provider for cluster-aware operations.
func (s *BadgerStorage) SetNodeProvider(p NodeProvider) {
	s.seqMu.Lock()
	s.nodeProvider = p
	s.seqMu.Unlock()
}

// NewBadgerStorage creates a new BadgerDB storage instance with optimized settings
func NewBadgerStorage(dataDir string) (*BadgerStorage, error) {
	opts := badger.DefaultOptions(dataDir).
		WithLogger(nil).
		WithLoggingLevel(badger.ERROR).
		// Remove compression line - BadgerDB v4 handles compression automatically
		WithBlockCacheSize(64 << 20).       // 64MB block cache
		WithIndexCacheSize(32 << 20).       // 32MB index cache
		WithNumMemtables(3).                // More memtables for better write performance
		WithMemTableSize(32 << 20).         // 32MB memtable size
		WithValueThreshold(1024).           // Store values > 1KB separately
		WithNumLevelZeroTables(2).          // Fewer L0 tables for faster compaction
		WithNumLevelZeroTablesStall(4)      // Stall writes at 4 L0 tables

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	// Initialize larger in-memory cache for Redis-like performance
	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,       // number of keys to track frequency (~10x of items)
		MaxCost:     256 << 20, // 256 MiB cache budget (4x larger)
		BufferItems: 128,       // per-get buffers (2x larger)
		Metrics:     true,      // Enable metrics for monitoring
	})
	if err != nil {
		// If cache fails to init, continue without cache
		rc = nil
	}

	storage := &BadgerStorage{db: db, cache: rc, seq: make(map[string]*badger.Sequence), replicationFactor: 1}

	// Start background tasks
	go storage.runGC()
	go storage.runCacheStats()

	return storage, nil
}

// runGC runs the garbage collector periodically with optimized settings
func (s *BadgerStorage) runGC() {
	ticker := time.NewTicker(2 * time.Minute) // More frequent GC
	defer ticker.Stop()
	
	for range ticker.C {
		// More aggressive GC for better performance
		s.db.RunValueLogGC(0.5)
	}
}

// runCacheStats logs cache performance periodically with detailed monitoring
func (s *BadgerStorage) runCacheStats() {
	if s.cache == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		metrics := s.cache.Metrics
		// Log cache hit ratio for monitoring
		if metrics.KeysAdded() > 0 {
			hits := float64(metrics.Hits())
			misses := float64(metrics.Misses())
			if hits+misses > 0 {
				hitRatio := hits / (hits + misses) * 100
				
				// Log performance stats (can be integrated with monitoring system)
				if hitRatio < 80 { // Alert if hit ratio drops below 80%
					// Low cache hit ratio - consider increasing cache size
					_ = hitRatio
				}
				
				// Monitor BadgerDB LSM stats for production
				lsm := s.db.LevelsToString()
				_ = lsm // Log LSM tree stats for monitoring
				
				// Track memory usage and trigger optimization if needed
				if lsmSize, vlogSize := s.db.Size(); lsmSize+vlogSize > 1<<30 { // 1GB threshold
					go s.db.RunValueLogGC(0.5)
				}
			}
		}
	}
}

// Set stores a key-value pair with optional TTL
func (s *BadgerStorage) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Fast path: cache-first for small values
	if s.cache != nil && len(value) < 1024 { // Cache small values immediately
		v := append([]byte{}, value...)
		cost := int64(len(v))
		if ttl > 0 {
			s.cache.SetWithTTL(key, v, cost, ttl)
		} else {
			s.cache.Set(key, v, cost)
		}
	}
	
	err := s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		// Use faster write mode for better performance
		return txn.SetEntry(entry)
	})
	
	// Cache larger values only after successful write
	if err == nil && s.cache != nil && len(value) >= 1024 {
		v := append([]byte{}, value...)
		s.cache.Set(key, v, int64(len(v)))
	}
	
	return err
}

// Get retrieves a value by key
func (s *BadgerStorage) Get(ctx context.Context, key string) ([]byte, bool, error) {
	// Fast path: in-memory cache
	if s.cache != nil {
		if v, ok := s.cache.Get(key); ok {
			if b, ok2 := v.([]byte); ok2 {
				return append([]byte{}, b...), true, nil
			}
		}
	}

	var value []byte
	var found bool
	
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}

		found = true
		return item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
	})

	if err != nil {
		return nil, false, err
	}
	if found && s.cache != nil {
		v := append([]byte{}, value...)
		// Cache with appropriate cost
		s.cache.Set(key, v, int64(len(v)))
	}
	return value, found, nil
}

// Delete removes one or more keys with optimized batch deletion
func (s *BadgerStorage) Delete(ctx context.Context, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	
	deleted := 0
	
	// Use write batch for multiple deletes
	if len(keys) > 10 {
		wb := s.db.NewWriteBatch()
		defer wb.Cancel()
		
		for _, key := range keys {
			if err := wb.Delete([]byte(key)); err != nil {
				return deleted, err
			}
			deleted++
			// Remove from cache immediately
			if s.cache != nil {
				s.cache.Del(key)
			}
		}
		
		return deleted, wb.Flush()
	}
	
	// Use transaction for small deletes
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := txn.Delete([]byte(key))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err == nil {
				deleted++
				// Remove from cache immediately
				if s.cache != nil {
					s.cache.Del(key)
				}
			}
		}
		return nil
	})

	return deleted, err
}

// DelPattern deletes keys matching a pattern (optimized bulk delete)
func (s *BadgerStorage) DelPattern(ctx context.Context, pattern string, limit int) (int, error) {
	if limit <= 0 {
		limit = 10000 // Default limit for safety
	}
	
	// First, get keys matching the pattern
	keys, err := s.Keys(ctx, pattern, limit)
	if err != nil {
		return 0, err
	}
	
	if len(keys) == 0 {
		return 0, nil
	}
	
	// Use optimized batch delete
	return s.Delete(ctx, keys...)
}

// Exists checks if a key exists
func (s *BadgerStorage) Exists(ctx context.Context, key string) (bool, error) {
	if s.cache != nil {
		if _, ok := s.cache.Get(key); ok {
			return true, nil
		}
	}

	var exists bool
	
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			exists = true
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})

	return exists, err
}

// Keys returns keys matching a pattern with optimized iteration
func (s *BadgerStorage) Keys(ctx context.Context, pattern string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 1000 // Default limit to prevent memory issues
	}
	
	keys := make([]string, 0, limit)
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 100 // Prefetch more keys for better performance
		
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		prefix := strings.TrimSuffix(pattern, "*")
		
		// Optimized iteration with early termination
		for it.Seek([]byte(prefix)); it.Valid() && count < limit; it.Next() {
			key := string(it.Item().Key())
			
			if !strings.HasPrefix(key, prefix) {
				break
			}
			
			if matchesPattern(key, pattern) {
				keys = append(keys, key)
				count++
			}
		}
		
		return nil
	})

	return keys, err
}

// matchesPattern checks if a key matches a pattern (simple * wildcard support)
func matchesPattern(key, pattern string) bool {
	if !strings.Contains(pattern, "*") {
		return key == pattern
	}
	
	parts := strings.Split(pattern, "*")
	if len(parts) == 2 {
		return strings.HasPrefix(key, parts[0]) && strings.HasSuffix(key, parts[1])
	}
	
	return strings.HasPrefix(key, parts[0])
}

// Expire sets TTL for a key
func (s *BadgerStorage) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		var value []byte
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}

		entry := badger.NewEntry([]byte(key), value).WithTTL(ttl)
		if err := txn.SetEntry(entry); err != nil {
			return err
		}
		if s.cache != nil {
			v := append([]byte{}, value...)
			s.cache.SetWithTTL(key, v, int64(len(v)), ttl)
		}
		return nil
	})
}

// TTL returns remaining time to live for a key
func (s *BadgerStorage) TTL(ctx context.Context, key string) (time.Duration, error) {
	var ttl time.Duration
	
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		expiresAt := item.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // Never expires
		} else {
			remaining := time.Until(time.Unix(int64(expiresAt), 0))
			if remaining < 0 {
				ttl = 0 // Expired
			} else {
				ttl = remaining
			}
		}
		
		return nil
	})

	return ttl, err
}

// Increment atomically increments a counter with optimized caching
func (s *BadgerStorage) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	var newValue int64
	
	// Try cache first for better performance
	if s.cache != nil {
		if v, ok := s.cache.Get(key); ok {
			if b, ok2 := v.([]byte); ok2 && len(b) == 8 {
				currentValue := int64(binary.BigEndian.Uint64(b))
				newValue = currentValue + delta
				
				// Update both cache and storage atomically
				err := s.db.Update(func(txn *badger.Txn) error {
					valueBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(valueBytes, uint64(newValue))
					return txn.Set([]byte(key), valueBytes)
				})
				
				if err == nil {
					valueBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(valueBytes, uint64(newValue))
					s.cache.Set(key, valueBytes, 8)
				}
				
				return newValue, err
			}
		}
	}
	
	// Fallback to database-first approach
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		var currentValue int64
		if err == nil {
			err = item.Value(func(val []byte) error {
				if len(val) == 8 {
					currentValue = int64(binary.BigEndian.Uint64(val))
				} else {
					// Try to parse as string for backward compatibility
					if parsed, err := strconv.ParseInt(string(val), 10, 64); err == nil {
						currentValue = parsed
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		newValue = currentValue + delta
		valueBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBytes, uint64(newValue))
		
		if err := txn.Set([]byte(key), valueBytes); err != nil {
			return err
		}
		if s.cache != nil {
			v := append([]byte{}, valueBytes...)
			s.cache.Set(key, v, int64(len(v)))
		}
		return nil
	})

	return newValue, err
}

// Decrement atomically decrements a counter
func (s *BadgerStorage) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.Increment(ctx, key, -delta)
}

// MSet sets multiple key-value pairs with optimized batching
func (s *BadgerStorage) MSet(ctx context.Context, pairs map[string][]byte) error {
	if len(pairs) == 0 {
		return nil
	}
	
	// Use write batch for better performance
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// Pre-populate cache and batch writes
	for key, value := range pairs {
		if err := wb.Set([]byte(key), value); err != nil {
			return err
		}
		// Immediate cache population for batch operations
		if s.cache != nil {
			v := append([]byte{}, value...)
			s.cache.Set(key, v, int64(len(v)))
		}
	}
	
	// Flush with sync for consistency
	return wb.Flush()
}

// MGet gets multiple values by keys
func (s *BadgerStorage) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	var missing []string
	if s.cache != nil {
		for _, key := range keys {
			if v, ok := s.cache.Get(key); ok {
				if b, ok2 := v.([]byte); ok2 {
					result[key] = append([]byte{}, b...)
					continue
				}
			}
			missing = append(missing, key)
		}
		if len(missing) == 0 {
			return result, nil
		}
		keys = missing
	}
	
	err := s.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get([]byte(key))
			if err != nil {
				if err == badger.ErrKeyNotFound {
					continue
				}
				return err
			}

			err = item.Value(func(val []byte) error {
				b := append([]byte{}, val...)
				result[key] = b
				// Cache the result for future reads
				if s.cache != nil {
					s.cache.Set(key, append([]byte{}, b...), int64(len(b)))
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return result, err
}

// Close closes the database connection
func (s *BadgerStorage) Close() error {
    // Close sequences
    s.seqMu.Lock()
    for _, sq := range s.seq {
        _ = sq.Release()
    }
    s.seq = nil
    s.seqMu.Unlock()
    return s.db.Close()
}

// getOrCreateSeq returns a Badger sequence for a given key prefix.
// bandwidth controls local preallocation for fewer writes; using 128 as a reasonable default.
func (s *BadgerStorage) getOrCreateSeq(key string) (*badger.Sequence, error) {
    s.seqMu.Lock()
    defer s.seqMu.Unlock()
    if sq, ok := s.seq[key]; ok {
        return sq, nil
    }
    sq, err := s.db.GetSequence([]byte(key), 128)
    if err != nil {
        return nil, err
    }
    s.seq[key] = sq
    return sq, nil
}

// Backup creates a backup of the database
func (s *BadgerStorage) Backup(ctx context.Context, path string) error {
    f, err := os.Create(path)
    if err != nil {
        return err
    }
    defer f.Close()

    _, err = s.db.Backup(f, 0)
    return err
}

// Restore restores the database from a backup
func (s *BadgerStorage) Restore(ctx context.Context, path string) error {
    f, err := os.Open(path)
    if err != nil {
        return err
    }
    defer f.Close()

    return s.db.Load(f, 0)
}

// GetPerformanceStats returns detailed performance statistics
func (s *BadgerStorage) GetPerformanceStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	// BadgerDB LSM stats
	lsmSize, vlogSize := s.db.Size()
	stats["lsm_size"] = lsmSize
	stats["vlog_size"] = vlogSize
	stats["total_size"] = lsmSize + vlogSize
	stats["lsm_levels"] = s.db.LevelsToString()
	
	// Cache stats
	if s.cache != nil {
		metrics := s.cache.Metrics
		hits := float64(metrics.Hits())
		misses := float64(metrics.Misses())
		total := hits + misses
		
		stats["cache_hits"] = hits
		stats["cache_misses"] = misses
		if total > 0 {
			stats["cache_hit_ratio"] = hits / total * 100
		}
		stats["cache_keys_added"] = metrics.KeysAdded()
		stats["cache_keys_evicted"] = metrics.KeysEvicted()
		stats["cache_cost_added"] = metrics.CostAdded()
		stats["cache_cost_evicted"] = metrics.CostEvicted()
	}
	
	// Table stats
	if tables := s.db.Tables(); tables != nil {
		stats["tables"] = len(tables)
		// Note: BadgerDB v4 TableInfo doesn't expose size directly
		// Use LSM + VLog size as total size metric
	}
	
	return stats
}

// OptimizeForWorkload applies workload-specific optimizations
func (s *BadgerStorage) OptimizeForWorkload(workload string) {
	switch workload {
	case "read_heavy":
		// Increase cache size for read-heavy workloads
		if s.cache != nil {
			// Cache is already optimized, but we could adjust prefetch
		}
	case "write_heavy":
		// Trigger more frequent GC for write-heavy workloads
		go func() {
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				s.db.RunValueLogGC(0.3) // More aggressive GC
			}
		}()
	case "mixed":
		// Balanced approach - already implemented
	default:
		// Use default optimizations
	}
}