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

// NewBadgerStorage creates a new BadgerDB storage instance
func NewBadgerStorage(dataDir string) (*BadgerStorage, error) {
	opts := badger.DefaultOptions(dataDir).
		WithLogger(nil).
		WithLoggingLevel(badger.ERROR)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	// Initialize a small in-memory cache to accelerate hot key reads
	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e6,      // number of keys to track frequency (~10x of items)
		MaxCost:     64 << 20, // 64 MiB cache budget
		BufferItems: 64,       // per-get buffers
	})
	if err != nil {
		// If cache fails to init, continue without cache
		rc = nil
	}

	storage := &BadgerStorage{db: db, cache: rc, seq: make(map[string]*badger.Sequence), replicationFactor: 1}

	// Start background tasks
	go storage.runGC()

	return storage, nil
}

// runGC runs the garbage collector periodically
func (s *BadgerStorage) runGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		s.db.RunValueLogGC(0.7)
	}
}

// Set stores a key-value pair with optional TTL
func (s *BadgerStorage) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
	if err != nil {
		return err
	}
	if s.cache != nil {
		// store a copy to avoid aliasing
		v := append([]byte{}, value...)
		if ttl > 0 {
			s.cache.SetWithTTL(key, v, int64(len(v)), ttl)
		} else {
			s.cache.Set(key, v, int64(len(v)))
		}
	}
	return nil
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
		s.cache.Set(key, v, int64(len(v)))
	}
	return value, found, nil
}

// Delete removes one or more keys
func (s *BadgerStorage) Delete(ctx context.Context, keys ...string) (int, error) {
	deleted := 0
	
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := txn.Delete([]byte(key))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err == nil {
				deleted++
				if s.cache != nil {
					s.cache.Del(key)
				}
			}
		}
		return nil
	})

	return deleted, err
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

// Keys returns keys matching a pattern
func (s *BadgerStorage) Keys(ctx context.Context, pattern string, limit int) ([]string, error) {
	var keys []string
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		prefix := strings.TrimSuffix(pattern, "*")
		
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

// Increment atomically increments a counter
func (s *BadgerStorage) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	var newValue int64
	
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
					// Try to parse as string
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

// MSet sets multiple key-value pairs
func (s *BadgerStorage) MSet(ctx context.Context, pairs map[string][]byte) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for key, value := range pairs {
		if err := wb.Set([]byte(key), value); err != nil {
			return err
		}
		if s.cache != nil {
			v := append([]byte{}, value...)
			s.cache.Set(key, v, int64(len(v)))
		}
	}
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