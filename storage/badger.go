package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// BadgerStorage implements Storage interface using BadgerDB
type BadgerStorage struct {
	db *badger.DB
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

	storage := &BadgerStorage{db: db}

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
	return s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// Get retrieves a value by key
func (s *BadgerStorage) Get(ctx context.Context, key string) ([]byte, bool, error) {
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

	return value, found, err
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
			}
		}
		return nil
	})

	return deleted, err
}

// Exists checks if a key exists
func (s *BadgerStorage) Exists(ctx context.Context, key string) (bool, error) {
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
		return txn.SetEntry(entry)
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
		
		return txn.Set([]byte(key), valueBytes)
	})

	return newValue, err
}

// Decrement atomically decrements a counter
func (s *BadgerStorage) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.Increment(ctx, key, -delta)
}

// MSet sets multiple key-value pairs
func (s *BadgerStorage) MSet(ctx context.Context, pairs map[string][]byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for key, value := range pairs {
			if err := txn.Set([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
}

// MGet gets multiple values by keys
func (s *BadgerStorage) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	
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
				result[key] = append([]byte{}, val...)
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
	return s.db.Close()
}

// Backup creates a backup of the database
func (s *BadgerStorage) Backup(ctx context.Context, path string) error {
	file, err := badger.DefaultOptions("").WithDir(filepath.Dir(path)).WithValueDir(filepath.Dir(path)).Open()
	if err != nil {
		return err
	}
	defer file.Close()

	return s.db.Backup(file, 0)
}

// Restore restores the database from a backup
func (s *BadgerStorage) Restore(ctx context.Context, path string) error {
	file, err := badger.DefaultOptions("").WithDir(filepath.Dir(path)).WithValueDir(filepath.Dir(path)).Open()
	if err != nil {
		return err
	}
	defer file.Close()

	return s.db.Load(file, 0)
}