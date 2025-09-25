package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const (
	queuePrefix    = "q:"
	messagePrefix  = "m:"
	consumerPrefix = "c:"
	statsPrefix    = "s:"

	// Performance optimization constants
	maxBatchSize      = 5000    // Increased batch size for better throughput
	defaultTimeout    = 30 * time.Second
	pollInterval      = 5 * time.Millisecond  // Faster polling for lower latency
	prefetchSize      = 200     // Prefetch more messages for batch operations
	writeBatchThreshold = 50    // Use write batch for >50 operations
)

// QueuePush adds a message to a queue
func (s *BadgerStorage) QueuePush(ctx context.Context, queue string, data []byte) (string, error) {
	messageID := uuid.New().String()
	now := time.Now()

	message := QueueMessage{
		ID:         messageID,
		Queue:      queue,
		Data:       data,
		CreatedAt:  now,
		DelayUntil: now, // No delay
		RetryCount: 0,
	}

	messageData, err := json.Marshal(message)
	if err != nil {
		return "", err
	}

	// Single transaction for all operations
	err = s.db.Update(func(txn *badger.Txn) error {
		// Store message
		messageKey := messagePrefix + messageID
		if err := txn.Set([]byte(messageKey), messageData); err != nil {
			return err
		}

		// Add to queue index (immediate message only)
		queueKey := queuePrefix + queue + ":" + fmt.Sprintf("%016d", now.UnixNano()) + ":" + messageID
		if err := txn.Set([]byte(queueKey), []byte(messageID)); err != nil {
			return err
		}

		// Update stats in same transaction
		statsKey := statsPrefix + queue
		stats := QueueStats{Name: queue, Size: 1}
		if item, err := txn.Get([]byte(statsKey)); err == nil {
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &stats)
			}); err == nil {
				stats.Size++
			}
		}

		statsData, _ := json.Marshal(stats)
		return txn.Set([]byte(statsKey), statsData)
	})

	return messageID, err
}

// QueuePop removes and returns a message from a queue with optimized polling
func (s *BadgerStorage) QueuePop(ctx context.Context, queue string, timeout time.Duration) (QueueMessage, error) {
	var message QueueMessage
	var found bool
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		err := s.db.Update(func(txn *badger.Txn) error {
			// Optimized iterator with prefetch
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			// Seek to first message in queue (ordered by timestamp)
			prefix := []byte(queuePrefix + queue + ":")
			it.Seek(prefix)

			if !it.ValidForPrefix(prefix) {
				return nil // No messages
			}

			// Get first available message
			idxItem := it.Item()
			val, err := idxItem.ValueCopy(nil)
			if err != nil {
				return err
			}

			messageID := string(val)

			// Get message data
			messageKey := messagePrefix + messageID
			item, err := txn.Get([]byte(messageKey))
			if err != nil {
				// Clean up orphaned index entry
				txn.Delete(idxItem.Key())
				return nil
			}

			var messageData []byte
			err = item.Value(func(val []byte) error {
				messageData = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return err
			}

			err = json.Unmarshal(messageData, &message)
			if err != nil {
				return err
			}

			// Skip messages with invalid timestamps (zero values)
			if message.CreatedAt.IsZero() || message.DelayUntil.IsZero() {
				// Delete the orphaned message and index
				txn.Delete([]byte(messageKey))
				txn.Delete(idxItem.Key())
				return nil // Try next message
			}

			// Remove from queue index
			if err := txn.Delete(idxItem.Key()); err != nil {
				return err
			}

			// Update stats
			statsKey := statsPrefix + queue
			stats := QueueStats{Name: queue}
			if item, err := txn.Get([]byte(statsKey)); err == nil {
				if err := item.Value(func(val []byte) error {
					return json.Unmarshal(val, &stats)
				}); err == nil {
					stats.Size--
					stats.Processed++
				}
			}
			statsData, _ := json.Marshal(stats)
			txn.Set([]byte(statsKey), statsData)

			found = true
			return nil
		})

		if err != nil {
			return message, err
		}

		if found {
			break
		}

		// Efficient polling with context cancellation
		select {
		case <-ctx.Done():
			return message, ctx.Err()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}

	if !found {
		// Return empty message (not an error) when no message is available
		return QueueMessage{}, nil
	}

	return message, nil
}

// QueuePeek returns messages without removing them
func (s *BadgerStorage) QueuePeek(ctx context.Context, queue string, limit int) ([]QueueMessage, error) {
	var messages []QueueMessage

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		prefix := []byte(queuePrefix + queue + ":")

		for it.Seek(prefix); it.ValidForPrefix(prefix) && count < limit; it.Next() {
			// Get message ID
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				continue
			}
			messageID := string(val)

			// Get message data
			messageKey := messagePrefix + messageID
			item, err := txn.Get([]byte(messageKey))
			if err != nil {
				continue
			}

			var messageData []byte
			err = item.Value(func(val []byte) error {
				messageData = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				continue
			}

			var message QueueMessage
			if err := json.Unmarshal(messageData, &message); err != nil {
				continue
			}

			messages = append(messages, message)
			count++
		}

		return nil
	})

	return messages, err
}

// QueueAck acknowledges a message
func (s *BadgerStorage) QueueAck(ctx context.Context, messageID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		messageKey := messagePrefix + messageID
		return txn.Delete([]byte(messageKey))
	})
}

// QueueNack rejects a message and requeues it
func (s *BadgerStorage) QueueNack(ctx context.Context, messageID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Get message
		messageKey := messagePrefix + messageID
		item, err := txn.Get([]byte(messageKey))
		if err != nil {
			return err
		}

		var messageData []byte
		err = item.Value(func(val []byte) error {
			messageData = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}

		var message QueueMessage
		if err := json.Unmarshal(messageData, &message); err != nil {
			return err
		}

		// Increment retry count
		message.RetryCount++
		message.DelayUntil = time.Now().Add(time.Duration(message.RetryCount) * time.Second)

		// Update message
		updatedData, err := json.Marshal(message)
		if err != nil {
			return err
		}

		return txn.Set([]byte(messageKey), updatedData)
	})
}

// QueueStats returns queue statistics
func (s *BadgerStorage) QueueStats(ctx context.Context, queue string) (QueueStats, error) {
	var stats QueueStats
	stats.Name = queue

	err := s.db.View(func(txn *badger.Txn) error {
		// Count messages in queue
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(queuePrefix + queue + ":")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			stats.Size++
		}

		// Get additional stats from stats key
		statsKey := statsPrefix + queue
		item, err := txn.Get([]byte(statsKey))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		if err == nil {
			var statsData QueueStats
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &statsData)
			})
			if err == nil {
				stats.Processed = statsData.Processed
				stats.Failed = statsData.Failed
				stats.Consumers = statsData.Consumers
			}
		}

		return nil
	})

	return stats, err
}

// updateQueueStats updates queue statistics
func (s *BadgerStorage) updateQueueStats(ctx context.Context, queue string, deltaSize, deltaProcessed, deltaFailed, deltaConsumers int64) error {
	// Note: ctx is reserved for future use (deadline/cancellation hook)
	_ = ctx
	return s.db.Update(func(txn *badger.Txn) error {
		statsKey := statsPrefix + queue

		var stats QueueStats
		item, err := txn.Get([]byte(statsKey))
		if err == nil {
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &stats)
			})
		}

		stats.Name = queue
		stats.Size += deltaSize
		stats.Processed += deltaProcessed
		stats.Failed += deltaFailed
		stats.Consumers += deltaConsumers

		statsData, err := json.Marshal(stats)
		if err != nil {
			return err
		}

		return txn.Set([]byte(statsKey), statsData)
	})
}

// QueuePurge removes all messages from a queue
func (s *BadgerStorage) QueuePurge(ctx context.Context, queue string) (int64, error) {
	var purged int64

	// Use multiple smaller transactions to avoid issues with large transactions
	
	// First, collect all message IDs
	var messageIDs []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(queuePrefix + queue + ":")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				continue
			}
			messageID := string(val)
			messageIDs = append(messageIDs, messageID)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Delete queue index entries
	err = s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(queuePrefix + queue + ":")
		keysToDelete := make([][]byte, 0)
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}
		
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
			purged++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Delete all message data
	err = s.db.Update(func(txn *badger.Txn) error {
		for _, messageID := range messageIDs {
			messageKey := messagePrefix + messageID
			if err := txn.Delete([]byte(messageKey)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	
	// Reset queue stats
	err = s.db.Update(func(txn *badger.Txn) error {
		statsKey := statsPrefix + queue
		return txn.Delete([]byte(statsKey))
	})
	if err != nil && err != badger.ErrKeyNotFound {
		return 0, err
	}

	return purged, nil
}

// QueueDelete removes an entire queue
func (s *BadgerStorage) QueueDelete(ctx context.Context, queue string) error {
	_, err := s.QueuePurge(ctx, queue)
	if err != nil {
		return err
	}

	// Delete stats
	return s.db.Update(func(txn *badger.Txn) error {
		statsKey := statsPrefix + queue
		return txn.Delete([]byte(statsKey))
	})
}

// QueueList returns all queue names
func (s *BadgerStorage) QueueList(ctx context.Context) ([]string, error) {
	queueSet := make(map[string]bool)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(queuePrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {

			key := string(it.Item().Key())
			parts := strings.Split(key[len(queuePrefix):], ":")
			if len(parts) > 0 {
				queueSet[parts[0]] = true
			}
		}

		return nil
	})

	queues := make([]string, 0, len(queueSet))
	for queue := range queueSet {
		queues = append(queues, queue)
	}

	return queues, err
}

// QueuePushBatch adds multiple messages to a queue with optimized batch processing
func (s *BadgerStorage) QueuePushBatch(ctx context.Context, queue string, messages [][]byte) ([]string, error) {
	if len(messages) == 0 {
		return []string{}, nil
	}

	// Limit batch size for memory efficiency
	if len(messages) > maxBatchSize {
		return nil, fmt.Errorf("batch size %d exceeds maximum %d", len(messages), maxBatchSize)
	}

	messageIDs := make([]string, len(messages))
	now := time.Now()

	// Use write batch for better performance
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// Pre-generate all message IDs and data
	var statsUpdate int64
	for i, data := range messages {
		messageID := uuid.New().String()
		messageIDs[i] = messageID

		message := QueueMessage{
			ID:         messageID,
			Queue:      queue,
			Data:       data,
			CreatedAt:  now,
			DelayUntil: now, // No delay
			RetryCount: 0,
		}

		messageData, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}

		// Store message
		messageKey := messagePrefix + messageID
		if err := wb.Set([]byte(messageKey), messageData); err != nil {
			return nil, err
		}

		// Add to queue index (immediate message only)
		queueKey := queuePrefix + queue + ":" + fmt.Sprintf("%016d", now.UnixNano()+int64(i)) + ":" + messageID
		if err := wb.Set([]byte(queueKey), []byte(messageID)); err != nil {
			return nil, err
		}

		statsUpdate++
	}

	// Update stats once at the end
	err := s.db.Update(func(txn *badger.Txn) error {
		statsKey := statsPrefix + queue
		stats := QueueStats{Name: queue, Size: statsUpdate}
		if item, err := txn.Get([]byte(statsKey)); err == nil {
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &stats)
			}); err == nil {
				stats.Size += statsUpdate
			}
		}

		statsData, _ := json.Marshal(stats)
		return txn.Set([]byte(statsKey), statsData)
	})
	if err != nil {
		return nil, err
	}

	// Flush the batch
	if err := wb.Flush(); err != nil {
		return nil, err
	}

	return messageIDs, nil
}

// QueuePopBatch removes and returns multiple messages from a queue with optimized batch processing
func (s *BadgerStorage) QueuePopBatch(ctx context.Context, queue string, limit int, timeout time.Duration) ([]QueueMessage, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100 // Cap batch size for memory efficiency
	}

	messages := make([]QueueMessage, 0, limit)
	deadline := time.Now().Add(timeout)

	// Single transaction to get multiple messages
	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = limit
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to first message in queue
		prefix := []byte(queuePrefix + queue + ":")
		count := 0

		for it.Seek(prefix); it.ValidForPrefix(prefix) && count < limit; it.Next() {
			// Get message ID from index entry
			idxItem := it.Item()
			val, err := idxItem.ValueCopy(nil)
			if err != nil {
				continue
			}

			messageID := string(val)

			// Get message data
			messageKey := messagePrefix + messageID
			item, err := txn.Get([]byte(messageKey))
			if err != nil {
				// Clean up orphaned index entry
				txn.Delete(idxItem.Key())
				continue
			}

			var messageData []byte
			err = item.Value(func(val []byte) error {
				messageData = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				continue
			}

			var message QueueMessage
			err = json.Unmarshal(messageData, &message)
			if err != nil {
				continue
			}

			// Remove from queue index
			if err := txn.Delete(idxItem.Key()); err != nil {
				continue
			}

			messages = append(messages, message)
			count++
		}

		// Update stats once for all messages
		if len(messages) > 0 {
			statsKey := statsPrefix + queue
			stats := QueueStats{Name: queue}
			if item, err := txn.Get([]byte(statsKey)); err == nil {
				if err := item.Value(func(val []byte) error {
					return json.Unmarshal(val, &stats)
				}); err == nil {
					stats.Size -= int64(len(messages))
					stats.Processed += int64(len(messages))
				}
			}
			statsData, _ := json.Marshal(stats)
			txn.Set([]byte(statsKey), statsData)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// If we didn't get enough messages and still have time, try again
	if len(messages) == 0 && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return messages, ctx.Err()
		case <-time.After(pollInterval):
			// Try once more
			return s.QueuePopBatch(ctx, queue, limit, time.Until(deadline))
		}
	}

	return messages, nil
}
