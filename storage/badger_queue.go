package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const (
	queuePrefix     = "q:"
	messagePrefix   = "m:"
	consumerPrefix  = "c:"
	statsPrefix     = "s:"
	delayedPrefix   = "d:"
)

// QueuePush adds a message to a queue
func (s *BadgerStorage) QueuePush(ctx context.Context, queue string, data []byte, delay time.Duration) (string, error) {
	messageID := uuid.New().String()
	
	message := QueueMessage{
		ID:         messageID,
		Queue:      queue,
		Data:       data,
		CreatedAt:  time.Now(),
		DelayUntil: time.Now().Add(delay),
		RetryCount: 0,
	}

	messageData, err := json.Marshal(message)
	if err != nil {
		return "", err
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		// Store message
		messageKey := messagePrefix + messageID
		if err := txn.Set([]byte(messageKey), messageData); err != nil {
			return err
		}

		// Add to queue index
		queueKey := queuePrefix + queue + ":" + messageID
		if delay > 0 {
			// Delayed message
			delayedKey := delayedPrefix + queue + ":" + fmt.Sprintf("%d", message.DelayUntil.Unix()) + ":" + messageID
			return txn.Set([]byte(delayedKey), []byte(messageID))
		} else {
			return txn.Set([]byte(queueKey), []byte(messageID))
		}
	})

	if err != nil {
		return "", err
	}

	// Update stats
	s.updateQueueStats(ctx, queue, 1, 0, 0, 0)

	return messageID, nil
}

// QueuePop removes and returns a message from a queue
func (s *BadgerStorage) QueuePop(ctx context.Context, queue string, timeout time.Duration) (QueueMessage, error) {
	var message QueueMessage
	var found bool

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		err := s.db.Update(func(txn *badger.Txn) error {
			// Check for ready messages first
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			prefix := []byte(queuePrefix + queue + ":")
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				// Get message ID from index entry value
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

				err = json.Unmarshal(messageData, &message)
				if err != nil {
					continue
				}

				// Check if message is ready
				if time.Now().Before(message.DelayUntil) {
					continue
				}

				// Remove from queue
				if err := txn.Delete(it.Item().Key()); err != nil {
					return err
				}

				found = true
				return nil
			}

			// Check delayed messages
			delayedPref := []byte(delayedPrefix + queue + ":")
			for it.Seek(delayedPref); it.ValidForPrefix(delayedPref); it.Next() {
				key := string(it.Item().Key())
				parts := strings.Split(key, ":")
				if len(parts) < 3 {
					continue
				}

				timestamp, err := strconv.ParseInt(parts[2], 10, 64)
				if err != nil {
					continue
				}

				if time.Unix(timestamp, 0).After(time.Now()) {
					break // Not ready yet
				}

				// Message is ready, move to regular queue
				val, err := it.Item().ValueCopy(nil)
				if err != nil {
					continue
				}
				messageID := string(val)
				queueKey := queuePrefix + queue + ":" + messageID

				if err := txn.Set([]byte(queueKey), []byte(messageID)); err != nil {
					return err
				}

				if err := txn.Delete(it.Item().Key()); err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			return message, err
		}

		if found {
			break
		}

		// Wait a bit before retrying
		time.Sleep(100 * time.Millisecond)
	}

	if !found {
		return message, fmt.Errorf("no message available in queue %s", queue)
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

	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(queuePrefix + queue + ":")
		wb := s.db.NewWriteBatch()
		defer wb.Cancel()
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keyCopy := it.Item().KeyCopy(nil)
			if err := wb.Delete(keyCopy); err != nil {
				return err
			}
			purged++
		}

		return wb.Flush()
	})

	return purged, err
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