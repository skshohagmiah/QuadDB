package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const (
	streamPrefix   = "st:"
	topicPrefix    = "tp:"
	offsetPrefix   = "of:"
	partitionPrefix = "pt:"
)

// StreamPublish publishes a message to a stream
func (s *BadgerStorage) StreamPublish(ctx context.Context, topic string, partitionKey string, data []byte, headers map[string]string) (StreamMessage, error) {
	messageID := uuid.New().String()
	timestamp := time.Now()

	// Determine partition (simple hash-based partitioning)
	partition := s.getPartition(partitionKey, topic)
	
	// Get next offset for this partition
	offset, err := s.getNextOffset(topic, partition)
	if err != nil {
		return StreamMessage{}, err
	}

	message := StreamMessage{
		ID:           messageID,
		Topic:        topic,
		PartitionKey: partitionKey,
		Data:         data,
		Offset:       offset,
		Timestamp:    timestamp,
		Headers:      headers,
		Partition:    partition,
	}

	messageData, err := json.Marshal(message)
	if err != nil {
		return StreamMessage{}, err
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		// Store message
		messageKey := fmt.Sprintf("%s%s:%d:%d", streamPrefix, topic, partition, offset)
		return txn.Set([]byte(messageKey), messageData)
	})

	return message, err
}

// getPartition determines which partition to use for a message
func (s *BadgerStorage) getPartition(partitionKey, topic string) int32 {
	if partitionKey == "" {
		return 0
	}

	// Simple hash-based partitioning
	hash := int32(0)
	for _, b := range []byte(partitionKey) {
		hash = hash*31 + int32(b)
	}

	// Get number of partitions for topic (default to 1)
	partitions := s.getTopicPartitions(topic)
	if partitions == 0 {
		partitions = 1
	}

	if hash < 0 {
		hash = -hash
	}
	
	return hash % partitions
}

// getTopicPartitions returns the number of partitions for a topic
func (s *BadgerStorage) getTopicPartitions(topic string) int32 {
	var partitions int32 = 1

	s.db.View(func(txn *badger.Txn) error {
		topicKey := topicPrefix + topic
		item, err := txn.Get([]byte(topicKey))
		if err != nil {
			return nil // Use default
		}

		err = item.Value(func(val []byte) error {
			var info TopicInfo
			if err := json.Unmarshal(val, &info); err == nil {
				partitions = info.Partitions
			}
			return nil
		})
		
		return nil
	})

	return partitions
}

// getNextOffset returns the next offset for a topic partition
func (s *BadgerStorage) getNextOffset(topic string, partition int32) (int64, error) {
	var nextOffset int64

	err := s.db.Update(func(txn *badger.Txn) error {
		offsetKey := fmt.Sprintf("%s%s:%d", offsetPrefix, topic, partition)
		
		item, err := txn.Get([]byte(offsetKey))
		if err == nil {
			err = item.Value(func(val []byte) error {
				if len(val) >= 8 {
					nextOffset = int64(binary.BigEndian.Uint64(val))
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		nextOffset++
		
		// Update offset
		offsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBytes, uint64(nextOffset))
		return txn.Set([]byte(offsetKey), offsetBytes)
	})

	return nextOffset, err
}

// StreamRead reads messages from a stream
func (s *BadgerStorage) StreamRead(ctx context.Context, topic string, partition int32, offset int64, limit int32) ([]StreamMessage, error) {
	var messages []StreamMessage

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := fmt.Sprintf("%s%s:%d:%d", streamPrefix, topic, partition, offset)
		prefix := fmt.Sprintf("%s%s:%d:", streamPrefix, topic, partition)

		count := int32(0)
		for it.Seek([]byte(startKey)); it.Valid() && count < limit; it.Next() {
			key := string(it.Item().Key())
			if !strings.HasPrefix(key, prefix) {
				break
			}

			var messageData []byte
			err := it.Item().Value(func(val []byte) error {
				messageData = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				continue
			}

			var message StreamMessage
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

// StreamSeek sets the offset for a consumer
func (s *BadgerStorage) StreamSeek(ctx context.Context, topic string, consumerID string, partition int32, offset int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		consumerKey := fmt.Sprintf("%s%s:%s:%d", consumerPrefix, topic, consumerID, partition)
		
		offsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBytes, uint64(offset))
		return txn.Set([]byte(consumerKey), offsetBytes)
	})
}

// StreamGetOffset gets the current offset for a consumer
func (s *BadgerStorage) StreamGetOffset(ctx context.Context, topic string, consumerID string, partition int32) (int64, error) {
	var offset int64

	err := s.db.View(func(txn *badger.Txn) error {
		consumerKey := fmt.Sprintf("%s%s:%s:%d", consumerPrefix, topic, consumerID, partition)
		
		item, err := txn.Get([]byte(consumerKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Return 0
			}
			return err
		}

		return item.Value(func(val []byte) error {
			if len(val) >= 8 {
				offset = int64(binary.BigEndian.Uint64(val))
			}
			return nil
		})
	})

	return offset, err
}

// StreamCreateTopic creates a new topic
func (s *BadgerStorage) StreamCreateTopic(ctx context.Context, topic string, partitions int32) error {
	info := TopicInfo{
		Name:       topic,
		Partitions: partitions,
		TotalMessages: 0,
	}

	infoData, err := json.Marshal(info)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		topicKey := topicPrefix + topic
		return txn.Set([]byte(topicKey), infoData)
	})
}

// StreamDeleteTopic deletes a topic and all its messages
func (s *BadgerStorage) StreamDeleteTopic(ctx context.Context, topic string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Delete topic info
		topicKey := topicPrefix + topic
		if err := txn.Delete([]byte(topicKey)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		// Delete all messages
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(streamPrefix + topic + ":")
		keysToDelete := [][]byte{}
		
		for it.Seek(prefix); it.Valid(); it.Next() {
			if !it.Item().HasPrefix(prefix) {
				break
			}
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Delete offsets
		offsetPrefixBytes := []byte(offsetPrefix + topic + ":")
		for it.Seek(offsetPrefixBytes); it.Valid(); it.Next() {
			if !it.Item().HasPrefix(offsetPrefixBytes) {
				break
			}
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Delete consumer offsets
		consumerPrefixBytes := []byte(consumerPrefix + topic + ":")
		for it.Seek(consumerPrefixBytes); it.Valid(); it.Next() {
			if !it.Item().HasPrefix(consumerPrefixBytes) {
				break
			}
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}

// StreamListTopics lists all topics
func (s *BadgerStorage) StreamListTopics(ctx context.Context) ([]string, error) {
	var topics []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(topicPrefix)
		for it.Seek(prefix); it.Valid(); it.Next() {
			if !it.Item().HasPrefix(prefix) {
				break
			}

			key := string(it.Item().Key())
			topic := key[len(topicPrefix):]
			topics = append(topics, topic)
		}

		return nil
	})

	return topics, err
}

// StreamGetTopicInfo gets information about a topic
func (s *BadgerStorage) StreamGetTopicInfo(ctx context.Context, topic string) (TopicInfo, error) {
	var info TopicInfo

	err := s.db.View(func(txn *badger.Txn) error {
		topicKey := topicPrefix + topic
		item, err := txn.Get([]byte(topicKey))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &info)
		})
	})

	// Count messages
	if err == nil {
		messageCount, _ := s.countTopicMessages(topic)
		info.TotalMessages = messageCount
	}

	return info, err
}

// countTopicMessages counts total messages in a topic
func (s *BadgerStorage) countTopicMessages(topic string) (int64, error) {
	var count int64

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(streamPrefix + topic + ":")
		for it.Seek(prefix); it.Valid(); it.Next() {
			if !it.Item().HasPrefix(prefix) {
				break
			}
			count++
		}

		return nil
	})

	return count, err
}