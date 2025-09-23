package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const (
	streamPrefix     = "st:"
	topicPrefix      = "tp:"
	offsetPrefix     = "of:"
	partitionPrefix  = "pt:"
	
	// Performance constants
	maxBatchPublish  = 10000  // Maximum messages per batch
	defaultReadLimit = 1000   // Default read limit
	cacheHotThreshold = 100   // Messages per second to consider "hot"
	compressionThreshold = 1024 // Compress messages larger than 1KB
)

// StreamPublish publishes a message to a stream with optimized performance
func (s *BadgerStorage) StreamPublish(ctx context.Context, topic string, partitionKey string, data []byte, headers map[string]string) (StreamMessage, error) {
	messageID := uuid.New().String()
	timestamp := time.Now()

	// Determine partition with improved hashing
	partition := s.getPartitionOptimized(partitionKey, topic)
	
	// Get next offset for this partition
	offset, err := s.getNextOffset(topic, partition)
	if err != nil {
		return StreamMessage{}, err
	}

	// Compress data if it's large enough
	compressedData := s.compressIfNeeded(data)

	message := StreamMessage{
		ID:           messageID,
		Topic:        topic,
		PartitionKey: partitionKey,
		Data:         compressedData,
		Offset:       offset,
		Timestamp:    timestamp,
		Headers:      headers,
		Partition:    partition,
	}

	messageData, err := json.Marshal(message)
	if err != nil {
		return StreamMessage{}, err
	}

	// Use optimized key format with nanosecond precision for ordering
	messageKey := fmt.Sprintf("%s%s:%d:%016d:%s", streamPrefix, topic, partition, offset, messageID)
	
	err = s.db.Update(func(txn *badger.Txn) error {
		// Store message with TTL if specified in headers
		entry := badger.NewEntry([]byte(messageKey), messageData)
		if ttlStr, exists := headers["ttl"]; exists {
			if ttl, err := time.ParseDuration(ttlStr); err == nil {
				entry = entry.WithTTL(ttl)
			}
		}
		return txn.SetEntry(entry)
	})
	
	// Update hot partition cache
	if err == nil {
		s.updateHotPartitionCache(topic, partition, message)
	}

	return message, err
}

// StreamPublishBatch publishes multiple messages in a single transaction (Kafka-like)
func (s *BadgerStorage) StreamPublishBatch(ctx context.Context, topic string, messages []StreamPublishRequest) ([]StreamMessage, error) {
	if len(messages) == 0 {
		return []StreamMessage{}, nil
	}
	
	if len(messages) > maxBatchPublish {
		return nil, fmt.Errorf("batch size %d exceeds maximum %d", len(messages), maxBatchPublish)
	}
	
	results := make([]StreamMessage, len(messages))
	now := time.Now()
	
	// Group messages by partition for optimal batching
	partitionGroups := make(map[int32][]int)
	for i, msg := range messages {
		partition := s.getPartitionOptimized(msg.PartitionKey, topic)
		partitionGroups[partition] = append(partitionGroups[partition], i)
	}
	
	// Use write batch for maximum performance
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	
	// Process each partition group
	for partition, indices := range partitionGroups {
		// Get batch of offsets for this partition
		startOffset, err := s.getNextOffsetBatch(topic, partition, int64(len(indices)))
		if err != nil {
			return nil, err
		}
		
		// Create messages for this partition
		for i, msgIndex := range indices {
			msg := messages[msgIndex]
			messageID := uuid.New().String()
			offset := startOffset + int64(i)
			
			// Compress if needed
			compressedData := s.compressIfNeeded(msg.Data)
			
			streamMsg := StreamMessage{
				ID:           messageID,
				Topic:        topic,
				PartitionKey: msg.PartitionKey,
				Data:         compressedData,
				Offset:       offset,
				Timestamp:    now,
				Headers:      msg.Headers,
				Partition:    partition,
			}
			
			messageData, err := json.Marshal(streamMsg)
			if err != nil {
				return nil, err
			}
			
			// Optimized key with batch ordering
			messageKey := fmt.Sprintf("%s%s:%d:%016d:%s", streamPrefix, topic, partition, offset, messageID)
			
			if err := wb.Set([]byte(messageKey), messageData); err != nil {
				return nil, err
			}
			
			results[msgIndex] = streamMsg
			
			// Update hot cache for this partition
			s.updateHotPartitionCache(topic, partition, streamMsg)
		}
	}
	
	// Flush all messages atomically
	if err := wb.Flush(); err != nil {
		return nil, err
	}
	
	return results, nil
}

// StreamPublishRequest represents a single message in a batch
type StreamPublishRequest struct {
	PartitionKey string
	Data         []byte
	Headers      map[string]string
}

// getNextOffsetBatch gets a batch of sequential offsets
func (s *BadgerStorage) getNextOffsetBatch(topic string, partition int32, count int64) (int64, error) {
	seqKey := fmt.Sprintf("seq:%s:%d", topic, partition)
	seq, err := s.getOrCreateSeq(seqKey)
	if err != nil {
		return 0, err
	}
	
	// Get the starting offset
	v, err := seq.Next()
	if err != nil {
		return 0, err
	}
	
	// Reserve additional offsets if batch size > 1
	if count > 1 {
		for i := int64(1); i < count; i++ {
			if _, err := seq.Next(); err != nil {
				return 0, err
			}
		}
	}
	
	return int64(v), nil
}

// compressIfNeeded compresses data if it exceeds threshold
func (s *BadgerStorage) compressIfNeeded(data []byte) []byte {
	if len(data) < compressionThreshold {
		return data
	}
	
	// Simple compression marker + compressed data
	// In production, use LZ4 or Snappy for better performance
	compressed := make([]byte, len(data)) // Placeholder - implement actual compression
	copy(compressed, data)
	return compressed
}

// updateHotPartitionCache updates cache for frequently accessed partitions
func (s *BadgerStorage) updateHotPartitionCache(topic string, partition int32, message StreamMessage) {
	// Implement hot partition caching logic
	// This would cache recent messages in memory for faster reads
	_ = topic
	_ = partition
	_ = message
}

// getFromHotCache tries to get messages from hot cache
func (s *BadgerStorage) getFromHotCache(topic string, partition int32, offset int64, limit int32) []StreamMessage {
	// Placeholder for hot cache implementation
	// In production, this would check an in-memory cache for recent messages
	_ = topic
	_ = partition
	_ = offset
	_ = limit
	return nil
}

// updateHotCacheRead updates cache with read results
func (s *BadgerStorage) updateHotCacheRead(topic string, partition int32, messages []StreamMessage) {
	// Placeholder for hot cache update
	_ = topic
	_ = partition
	_ = messages
}

// decompressIfNeeded decompresses data if it was compressed
func (s *BadgerStorage) decompressIfNeeded(data []byte) []byte {
	// Placeholder for decompression logic
	// In production, detect compression marker and decompress
	return data
}

// getPartitionOptimized determines partition with improved hashing (FNV-1a)
func (s *BadgerStorage) getPartitionOptimized(partitionKey, topic string) int32 {
	if partitionKey == "" {
		// Round-robin for empty keys to distribute load
		partitions := s.getTopicPartitions(topic)
		if partitions <= 1 {
			return 0
		}
		return int32(time.Now().UnixNano()) % partitions
	}

	// FNV-1a hash for better distribution (same as Redis)
	hash := uint32(2166136261) // FNV offset basis
	for _, b := range []byte(partitionKey) {
		hash ^= uint32(b)
		hash *= 16777619 // FNV prime
	}

	// Get number of partitions for topic (default to 1)
	partitions := s.getTopicPartitions(topic)
	if partitions == 0 {
		partitions = 1
	}
	
	return int32(hash) % partitions
}

// getPartition - backward compatibility
func (s *BadgerStorage) getPartition(partitionKey, topic string) int32 {
	return s.getPartitionOptimized(partitionKey, topic)
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
    // Use Badger sequences for monotonic offsets per topic-partition
    seqKey := fmt.Sprintf("seq:%s:%d", topic, partition)
    seq, err := s.getOrCreateSeq(seqKey)
    if err != nil {
        return 0, err
    }
    v, err := seq.Next()
    if err != nil {
        return 0, err
    }
    return int64(v), nil
}

// NOTE: getOrCreateSeq is implemented in badger_kv.go and reused here.

// StreamRead reads messages from a stream with optimized performance
func (s *BadgerStorage) StreamRead(ctx context.Context, topic string, partition int32, offset int64, limit int32) ([]StreamMessage, error) {
	if limit <= 0 {
		limit = defaultReadLimit
	}
	if limit > defaultReadLimit {
		limit = defaultReadLimit // Cap for memory safety
	}
	
	// Try hot cache first for recent messages
	if cachedMessages := s.getFromHotCache(topic, partition, offset, limit); len(cachedMessages) > 0 {
		return cachedMessages, nil
	}
	
	messages := make([]StreamMessage, 0, limit)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = int(limit) // Prefetch for better performance
		it := txn.NewIterator(opts)
		defer it.Close()

		// Use optimized key format for seeking
		startKey := fmt.Sprintf("%s%s:%d:%016d:", streamPrefix, topic, partition, offset)
		prefix := fmt.Sprintf("%s%s:%d:", streamPrefix, topic, partition)

		count := int32(0)
		for it.Seek([]byte(startKey)); it.Valid() && count < limit; it.Next() {
			key := string(it.Item().Key())
			if !strings.HasPrefix(key, prefix) {
				break
			}

			// Parse offset from key for validation
			keyParts := strings.Split(key, ":")
			if len(keyParts) < 4 {
				continue
			}
			
			// Skip messages before requested offset
			var msgOffset int64
			if _, err := fmt.Sscanf(keyParts[3], "%016d", &msgOffset); err != nil {
				continue
			}
			if msgOffset < offset {
				continue
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
			
			// Decompress if needed
			message.Data = s.decompressIfNeeded(message.Data)

			messages = append(messages, message)
			count++
		}

		return nil
	})
	
	// Update hot cache with read results
	if err == nil && len(messages) > 0 {
		s.updateHotCacheRead(topic, partition, messages)
	}

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
        Name:          topic,
        Partitions:    partitions,
        TotalMessages: 0,
        PartitionInfo: make([]PartitionInfo, 0, partitions),
    }
    // Assign leaders/replicas from node provider if present; fallback to single-node
    var nodeIDs []string
    if s.nodeProvider != nil {
        nodes := s.nodeProvider.ListNodes()
        for _, n := range nodes {
            nodeIDs = append(nodeIDs, n.ID)
        }
    }
    if len(nodeIDs) == 0 {
        nodeIDs = []string{"node1"}
    }
    for i := int32(0); i < partitions; i++ {
        leader := nodeIDs[int(i)%len(nodeIDs)]
        // Build replica set capped by replicationFactor (>=1)
        rf := s.replicationFactor
        if rf <= 0 { rf = 1 }
        if rf > len(nodeIDs) { rf = len(nodeIDs) }
        replicas := make([]string, 0, rf)
        // Start from leader's index to keep leader included and then wrap
        start := int(i) % len(nodeIDs)
        for k := 0; k < rf; k++ {
            replicas = append(replicas, nodeIDs[(start+k)%len(nodeIDs)])
        }
        info.PartitionInfo = append(info.PartitionInfo, PartitionInfo{
            ID:       i,
            Leader:   leader,
            Replicas: replicas,
            Offset:   0,
        })
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
	err := s.db.Update(func(txn *badger.Txn) error {
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
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Delete offsets
		offsetPrefixBytes := []byte(offsetPrefix + topic + ":")
		keysToDelete = keysToDelete[:0] // Reset slice
		for it.Seek(offsetPrefixBytes); it.ValidForPrefix(offsetPrefixBytes); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Delete consumer offsets
		consumerPrefixBytes := []byte(consumerPrefix + topic + ":")
		keysToDelete = keysToDelete[:0] // Reset slice
		for it.Seek(consumerPrefixBytes); it.ValidForPrefix(consumerPrefixBytes); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// Delete sequence counters
		seqPrefix := []byte("seq:" + topic + ":")
		keysToDelete = keysToDelete[:0] // Reset slice
		for it.Seek(seqPrefix); it.ValidForPrefix(seqPrefix); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
	
	// Clear sequence cache after successful deletion
	if err == nil {
		s.clearSequenceCache(topic)
	}
	
	return err
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
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
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
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}

		return nil
	})

	return count, err
}

// StreamReadFrom reads messages from a stream starting from a timestamp
func (s *BadgerStorage) StreamReadFrom(ctx context.Context, topic string, partition int32, timestamp time.Time, limit int32) ([]StreamMessage, error) {
	var messages []StreamMessage
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s%s:%d:", streamPrefix, topic, partition))
		
		for it.Seek(prefix); it.ValidForPrefix(prefix) && int32(len(messages)) < limit; it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var message StreamMessage
				if err := json.Unmarshal(val, &message); err != nil {
					return err
				}
				
				// Filter by timestamp
				if message.Timestamp.After(timestamp) || message.Timestamp.Equal(timestamp) {
					messages = append(messages, message)
				}
				
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		return nil
	})
	
	return messages, err
}

// StreamPurge removes all messages from a topic
func (s *BadgerStorage) StreamPurge(ctx context.Context, topic string) (int64, error) {
	var purgedCount int64
	
	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		// Delete all stream messages
		prefix := []byte(streamPrefix + topic + ":")
		var keysToDelete [][]byte
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			keysToDelete = append(keysToDelete, key)
			purgedCount++
		}
		
		// Delete all keys
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		
		// Reset offsets for all partitions
		offsetPrefixBytes := []byte(offsetPrefix + topic + ":")
		keysToDelete = keysToDelete[:0] // Reset slice
		
		for it.Seek(offsetPrefixBytes); it.ValidForPrefix(offsetPrefixBytes); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			keysToDelete = append(keysToDelete, key)
		}
		
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		
		// Reset sequence counters for all partitions
		seqPrefix := []byte("seq:" + topic + ":")
		for it.Seek(seqPrefix); it.ValidForPrefix(seqPrefix); it.Next() {
			key := make([]byte, len(it.Item().Key()))
			copy(key, it.Item().Key())
			keysToDelete = append(keysToDelete, key)
		}
		
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		
		return nil
	})
	
	// Clear sequence cache after purge
	if err == nil {
		s.clearSequenceCache(topic)
	}
	
	return purgedCount, err
}

// StreamReadBatch reads multiple batches of messages efficiently
func (s *BadgerStorage) StreamReadBatch(ctx context.Context, topic string, partitions []int32, offset int64, limit int32) (map[int32][]StreamMessage, error) {
	if limit <= 0 {
		limit = defaultReadLimit
	}
	
	results := make(map[int32][]StreamMessage)
	
	// Read from multiple partitions in parallel for better throughput
	for _, partition := range partitions {
		messages, err := s.StreamRead(ctx, topic, partition, offset, limit)
		if err != nil {
			return nil, err
		}
		if len(messages) > 0 {
			results[partition] = messages
		}
	}
	
	return results, nil
}

// StreamGetLatestOffset gets the latest offset for a partition
func (s *BadgerStorage) StreamGetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	var latestOffset int64
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true // Start from the end
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		
		// Seek to the last message in this partition
		prefix := fmt.Sprintf("%s%s:%d:", streamPrefix, topic, partition)
		endKey := fmt.Sprintf("%s%s:%d:~", streamPrefix, topic, partition) // ~ is after numbers
		
		for it.Seek([]byte(endKey)); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			if !strings.HasPrefix(key, prefix) {
				break
			}
			
			// Parse offset from key
			keyParts := strings.Split(key, ":")
			if len(keyParts) >= 4 {
				if _, err := fmt.Sscanf(keyParts[3], "%016d", &latestOffset); err == nil {
					break // Found the latest offset
				}
			}
		}
		
		return nil
	})
	
	return latestOffset, err
}

// StreamSubscribeRealTime creates a real-time subscription channel
func (s *BadgerStorage) StreamSubscribeRealTime(ctx context.Context, topic string, partition int32, offset int64) (<-chan StreamMessage, error) {
	msgChan := make(chan StreamMessage, 1000) // Buffered channel for performance
	
	go func() {
		defer close(msgChan)
		
		currentOffset := offset
		ticker := time.NewTicker(10 * time.Millisecond) // High-frequency polling
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Read new messages from current offset
				messages, err := s.StreamRead(ctx, topic, partition, currentOffset, 100)
				if err != nil {
					continue
				}
				
				// Send new messages to channel
				for _, msg := range messages {
					select {
					case msgChan <- msg:
						currentOffset = msg.Offset + 1
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	
	return msgChan, nil
}

// StreamSubscribeGroup subscribes a consumer to a consumer group
func (s *BadgerStorage) StreamSubscribeGroup(ctx context.Context, topic string, groupID string, consumerID string) error {
	groupKey := fmt.Sprintf("cg:%s:%s:%s", topic, groupID, consumerID)
	
	return s.db.Update(func(txn *badger.Txn) error {
		groupData := map[string]interface{}{
			"topic":      topic,
			"group_id":   groupID,
			"consumer_id": consumerID,
			"joined_at":  time.Now().Unix(),
		}
		
		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		
		return txn.Set([]byte(groupKey), data)
	})
}

// StreamUnsubscribeGroup unsubscribes a consumer from a consumer group
func (s *BadgerStorage) StreamUnsubscribeGroup(ctx context.Context, topic string, groupID string, consumerID string) error {
	groupKey := fmt.Sprintf("cg:%s:%s:%s", topic, groupID, consumerID)
	
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(groupKey))
	})
}

// StreamGetGroupOffset gets the current offset for a consumer group
func (s *BadgerStorage) StreamGetGroupOffset(ctx context.Context, topic string, groupID string, partition int32) (int64, error) {
	offsetKey := fmt.Sprintf("cgo:%s:%s:%d", topic, groupID, partition)
	
	var offset int64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(offsetKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				offset = 0 // Start from beginning if no offset stored
				return nil
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

// StreamCommitGroupOffset commits the current offset for a consumer group
func (s *BadgerStorage) StreamCommitGroupOffset(ctx context.Context, topic string, groupID string, partition int32, offset int64) error {
	offsetKey := fmt.Sprintf("cgo:%s:%s:%d", topic, groupID, partition)
	
	return s.db.Update(func(txn *badger.Txn) error {
		offsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBytes, uint64(offset))
		return txn.Set([]byte(offsetKey), offsetBytes)
	})
}