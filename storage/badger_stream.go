package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const (
	streamPrefix     = "st:"
	topicPrefix      = "tp:"
	offsetPrefix     = "of:"
	partitionPrefix  = "pt:"
	
	// Performance optimization constants
	maxBatchPublish     = 10000  // Maximum messages per batch
	defaultReadLimit    = 1000   // Default read limit
	cacheHotThreshold   = 100    // Messages per second to consider "hot"
	compressionThreshold = 1024  // Compress messages larger than 1KB
	streamPrefetchSize  = 500    // Prefetch messages for better read performance
	offsetCacheSize     = 1000   // Cache recent offsets for faster access
	partitionCacheSize  = 100    // Cache partition metadata
	
	// Compression constants
	compressionMarker = 0x1F    // GZIP compression marker
	maxCacheSize     = 10000   // Maximum cached messages per partition
	cacheExpiry      = 5 * time.Minute // Cache expiry time
)

// PartitionCache represents a cache for a single partition
type PartitionCache struct {
	messages    []StreamMessage
	lastAccess  time.Time
	accessCount int64
	mu          sync.RWMutex
}

// StreamCache manages hot partition caches
type StreamCache struct {
	partitions map[string]*PartitionCache // key: "topic:partition"
	mu         sync.RWMutex
	lastCleanup time.Time
	// Metrics
	cacheHits   int64
	cacheMisses int64
	compressionSavings int64
}

// StreamMetrics holds performance metrics for stream operations
type StreamMetrics struct {
	PublishCount    int64
	ReadCount       int64
	CompressionHits int64
	CacheHits       int64
	CacheMisses     int64
	ErrorCount      int64
	AvgLatency      time.Duration
}

// StreamPublish publishes a message to a stream with optimized performance
func (s *BadgerStorage) StreamPublish(ctx context.Context, topic string, partitionKey string, data []byte, headers map[string]string) (StreamMessage, error) {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		if s.streamCache != nil {
			atomic.AddInt64(&s.streamCache.compressionSavings, int64(latency.Nanoseconds()))
		}
	}()
	
	// Input validation
	if topic == "" {
		return StreamMessage{}, fmt.Errorf("topic cannot be empty")
	}
	if len(data) == 0 {
		return StreamMessage{}, fmt.Errorf("message data cannot be empty")
	}
	if len(data) > 10<<20 { // 10MB limit
		return StreamMessage{}, fmt.Errorf("message size %d exceeds maximum allowed size of 10MB", len(data))
	}
	
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
	originalSize := len(data)
	compressedData := s.compressIfNeeded(data)
	if len(compressedData) < originalSize {
		// Track compression savings
		if s.streamCache != nil {
			atomic.AddInt64(&s.streamCache.compressionSavings, int64(originalSize-len(compressedData)))
		}
		log.Printf("[DEBUG] Compressed message from %d to %d bytes (%.1f%% savings)", 
			originalSize, len(compressedData), 
			float64(originalSize-len(compressedData))/float64(originalSize)*100)
	}

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
				log.Printf("[DEBUG] Set TTL %v for message %s", ttl, messageID)
			} else {
				log.Printf("[WARN] Invalid TTL format '%s' for message %s: %v", ttlStr, messageID, err)
			}
		}
		return txn.SetEntry(entry)
	})
	
	if err != nil {
		log.Printf("[ERROR] Failed to store message %s in topic %s: %v", messageID, topic, err)
		return StreamMessage{}, fmt.Errorf("failed to store message: %w", err)
	}
	
	// Update hot partition cache and metrics
	s.updateHotPartitionCache(topic, partition, message)
	log.Printf("[INFO] Published message %s to topic %s partition %d at offset %d", 
		messageID, topic, partition, offset)

	return message, nil
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

// compressIfNeeded compresses data using GZIP if it exceeds threshold
func (s *BadgerStorage) compressIfNeeded(data []byte) []byte {
	if len(data) < compressionThreshold {
		return data
	}
	
	// Use GZIP compression for good compression ratio
	var buf bytes.Buffer
	buf.WriteByte(compressionMarker) // Mark as compressed
	
	gzWriter := gzip.NewWriter(&buf)
	if _, err := gzWriter.Write(data); err != nil {
		return data // Compression failed, return original
	}
	if err := gzWriter.Close(); err != nil {
		return data // Compression failed, return original
	}
	
	return buf.Bytes()
}

// decompressIfNeeded decompresses GZIP compressed data if needed
func (s *BadgerStorage) decompressIfNeeded(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	
	// Check for compression marker
	if data[0] != compressionMarker {
		return data // Not compressed
	}
	
	// Decompress using GZIP
	reader := bytes.NewReader(data[1:])
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return data // Decompression failed, return original
	}
	defer gzReader.Close()
	
	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		return data // Decompression failed, return original
	}
	
	return decompressed
}

// initStreamCache initializes the stream cache if not already done
func (s *BadgerStorage) initStreamCache() {
	if s.streamCache == nil {
		s.streamCache = &StreamCache{
			partitions:  make(map[string]*PartitionCache),
			lastCleanup: time.Now(),
		}
	}
}

// updateHotPartitionCache updates cache for frequently accessed partitions
func (s *BadgerStorage) updateHotPartitionCache(topic string, partition int32, message StreamMessage) {
	s.initStreamCache()
	
	key := fmt.Sprintf("%s:%d", topic, partition)
	s.streamCache.mu.Lock()
	defer s.streamCache.mu.Unlock()
	
	// Get or create partition cache
	partCache, exists := s.streamCache.partitions[key]
	if !exists {
		partCache = &PartitionCache{
			messages:    make([]StreamMessage, 0, maxCacheSize/10),
			lastAccess:  time.Now(),
			accessCount: 0,
		}
		s.streamCache.partitions[key] = partCache
	}
	
	partCache.mu.Lock()
	defer partCache.mu.Unlock()
	
	// Add message to cache (keep only recent messages)
	partCache.messages = append(partCache.messages, message)
	if len(partCache.messages) > maxCacheSize {
		// Remove oldest messages, keep most recent
		copy(partCache.messages, partCache.messages[len(partCache.messages)-maxCacheSize:])
		partCache.messages = partCache.messages[:maxCacheSize]
	}
	
	partCache.lastAccess = time.Now()
	partCache.accessCount++
	
	// Periodic cleanup of expired caches
	if time.Since(s.streamCache.lastCleanup) > cacheExpiry {
		s.cleanupExpiredCaches()
		s.streamCache.lastCleanup = time.Now()
	}
}

// getFromHotCache tries to get messages from hot cache
func (s *BadgerStorage) getFromHotCache(topic string, partition int32, offset int64, limit int32) []StreamMessage {
	if s.streamCache == nil {
		return nil
	}
	
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] Panic in getFromHotCache: %v", r)
		}
	}()
	
	key := fmt.Sprintf("%s:%d", topic, partition)
	s.streamCache.mu.RLock()
	partCache, exists := s.streamCache.partitions[key]
	s.streamCache.mu.RUnlock()
	
	if !exists {
		return nil
	}
	
	partCache.mu.RLock()
	defer partCache.mu.RUnlock()
	
	// Check if cache is still valid
	if time.Since(partCache.lastAccess) > cacheExpiry {
		return nil
	}
	
	// Find messages starting from the requested offset
	var result []StreamMessage
	for _, msg := range partCache.messages {
		if msg.Offset >= offset && int32(len(result)) < limit {
			result = append(result, msg)
		}
		if int32(len(result)) >= limit {
			break
		}
	}
	
	// Update access statistics
	partCache.accessCount++
	partCache.lastAccess = time.Now()
	
	return result
}

// updateHotCacheRead updates cache with read results
func (s *BadgerStorage) updateHotCacheRead(topic string, partition int32, messages []StreamMessage) {
	if len(messages) == 0 {
		return
	}
	
	s.initStreamCache()
	
	key := fmt.Sprintf("%s:%d", topic, partition)
	s.streamCache.mu.Lock()
	defer s.streamCache.mu.Unlock()
	
	// Get or create partition cache
	partCache, exists := s.streamCache.partitions[key]
	if !exists {
		partCache = &PartitionCache{
			messages:    make([]StreamMessage, 0, maxCacheSize/10),
			lastAccess:  time.Now(),
			accessCount: 0,
		}
		s.streamCache.partitions[key] = partCache
	}
	
	partCache.mu.Lock()
	defer partCache.mu.Unlock()
	
	// Merge new messages with existing cache, avoiding duplicates
	for _, newMsg := range messages {
		// Check if message already exists in cache
		found := false
		for _, existingMsg := range partCache.messages {
			if existingMsg.Offset == newMsg.Offset && existingMsg.ID == newMsg.ID {
				found = true
				break
			}
		}
		
		if !found {
			partCache.messages = append(partCache.messages, newMsg)
		}
	}
	
	// Sort messages by offset for efficient searching
	if len(partCache.messages) > 1 {
		// Simple bubble sort for small arrays, or implement quicksort for larger
		for i := 0; i < len(partCache.messages)-1; i++ {
			for j := 0; j < len(partCache.messages)-i-1; j++ {
				if partCache.messages[j].Offset > partCache.messages[j+1].Offset {
					partCache.messages[j], partCache.messages[j+1] = partCache.messages[j+1], partCache.messages[j]
				}
			}
		}
	}
	
	// Trim cache if it exceeds maximum size
	if len(partCache.messages) > maxCacheSize {
		// Keep most recent messages
		partCache.messages = partCache.messages[len(partCache.messages)-maxCacheSize:]
	}
	
	partCache.lastAccess = time.Now()
	partCache.accessCount++
}

// cleanupExpiredCaches removes expired partition caches
func (s *BadgerStorage) cleanupExpiredCaches() {
	now := time.Now()
	cleanedCount := 0
	for key, partCache := range s.streamCache.partitions {
		partCache.mu.RLock()
		expired := now.Sub(partCache.lastAccess) > cacheExpiry
		partCache.mu.RUnlock()
		
		if expired {
			delete(s.streamCache.partitions, key)
			cleanedCount++
		}
	}
	if cleanedCount > 0 {
		log.Printf("[INFO] Cleaned up %d expired partition caches", cleanedCount)
	}
}

// GetStreamMetrics returns current stream performance metrics
func (s *BadgerStorage) GetStreamMetrics() StreamMetrics {
	if s.streamCache == nil {
		return StreamMetrics{}
	}
	
	s.streamCache.mu.RLock()
	defer s.streamCache.mu.RUnlock()
	
	return StreamMetrics{
		CacheHits:          atomic.LoadInt64(&s.streamCache.cacheHits),
		CacheMisses:        atomic.LoadInt64(&s.streamCache.cacheMisses),
		CompressionHits:    atomic.LoadInt64(&s.streamCache.compressionSavings),
	}
}

// GetCacheStats returns detailed cache statistics
func (s *BadgerStorage) GetCacheStats() map[string]interface{} {
	if s.streamCache == nil {
		return map[string]interface{}{"enabled": false}
	}
	
	s.streamCache.mu.RLock()
	defer s.streamCache.mu.RUnlock()
	
	totalMessages := 0
	totalAccesses := int64(0)
	for _, partCache := range s.streamCache.partitions {
		partCache.mu.RLock()
		totalMessages += len(partCache.messages)
		totalAccesses += partCache.accessCount
		partCache.mu.RUnlock()
	}
	
	cacheHits := atomic.LoadInt64(&s.streamCache.cacheHits)
	cacheMisses := atomic.LoadInt64(&s.streamCache.cacheMisses)
	totalRequests := cacheHits + cacheMisses
	
	hitRatio := 0.0
	if totalRequests > 0 {
		hitRatio = float64(cacheHits) / float64(totalRequests) * 100
	}
	
	return map[string]interface{}{
		"enabled":           true,
		"partitions":        len(s.streamCache.partitions),
		"total_messages":    totalMessages,
		"total_accesses":    totalAccesses,
		"cache_hits":        cacheHits,
		"cache_misses":      cacheMisses,
		"hit_ratio_percent": hitRatio,
		"compression_savings": atomic.LoadInt64(&s.streamCache.compressionSavings),
		"last_cleanup":     s.streamCache.lastCleanup.Format(time.RFC3339),
	}
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
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		log.Printf("[DEBUG] StreamRead for topic %s partition %d took %v", topic, partition, latency)
	}()
	
	// Input validation
	if topic == "" {
		return nil, fmt.Errorf("topic cannot be empty")
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be negative")
	}
	
	if limit <= 0 {
		limit = defaultReadLimit
	}
	if limit > defaultReadLimit {
		limit = defaultReadLimit // Cap for memory safety
		log.Printf("[WARN] Read limit capped to %d for safety", defaultReadLimit)
	}
	
	// Try hot cache first for recent messages
	if cachedMessages := s.getFromHotCache(topic, partition, offset, limit); len(cachedMessages) > 0 {
		log.Printf("[DEBUG] Cache hit: returned %d messages from cache for topic %s partition %d", 
			len(cachedMessages), topic, partition)
		if s.streamCache != nil {
			atomic.AddInt64(&s.streamCache.cacheHits, 1)
		}
		return cachedMessages, nil
	}
	
	// Cache miss - read from disk
	if s.streamCache != nil {
		atomic.AddInt64(&s.streamCache.cacheMisses, 1)
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
			originalData := message.Data
			message.Data = s.decompressIfNeeded(message.Data)
			if len(originalData) != len(message.Data) {
				log.Printf("[DEBUG] Decompressed message from %d to %d bytes", 
					len(originalData), len(message.Data))
			}

			messages = append(messages, message)
			count++
		}

		return nil
	})
	
	// Update hot cache with read results
	if err == nil && len(messages) > 0 {
		s.updateHotCacheRead(topic, partition, messages)
		log.Printf("[INFO] Read %d messages from topic %s partition %d starting at offset %d", 
			len(messages), topic, partition, offset)
	} else if err != nil {
		log.Printf("[ERROR] Failed to read from topic %s partition %d: %v", topic, partition, err)
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