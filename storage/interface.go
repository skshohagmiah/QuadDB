package storage

import (
	"context"
	"time"
)

// Storage defines the interface for the storage backend
type Storage interface {
	// Key-Value operations
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Delete(ctx context.Context, keys ...string) (int, error)
	Exists(ctx context.Context, key string) (bool, error)
	Keys(ctx context.Context, pattern string, limit int) ([]string, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)

	// Atomic operations
	Increment(ctx context.Context, key string, delta int64) (int64, error)
	Decrement(ctx context.Context, key string, delta int64) (int64, error)

	// Batch operations
	MSet(ctx context.Context, pairs map[string][]byte) error
	MGet(ctx context.Context, keys []string) (map[string][]byte, error)

	// Queue operations
	QueuePush(ctx context.Context, queue string, message []byte, delay time.Duration) (string, error)
	QueuePop(ctx context.Context, queue string, timeout time.Duration) (QueueMessage, error)
	QueuePeek(ctx context.Context, queue string, limit int) ([]QueueMessage, error)
	QueueAck(ctx context.Context, messageID string) error
	QueueNack(ctx context.Context, messageID string) error
	QueueStats(ctx context.Context, queue string) (QueueStats, error)
	QueuePurge(ctx context.Context, queue string) (int64, error)
	QueueDelete(ctx context.Context, queue string) error
	QueueList(ctx context.Context) ([]string, error)

	// Stream operations
	StreamPublish(ctx context.Context, topic string, partitionKey string, data []byte, headers map[string]string) (StreamMessage, error)
	StreamRead(ctx context.Context, topic string, partition int32, offset int64, limit int32) ([]StreamMessage, error)
	StreamSeek(ctx context.Context, topic string, consumerID string, partition int32, offset int64) error
	StreamGetOffset(ctx context.Context, topic string, consumerID string, partition int32) (int64, error)
	StreamCreateTopic(ctx context.Context, topic string, partitions int32) error
	StreamDeleteTopic(ctx context.Context, topic string) error
	StreamListTopics(ctx context.Context) ([]string, error)
	StreamGetTopicInfo(ctx context.Context, topic string) (TopicInfo, error)

	// Lifecycle
	Close() error
	Backup(ctx context.Context, path string) error
	Restore(ctx context.Context, path string) error
}

// QueueMessage represents a message in a queue
type QueueMessage struct {
	ID          string
	Queue       string
	Data        []byte
	CreatedAt   time.Time
	DelayUntil  time.Time
	RetryCount  int32
	ConsumerID  string
}

// QueueStats represents queue statistics
type QueueStats struct {
	Name      string
	Size      int64
	Consumers int64
	Pending   int64
	Processed int64
	Failed    int64
}

// StreamMessage represents a message in a stream
type StreamMessage struct {
	ID           string
	Topic        string
	PartitionKey string
	Data         []byte
	Offset       int64
	Timestamp    time.Time
	Headers      map[string]string
	Partition    int32
}

// TopicInfo represents information about a topic
type TopicInfo struct {
	Name            string
	Partitions      int32
	TotalMessages   int64
	PartitionInfo   []PartitionInfo
}

// PartitionInfo represents information about a partition
type PartitionInfo struct {
	ID       int32
	Leader   string
	Replicas []string
	Offset   int64
}