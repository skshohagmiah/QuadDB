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
	QueuePush(ctx context.Context, queue string, message []byte) (string, error)
	QueuePop(ctx context.Context, queue string, timeout time.Duration) (QueueMessage, error)
	QueuePeek(ctx context.Context, queue string, limit int) ([]QueueMessage, error)
	QueueAck(ctx context.Context, messageID string) error
	QueueNack(ctx context.Context, messageID string) error
	QueueStats(ctx context.Context, queue string) (QueueStats, error)
	QueuePurge(ctx context.Context, queue string) (int64, error)
	QueueDelete(ctx context.Context, queue string) error
	QueueList(ctx context.Context) ([]string, error)
	
	// Queue batch operations
	QueuePushBatch(ctx context.Context, queue string, messages [][]byte) ([]string, error)
	QueuePopBatch(ctx context.Context, queue string, limit int, timeout time.Duration) ([]QueueMessage, error)

	// Stream operations
	StreamPublish(ctx context.Context, topic string, partitionKey string, data []byte, headers map[string]string) (StreamMessage, error)
	StreamRead(ctx context.Context, topic string, partition int32, offset int64, limit int32) ([]StreamMessage, error)
	StreamReadFrom(ctx context.Context, topic string, partition int32, timestamp time.Time, limit int32) ([]StreamMessage, error)
	StreamSeek(ctx context.Context, topic string, consumerID string, partition int32, offset int64) error
	StreamGetOffset(ctx context.Context, topic string, consumerID string, partition int32) (int64, error)
	StreamCreateTopic(ctx context.Context, topic string, partitions int32) error
	StreamDeleteTopic(ctx context.Context, topic string) error
	StreamListTopics(ctx context.Context) ([]string, error)
	StreamGetTopicInfo(ctx context.Context, topic string) (TopicInfo, error)
	StreamPurge(ctx context.Context, topic string) (int64, error)
	// Stream consumer group operations
	StreamSubscribeGroup(ctx context.Context, topic string, groupID string, consumerID string) error
	StreamUnsubscribeGroup(ctx context.Context, topic string, groupID string, consumerID string) error
	StreamGetGroupOffset(ctx context.Context, topic string, groupID string, partition int32) (int64, error)
	StreamCommitGroupOffset(ctx context.Context, topic string, groupID string, partition int32, offset int64) error

	// Document database operations
	// Collection management
	DocCreateCollection(ctx context.Context, collection string, options CollectionOptions) error
	DocDropCollection(ctx context.Context, collection string) error
	DocListCollections(ctx context.Context) ([]string, error)
	DocGetCollectionInfo(ctx context.Context, collection string) (CollectionInfo, error)
	
	// Document operations
	DocInsertOne(ctx context.Context, collection string, document map[string]interface{}) (string, error)
	DocInsertMany(ctx context.Context, collection string, documents []map[string]interface{}, ordered bool) ([]string, error)
	DocFindOne(ctx context.Context, collection string, filter map[string]interface{}, projection map[string]interface{}) (DocumentResult, error)
	DocFindMany(ctx context.Context, collection string, query DocumentQuery) ([]DocumentResult, int64, error)
	DocUpdateOne(ctx context.Context, collection string, filter map[string]interface{}, update DocumentUpdate, upsert bool) (DocumentWriteResult, error)
	DocUpdateMany(ctx context.Context, collection string, filter map[string]interface{}, update DocumentUpdate) (DocumentWriteResult, error)
	DocDeleteOne(ctx context.Context, collection string, filter map[string]interface{}) (DocumentWriteResult, error)
	DocDeleteMany(ctx context.Context, collection string, filter map[string]interface{}) (DocumentWriteResult, error)
	DocReplaceOne(ctx context.Context, collection string, filter map[string]interface{}, replacement map[string]interface{}, upsert bool) (DocumentWriteResult, error)
	
	// Aggregation operations
	DocAggregate(ctx context.Context, collection string, pipeline []map[string]interface{}) ([]map[string]interface{}, error)
	DocCount(ctx context.Context, collection string, filter map[string]interface{}) (int64, error)
	DocDistinct(ctx context.Context, collection string, field string, filter map[string]interface{}) ([]interface{}, error)
	
	// Index management
	DocCreateIndex(ctx context.Context, collection string, index IndexSpec) error
	DocDropIndex(ctx context.Context, collection string, indexName string) error
	DocListIndexes(ctx context.Context, collection string) ([]IndexSpec, error)

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

// Document database types

// CollectionOptions represents options for creating a collection
type CollectionOptions struct {
	Capped       bool                   `json:"capped,omitempty"`
	MaxSize      int64                  `json:"maxSize,omitempty"`
	MaxDocuments int64                  `json:"maxDocuments,omitempty"`
	Validator    map[string]interface{} `json:"validator,omitempty"`
}

// CollectionInfo represents information about a collection
type CollectionInfo struct {
	Name          string            `json:"name"`
	DocumentCount int64             `json:"documentCount"`
	SizeBytes     int64             `json:"sizeBytes"`
	Indexes       []IndexSpec       `json:"indexes"`
	CreatedAt     time.Time         `json:"createdAt"`
	Options       CollectionOptions `json:"options"`
}

// DocumentResult represents a document returned from a query
type DocumentResult struct {
	ID        string                 `json:"_id"`
	Data      map[string]interface{} `json:"data"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`
	Version   int64                  `json:"version"`
}

// DocumentQuery represents a query for finding documents
type DocumentQuery struct {
	Filter     map[string]interface{} `json:"filter"`
	Sort       map[string]interface{} `json:"sort"`
	Limit      int32                  `json:"limit"`
	Skip       int32                  `json:"skip"`
	Projection map[string]interface{} `json:"projection"`
}

// DocumentUpdate represents an update operation
type DocumentUpdate struct {
	Set      map[string]interface{} `json:"$set,omitempty"`
	Unset    map[string]interface{} `json:"$unset,omitempty"`
	Inc      map[string]interface{} `json:"$inc,omitempty"`
	Push     map[string]interface{} `json:"$push,omitempty"`
	Pull     map[string]interface{} `json:"$pull,omitempty"`
	AddToSet map[string]interface{} `json:"$addToSet,omitempty"`
}

// DocumentWriteResult represents the result of a write operation
type DocumentWriteResult struct {
	Acknowledged  bool     `json:"acknowledged"`
	InsertedCount int64    `json:"insertedCount"`
	MatchedCount  int64    `json:"matchedCount"`
	ModifiedCount int64    `json:"modifiedCount"`
	DeletedCount  int64    `json:"deletedCount"`
	InsertedIDs   []string `json:"insertedIds"`
	UpsertedIDs   []string `json:"upsertedIds"`
}

// IndexSpec represents an index specification
type IndexSpec struct {
	Name          string                 `json:"name"`
	Keys          map[string]interface{} `json:"keys"`
	Unique        bool                   `json:"unique"`
	Sparse        bool                   `json:"sparse"`
	TTLSeconds    int32                  `json:"ttlSeconds"`
	PartialFilter map[string]interface{} `json:"partialFilter"`
}