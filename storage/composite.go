package storage

import (
	"context"
	"fmt"
	"time"
)

// CompositeStorage routes KV operations to an in-memory backend (by default)
// while using Badger for queues and streams. If KV backend is set to "badger",
// all operations go to Badger.
type CompositeStorage struct {
	badger *BadgerStorage
	memKV  *MemoryKV // non-nil when KV is in-memory
}

// NewCompositeStorage creates a storage that can switch KV backend.
// kvBackend: "memory" (default) or "badger".
func NewCompositeStorage(dataDir, kvBackend string) (Storage, error) {
	badger, err := NewBadgerStorage(dataDir)
	if err != nil {
		return nil, err
	}
	cs := &CompositeStorage{badger: badger}
	if kvBackend == "" || kvBackend == "memory" {
		cs.memKV = NewMemoryKV()
	}
	return cs, nil
}

// Close closes all underlying resources.
func (c *CompositeStorage) Close() error {
	if c.memKV != nil {
		_ = c.memKV.Close()
	}
	return c.badger.Close()
}

// Backup delegates to Badger (KV memory is ephemeral; document-only).
func (c *CompositeStorage) Backup(ctx context.Context, path string) error {
	return c.badger.Backup(ctx, path)
}

// Restore delegates to Badger (KV memory is empty after restore).
func (c *CompositeStorage) Restore(ctx context.Context, path string) error {
	return c.badger.Restore(ctx, path)
}

// ------- KV operations -------

func (c *CompositeStorage) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
    if c.memKV != nil { return c.memKV.Set(ctx, key, value, ttl) }
    return c.badger.Set(ctx, key, value, ttl)
}

func (c *CompositeStorage) Get(ctx context.Context, key string) ([]byte, bool, error) {
    if c.memKV != nil { return c.memKV.Get(ctx, key) }
    return c.badger.Get(ctx, key)
}

func (c *CompositeStorage) Delete(ctx context.Context, keys ...string) (int, error) {
    if c.memKV != nil { return c.memKV.Delete(ctx, keys...) }
    return c.badger.Delete(ctx, keys...)
}

func (c *CompositeStorage) Exists(ctx context.Context, key string) (bool, error) {
    if c.memKV != nil { return c.memKV.Exists(ctx, key) }
    return c.badger.Exists(ctx, key)
}

func (c *CompositeStorage) Keys(ctx context.Context, pattern string, limit int) ([]string, error) {
    if c.memKV != nil { return c.memKV.Keys(ctx, pattern, limit) }
    return c.badger.Keys(ctx, pattern, limit)
}

func (c *CompositeStorage) Expire(ctx context.Context, key string, ttl time.Duration) error {
    if c.memKV != nil { return c.memKV.Expire(ctx, key, ttl) }
    return c.badger.Expire(ctx, key, ttl)
}

func (c *CompositeStorage) TTL(ctx context.Context, key string) (time.Duration, error) {
    if c.memKV != nil { return c.memKV.TTL(ctx, key) }
    return c.badger.TTL(ctx, key)
}

func (c *CompositeStorage) Increment(ctx context.Context, key string, delta int64) (int64, error) {
    if c.memKV != nil { return c.memKV.Increment(ctx, key, delta) }
    return c.badger.Increment(ctx, key, delta)
}

func (c *CompositeStorage) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
    if c.memKV != nil { return c.memKV.Decrement(ctx, key, delta) }
    return c.badger.Decrement(ctx, key, delta)
}

func (c *CompositeStorage) MSet(ctx context.Context, pairs map[string][]byte) error {
    if c.memKV != nil { return c.memKV.MSet(ctx, pairs) }
    return c.badger.MSet(ctx, pairs)
}

func (c *CompositeStorage) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
    if c.memKV != nil { return c.memKV.MGet(ctx, keys) }
    return c.badger.MGet(ctx, keys)
}

// ------- Queue operations (Badger) -------

func (c *CompositeStorage) QueuePush(ctx context.Context, queue string, message []byte, delay time.Duration) (string, error) {
	return c.badger.QueuePush(ctx, queue, message, delay)
}

func (c *CompositeStorage) QueuePop(ctx context.Context, queue string, timeout time.Duration) (QueueMessage, error) {
	return c.badger.QueuePop(ctx, queue, timeout)
}

func (c *CompositeStorage) QueuePeek(ctx context.Context, queue string, limit int) ([]QueueMessage, error) {
	return c.badger.QueuePeek(ctx, queue, limit)
}

func (c *CompositeStorage) QueueAck(ctx context.Context, messageID string) error {
	return c.badger.QueueAck(ctx, messageID)
}

func (c *CompositeStorage) QueueNack(ctx context.Context, messageID string) error {
	return c.badger.QueueNack(ctx, messageID)
}

func (c *CompositeStorage) QueueStats(ctx context.Context, queue string) (QueueStats, error) {
	return c.badger.QueueStats(ctx, queue)
}

func (c *CompositeStorage) QueuePurge(ctx context.Context, queue string) (int64, error) {
	return c.badger.QueuePurge(ctx, queue)
}

func (c *CompositeStorage) QueueDelete(ctx context.Context, queue string) error {
	return c.badger.QueueDelete(ctx, queue)
}

func (c *CompositeStorage) QueueList(ctx context.Context) ([]string, error) {
	return c.badger.QueueList(ctx)
}

// ------- Stream operations (Badger) -------

func (c *CompositeStorage) StreamPublish(ctx context.Context, topic string, partitionKey string, data []byte, headers map[string]string) (StreamMessage, error) {
	return c.badger.StreamPublish(ctx, topic, partitionKey, data, headers)
}

func (c *CompositeStorage) StreamRead(ctx context.Context, topic string, partition int32, offset int64, limit int32) ([]StreamMessage, error) {
	return c.badger.StreamRead(ctx, topic, partition, offset, limit)
}

func (c *CompositeStorage) StreamSeek(ctx context.Context, topic string, consumerID string, partition int32, offset int64) error {
	return c.badger.StreamSeek(ctx, topic, consumerID, partition, offset)
}

func (c *CompositeStorage) StreamGetOffset(ctx context.Context, topic string, consumerID string, partition int32) (int64, error) {
	return c.badger.StreamGetOffset(ctx, topic, consumerID, partition)
}

func (c *CompositeStorage) StreamCreateTopic(ctx context.Context, topic string, partitions int32) error {
	return c.badger.StreamCreateTopic(ctx, topic, partitions)
}

func (c *CompositeStorage) StreamDeleteTopic(ctx context.Context, topic string) error {
	return c.badger.StreamDeleteTopic(ctx, topic)
}

func (c *CompositeStorage) StreamListTopics(ctx context.Context) ([]string, error) {
	return c.badger.StreamListTopics(ctx)
}

func (c *CompositeStorage) StreamGetTopicInfo(ctx context.Context, topic string) (TopicInfo, error) {
	return c.badger.StreamGetTopicInfo(ctx, topic)
}

// Helpers to expose Badger-only capabilities where needed.
func (c *CompositeStorage) SetNodeProviderIfSupported(p NodeProvider) {
    if c.badger != nil {
        c.badger.SetNodeProvider(p)
    }
}

func (c *CompositeStorage) SetReplicationFactorIfSupported(n int) {
    if c.badger != nil {
        c.badger.SetReplicationFactor(n)
    }
}

// String implements fmt.Stringer for debugging
func (c *CompositeStorage) String() string {
	mode := "badger"
	if c.memKV != nil {
		mode = "memory+badger"
	}
	return fmt.Sprintf("CompositeStorage{%s}", mode)
}
