package stream

import (
	"context"
	"hash/fnv"
	"time"

	"gomsg/storage"
)

// Storage abstracts the backend used by the stream package. We rely on the existing storage.Storage.
// We alias it to allow easier mocking in tests.
type Storage = storage.Storage

// Message is an alias of storage.StreamMessage for convenience.
type Message = storage.StreamMessage

// TopicInfo is an alias of storage.TopicInfo for convenience.
type TopicInfo = storage.TopicInfo

// PartitionFunc decides which partition a key is assigned to.
// If returns negative, implementation will fallback to 0.
type PartitionFunc func(partitions int32, key string) int32

// DefaultPartitioner uses a simple FNV-1a hash to choose a partition.
func DefaultPartitioner(partitions int32, key string) int32 {
	if partitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	p := int32(h.Sum32() % uint32(partitions))
	if p < 0 {
		p = -p
	}
	return p
}

// ProducerOptions configures a Producer.
type ProducerOptions struct {
	// Workers controls the number of background workers reading the buffer.
	Workers int
	// MaxBatch controls max items per flush. If 1, flushes every message.
	MaxBatch int
	// Linger waits up to this duration to form a batch before flushing.
	Linger time.Duration
	// BufferSize is the size of buffered channel for incoming records.
	BufferSize int
	// RetryMax is the maximum number of retries per message publish. 0 disables retries.
	RetryMax int
	// RetryBackoffMin is the initial backoff for retries.
	RetryBackoffMin time.Duration
	// RetryBackoffMax is the maximum backoff for retries.
	RetryBackoffMax time.Duration
	// RetryJitter is a fractional jitter [0,1] applied to backoff.
	RetryJitter float64
	// Idempotent adds an idempotency key to each message if not present in headers.
	Idempotent bool
}

func (o *ProducerOptions) withDefaults() ProducerOptions {
	res := *o
	if res.Workers <= 0 {
		res.Workers = 4
	}
	if res.MaxBatch <= 0 {
		res.MaxBatch = 256
	}
	if res.Linger <= 0 {
		res.Linger = 5 * time.Millisecond
	}
	if res.BufferSize <= 0 {
		res.BufferSize = 4096
	}
	if res.RetryMax < 0 {
		res.RetryMax = 0
	}
	if res.RetryBackoffMin <= 0 {
		res.RetryBackoffMin = 10 * time.Millisecond
	}
	if res.RetryBackoffMax <= 0 {
		res.RetryBackoffMax = 2 * time.Second
	}
	if res.RetryJitter < 0 || res.RetryJitter > 1 {
		res.RetryJitter = 0.2
	}
	return res
}

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	// Batch is the max number of messages fetched per poll/read per partition.
	Batch int32
	// PollInterval is the sleep between empty polls per partition.
	PollInterval time.Duration
	// AutoCommitInterval writes offsets at this cadence if enabled.
	AutoCommitInterval time.Duration
	// Partitions limits partitions to consume. If nil or empty, consume all by probing.
	Partitions []int32
	// BackoffMin is the initial backoff used on read/handler errors.
	BackoffMin time.Duration
	// BackoffMax is the maximum backoff used on read/handler errors.
	BackoffMax time.Duration
	// BackoffJitter is a fractional jitter [0,1] applied to backoff.
	BackoffJitter float64
	// Deduplicate skips messages with the same idempotency key if already processed.
	Deduplicate bool
	// DedupTTL controls how long to remember processed idempotency keys.
	DedupTTL time.Duration
	// SessionTimeout determines how long this consumer instance is considered alive for group membership.
	SessionTimeout time.Duration
	// RebalanceInterval controls how frequently group membership and partition assignments are refreshed.
	RebalanceInterval time.Duration
}

func (o *ConsumerOptions) withDefaults() ConsumerOptions {
	res := *o
	if res.Batch <= 0 {
		res.Batch = 256
	}
	if res.PollInterval <= 0 {
		res.PollInterval = 10 * time.Millisecond
	}
	if res.AutoCommitInterval <= 0 {
		res.AutoCommitInterval = 500 * time.Millisecond
	}
	if res.BackoffMin <= 0 {
		res.BackoffMin = 50 * time.Millisecond
	}
	if res.BackoffMax <= 0 {
		res.BackoffMax = 2 * time.Second
	}
	if res.BackoffJitter < 0 || res.BackoffJitter > 1 {
		res.BackoffJitter = 0.2
	}
	if res.Deduplicate && res.DedupTTL <= 0 {
		res.DedupTTL = 24 * time.Hour
	}
	if res.SessionTimeout <= 0 {
		res.SessionTimeout = 10 * time.Second
	}
	if res.RebalanceInterval <= 0 {
		res.RebalanceInterval = 2 * time.Second
	}
	return res
}

const (
	// HeaderIdempotencyKey is the message header used to carry an idempotency key.
	HeaderIdempotencyKey = "x-idempotency-key"
)

// Record is the input to Producer.
type Record struct {
	Topic        string
	PartitionKey string
	Data         []byte
	Headers      map[string]string
}

// Handler processes consumed messages. Return error to retry later.
type Handler func(ctx context.Context, msg Message) error
