package queue

import (
	"context"
	"time"

	"gomsg/storage"
)

// Producer publishes messages into a queue.
type Producer struct {
	s storage.Storage
}

func NewProducer(s storage.Storage) *Producer { return &Producer{s: s} }

// Push enqueues a message and returns its ID.
func (p *Producer) Push(ctx context.Context, queue string, data []byte) (string, error) {
	return p.s.QueuePush(ctx, queue, data, 0)
}

// PushDelayed enqueues a message with a delay.
func (p *Producer) PushDelayed(ctx context.Context, queue string, data []byte, delay time.Duration) (string, error) {
	return p.s.QueuePush(ctx, queue, data, delay)
}
