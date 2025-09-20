package queue

import (
	"context"
	"time"

	"gomsg/storage"
)

// Consumer pops and peeks messages from a queue.
type Consumer struct {
	s storage.Storage
}

func NewConsumer(s storage.Storage) *Consumer { return &Consumer{s: s} }

// Pop blocks up to timeout waiting for a message.
func (c *Consumer) Pop(ctx context.Context, queue string, timeout time.Duration) (storage.QueueMessage, error) {
	return c.s.QueuePop(ctx, queue, timeout)
}

// Peek returns up to limit messages without removing them.
func (c *Consumer) Peek(ctx context.Context, queue string, limit int) ([]storage.QueueMessage, error) {
	return c.s.QueuePeek(ctx, queue, limit)
}
