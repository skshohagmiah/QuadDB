package queue

import (
	"context"
	"time"

	"gomsg/storage"
)

// Delayed provides helpers for delayed enqueueing and (optionally) background
// promotion logic if needed in future. Storage currently promotes on Pop.
// This type exists to mirror the README structure and centralize delayed
// patterns in one place.

type Delayed struct {
	s storage.Storage
}

func NewDelayed(s storage.Storage) *Delayed { return &Delayed{s: s} }

// Enqueue schedules a message after the given delay.
func (d *Delayed) Enqueue(ctx context.Context, queue string, data []byte, delay time.Duration) (string, error) {
	return d.s.QueuePush(ctx, queue, data, delay)
}
