package queue

import (
	"context"

	"gomsg/storage"
)

// Acker acknowledges or requeues messages using IDs.
type Acker struct { s storage.Storage }

func NewAcker(s storage.Storage) *Acker { return &Acker{s: s} }

func (a *Acker) Ack(ctx context.Context, messageID string) error {
	return a.s.QueueAck(ctx, messageID)
}

func (a *Acker) Nack(ctx context.Context, messageID string) error {
	return a.s.QueueNack(ctx, messageID)
}
