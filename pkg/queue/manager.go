package queue

import (
	"context"

	"gomsg/storage"
)

// Manager provides queue management operations (list, purge, delete, stats).
// It delegates to the underlying storage implementation for performance.
//
// Directory structure mirrors README for maintainability.

type Manager struct {
	s storage.Storage
}

func NewManager(s storage.Storage) *Manager { return &Manager{s: s} }

// List returns all queue names.
func (m *Manager) List(ctx context.Context) ([]string, error) {
	return m.s.QueueList(ctx)
}

// Purge removes all messages from a queue and returns the purged count.
func (m *Manager) Purge(ctx context.Context, queue string) (int64, error) {
	return m.s.QueuePurge(ctx, queue)
}

// Delete removes an entire queue (including stats/metadata).
func (m *Manager) Delete(ctx context.Context, queue string) error {
	return m.s.QueueDelete(ctx, queue)
}

// Stats returns queue statistics.
func (m *Manager) Stats(ctx context.Context, queue string) (storage.QueueStats, error) {
	return m.s.QueueStats(ctx, queue)
}
