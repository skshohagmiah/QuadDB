package kv

import (
	"context"

	"gomsg/storage"
)

// Store provides a high-level KV API over the storage backend.
// It delegates all persistence to the underlying storage.Storage implementation.
// The package structure mirrors README to keep responsibilities clear.
//
type Store struct {
	s storage.Storage
}

// New returns a new KV store wrapper.
func New(s storage.Storage) *Store {
	return &Store{s: s}
}

// Underlying exposes the raw storage for advanced scenarios.
func (st *Store) Underlying() storage.Storage { return st.s }

// Ping provides a cheap health check path.
func (st *Store) Ping(ctx context.Context) error {
    // For now we rely on a lightweight operation against storage
    // Exists on a non-existing key should be very cheap.
    _, err := st.s.Exists(ctx, "__ping__")
    if err != nil {
        return err
    }
    return nil
}
