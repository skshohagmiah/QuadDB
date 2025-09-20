package kv

import (
	"context"
	"time"
)

// SetTTL stores a value with a TTL in seconds.
func (st *Store) SetTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return st.s.Set(ctx, key, value, ttl)
}

// Expire sets TTL on an existing key.
func (st *Store) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return st.s.Expire(ctx, key, ttl)
}

// TTL returns remaining time-to-live. Negative means no TTL, zero means expired.
func (st *Store) TTL(ctx context.Context, key string) (time.Duration, error) {
	return st.s.TTL(ctx, key)
}
