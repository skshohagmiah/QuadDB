package kv

import (
	"context"
)

// Set stores a key-value pair with optional TTL handled via Expire/SetTTL.
func (st *Store) Set(ctx context.Context, key string, value []byte) error {
	return st.s.Set(ctx, key, value, 0)
}

// Get retrieves a value and whether it exists.
func (st *Store) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return st.s.Get(ctx, key)
}

// Del deletes one or more keys and returns the number of deleted keys.
func (st *Store) Del(ctx context.Context, keys ...string) (int, error) {
	return st.s.Delete(ctx, keys...)
}

// Exists checks whether the key exists.
func (st *Store) Exists(ctx context.Context, key string) (bool, error) {
	return st.s.Exists(ctx, key)
}

// Incr increments a counter by 1 and returns the new value.
func (st *Store) Incr(ctx context.Context, key string) (int64, error) {
	return st.s.Increment(ctx, key, 1)
}

// IncrBy increments a counter by delta and returns the new value.
func (st *Store) IncrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return st.s.Increment(ctx, key, delta)
}

// Decr decrements a counter by 1 and returns the new value.
func (st *Store) Decr(ctx context.Context, key string) (int64, error) {
	return st.s.Decrement(ctx, key, 1)
}

// DecrBy decrements a counter by delta and returns the new value.
func (st *Store) DecrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return st.s.Decrement(ctx, key, delta)
}
