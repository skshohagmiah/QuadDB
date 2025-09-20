package kv

import (
	"context"
)

// Keys returns up to limit keys matching the pattern. Supports prefix* patterns
// according to the current storage implementation.
func (st *Store) Keys(ctx context.Context, pattern string, limit int) ([]string, error) {
	return st.s.Keys(ctx, pattern, limit)
}

// DelPattern deletes keys that match a given pattern and returns deleted count.
func (st *Store) DelPattern(ctx context.Context, pattern string, limit int) (int, error) {
	keys, err := st.s.Keys(ctx, pattern, limit)
	if err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return st.s.Delete(ctx, keys...)
}
