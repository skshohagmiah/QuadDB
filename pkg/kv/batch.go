package kv

import (
	"context"
)

// MSet sets multiple key-value pairs.
func (st *Store) MSet(ctx context.Context, pairs map[string][]byte) error {
	return st.s.MSet(ctx, pairs)
}

// MGet gets multiple keys and returns a map of key->value for existing keys.
func (st *Store) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	return st.s.MGet(ctx, keys)
}
