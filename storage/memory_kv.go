package storage

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MemoryKV provides an in-memory KV store with TTL and basic ops.
type MemoryKV struct {
	mu   sync.RWMutex
	data map[string]memEntry
	stop chan struct{}
}

type memEntry struct {
	val       []byte
	expiresAt time.Time // zero means no expiry
}

func NewMemoryKV() *MemoryKV {
	m := &MemoryKV{data: make(map[string]memEntry), stop: make(chan struct{})}
	go m.janitor()
	return m
}

func (m *MemoryKV) Close() error {
	close(m.stop)
	return nil
}

func (m *MemoryKV) janitor() {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			now := time.Now()
			m.mu.Lock()
			for k, e := range m.data {
				if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
					delete(m.data, k)
				}
			}
			m.mu.Unlock()
		}
	}
}

// KV operations

func (m *MemoryKV) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	m.data[key] = memEntry{val: append([]byte(nil), value...), expiresAt: exp}
	return nil
}

func (m *MemoryKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	_ = ctx
	m.mu.RLock()
	e, ok := m.data[key]
	m.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		m.mu.Lock()
		delete(m.data, key)
		m.mu.Unlock()
		return nil, false, nil
	}
	return append([]byte(nil), e.val...), true, nil
}

func (m *MemoryKV) Delete(ctx context.Context, keys ...string) (int, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	cnt := 0
	for _, k := range keys {
		if _, ok := m.data[k]; ok {
			delete(m.data, k)
			cnt++
		}
	}
	return cnt, nil
}

func (m *MemoryKV) Exists(ctx context.Context, key string) (bool, error) {
	_, ok, _ := m.Get(ctx, key)
	return ok, nil
}

func (m *MemoryKV) Keys(ctx context.Context, pattern string, limit int) ([]string, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pattern == "" || pattern == "*" {
		res := make([]string, 0, min(limit, len(m.data)))
		for k := range m.data {
			res = append(res, k)
			if limit > 0 && len(res) >= limit {
				break
			}
		}
		return res, nil
	}
	// simple contains/wildcard match
	p := strings.Trim(pattern, "*")
	prefix := strings.HasPrefix(pattern, "*") == false
	suffix := strings.HasSuffix(pattern, "*") == false
	res := make([]string, 0, limit)
	for k := range m.data {
		ok := true
		if prefix && !strings.HasPrefix(k, p) {
			ok = false
		}
		if suffix && !strings.HasSuffix(k, p) {
			ok = false
		}
		if strings.Contains(k, p) && ok {
			res = append(res, k)
			if limit > 0 && len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (m *MemoryKV) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.data[key]
	if !ok {
		return nil
	}
	if ttl <= 0 {
		e.expiresAt = time.Time{}
	} else {
		e.expiresAt = time.Now().Add(ttl)
	}
	m.data[key] = e
	return nil
}

func (m *MemoryKV) TTL(ctx context.Context, key string) (time.Duration, error) {
	_ = ctx
	m.mu.RLock()
	e, ok := m.data[key]
	m.mu.RUnlock()
	if !ok || e.expiresAt.IsZero() {
		return 0, nil
	}
	if time.Now().After(e.expiresAt) {
		return 0, nil
	}
	return time.Until(e.expiresAt), nil
}

func (m *MemoryKV) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.data[key]
	cur := int64(0)
	if len(e.val) > 0 {
		if v, err := strconv.ParseInt(string(e.val), 10, 64); err == nil {
			cur = v
		}
	}
	cur += delta
	e.val = []byte(strconv.FormatInt(cur, 10))
	m.data[key] = e
	return cur, nil
}

func (m *MemoryKV) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return m.Increment(ctx, key, -delta)
}

func (m *MemoryKV) MSet(ctx context.Context, pairs map[string][]byte) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range pairs {
		m.data[k] = memEntry{val: append([]byte(nil), v...)}
	}
	return nil
}

func (m *MemoryKV) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if e, ok := m.data[k]; ok {
			if e.expiresAt.IsZero() || time.Now().Before(e.expiresAt) {
				res[k] = append([]byte(nil), e.val...)
			}
		}
	}
	return res, nil
}

func min(a, b int) int { if a == 0 { return b }; if b == 0 { return a }; if a < b { return a }; return b }
