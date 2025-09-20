package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Consumer reads from a topic with partition-parallelism and manages offsets per consumerID.
type Consumer struct {
	st       Storage
	topic    string
	groupID  string // used as consumerID for offsets
	opts     ConsumerOptions
	handler  Handler

	ctx   context.Context
	stop  context.CancelFunc
	wg    sync.WaitGroup

	// group membership
	memberID   string
	owned      map[int32]context.CancelFunc // partition -> cancel func
}

// NewConsumer constructs a Consumer. Call Start to begin.
func NewConsumer(st Storage, topic, consumerID string, handler Handler, opts ConsumerOptions) *Consumer {
	opts = opts.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		st:      st,
		topic:   topic,
		groupID: consumerID,
		opts:    opts,
		handler: handler,
		ctx:     ctx,
		stop:    cancel,
		memberID: uuid.NewString(),
		owned:    make(map[int32]context.CancelFunc),
	}
}

// Start launches group membership, rebalancer, and partition workers.
func (c *Consumer) Start() error {
	// If explicit partitions provided, bypass group rebalancing and run those partitions
	if len(c.opts.Partitions) > 0 {
		for _, p := range c.opts.Partitions {
			c.startPartition(p)
		}
		return nil
	}

	// Use group membership to assign partitions across members
	info, err := c.st.StreamGetTopicInfo(c.ctx, c.topic)
	if err != nil {
		return err
	}

	// heartbeat loop
	hbTicker := time.NewTicker(c.opts.SessionTimeout / 2)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer hbTicker.Stop()
		for {
			if err := heartbeat(c.ctx, c.st, c.topic, c.groupID, c.memberID, c.opts.SessionTimeout); err != nil {
				// non-fatal; continue trying
			}
			select {
			case <-c.ctx.Done():
				return
			case <-hbTicker.C:
			}
		}
	}()

	// rebalance loop
	rbTicker := time.NewTicker(c.opts.RebalanceInterval)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer rbTicker.Stop()
		var lastAssign []int32
		for {
			members, err := listMembers(c.ctx, c.st, c.topic, c.groupID)
			if err == nil && len(members) > 0 {
				assign := computeAssignments(info.Partitions, members, c.memberID)
				// update running partitions if changed
				if !equalInt32Slices(assign, lastAssign) {
					c.updateAssignments(assign)
					lastAssign = assign
				}
			}
			select {
			case <-c.ctx.Done():
				return
			case <-rbTicker.C:
			}
		}
	}()

	return nil
}

// Close stops all workers and waits for them to finish.
func (c *Consumer) Close() {
	c.stop()
	// stop owned partitions
	for p, cancel := range c.owned {
		_ = p
		cancel()
	}
	c.wg.Wait()
}

func (c *Consumer) startPartition(p int32) {
	if _, ok := c.owned[p]; ok {
		return
	}
	pctx, cancel := context.WithCancel(c.ctx)
	c.owned[p] = cancel
	c.wg.Add(1)
	go func() {
		defer func() {
			c.wg.Done()
			delete(c.owned, p)
		}()
		c.runPartitionWithCtx(pctx, p)
	}()
}

func (c *Consumer) stopPartition(p int32) {
	if cancel, ok := c.owned[p]; ok {
		cancel()
		delete(c.owned, p)
	}
}

func (c *Consumer) updateAssignments(assign []int32) {
	// build set of desired
	desired := make(map[int32]struct{}, len(assign))
	for _, p := range assign { desired[p] = struct{}{} }
	// stop partitions not desired
	for p := range c.owned {
		if _, ok := desired[p]; !ok {
			c.stopPartition(p)
		}
	}
	// start new desired partitions
	for p := range desired {
		if _, ok := c.owned[p]; !ok {
			c.startPartition(p)
		}
	}
}

func (c *Consumer) runPartition(partition int32) {
	defer c.wg.Done()

	// committed offset is the NEXT offset to read (Kafka-style)
	offset, err := c.st.StreamGetOffset(c.ctx, c.topic, c.groupID, partition)
	if err != nil {
		// default to 0 if not found or error
		offset = 0
	}

	autocommit := time.NewTicker(c.opts.AutoCommitInterval)
	defer autocommit.Stop()

	var lastProcessed int64 = offset - 1 // no processed yet
	var lastCommit int64 = offset         // next offset to read
	backoff := c.opts.BackoffMin

	for {
		select {
		case <-c.ctx.Done():
			// final commit
			if lastCommit > 0 {
				_, _ = lastCommit, c.st.StreamSeek(c.ctx, c.topic, c.groupID, partition, lastCommit)
			}
			return
		case <-autocommit.C:
			if lastCommit > 0 {
				_, _ = lastCommit, c.st.StreamSeek(c.ctx, c.topic, c.groupID, partition, lastCommit)
			}
		default:
		}

		msgs, err := c.st.StreamRead(c.ctx, c.topic, partition, lastCommit, c.opts.Batch)
		if err != nil {
			// Backoff on error
			time.Sleep(jitterDuration(backoff, c.opts.BackoffJitter))
			// increase backoff up to max
			backoff *= 2
			if backoff > c.opts.BackoffMax {
				backoff = c.opts.BackoffMax
			}
			continue
		}

		if len(msgs) == 0 {
			// No messages: light sleep to avoid hot loop; reset backoff slowly
			time.Sleep(c.opts.PollInterval)
			if backoff > c.opts.BackoffMin {
				backoff = c.opts.BackoffMin
			}
			continue
		}

		// Got some messages, reset backoff
		if backoff > c.opts.BackoffMin {
			backoff = c.opts.BackoffMin
		}

		for _, m := range msgs {
			// de-duplication by idempotency key (optional)
			if c.opts.Deduplicate {
				if key, ok := m.Headers[HeaderIdempotencyKey]; ok && key != "" {
					dedupKey := fmt.Sprintf("stream:dedup:%s:%s", c.topic, key)
					exists, _ := c.st.Exists(c.ctx, dedupKey)
					if exists {
						// already processed, skip and advance offset
						lastProcessed = m.Offset
						lastCommit = lastProcessed + 1
						continue
					}
				}
			}
			if err := c.handler(c.ctx, m); err != nil {
				// If handler fails, stop processing further in this batch and retry later
				time.Sleep(jitterDuration(backoff, c.opts.BackoffJitter))
				backoff *= 2
				if backoff > c.opts.BackoffMax { backoff = c.opts.BackoffMax }
				break
			}
			lastProcessed = m.Offset
			lastCommit = lastProcessed + 1
			if c.opts.Deduplicate {
				if key, ok := m.Headers[HeaderIdempotencyKey]; ok && key != "" {
					dedupKey := fmt.Sprintf("stream:dedup:%s:%s", c.topic, key)
					// best-effort mark; ignore error
					_ = c.st.Set(c.ctx, dedupKey, []byte("1"), c.opts.DedupTTL)
				}
			}
		}
	}
}

// runPartitionWithCtx is the same as runPartition but binds to specific context (for dynamic assignment control).
func (c *Consumer) runPartitionWithCtx(ctx context.Context, partition int32) {
	// committed offset is the NEXT offset to read (Kafka-style)
	offset, err := c.st.StreamGetOffset(ctx, c.topic, c.groupID, partition)
	if err != nil { offset = 0 }
	autocommit := time.NewTicker(c.opts.AutoCommitInterval)
	defer autocommit.Stop()
	var lastProcessed int64 = offset - 1
	var lastCommit int64 = offset
	backoff := c.opts.BackoffMin
	for {
		select {
		case <-ctx.Done():
			if lastCommit > 0 { _ = c.st.StreamSeek(context.Background(), c.topic, c.groupID, partition, lastCommit) }
			return
		case <-autocommit.C:
			if lastCommit > 0 { _ = c.st.StreamSeek(context.Background(), c.topic, c.groupID, partition, lastCommit) }
		default:
		}
		msgs, err := c.st.StreamRead(ctx, c.topic, partition, lastCommit, c.opts.Batch)
		if err != nil {
			time.Sleep(jitterDuration(backoff, c.opts.BackoffJitter))
			backoff *= 2; if backoff > c.opts.BackoffMax { backoff = c.opts.BackoffMax }
			continue
		}
		if len(msgs) == 0 {
			time.Sleep(c.opts.PollInterval)
			if backoff > c.opts.BackoffMin { backoff = c.opts.BackoffMin }
			continue
		}
		if backoff > c.opts.BackoffMin { backoff = c.opts.BackoffMin }
		for _, m := range msgs {
			if c.opts.Deduplicate {
				if key, ok := m.Headers[HeaderIdempotencyKey]; ok && key != "" {
					dedupKey := fmt.Sprintf("stream:dedup:%s:%s", c.topic, key)
					exists, _ := c.st.Exists(ctx, dedupKey)
					if exists {
						lastProcessed = m.Offset
						lastCommit = lastProcessed + 1
						continue
					}
				}
			}
			if err := c.handler(ctx, m); err != nil {
				time.Sleep(jitterDuration(backoff, c.opts.BackoffJitter))
				backoff *= 2; if backoff > c.opts.BackoffMax { backoff = c.opts.BackoffMax }
				break
			}
			lastProcessed = m.Offset
			lastCommit = lastProcessed + 1
			if c.opts.Deduplicate {
				if key, ok := m.Headers[HeaderIdempotencyKey]; ok && key != "" {
					dedupKey := fmt.Sprintf("stream:dedup:%s:%s", c.topic, key)
					_ = c.st.Set(ctx, dedupKey, []byte("1"), c.opts.DedupTTL)
				}
			}
		}
	}
}

// equalInt32Slices compares two int32 slices for equality (order-sensitive).
func equalInt32Slices(a, b []int32) bool {
	if len(a) != len(b) { return false }
	for i := range a { if a[i] != b[i] { return false } }
	return true
}
