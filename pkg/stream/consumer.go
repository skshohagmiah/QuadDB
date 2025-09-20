package stream

import (
	"context"
	"sync"
	"time"
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
	}
}

// Start launches partition workers.
func (c *Consumer) Start() error {
	parts := c.opts.Partitions
	if len(parts) == 0 {
		info, err := c.st.StreamGetTopicInfo(c.ctx, c.topic)
		if err != nil {
			return err
		}
		for i := int32(0); i < info.Partitions; i++ {
			parts = append(parts, i)
		}
	}

	for _, p := range parts {
		c.wg.Add(1)
		go c.runPartition(p)
	}
	return nil
}

// Close stops all workers and waits for them to finish.
func (c *Consumer) Close() {
	c.stop()
	c.wg.Wait()
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
				_ = c.st.StreamSeek(c.ctx, c.topic, c.groupID, partition, lastCommit)
			}
		default:
		}

		msgs, err := c.st.StreamRead(c.ctx, c.topic, partition, lastCommit, c.opts.Batch)
		if err != nil {
			// Backoff on error
			time.Sleep(c.opts.PollInterval)
			continue
		}

		if len(msgs) == 0 {
			time.Sleep(c.opts.PollInterval)
			continue
		}

		for _, m := range msgs {
			if err := c.handler(c.ctx, m); err != nil {
				// If handler fails, stop processing further in this batch and retry later
				time.Sleep(c.opts.PollInterval)
				break
			}
			lastProcessed = m.Offset
			lastCommit = lastProcessed + 1
		}
	}
}
