package stream

import (
	"context"
	"github.com/google/uuid"
	"time"
)

// Producer is a high-throughput, buffered writer for topics.
type Producer struct {
	st        Storage
	opts      ProducerOptions
	partition PartitionFunc

	in    chan Record
	ctx   context.Context
	stop  context.CancelFunc
	doneC chan struct{}
}

// NewProducer creates a new Producer.
func NewProducer(st Storage, partitioner PartitionFunc, opts ProducerOptions) *Producer {
	if partitioner == nil {
		partitioner = DefaultPartitioner
	}
	opts = opts.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		st:        st,
		opts:      opts,
		partition: partitioner,
		in:        make(chan Record, opts.BufferSize),
		ctx:       ctx,
		stop:      cancel,
		doneC:     make(chan struct{}),
	}
	go p.run()
	return p
}

// Produce enqueues a record for asynchronous publishing.
func (p *Producer) Produce(rec Record) {
	select {
	case p.in <- rec:
	default:
		// If buffer is full, drop into blocking to exert backpressure
		p.in <- rec
	}
}

// Close flushes and stops background workers.
func (p *Producer) Close() {
	p.stop()
	<-p.doneC
}

func (p *Producer) run() {
	defer close(p.doneC)

	type batch struct{ items []Record }

	flush := func(b *batch) {
		for _, r := range b.items {
			if p.opts.Idempotent {
				if r.Headers == nil {
					r.Headers = make(map[string]string)
				}
				if _, ok := r.Headers[HeaderIdempotencyKey]; !ok {
					r.Headers[HeaderIdempotencyKey] = uuid.NewString()
				}
			}
			// Publish with retry/backoff
			var attempt int
			backoff := p.opts.RetryBackoffMin
			for {
				// Partition selection is delegated to storage via partitionKey; storage decides mapping
				_, err := p.st.StreamPublish(p.ctx, r.Topic, r.PartitionKey, r.Data, r.Headers)
				if err == nil {
					break
				}
				if p.opts.RetryMax == 0 || attempt >= p.opts.RetryMax {
					// give up; drop record to avoid blocking pipeline
					break
				}
				// sleep with jittered backoff
				sleep := jitterDuration(backoff, p.opts.RetryJitter)
				select {
				case <-p.ctx.Done():
					return
				case <-time.After(sleep):
				}
				// increase backoff
				backoff *= 2
				if backoff > p.opts.RetryBackoffMax {
					backoff = p.opts.RetryBackoffMax
				}
				attempt++
			}
		}
		b.items = b.items[:0]
	}

	b := &batch{items: make([]Record, 0, p.opts.MaxBatch)}
	ticker := time.NewTicker(p.opts.Linger)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			if len(b.items) > 0 {
				flush(b)
			}
			return
		case <-ticker.C:
			if len(b.items) > 0 {
				flush(b)
			}
		case rec := <-p.in:
			b.items = append(b.items, rec)
			if len(b.items) >= p.opts.MaxBatch {
				flush(b)
			}
		}
	}
}
