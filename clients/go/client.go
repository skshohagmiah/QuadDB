package client

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterpb "gomsg/api/generated/cluster"
	kvpb "gomsg/api/generated/kv"
	queuepb "gomsg/api/generated/queue"
)

// Client is a typed SDK for gomsg services.
type Client struct {
	conn    *grpc.ClientConn
	KV      kvpb.KVServiceClient
	Queue   queuepb.QueueServiceClient
	Cluster clusterpb.ClusterServiceClient
}

// Options control Client behavior.
type Options struct {
	// DialTimeout is the timeout for establishing the initial connection.
	DialTimeout time.Duration
	// Insecure skips TLS (default true for local dev).
	Insecure bool
}

// New dials the gomsg server at address (host:port) and returns a Client.
func New(ctx context.Context, address string, opts *Options) (*Client, error) {
	if opts == nil {
		opts = &Options{Insecure: true, DialTimeout: 5 * time.Second}
	}
	var dialOpts []grpc.DialOption
	if opts.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if opts.DialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.DialTimeout)
		defer cancel()
	}
	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:    conn,
		KV:      kvpb.NewKVServiceClient(conn),
		Queue:   queuepb.NewQueueServiceClient(conn),
		Cluster: clusterpb.NewClusterServiceClient(conn),
	}
	return c, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }
