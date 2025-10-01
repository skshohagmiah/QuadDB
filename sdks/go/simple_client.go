package fluxdl

import (
	"context"
	"fmt"
	"time"

	kvpb "github.com/skshohagmiah/fluxdl-go-sdk/api/generated/kv"
	queuepb "github.com/skshohagmiah/fluxdl-go-sdk/api/generated/queue"
	streampb "github.com/skshohagmiah/fluxdl-go-sdk/api/generated/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SimpleClient provides a basic GoMsg client
type SimpleClient struct {
	conn   *grpc.ClientConn
	KV     SimpleKVClient
	Queue  SimpleQueueClient
	Stream SimpleStreamClient
}

// SimpleConfig holds basic client configuration
type SimpleConfig struct {
	Address string
	Timeout time.Duration
}

// DefaultSimpleConfig returns default configuration
func DefaultSimpleConfig() *SimpleConfig {
	return &SimpleConfig{
		Address: "localhost:9000",
		Timeout: 30 * time.Second,
	}
}

// NewSimpleClient creates a new simple GoMsg client
func NewSimpleClient(config *SimpleConfig) (*SimpleClient, error) {
	// Connect to GoMsg server
	conn, err := grpc.Dial(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to GoMsg server: %w", err)
	}

	client := &SimpleClient{
		conn: conn,
	}

	// Initialize service clients
	client.KV = &simpleKVClient{
		client: kvpb.NewKVServiceClient(conn),
		timeout: config.Timeout,
	}
	
	client.Queue = &simpleQueueClient{
		client: queuepb.NewQueueServiceClient(conn),
		timeout: config.Timeout,
	}
	
	client.Stream = &simpleStreamClient{
		client: streampb.NewStreamServiceClient(conn),
		timeout: config.Timeout,
	}

	return client, nil
}

// Close closes the client connection
func (c *SimpleClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SimpleKVClient provides key-value operations
type SimpleKVClient interface {
	Set(ctx context.Context, key, value string) (string, error)
	Get(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) error
}

type simpleKVClient struct {
	client  kvpb.KVServiceClient
	timeout time.Duration
}

func (c *simpleKVClient) Set(ctx context.Context, key, value string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req := &kvpb.SetRequest{
		Key:   key,
		Value: []byte(value),
	}

	resp, err := c.client.Set(ctx, req)
	if err != nil {
		return "", err
	}

	if resp.Status != nil {
		return resp.Status.Message, nil
	}
	return "OK", nil
}

func (c *simpleKVClient) Get(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req := &kvpb.GetRequest{
		Key: key,
	}

	resp, err := c.client.Get(ctx, req)
	if err != nil {
		return "", err
	}

	return string(resp.Value), nil
}

func (c *simpleKVClient) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req := &kvpb.DelRequest{
		Keys: []string{key},
	}

	_, err := c.client.Del(ctx, req)
	return err
}

// SimpleQueueClient provides queue operations
type SimpleQueueClient interface {
	Push(ctx context.Context, queue, message string) (string, error)
	Pop(ctx context.Context, queue string) (*QueueMessage, error)
}

type QueueMessage struct {
	ID   string
	Data string
}

type simpleQueueClient struct {
	client  queuepb.QueueServiceClient
	timeout time.Duration
}

func (c *simpleQueueClient) Push(ctx context.Context, queue, message string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req := &queuepb.PushRequest{
		Queue: queue,
		Data:  []byte(message),
	}

	resp, err := c.client.Push(ctx, req)
	if err != nil {
		return "", err
	}

	return resp.MessageId, nil
}

func (c *simpleQueueClient) Pop(ctx context.Context, queue string) (*QueueMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req := &queuepb.PopRequest{
		Queue: queue,
	}

	resp, err := c.client.Pop(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.Message == nil {
		return nil, nil // No message available
	}

	return &QueueMessage{
		ID:   resp.Message.Id,
		Data: string(resp.Message.Data),
	}, nil
}

// SimpleStreamClient provides stream operations
type SimpleStreamClient interface {
	Publish(ctx context.Context, topic, message string) (string, error)
}

type simpleStreamClient struct {
	client  streampb.StreamServiceClient
	timeout time.Duration
}

func (c *simpleStreamClient) Publish(ctx context.Context, topic, message string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req := &streampb.PublishRequest{
		Topic: topic,
		Data:  []byte(message),
	}

	resp, err := c.client.Publish(ctx, req)
	if err != nil {
		return "", err
	}

	return resp.MessageId, nil
}
