package gomsg

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

// QueueClient provides queue operations (RabbitMQ-like)
type QueueClient interface {
	Push(ctx context.Context, queue, message string) error
	Pop(ctx context.Context, queue string) (string, error)
	Peek(ctx context.Context, queue string) (string, error)
	List(ctx context.Context) ([]string, error)
	Stats(ctx context.Context, queue string) (*QueueStats, error)
	Purge(ctx context.Context, queue string) error
}

// QueueStats represents queue statistics
type QueueStats struct {
	Name     string
	Messages int64
	Size     int64
}

type queueClient struct {
	conn *grpc.ClientConn
}

// NewQueueClient creates a new Queue client
func NewQueueClient(conn *grpc.ClientConn) QueueClient {
	return &queueClient{conn: conn}
}

// Push adds a message to a queue
func (c *queueClient) Push(ctx context.Context, queue, message string) error {
	return fmt.Errorf("not implemented - requires protobuf client generation")
}

// Pop removes and returns a message from a queue
func (c *queueClient) Pop(ctx context.Context, queue string) (string, error) {
	return "", fmt.Errorf("not implemented - requires protobuf client generation")
}

// Peek returns a message from a queue without removing it
func (c *queueClient) Peek(ctx context.Context, queue string) (string, error) {
	return "", fmt.Errorf("not implemented - requires protobuf client generation")
}

// List returns all queue names
func (c *queueClient) List(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("not implemented - requires protobuf client generation")
}

// Stats returns queue statistics
func (c *queueClient) Stats(ctx context.Context, queue string) (*QueueStats, error) {
	return nil, fmt.Errorf("not implemented - requires protobuf client generation")
}

// Purge removes all messages from a queue
func (c *queueClient) Purge(ctx context.Context, queue string) error {
	return fmt.Errorf("not implemented - requires protobuf client generation")
}
