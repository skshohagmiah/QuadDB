package gomsg

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a GoMsg client connection
type Client struct {
	conn   *grpc.ClientConn
	KV     KVClient
	Queue  QueueClient
	Stream StreamClient
}

// Config holds client configuration
type Config struct {
	Address string
	Timeout time.Duration
}

// DefaultConfig returns default client configuration
func DefaultConfig() *Config {
	return &Config{
		Address: "localhost:9000",
		Timeout: 30 * time.Second,
	}
}

// NewClient creates a new GoMsg client
func NewClient(config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Create gRPC connection
	conn, err := grpc.Dial(config.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithTimeout(config.Timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to GoMsg server: %w", err)
	}

	client := &Client{
		conn:   conn,
		KV:     NewKVClient(conn),
		Queue:  NewQueueClient(conn),
		Stream: NewStreamClient(conn),
	}

	return client, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}


// Ping tests the connection to the server
func (c *Client) Ping(ctx context.Context) error {
	// Use a simple KV operation to test connectivity
	_, err := c.KV.Set(ctx, "ping", "pong")
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	// Clean up the ping key
	_ = c.KV.Delete(ctx, "ping")
	return nil
}
