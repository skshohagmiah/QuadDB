package gomsg

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
	kvpb "gomsg/api/generated/kv"
	queuepb "gomsg/api/generated/queue"
	streampb "gomsg/api/generated/stream"
)

// Client represents a GoMsg client that connects to GoMsg Docker containers
type Client struct {
	mu sync.RWMutex
	
	// Connection management
	nodes       []string
	connections map[string]*grpc.ClientConn
	
	// Service clients
	kvClients     map[string]kvpb.KVServiceClient
	queueClients  map[string]queuepb.QueueServiceClient
	streamClients map[string]streampb.StreamServiceClient
	
	// Configuration
	config *Config
	
	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	
	// Service-specific clients
	KV     *KVClient
	Queue  *QueueClient
	Stream *StreamClient
}

// Config defines client configuration
type Config struct {
	Nodes          []string      // GoMsg cluster nodes (e.g., ["localhost:8080", "localhost:8081"])
	ConnectTimeout time.Duration // Connection timeout (default: 10s)
	RequestTimeout time.Duration // Request timeout (default: 30s)
	RetryAttempts  int           // Retry attempts (default: 3)
}

// DefaultConfig returns default client configuration
func DefaultConfig() *Config {
	return &Config{
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 30 * time.Second,
		RetryAttempts:  3,
	}
}

// NewClient creates a new GoMsg client that connects to GoMsg Docker containers
func NewClient(ctx context.Context, config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}
	
	if len(config.Nodes) == 0 {
		return nil, fmt.Errorf("at least one GoMsg node must be specified")
	}
	
	ctx, cancel := context.WithCancel(ctx)
	
	client := &Client{
		nodes:         config.Nodes,
		connections:   make(map[string]*grpc.ClientConn),
		kvClients:     make(map[string]kvpb.KVServiceClient),
		queueClients:  make(map[string]queuepb.QueueServiceClient),
		streamClients: make(map[string]streampb.StreamServiceClient),
		config:        config,
		ctx:           ctx,
		cancel:        cancel,
	}
	
	// Initialize connections to GoMsg cluster
	if err := client.connect(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to GoMsg cluster: %w", err)
	}
	
	// Initialize service-specific clients
	client.KV = &KVClient{client: client}
	client.Queue = &QueueClient{client: client}
	client.Stream = &StreamClient{client: client}
	
	return client, nil
}

// connect establishes gRPC connections to all GoMsg nodes
func (c *Client) connect() error {
	for _, node := range c.nodes {
		conn, err := grpc.Dial(node, 
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithTimeout(c.config.ConnectTimeout),
		)
		if err != nil {
			return fmt.Errorf("failed to connect to GoMsg node %s: %w", node, err)
		}
		
		c.connections[node] = conn
		c.kvClients[node] = kvpb.NewKVServiceClient(conn)
		c.queueClients[node] = queuepb.NewQueueServiceClient(conn)
		c.streamClients[node] = streampb.NewStreamServiceClient(conn)
	}
	
	return nil
}

// getRandomNode returns a random healthy node for load balancing
func (c *Client) getRandomNode() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if len(c.nodes) == 0 {
		return ""
	}
	
	return c.nodes[rand.Intn(len(c.nodes))]
}

// executeWithRetry executes a function with retry logic and failover
func (c *Client) executeWithRetry(fn func(node string) error) error {
	var lastErr error
	
	for attempt := 0; attempt < c.config.RetryAttempts; attempt++ {
		// Try a random node (GoMsg server handles partitioning)
		node := c.getRandomNode()
		if node == "" {
			return fmt.Errorf("no GoMsg nodes available")
		}
		
		if err := fn(node); err != nil {
			lastErr = err
			if attempt < c.config.RetryAttempts-1 {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
			}
			continue
		}
		return nil
	}
	
	return fmt.Errorf("all retry attempts failed, last error: %w", lastErr)
}

// Close closes all connections
func (c *Client) Close() error {
	c.cancel()
	
	var errs []error
	for node, conn := range c.connections {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection to %s: %w", node, err))
		}
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	
	return nil
}

// Health checks cluster health
func (c *Client) Health(ctx context.Context) (map[string]bool, error) {
	results := make(map[string]bool)
	
	for node := range c.connections {
		kvClient := c.kvClients[node]
		_, err := kvClient.Get(ctx, &kvpb.GetRequest{Key: "__health_check__"})
		// Node is healthy if it responds (even with "key not found")
		results[node] = err == nil || (err != nil && err.Error() != "connection refused")
	}
	
	return results, nil
}
