package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/skshohagmiah/gomsg/config"
	"github.com/skshohagmiah/gomsg/pkg/server"
	"github.com/skshohagmiah/gomsg/storage"
)

// TestServer manages a test server instance
type TestServer struct {
	server  *server.Server
	config  *config.Config
	storage storage.Storage
	ctx     context.Context
	cancel  context.CancelFunc
	dataDir string
}

// StartTestServer starts a test server on a random available port
func StartTestServer(t *testing.T) *TestServer {
	// Create temporary data directory
	dataDir, err := os.MkdirTemp("", "fluxdl-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Find an available port
	port := findAvailablePort(t)

	// Create test configuration
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:           "localhost",
			Port:           port,
			ReadTimeout:    30,
			WriteTimeout:   30,
			MaxConnections: 100,
		},
		Storage: config.StorageConfig{
			Backend:   "badger",
			DataDir:   dataDir,
			BackupDir: filepath.Join(dataDir, "backups"),
			KVBackend: "memory", // Use memory for faster tests
		},
		Cluster: config.ClusterConfig{
			Enabled: false, // Disable clustering for unit tests
		},
		Logging: config.LoggingConfig{
			Level:  "error", // Reduce log noise in tests
			Format: "text",
		},
		Metrics: config.MetricsConfig{
			Enabled: false, // Disable metrics for tests
		},
	}

	// Initialize storage
	store, err := storage.NewCompositeStorage(cfg.Storage.DataDir, cfg.Storage.KVBackend)
	if err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	// Create server
	srv, err := server.NewServer(cfg, store)
	if err != nil {
		store.Close()
		os.RemoveAll(dataDir)
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create context for server lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	testServer := &TestServer{
		server:  srv,
		config:  cfg,
		storage: store,
		ctx:     ctx,
		cancel:  cancel,
		dataDir: dataDir,
	}

	// Start server in background
	go func() {
		if err := srv.Start(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("Server error: %v", err)
		}
	}()

	// Wait for server to be ready
	if !waitForServer(cfg.Server.Host, cfg.Server.Port, 10*time.Second) {
		testServer.Stop()
		t.Fatalf("Server failed to start within timeout")
	}

	// Cleanup on test completion
	t.Cleanup(func() {
		testServer.Stop()
	})

	return testServer
}

// GetAddress returns the server address
func (ts *TestServer) GetAddress() string {
	return net.JoinHostPort(ts.config.Server.Host, fmt.Sprintf("%d", ts.config.Server.Port))
}

// Stop stops the test server and cleans up resources
func (ts *TestServer) Stop() {
	if ts.cancel != nil {
		ts.cancel()
	}

	if ts.storage != nil {
		ts.storage.Close()
	}

	if ts.dataDir != "" {
		os.RemoveAll(ts.dataDir)
	}
}

// findAvailablePort finds an available port for testing
func findAvailablePort(t *testing.T) int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port
}

// waitForServer waits for the server to be ready to accept connections
func waitForServer(host string, port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	address := net.JoinHostPort(host, fmt.Sprintf("%d", port))

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}

	return false
}
