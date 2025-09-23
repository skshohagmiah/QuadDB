package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	DefaultServerAddr = "localhost:9000"
	TestTimeout       = 30 * time.Second
	RetryAttempts     = 5
	RetryDelay        = 2 * time.Second
)

// TestConfig holds configuration for tests
type TestConfig struct {
	ServerAddr string
	Timeout    time.Duration
}

// GetTestConfig returns test configuration from environment or defaults
func GetTestConfig() *TestConfig {
	addr := os.Getenv("fluxdl_TEST_ADDR")
	if addr == "" {
		addr = DefaultServerAddr
	}

	return &TestConfig{
		ServerAddr: addr,
		Timeout:    TestTimeout,
	}
}

// WaitForServer waits for the fluxdl server to be ready
func WaitForServer(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), TestTimeout)
	defer cancel()

	for i := 0; i < RetryAttempts; i++ {
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err == nil && resp.Status == grpc_health_v1.HealthCheckResponse_SERVING {
			return nil
		}

		if i < RetryAttempts-1 {
			log.Printf("Server not ready, retrying in %v... (attempt %d/%d)", RetryDelay, i+1, RetryAttempts)
			time.Sleep(RetryDelay)
		}
	}

	return fmt.Errorf("server not ready after %d attempts", RetryAttempts)
}

// SetupTest performs common test setup
func SetupTest(t *testing.T) *TestConfig {
	config := GetTestConfig()

	// Check if server is running
	if err := WaitForServer(config.ServerAddr); err != nil {
		t.Skipf("fluxdl server not available at %s: %v", config.ServerAddr, err)
	}

	return config
}

// CleanupTest performs common test cleanup
func CleanupTest(t *testing.T, config *TestConfig) {
	// Add any cleanup logic here
	// For example, clearing test data, resetting counters, etc.
}

// TestMain can be used to setup/teardown for the entire test suite
func TestMain(m *testing.M) {
	// Setup
	log.Println("Setting up test suite...")

	config := GetTestConfig()
	log.Printf("Testing against server: %s", config.ServerAddr)

	// Check server availability
	if err := WaitForServer(config.ServerAddr); err != nil {
		log.Printf("Warning: Server not available: %v", err)
		log.Println("Some tests may be skipped")
	}

	// Run tests
	code := m.Run()

	// Teardown
	log.Println("Test suite completed")

	os.Exit(code)
}
