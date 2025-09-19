package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"gomsg/config"
	"gomsg/storage"
	
	kvpb "gomsg/api/generated/kv"
	queuepb "gomsg/api/generated/queue"
	streampb "gomsg/api/generated/stream"
	clusterpb "gomsg/api/generated/cluster"
)

// Server represents the gRPC server
type Server struct {
	config  *config.Config
	storage storage.Storage
	grpc    *grpc.Server
	
	kvService      *KVService
	queueService   *QueueService
	streamService  *StreamService
	clusterService *ClusterService
}

// NewServer creates a new server instance
func NewServer(cfg *config.Config, store storage.Storage) (*Server, error) {
	// Configure gRPC server options
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Second,
			MaxConnectionAge:      30 * time.Second,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  5 * time.Second,
			Timeout:               1 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(4 * 1024 * 1024),  // 4MB
		grpc.MaxSendMsgSize(4 * 1024 * 1024),  // 4MB
	}

	grpcServer := grpc.NewServer(opts...)

	server := &Server{
		config:  cfg,
		storage: store,
		grpc:    grpcServer,
	}

	// Initialize services
	server.kvService = NewKVService(store)
	server.queueService = NewQueueService(store)
	server.streamService = NewStreamService(store)
	server.clusterService = NewClusterService(store)

	// Register services
	kvpb.RegisterKVServiceServer(grpcServer, server.kvService)
	queuepb.RegisterQueueServiceServer(grpcServer, server.queueService)
	streampb.RegisterStreamServiceServer(grpcServer, server.streamService)
	clusterpb.RegisterClusterServiceServer(grpcServer, server.clusterService)

	return server, nil
}

// Start starts the server
func (s *Server) Start(ctx context.Context) error {
	address := fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port)
	
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	log.Printf("Starting gomsg server on %s", address)

	// Start gRPC server in a goroutine
	go func() {
		if err := s.grpc.Serve(listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	
	return s.Stop()
}

// Stop stops the server gracefully
func (s *Server) Stop() error {
	log.Println("Stopping gomsg server...")
	
	// Graceful stop with timeout
	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Server stopped gracefully")
	case <-time.After(30 * time.Second):
		log.Println("Force stopping server...")
		s.grpc.Stop()
	}

	return nil
}

// Health returns the server health status
func (s *Server) Health() bool {
	// Basic health check - could be enhanced with storage checks
	return true
}