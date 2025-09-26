package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	clusterpb "github.com/skshohagmiah/gomsg/api/generated/cluster"
	documentpb "github.com/skshohagmiah/gomsg/api/generated/document"
	kvpb "github.com/skshohagmiah/gomsg/api/generated/kv"
	queuepb "github.com/skshohagmiah/gomsg/api/generated/queue"
	streampb "github.com/skshohagmiah/gomsg/api/generated/stream"
	"github.com/skshohagmiah/gomsg/config"
	"github.com/skshohagmiah/gomsg/pkg/cluster"
	"github.com/skshohagmiah/gomsg/storage"
)

// Server represents the gRPC server
type Server struct {
	config  *config.Config
	storage storage.Storage
	grpc    *grpc.Server

	kvService       *KVService
	queueService    *QueueService
	streamService   *StreamService
	dbService *DBService
	clusterService  *ClusterService
	cluster         *cluster.Cluster
}

// nodeProviderAdapter adapts the cluster to storage.NodeProvider
type nodeProviderAdapter struct{ c *cluster.Cluster }

func (a nodeProviderAdapter) ListNodes() []storage.NodeInfo {
	if a.c == nil {
		return []storage.NodeInfo{}
	}
	nodes := a.c.GetNodes()
	out := make([]storage.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, storage.NodeInfo{ID: n.ID, Address: n.Address})
	}
	return out
}

func (a nodeProviderAdapter) LeaderID() string {
	if a.c == nil {
		return ""
	}
	health := a.c.GetClusterHealth()
	return health.LeaderID
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
		grpc.MaxRecvMsgSize(4 * 1024 * 1024), // 4MB
		grpc.MaxSendMsgSize(4 * 1024 * 1024), // 4MB
	}

	grpcServer := grpc.NewServer(opts...)

	server := &Server{
		config:  cfg,
		storage: store,
		grpc:    grpcServer,
	}

	// Initialize cluster if enabled
	if cfg.Cluster.Enabled {
		clusterConfig := cluster.Config{
			NodeID:            cfg.Cluster.NodeID,
			BindAddr:          cfg.Cluster.BindAddr,
			DataDir:           cfg.Storage.DataDir,
			Bootstrap:         cfg.Cluster.Bootstrap,
			Partitions:        32,         // Default partitions
			ReplicationFactor: 3,          // Default replication
			SeedNodes:         []string{}, // Would be populated from config
		}

		clstr, err := cluster.New(context.Background(), store, clusterConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to start cluster: %w", err)
		}
		server.cluster = clstr
	} else {
		server.cluster = nil
	}

	// Adapt cluster to storage.NodeProvider for stream leader/replica assignment
	if server.cluster != nil {
		if bs, ok := store.(*storage.BadgerStorage); ok {
			bs.SetNodeProvider(nodeProviderAdapter{c: server.cluster})
			if cfg.Cluster.Replicas > 0 {
				bs.SetReplicationFactor(cfg.Cluster.Replicas)
			}
		}
		if cs, ok := store.(*storage.CompositeStorage); ok {
			cs.SetNodeProviderIfSupported(nodeProviderAdapter{c: server.cluster})
			if cfg.Cluster.Replicas > 0 {
				cs.SetReplicationFactorIfSupported(cfg.Cluster.Replicas)
			}
		}
	}

	// Initialize services
	server.kvService = NewKVService(store)
	server.queueService = NewQueueService(store)
	server.streamService = NewStreamService(store)
	server.dbService = NewDBService(store)
	server.clusterService = NewClusterService(server.cluster, store)

	// Register services
	log.Printf("Registering KV service...")
	kvpb.RegisterKVServiceServer(grpcServer, server.kvService)
	log.Printf("Registering Queue service...")
	queuepb.RegisterQueueServiceServer(grpcServer, server.queueService)
	log.Printf("Registering Stream service...")
	streampb.RegisterStreamServiceServer(grpcServer, server.streamService)
	log.Printf("Registering Document service...")
	documentpb.RegisterDocumentServiceServer(grpcServer, server.dbService)
	log.Printf("Registering Cluster service...")
	clusterpb.RegisterClusterServiceServer(grpcServer, server.clusterService)

	// Register reflection service for debugging
	log.Printf("Registering reflection service...")
	reflection.Register(grpcServer)
	log.Printf("All services registered successfully")

	return server, nil
}

// Start starts the server
func (s *Server) Start(ctx context.Context) error {
	address := fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	log.Printf("Starting fluxdl server on %s", address)

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
	log.Println("Stopping fluxdl server...")

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

	if s.cluster != nil {
		s.cluster.Close()
	}

	return nil
}

// Health returns the server health status
func (s *Server) Health() bool {
	// Basic health check - could be enhanced with storage checks
	return true
}
