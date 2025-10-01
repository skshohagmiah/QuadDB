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
	
	// For async cluster initialization
	pendingClusterConfig *cluster.Config
	pendingStore         storage.Storage
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
		log.Printf("Initializing cluster for node %s", cfg.Cluster.NodeID)
		bindAddr := cfg.Cluster.BindAddr
		if bindAddr == "" {
			// Use server host with a different port for Raft
			bindAddr = fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port+1000)
		}
		log.Printf("Cluster bind address: %s", bindAddr)
		
		clusterDataDir := cfg.Cluster.DataDir
		if clusterDataDir == "" {
			clusterDataDir = cfg.Storage.DataDir + "/cluster"
		}
		log.Printf("Cluster data directory: %s", clusterDataDir)
		
		clusterConfig := cluster.Config{
			NodeID:    cfg.Cluster.NodeID,
			BindAddr:  bindAddr,
			DataDir:   clusterDataDir,
			Bootstrap: cfg.Cluster.Bootstrap,
			SeedNodes: cfg.Cluster.JoinAddresses,
		}

		if cfg.Cluster.Bootstrap {
			// Bootstrap nodes need synchronous initialization
			log.Printf("Creating bootstrap cluster with config: %+v", clusterConfig)
			clstr, err := cluster.New(context.Background(), store, clusterConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to start cluster: %w", err)
			}
			log.Printf("Bootstrap cluster initialized successfully")
			server.cluster = clstr
		} else {
			// Non-bootstrap nodes: initialize cluster asynchronously after server starts
			log.Printf("Scheduling async cluster initialization for joining node")
			server.cluster = nil // Will be set when cluster initialization completes
			
			// Store config for later initialization
			server.pendingClusterConfig = &clusterConfig
			server.pendingStore = store
		}
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

	// Initialize cluster asynchronously after server is running
	if s.pendingClusterConfig != nil {
		go s.initializeClusterAsync()
	}

	// Wait for context cancellation
	<-ctx.Done()

	return s.Stop()
}

// initializeClusterAsync initializes the cluster after the gRPC server is running
func (s *Server) initializeClusterAsync() {
	log.Printf("Starting async cluster initialization")
	
	// Wait for gRPC server to be fully ready
	time.Sleep(2 * time.Second)
	
	log.Printf("Creating joining cluster with config: %+v", *s.pendingClusterConfig)
	clstr, err := cluster.New(context.Background(), s.pendingStore, *s.pendingClusterConfig)
	if err != nil {
		log.Printf("Failed to initialize cluster: %v", err)
		return
	}
	
	log.Printf("Joining cluster initialized successfully")
	s.cluster = clstr
	
	// Update cluster service with the new cluster instance
	if s.clusterService != nil {
		s.clusterService.SetCluster(clstr)
	}
	
	// Update storage adapter
	if bs, ok := s.pendingStore.(*storage.BadgerStorage); ok {
		bs.SetNodeProvider(nodeProviderAdapter{c: clstr})
		if s.config.Cluster.Replicas > 0 {
			bs.SetReplicationFactor(s.config.Cluster.Replicas)
		}
	}
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
