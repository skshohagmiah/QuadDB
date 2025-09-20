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
	"gomsg/pkg/cluster"
	raftex "gomsg/pkg/cluster/raft"
	"gomsg/storage"

	clusterpb "gomsg/api/generated/cluster"
	kvpb "gomsg/api/generated/kv"
	queuepb "gomsg/api/generated/queue"
)

// Server represents the gRPC server
type Server struct {
	config  *config.Config
	storage storage.Storage
	grpc    *grpc.Server

	kvService      *KVService
	queueService   *QueueService
	clusterService *ClusterService
	clusterMgr     *cluster.Manager
	raftNode       *raftex.Node
	submitter      *raftex.Submitter
}

// nodeProviderAdapter adapts the in-process cluster.Manager to storage.NodeProvider
type nodeProviderAdapter struct{ m *cluster.Manager }

func (a nodeProviderAdapter) ListNodes() []storage.NodeInfo {
	nodes := a.m.GetNodes()
	out := make([]storage.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, storage.NodeInfo{ID: n.ID, Address: n.Address})
	}
	return out
}

func (a nodeProviderAdapter) LeaderID() string {
	if ln, ok := a.m.GetLeader(); ok {
		return ln.ID
	}
	return ""
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

	// Initialize cluster manager (in-process stub)
	server.clusterMgr = cluster.NewManager(cluster.Config{
		NodeID:       cfg.Cluster.NodeID,
		Address:      cfg.Cluster.BindAddr,
		HeartbeatTTL: 10 * time.Second,
	})

	// Adapt cluster manager to storage.NodeProvider
	if bs, ok := store.(*storage.BadgerStorage); ok {
		bs.SetNodeProvider(nodeProviderAdapter{m: server.clusterMgr})
		if cfg.Cluster.Replicas > 0 {
			bs.SetReplicationFactor(cfg.Cluster.Replicas)
		}
	}

	// Initialize services
	server.kvService = NewKVService(store, nil, server.clusterMgr)
	server.queueService = NewQueueService(store)
	server.clusterService = NewClusterService(server.clusterMgr, store)

	// Start Raft if clustering is enabled
	if cfg.Cluster.Enabled {
		rn, err := raftex.Start(raftex.Config{
			NodeID:   cfg.Cluster.NodeID,
			BindAddr: cfg.Cluster.BindAddr,
			DataDir:  cfg.Cluster.DataDir,
			Bootstrap: cfg.Cluster.Bootstrap,
			JoinAddrs: cfg.Cluster.JoinAddresses,
		}, raftex.NewFSM(store))
		if err != nil {
			return nil, fmt.Errorf("raft start: %w", err)
		}
		server.raftNode = rn
		server.submitter = raftex.NewSubmitter(rn)
		// Recreate KV service with submitter support
		server.kvService = NewKVService(store, server.submitter, server.clusterMgr)
	}

	// Register services
	kvpb.RegisterKVServiceServer(grpcServer, server.kvService)
	queuepb.RegisterQueueServiceServer(grpcServer, server.queueService)
	// TODO: Enable after generating stream gRPC code
	// streampb.RegisterStreamServiceServer(grpcServer, server.streamService)
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
	
	// Stop Raft node if it's running
	if s.raftNode != nil {
		if err := s.raftNode.Shutdown(); err != nil {
			log.Printf("raft shutdown: %v", err)
		}
	}

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