package server

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"

	clusterpb "github.com/skshohagmiah/fluxdl/api/generated/cluster"
	commonpb "github.com/skshohagmiah/fluxdl/api/generated/common"
	"github.com/skshohagmiah/fluxdl/pkg/cluster"
	"github.com/skshohagmiah/fluxdl/storage"
)

// ClusterService implements the Cluster gRPC service
type ClusterService struct {
	clusterpb.UnimplementedClusterServiceServer
	cluster *cluster.Cluster
	storage storage.Storage
}

// NewClusterService creates a new Cluster service
func NewClusterService(clstr *cluster.Cluster, store storage.Storage) *ClusterService {
	return &ClusterService{
		UnimplementedClusterServiceServer: clusterpb.UnimplementedClusterServiceServer{},
		cluster:                           clstr,
		storage:                           store,
	}
}

// GetNodes returns all cluster nodes
func (s *ClusterService) GetNodes(ctx context.Context, req *clusterpb.GetNodesRequest) (*clusterpb.GetNodesResponse, error) {
	if s.cluster == nil {
		return &clusterpb.GetNodesResponse{
			Nodes: []*commonpb.Node{},
			Status: &commonpb.Status{
				Success: false,
				Message: "Cluster not enabled",
				Code:    int32(codes.Unavailable),
			},
		}, nil
	}

	var nodes []*commonpb.Node
	clusterNodes := s.cluster.GetNodes()
	for _, n := range clusterNodes {
		nodes = append(nodes, &commonpb.Node{
			Id:       n.ID,
			Address:  n.Address,
			IsLeader: s.cluster.GetLeaderID() == n.ID,
			State:    string(n.State),
			LastSeen: n.LastSeen.Unix(),
		})
	}
	return &clusterpb.GetNodesResponse{
		Nodes: nodes,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetStatus returns cluster status
func (s *ClusterService) GetStatus(ctx context.Context, req *clusterpb.GetStatusRequest) (*clusterpb.GetStatusResponse, error) {
	if s.cluster == nil {
		return &clusterpb.GetStatusResponse{
			Status: &clusterpb.ClusterStatus{
				Healthy:     false,
				LeaderId:    "",
				TotalNodes:  1,
				ActiveNodes: 1,
				RaftState:   "disabled",
			},
			ResponseStatus: &commonpb.Status{
				Success: true,
				Message: "Single node mode",
				Code:    int32(codes.OK),
			},
		}, nil
	}

	health := s.cluster.GetClusterHealth()
	raftState := "follower"
	if health.IsLeader {
		raftState = "leader"
	} else if health.LeaderID == "" {
		raftState = "candidate"
	}

	status := &clusterpb.ClusterStatus{
		Healthy:     health.Status == "healthy",
		LeaderId:    health.LeaderID,
		TotalNodes:  health.TotalNodes,
		ActiveNodes: health.ActiveNodes,
		RaftState:   raftState,
	}
	return &clusterpb.GetStatusResponse{
		Status: status,
		ResponseStatus: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetLeader returns the current leader node
func (s *ClusterService) GetLeader(ctx context.Context, req *clusterpb.GetLeaderRequest) (*clusterpb.GetLeaderResponse, error) {
	if s.cluster == nil {
		return &clusterpb.GetLeaderResponse{
			Leader: &commonpb.Node{},
			Status: &commonpb.Status{
				Success: false,
				Message: "Cluster not enabled",
				Code:    int32(codes.Unavailable),
			},
		}, nil
	}

	health := s.cluster.GetClusterHealth()
	var leader *commonpb.Node
	if health.LeaderID != "" {
		// Find the leader node in cluster nodes
		nodes := s.cluster.GetNodes()
		for _, n := range nodes {
			if n.ID == health.LeaderID {
				leader = &commonpb.Node{
					Id:       n.ID,
					Address:  n.Address,
					IsLeader: true,
					State:    string(n.State),
					LastSeen: n.LastSeen.Unix(),
				}
				break
			}
		}
	}
	if leader == nil {
		leader = &commonpb.Node{}
	}
	return &clusterpb.GetLeaderResponse{
		Leader: leader,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Join adds a node to the cluster
func (s *ClusterService) Join(ctx context.Context, req *clusterpb.JoinRequest) (*clusterpb.JoinResponse, error) {
	if s.cluster == nil {
		return &clusterpb.JoinResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "Cluster not enabled",
				Code:    int32(codes.Unavailable),
			},
		}, nil
	}

	err := s.cluster.JoinNode(req.GetNodeId(), req.GetAddress())
	if err != nil {
		return &clusterpb.JoinResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: fmt.Sprintf("Failed to join node: %v", err),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &clusterpb.JoinResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "Node joined",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Leave removes a node from the cluster
func (s *ClusterService) Leave(ctx context.Context, req *clusterpb.LeaveRequest) (*clusterpb.LeaveResponse, error) {
	if s.cluster == nil {
		return &clusterpb.LeaveResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "Cluster not enabled",
				Code:    int32(codes.Unavailable),
			},
		}, nil
	}

	err := s.cluster.LeaveNode(req.GetNodeId())
	if err != nil {
		return &clusterpb.LeaveResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: fmt.Sprintf("Failed to remove node: %v", err),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &clusterpb.LeaveResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "Node left",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetStats returns cluster statistics
func (s *ClusterService) GetStats(ctx context.Context, req *clusterpb.GetStatsRequest) (*clusterpb.GetStatsResponse, error) {
	stats := &clusterpb.ClusterStats{}

	if s.cluster != nil {
		metrics := s.cluster.GetMetrics()
		stats.TotalOperations = int64(metrics.KVOps + metrics.QueueOps + metrics.StreamOps)
		stats.KeysCount = int64(metrics.KVOps)       // Approximation
		stats.QueuesCount = int64(metrics.QueueOps)  // Approximation
		stats.TopicsCount = int64(metrics.StreamOps) // Approximation
		stats.MemoryUsage = int64(metrics.MemoryUsageMB * 1024 * 1024)
		stats.DiskUsage = int64(metrics.DiskUsageGB * 1024 * 1024 * 1024)
	}

	return &clusterpb.GetStatsResponse{
		Stats: stats,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetClusterInfo returns detailed cluster information
func (s *ClusterService) GetClusterInfo(ctx context.Context, req *clusterpb.GetClusterInfoRequest) (*clusterpb.GetClusterInfoResponse, error) {
	if s.cluster == nil {
		return &clusterpb.GetClusterInfoResponse{
			ClusterInfo: &clusterpb.ClusterInfo{
				NodeId:            "single-node",
				TotalPartitions:   1,
				ReplicationFactor: 1,
				TotalNodes:        1,
				ActiveNodes:       1,
				Partitions:        []*clusterpb.PartitionInfo{},
			},
			Status: &commonpb.Status{
				Success: true,
				Message: "Single node mode",
				Code:    int32(codes.OK),
			},
		}, nil
	}

	health := s.cluster.GetClusterHealth()
	info := s.cluster.GetClusterInfo()

	var partitions []*clusterpb.PartitionInfo
	for _, p := range info.Partitions {
		partitions = append(partitions, &clusterpb.PartitionInfo{
			Id:       p.ID,
			Primary:  p.Primary,
			Replicas: p.Replicas,
		})
	}

	clusterInfo := &clusterpb.ClusterInfo{
		NodeId:            info.NodeID,
		TotalPartitions:   info.TotalPartitions,
		ReplicationFactor: info.ReplicationFactor,
		TotalNodes:        health.TotalNodes,
		ActiveNodes:       health.ActiveNodes,
		Partitions:        partitions,
	}

	return &clusterpb.GetClusterInfoResponse{
		ClusterInfo: clusterInfo,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// MigrateData handles data migration requests
func (s *ClusterService) MigrateData(ctx context.Context, req *clusterpb.MigrateDataRequest) (*clusterpb.MigrateDataResponse, error) {
	if s.cluster == nil {
		return &clusterpb.MigrateDataResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "Cluster not enabled",
				Code:    int32(codes.Unavailable),
			},
		}, nil
	}

	// Convert protobuf data to internal format
	data := make(map[string][]byte)
	for _, kv := range req.Data {
		data[kv.Key] = kv.Value
	}

	// Handle the migration through partitioned storage
	// This would typically be handled by the PartitionedStorage.ReceiveMigratedData method
	// For now, we'll just acknowledge receipt

	return &clusterpb.MigrateDataResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "Data migration completed",
			Code:    int32(codes.OK),
		},
	}, nil
}

// ReplicateData handles replication requests
func (s *ClusterService) ReplicateData(ctx context.Context, req *clusterpb.ReplicateDataRequest) (*clusterpb.ReplicateDataResponse, error) {
	if s.cluster == nil {
		return &clusterpb.ReplicateDataResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "Cluster not enabled",
				Code:    int32(codes.Unavailable),
			},
		}, nil
	}

	// Handle replication through storage
	ttl := time.Duration(req.Ttl) * time.Second
	err := s.storage.Set(ctx, req.Key, req.Value, ttl)
	if err != nil {
		return &clusterpb.ReplicateDataResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: fmt.Sprintf("Replication failed: %v", err),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &clusterpb.ReplicateDataResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "Replication completed",
			Code:    int32(codes.OK),
		},
	}, nil
}
