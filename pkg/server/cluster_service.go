package server

import (
	"context"

	"google.golang.org/grpc/codes"

	"gomsg/storage"
	clusterpb "gomsg/api/generated/cluster"
	commonpb "gomsg/api/generated/common"
)

// ClusterService implements the Cluster gRPC service
type ClusterService struct {
	clusterpb.UnimplementedClusterServiceServer
	storage storage.Storage
}

// NewClusterService creates a new Cluster service
func NewClusterService(store storage.Storage) *ClusterService {
	return &ClusterService{
		storage: store,
	}
}

// GetNodes returns all cluster nodes
func (s *ClusterService) GetNodes(ctx context.Context, req *clusterpb.GetNodesRequest) (*clusterpb.GetNodesResponse, error) {
	// This is a basic implementation - in a real system you'd implement proper clustering
	nodes := []*commonpb.Node{
		{
			Id:        "node1",
			Address:   "localhost:9000",
			IsLeader:  true,
			State:     "active",
			LastSeen:  0,
		},
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
	status := &clusterpb.ClusterStatus{
		Healthy:     true,
		LeaderId:    "node1",
		TotalNodes:  1,
		ActiveNodes: 1,
		RaftState:   "leader",
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
	leader := &commonpb.Node{
		Id:        "node1",
		Address:   "localhost:9000",
		IsLeader:  true,
		State:     "active",
		LastSeen:  0,
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
	// Basic implementation - in a real system you'd implement proper node joining
	return &clusterpb.JoinResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "Node joined successfully",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Leave removes a node from the cluster
func (s *ClusterService) Leave(ctx context.Context, req *clusterpb.LeaveRequest) (*clusterpb.LeaveResponse, error) {
	// Basic implementation - in a real system you'd implement proper node removal
	return &clusterpb.LeaveResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "Node left successfully",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetStats returns cluster statistics
func (s *ClusterService) GetStats(ctx context.Context, req *clusterpb.GetStatsRequest) (*clusterpb.GetStatsResponse, error) {
	stats := &clusterpb.ClusterStats{
		TotalOperations: 0,
		KeysCount:       0,
		QueuesCount:     0,
		TopicsCount:     0,
		MemoryUsage:     0,
		DiskUsage:       0,
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