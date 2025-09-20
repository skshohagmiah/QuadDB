package server

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"

	clusterpb "gomsg/api/generated/cluster"
	commonpb "gomsg/api/generated/common"
	"gomsg/pkg/cluster"
	"gomsg/storage"
)

// ClusterService implements the Cluster gRPC service
type ClusterService struct {
    clusterpb.UnimplementedClusterServiceServer
    manager *cluster.Manager
    storage storage.Storage
}

// NewClusterService creates a new Cluster service
func NewClusterService(mgr *cluster.Manager, store storage.Storage) *ClusterService {
    return &ClusterService{
        manager: mgr,
        storage: store,
    }
}

// GetNodes returns all cluster nodes
func (s *ClusterService) GetNodes(ctx context.Context, req *clusterpb.GetNodesRequest) (*clusterpb.GetNodesResponse, error) {
    var nodes []*commonpb.Node
    for _, n := range s.manager.GetNodes() {
        nodes = append(nodes, &commonpb.Node{
            Id:       n.ID,
            Address:  n.Address,
            IsLeader: n.Role == cluster.RoleLeader,
            State:    n.State,
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
    nodes := s.manager.GetNodes()
    leader, hasLeader := s.manager.GetLeader()
    leaderID := ""
    if hasLeader {
        leaderID = leader.ID
    }
    status := &clusterpb.ClusterStatus{
        Healthy:     true,
        LeaderId:    leaderID,
        TotalNodes:  int32(len(nodes)),
        ActiveNodes: int32(len(nodes)),
        RaftState:   "stub", // replace when real RAFT added
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
    ln, ok := s.manager.GetLeader()
    var leader *commonpb.Node
    if ok {
        leader = &commonpb.Node{Id: ln.ID, Address: ln.Address, IsLeader: true, State: ln.State, LastSeen: ln.LastSeen.Unix()}
    } else {
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
    s.manager.Join(req.GetNodeId(), req.GetAddress())
    // Treat a join as an implicit heartbeat to mark freshness
    s.manager.Heartbeat(req.GetNodeId())
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
    s.manager.Leave(req.GetNodeId())
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
    // Placeholder: tie into metrics later
    stats := &clusterpb.ClusterStats{}
    _ = time.Now()
    return &clusterpb.GetStatsResponse{
        Stats: stats,
        Status: &commonpb.Status{
            Success: true,
            Message: "OK",
            Code:    int32(codes.OK),
        },
    }, nil
}