// Package server provides the Stream gRPC service implementation.
package server

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "gomsg/api/generated/common"
	streampb "gomsg/api/generated/stream"
	"gomsg/storage"
)

// StreamService implements the Stream gRPC service
type StreamService struct {
	streampb.UnimplementedStreamServiceServer
	storage storage.Storage
}

// NewStreamService creates a new Stream service
func NewStreamService(store storage.Storage) *StreamService {
	return &StreamService{
		storage: store,
	}
}

// Publish publishes a message to a stream
func (s *StreamService) Publish(ctx context.Context, req *streampb.PublishRequest) (*streampb.PublishResponse, error) {
	if req.Topic == "" {
		return &streampb.PublishResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	message, err := s.storage.StreamPublish(ctx, req.Topic, req.PartitionKey, req.Data, req.Headers)
	if err != nil {
		return &streampb.PublishResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.PublishResponse{
		MessageId: message.ID,
		Offset:    message.Offset,
		Partition: message.Partition,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Subscribe subscribes to a stream (streaming RPC)
func (s *StreamService) Subscribe(req *streampb.SubscribeRequest, stream streampb.StreamService_SubscribeServer) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic cannot be empty")
	}

	// This is a simplified implementation
	// In a real system, you'd implement proper streaming with consumer groups
	// For now, we'll return an error indicating this needs implementation
	return status.Error(codes.Unimplemented, "streaming subscription not yet implemented")
}

// Read reads messages from a stream
func (s *StreamService) Read(ctx context.Context, req *streampb.ReadRequest) (*streampb.ReadResponse, error) {
	if req.Topic == "" {
		return &streampb.ReadResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}

	messages, err := s.storage.StreamRead(ctx, req.Topic, req.Partition, req.FromOffset, limit)
	if err != nil {
		return &streampb.ReadResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	var streamMsgs []*streampb.StreamMessage
	var nextOffset int64

	for _, message := range messages {
		streamMsgs = append(streamMsgs, &streampb.StreamMessage{
			Id:           message.ID,
			Topic:        message.Topic,
			PartitionKey: message.PartitionKey,
			Data:         message.Data,
			Offset:       message.Offset,
			Timestamp:    message.Timestamp.Unix(),
			Headers:      message.Headers,
		})
		
		if message.Offset > nextOffset {
			nextOffset = message.Offset
		}
	}

	return &streampb.ReadResponse{
		Messages:   streamMsgs,
		NextOffset: nextOffset + 1,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Seek sets the offset for a consumer
func (s *StreamService) Seek(ctx context.Context, req *streampb.SeekRequest) (*streampb.SeekResponse, error) {
	if req.Topic == "" {
		return &streampb.SeekResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	if req.ConsumerId == "" {
		return &streampb.SeekResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "consumer ID cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	err := s.storage.StreamSeek(ctx, req.Topic, req.ConsumerId, req.Partition, req.Offset)
	if err != nil {
		return &streampb.SeekResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.SeekResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetOffset gets the current offset for a consumer
func (s *StreamService) GetOffset(ctx context.Context, req *streampb.GetOffsetRequest) (*streampb.GetOffsetResponse, error) {
	if req.Topic == "" {
		return &streampb.GetOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	if req.ConsumerId == "" {
		return &streampb.GetOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "consumer ID cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	offset, err := s.storage.StreamGetOffset(ctx, req.Topic, req.ConsumerId, req.Partition)
	if err != nil {
		return &streampb.GetOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.GetOffsetResponse{
		Offset: offset,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// CreateTopic creates a new topic
func (s *StreamService) CreateTopic(ctx context.Context, req *streampb.CreateTopicRequest) (*streampb.CreateTopicResponse, error) {
	if req.Name == "" {
		return &streampb.CreateTopicResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	partitions := req.Partitions
	if partitions <= 0 {
		partitions = 1
	}

	err := s.storage.StreamCreateTopic(ctx, req.Name, partitions)
	if err != nil {
		return &streampb.CreateTopicResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.CreateTopicResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// DeleteTopic deletes a topic
func (s *StreamService) DeleteTopic(ctx context.Context, req *streampb.DeleteTopicRequest) (*streampb.DeleteTopicResponse, error) {
	if req.Name == "" {
		return &streampb.DeleteTopicResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	err := s.storage.StreamDeleteTopic(ctx, req.Name)
	if err != nil {
		return &streampb.DeleteTopicResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.DeleteTopicResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// ListTopics lists all topics
func (s *StreamService) ListTopics(ctx context.Context, req *streampb.ListTopicsRequest) (*streampb.ListTopicsResponse, error) {
	topics, err := s.storage.StreamListTopics(ctx)
	if err != nil {
		return &streampb.ListTopicsResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.ListTopicsResponse{
		Topics: topics,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetTopicInfo gets information about a topic
func (s *StreamService) GetTopicInfo(ctx context.Context, req *streampb.GetTopicInfoRequest) (*streampb.GetTopicInfoResponse, error) {
	if req.Topic == "" {
		return &streampb.GetTopicInfoResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	info, err := s.storage.StreamGetTopicInfo(ctx, req.Topic)
	if err != nil {
		// Return gRPC error for topic not found
		if err == badger.ErrKeyNotFound {
			return nil, status.Error(codes.NotFound, "topic not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var partitionInfo []*commonpb.Partition
	for _, p := range info.PartitionInfo {
		partitionInfo = append(partitionInfo, &commonpb.Partition{
			Id:       p.ID,
			Leader:   p.Leader,
			Replicas: p.Replicas,
			Offset:   p.Offset,
		})
	}

	topicInfo := &streampb.TopicInfo{
		Name:          info.Name,
		Partitions:    info.Partitions,
		TotalMessages: info.TotalMessages,
		PartitionInfo: partitionInfo,
	}

	return &streampb.GetTopicInfoResponse{
		Info: topicInfo,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// ReadFrom reads messages from a stream starting from a timestamp
func (s *StreamService) ReadFrom(ctx context.Context, req *streampb.ReadFromRequest) (*streampb.ReadFromResponse, error) {
	if req.Topic == "" {
		return &streampb.ReadFromResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}

	timestamp := time.Unix(req.Timestamp, 0)
	messages, err := s.storage.StreamReadFrom(ctx, req.Topic, req.Partition, timestamp, limit)
	if err != nil {
		return &streampb.ReadFromResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	var streamMsgs []*streampb.StreamMessage
	var nextOffset int64

	for _, message := range messages {
		streamMsgs = append(streamMsgs, &streampb.StreamMessage{
			Id:           message.ID,
			Topic:        message.Topic,
			PartitionKey: message.PartitionKey,
			Data:         message.Data,
			Offset:       message.Offset,
			Timestamp:    message.Timestamp.Unix(),
			Headers:      message.Headers,
		})
		
		if message.Offset > nextOffset {
			nextOffset = message.Offset
		}
	}

	return &streampb.ReadFromResponse{
		Messages:   streamMsgs,
		NextOffset: nextOffset + 1,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Purge removes all messages from a topic
func (s *StreamService) Purge(ctx context.Context, req *streampb.PurgeRequest) (*streampb.PurgeResponse, error) {
	if req.Topic == "" {
		return &streampb.PurgeResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	purgedCount, err := s.storage.StreamPurge(ctx, req.Topic)
	if err != nil {
		return &streampb.PurgeResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.PurgeResponse{
		PurgedCount: purgedCount,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// SubscribeGroup subscribes to a stream with consumer group
func (s *StreamService) SubscribeGroup(req *streampb.SubscribeGroupRequest, stream streampb.StreamService_SubscribeGroupServer) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic cannot be empty")
	}
	
	if req.GroupId == "" {
		return status.Error(codes.InvalidArgument, "group ID cannot be empty")
	}
	
	if req.ConsumerId == "" {
		return status.Error(codes.InvalidArgument, "consumer ID cannot be empty")
	}

	ctx := stream.Context()
	
	// Subscribe to the consumer group
	err := s.storage.StreamSubscribeGroup(ctx, req.Topic, req.GroupId, req.ConsumerId)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to subscribe to group: %v", err))
	}

	// Cleanup on disconnect
	defer func() {
		s.storage.StreamUnsubscribeGroup(context.Background(), req.Topic, req.GroupId, req.ConsumerId)
	}()

	// Start consuming messages
	ticker := time.NewTicker(100 * time.Millisecond) // Poll every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Get current group offset for partition 0 (simplified)
			offset, err := s.storage.StreamGetGroupOffset(ctx, req.Topic, req.GroupId, 0)
			if err != nil {
				continue
			}

			// Read messages from current offset
			messages, err := s.storage.StreamRead(ctx, req.Topic, 0, offset, 10)
			if err != nil {
				continue
			}

			// Send messages to client
			for _, message := range messages {
				streamMsg := &streampb.StreamMessage{
					Id:           message.ID,
					Topic:        message.Topic,
					PartitionKey: message.PartitionKey,
					Data:         message.Data,
					Offset:       message.Offset,
					Timestamp:    message.Timestamp.Unix(),
					Headers:      message.Headers,
				}

				response := &streampb.SubscribeResponse{
					Message: streamMsg,
					Status: &commonpb.Status{
						Success: true,
						Message: "OK",
					},
				}
				
				if err := stream.Send(response); err != nil {
					return err
				}

				// Commit offset after successful send
				s.storage.StreamCommitGroupOffset(ctx, req.Topic, req.GroupId, 0, message.Offset+1)
			}
		}
	}
}

// CommitGroupOffset commits the offset for a consumer group
func (s *StreamService) CommitGroupOffset(ctx context.Context, req *streampb.CommitGroupOffsetRequest) (*streampb.CommitGroupOffsetResponse, error) {
	if req.Topic == "" {
		return &streampb.CommitGroupOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	if req.GroupId == "" {
		return &streampb.CommitGroupOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "group ID cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	err := s.storage.StreamCommitGroupOffset(ctx, req.Topic, req.GroupId, req.Partition, req.Offset)
	if err != nil {
		return &streampb.CommitGroupOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.CommitGroupOffsetResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// GetGroupOffset gets the current offset for a consumer group
func (s *StreamService) GetGroupOffset(ctx context.Context, req *streampb.GetGroupOffsetRequest) (*streampb.GetGroupOffsetResponse, error) {
	if req.Topic == "" {
		return &streampb.GetGroupOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "topic cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	if req.GroupId == "" {
		return &streampb.GetGroupOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "group ID cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	offset, err := s.storage.StreamGetGroupOffset(ctx, req.Topic, req.GroupId, req.Partition)
	if err != nil {
		return &streampb.GetGroupOffsetResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &streampb.GetGroupOffsetResponse{
		Offset: offset,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}