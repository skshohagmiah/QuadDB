package server

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gomsg/storage"
	queuepb "gomsg/api/generated/queue"
	commonpb "gomsg/api/generated/common"
)

// QueueService implements the Queue gRPC service
type QueueService struct {
	queuepb.UnimplementedQueueServiceServer
	storage storage.Storage
}

// NewQueueService creates a new Queue service
func NewQueueService(store storage.Storage) *QueueService {
	return &QueueService{
		storage: store,
	}
}

// Push adds a message to a queue
func (s *QueueService) Push(ctx context.Context, req *queuepb.PushRequest) (*queuepb.PushResponse, error) {
	if req.Queue == "" {
		return &queuepb.PushResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "queue name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	var delay time.Duration
	if req.Delay > 0 {
		delay = time.Duration(req.Delay) * time.Second
	}

	messageID, err := s.storage.QueuePush(ctx, req.Queue, req.Data, delay)
	if err != nil {
		return &queuepb.PushResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &queuepb.PushResponse{
		MessageId: messageID,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Pop removes and returns a message from a queue
func (s *QueueService) Pop(ctx context.Context, req *queuepb.PopRequest) (*queuepb.PopResponse, error) {
	if req.Queue == "" {
		return &queuepb.PopResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "queue name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	timeout := time.Duration(req.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	message, err := s.storage.QueuePop(ctx, req.Queue, timeout)
	if err != nil {
		return &queuepb.PopResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	queueMsg := &queuepb.QueueMessage{
		Id:         message.ID,
		Queue:      message.Queue,
		Data:       message.Data,
		CreatedAt:  message.CreatedAt.Unix(),
		DelayUntil: message.DelayUntil.Unix(),
		RetryCount: message.RetryCount,
		ConsumerId: message.ConsumerID,
	}

	return &queuepb.PopResponse{
		Message: queueMsg,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Peek returns messages without removing them
func (s *QueueService) Peek(ctx context.Context, req *queuepb.PeekRequest) (*queuepb.PeekResponse, error) {
	if req.Queue == "" {
		return &queuepb.PeekResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "queue name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}

	messages, err := s.storage.QueuePeek(ctx, req.Queue, limit)
	if err != nil {
		return &queuepb.PeekResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	var queueMsgs []*queuepb.QueueMessage
	for _, message := range messages {
		queueMsgs = append(queueMsgs, &queuepb.QueueMessage{
			Id:         message.ID,
			Queue:      message.Queue,
			Data:       message.Data,
			CreatedAt:  message.CreatedAt.Unix(),
			DelayUntil: message.DelayUntil.Unix(),
			RetryCount: message.RetryCount,
			ConsumerId: message.ConsumerID,
		})
	}

	return &queuepb.PeekResponse{
		Messages: queueMsgs,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Ack acknowledges a message
func (s *QueueService) Ack(ctx context.Context, req *queuepb.AckRequest) (*queuepb.AckResponse, error) {
	if req.MessageId == "" {
		return &queuepb.AckResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "message ID cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	err := s.storage.QueueAck(ctx, req.MessageId)
	if err != nil {
		return &queuepb.AckResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &queuepb.AckResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Nack rejects a message and requeues it
func (s *QueueService) Nack(ctx context.Context, req *queuepb.NackRequest) (*queuepb.NackResponse, error) {
	if req.MessageId == "" {
		return &queuepb.NackResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "message ID cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	err := s.storage.QueueNack(ctx, req.MessageId)
	if err != nil {
		return &queuepb.NackResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &queuepb.NackResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Stats returns queue statistics
func (s *QueueService) Stats(ctx context.Context, req *queuepb.StatsRequest) (*queuepb.StatsResponse, error) {
	if req.Queue == "" {
		return &queuepb.StatsResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "queue name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	stats, err := s.storage.QueueStats(ctx, req.Queue)
	if err != nil {
		return &queuepb.StatsResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	queueStats := &queuepb.QueueStats{
		Name:      stats.Name,
		Size:      stats.Size,
		Consumers: stats.Consumers,
		Pending:   stats.Pending,
		Processed: stats.Processed,
		Failed:    stats.Failed,
	}

	return &queuepb.StatsResponse{
		Stats: queueStats,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Purge removes all messages from a queue
func (s *QueueService) Purge(ctx context.Context, req *queuepb.PurgeRequest) (*queuepb.PurgeResponse, error) {
	if req.Queue == "" {
		return &queuepb.PurgeResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "queue name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	purged, err := s.storage.QueuePurge(ctx, req.Queue)
	if err != nil {
		return &queuepb.PurgeResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &queuepb.PurgeResponse{
		PurgedCount: purged,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// Delete removes an entire queue
func (s *QueueService) Delete(ctx context.Context, req *queuepb.DeleteRequest) (*queuepb.DeleteResponse, error) {
	if req.Queue == "" {
		return &queuepb.DeleteResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: "queue name cannot be empty",
				Code:    int32(codes.InvalidArgument),
			},
		}, nil
	}

	err := s.storage.QueueDelete(ctx, req.Queue)
	if err != nil {
		return &queuepb.DeleteResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &queuepb.DeleteResponse{
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}

// List returns all queue names
func (s *QueueService) List(ctx context.Context, req *queuepb.ListRequest) (*queuepb.ListResponse, error) {
	queues, err := s.storage.QueueList(ctx)
	if err != nil {
		return &queuepb.ListResponse{
			Status: &commonpb.Status{
				Success: false,
				Message: err.Error(),
				Code:    int32(codes.Internal),
			},
		}, nil
	}

	return &queuepb.ListResponse{
		Queues: queues,
		Status: &commonpb.Status{
			Success: true,
			Message: "OK",
			Code:    int32(codes.OK),
		},
	}, nil
}