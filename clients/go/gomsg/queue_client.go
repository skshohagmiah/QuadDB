package gomsg

import (
	"context"
	"fmt"
	"time"

	queuepb "gomsg/api/generated/queue"
)

// QueueMessage represents a queue message
type QueueMessage struct {
	ID        string
	Queue     string
	Data      []byte
	Timestamp time.Time
	Headers   map[string]string
}

// QueueClient provides queue operations with automatic partitioning
type QueueClient struct {
	client *Client
}

// Push pushes a message to a queue
func (q *QueueClient) Push(ctx context.Context, queue string, data []byte, delay time.Duration) (string, error) {
	var messageID string
	
	err := q.client.executeWithRetry(func(node string) error {
		client := q.client.queueClients[node]
		
		req := &queuepb.PushRequest{
			Queue: queue,
			Data:  data,
		}
		
		if delay > 0 {
			req.DelaySeconds = int64(delay.Seconds())
		}
		
		resp, err := client.Push(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("queue push failed: %s", resp.Status.Message)
		}
		
		messageID = resp.MessageId
		return nil
	})
	
	return messageID, err
}

// Pop pops a message from a queue
func (q *QueueClient) Pop(ctx context.Context, queue string, timeout time.Duration) (*QueueMessage, error) {
	var message *QueueMessage
	
	err := q.client.executeWithRetry(func(node string) error {
		client := q.client.queueClients[node]
		
		req := &queuepb.PopRequest{
			Queue: queue,
		}
		
		if timeout > 0 {
			req.TimeoutSeconds = int64(timeout.Seconds())
		}
		
		resp, err := client.Pop(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("queue pop failed: %s", resp.Status.Message)
		}
		
		if resp.Message != nil {
			message = &QueueMessage{
				ID:        resp.Message.Id,
				Queue:     resp.Message.Queue,
				Data:      resp.Message.Data,
				Timestamp: time.Unix(resp.Message.Timestamp, 0),
				Headers:   resp.Message.Headers,
			}
		}
		
		return nil
	})
	
	return message, err
}

// PushBatch pushes multiple messages to a queue in a batch
func (q *QueueClient) PushBatch(ctx context.Context, queue string, messages [][]byte, delays []time.Duration) ([]string, error) {
	var messageIDs []string
	
	err := q.client.executeWithRetry(func(node string) error {
		client := q.client.queueClients[node]
		
		req := &queuepb.PushBatchRequest{
			Queue:    queue,
			Messages: messages,
		}
		
		// Convert delays to seconds
		if len(delays) > 0 {
			req.DelaySeconds = make([]int64, len(delays))
			for i, delay := range delays {
				req.DelaySeconds[i] = int64(delay.Seconds())
			}
		}
		
		resp, err := client.PushBatch(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("queue push batch failed: %s", resp.Status.Message)
		}
		
		messageIDs = resp.MessageIds
		return nil
	})
	
	return messageIDs, err
}

// PopBatch pops multiple messages from a queue
func (q *QueueClient) PopBatch(ctx context.Context, queue string, limit int, timeout time.Duration) ([]*QueueMessage, error) {
	var messages []*QueueMessage
	
	err := q.client.executeWithRetry(func(node string) error {
		client := q.client.queueClients[node]
		
		req := &queuepb.PopBatchRequest{
			Queue: queue,
			Limit: int32(limit),
		}
		
		if timeout > 0 {
			req.TimeoutSeconds = int64(timeout.Seconds())
		}
		
		resp, err := client.PopBatch(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("queue pop batch failed: %s", resp.Status.Message)
		}
		
		messages = make([]*QueueMessage, len(resp.Messages))
		for i, msg := range resp.Messages {
			messages[i] = &QueueMessage{
				ID:        msg.Id,
				Queue:     msg.Queue,
				Data:      msg.Data,
				Timestamp: time.Unix(msg.Timestamp, 0),
				Headers:   msg.Headers,
			}
		}
		
		return nil
	})
	
	return messages, err
}

// Size returns the size of a queue
func (q *QueueClient) Size(ctx context.Context, queue string) (int64, error) {
	var size int64
	
	err := q.client.executeWithRetry(func(node string) error {
		client := q.client.queueClients[node]
		
		resp, err := client.Size(ctx, &queuepb.SizeRequest{Queue: queue})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("queue size failed: %s", resp.Status.Message)
		}
		
		size = resp.Size
		return nil
	})
	
	return size, err
}

// Purge removes all messages from a queue
func (q *QueueClient) Purge(ctx context.Context, queue string) (int64, error) {
	var purgedCount int64
	
	err := q.client.executeWithRetry(func(node string) error {
		client := q.client.queueClients[node]
		
		resp, err := client.Purge(ctx, &queuepb.PurgeRequest{Queue: queue})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("queue purge failed: %s", resp.Status.Message)
		}
		
		purgedCount = resp.PurgedCount
		return nil
	})
	
	return purgedCount, err
}

// Stats returns statistics for a queue
func (q *QueueClient) Stats(ctx context.Context, queue string) (*QueueStats, error) {
	var stats *QueueStats
	
	err := q.client.executeWithRetry(func(node string) error {
		client := q.client.queueClients[node]
		
		resp, err := client.Stats(ctx, &queuepb.StatsRequest{Queue: queue})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("queue stats failed: %s", resp.Status.Message)
		}
		
		if resp.Stats != nil {
			stats = &QueueStats{
				Name:      resp.Stats.Name,
				Size:      resp.Stats.Size,
				Processed: resp.Stats.Processed,
				Failed:    resp.Stats.Failed,
				Consumers: resp.Stats.Consumers,
			}
		}
		
		return nil
	})
	
	return stats, err
}

// QueueStats represents queue statistics
type QueueStats struct {
	Name      string
	Size      int64
	Processed int64
	Failed    int64
	Consumers int64
}
