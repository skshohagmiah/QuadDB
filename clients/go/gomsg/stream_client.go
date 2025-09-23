package gomsg

import (
	"context"
	"fmt"
	"io"
	"time"

	streampb "gomsg/api/generated/stream"
)

// StreamMessage represents a stream message
type StreamMessage struct {
	ID           string
	Topic        string
	PartitionKey string
	Data         []byte
	Offset       int64
	Timestamp    time.Time
	Headers      map[string]string
	Partition    int32
}

// StreamClient provides stream operations with automatic partitioning
type StreamClient struct {
	client *Client
}

// Publish publishes a message to a stream
func (s *StreamClient) Publish(ctx context.Context, topic, partitionKey string, data []byte, headers map[string]string) (*StreamMessage, error) {
	var message *StreamMessage
	
	err := s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.PublishRequest{
			Topic:        topic,
			PartitionKey: partitionKey,
			Data:         data,
			Headers:      headers,
		}
		
		resp, err := client.Publish(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("stream publish failed: %s", resp.Status.Message)
		}
		
		message = &StreamMessage{
			ID:        resp.MessageId,
			Topic:     topic,
			Data:      data,
			Offset:    resp.Offset,
			Partition: resp.Partition,
			Headers:   headers,
		}
		
		return nil
	})
	
	return message, err
}

// Read reads messages from a stream
func (s *StreamClient) Read(ctx context.Context, topic string, partition int32, fromOffset int64, limit int32) ([]*StreamMessage, error) {
	var messages []*StreamMessage
	
	err := s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.ReadRequest{
			Topic:      topic,
			Partition:  partition,
			FromOffset: fromOffset,
			Limit:      limit,
		}
		
		resp, err := client.Read(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("stream read failed: %s", resp.Status.Message)
		}
		
		messages = make([]*StreamMessage, len(resp.Messages))
		for i, msg := range resp.Messages {
			messages[i] = &StreamMessage{
				ID:           msg.Id,
				Topic:        msg.Topic,
				PartitionKey: msg.PartitionKey,
				Data:         msg.Data,
				Offset:       msg.Offset,
				Timestamp:    time.Unix(msg.Timestamp, 0),
				Headers:      msg.Headers,
			}
		}
		
		return nil
	})
	
	return messages, err
}

// ReadFrom reads messages from a stream starting from a specific timestamp
func (s *StreamClient) ReadFrom(ctx context.Context, topic string, partition int32, fromTime time.Time, limit int32) ([]*StreamMessage, error) {
	var messages []*StreamMessage
	
	err := s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.ReadFromRequest{
			Topic:     topic,
			Partition: partition,
			FromTime:  fromTime.Unix(),
			Limit:     limit,
		}
		
		resp, err := client.ReadFrom(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("stream read from failed: %s", resp.Status.Message)
		}
		
		messages = make([]*StreamMessage, len(resp.Messages))
		for i, msg := range resp.Messages {
			messages[i] = &StreamMessage{
				ID:           msg.Id,
				Topic:        msg.Topic,
				PartitionKey: msg.PartitionKey,
				Data:         msg.Data,
				Offset:       msg.Offset,
				Timestamp:    time.Unix(msg.Timestamp, 0),
				Headers:      msg.Headers,
			}
		}
		
		return nil
	})
	
	return messages, err
}

// CreateTopic creates a new topic
func (s *StreamClient) CreateTopic(ctx context.Context, topic string, partitions int32) error {
	return s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.CreateTopicRequest{
			Name:       topic,
			Partitions: partitions,
		}
		
		resp, err := client.CreateTopic(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("create topic failed: %s", resp.Status.Message)
		}
		
		return nil
	})
}

// DeleteTopic deletes a topic
func (s *StreamClient) DeleteTopic(ctx context.Context, topic string) error {
	return s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.DeleteTopicRequest{Name: topic}
		
		resp, err := client.DeleteTopic(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("delete topic failed: %s", resp.Status.Message)
		}
		
		return nil
	})
}

// ListTopics lists all topics
func (s *StreamClient) ListTopics(ctx context.Context) ([]string, error) {
	var topics []string
	
	err := s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		resp, err := client.ListTopics(ctx, &streampb.ListTopicsRequest{})
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("list topics failed: %s", resp.Status.Message)
		}
		
		topics = resp.Topics
		return nil
	})
	
	return topics, err
}

// Purge removes all messages from a topic
func (s *StreamClient) Purge(ctx context.Context, topic string) (int64, error) {
	var purgedCount int64
	
	err := s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.PurgeRequest{Topic: topic}
		
		resp, err := client.Purge(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("stream purge failed: %s", resp.Status.Message)
		}
		
		purgedCount = resp.PurgedCount
		return nil
	})
	
	return purgedCount, err
}

// Subscribe creates a real-time subscription to a stream
func (s *StreamClient) Subscribe(ctx context.Context, topic string, partition int32, fromOffset int64) (<-chan *StreamMessage, <-chan error, error) {
	// Use first available node for streaming
	node := s.client.getRandomNode()
	client := s.client.streamClients[node]
	
	req := &streampb.SubscribeRequest{
		Topic:      topic,
		Partition:  partition,
		FromOffset: fromOffset,
	}
	
	stream, err := client.Subscribe(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to subscribe to stream: %w", err)
	}
	
	msgChan := make(chan *StreamMessage, 100)
	errChan := make(chan error, 1)
	
	go func() {
		defer close(msgChan)
		defer close(errChan)
		
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				errChan <- err
				return
			}
			
			streamMsg := &StreamMessage{
				ID:           msg.Id,
				Topic:        msg.Topic,
				PartitionKey: msg.PartitionKey,
				Data:         msg.Data,
				Offset:       msg.Offset,
				Timestamp:    time.Unix(msg.Timestamp, 0),
				Headers:      msg.Headers,
			}
			
			select {
			case msgChan <- streamMsg:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return msgChan, errChan, nil
}

// SubscribeGroup subscribes to a stream with consumer group
func (s *StreamClient) SubscribeGroup(ctx context.Context, topic, groupID, consumerID string) (<-chan *StreamMessage, <-chan error, error) {
	// Use first available node for streaming
	node := s.client.getRandomNode()
	client := s.client.streamClients[node]
	
	req := &streampb.SubscribeGroupRequest{
		Topic:      topic,
		GroupId:    groupID,
		ConsumerId: consumerID,
	}
	
	stream, err := client.SubscribeGroup(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to subscribe to stream group: %w", err)
	}
	
	msgChan := make(chan *StreamMessage, 100)
	errChan := make(chan error, 1)
	
	go func() {
		defer close(msgChan)
		defer close(errChan)
		
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				errChan <- err
				return
			}
			
			streamMsg := &StreamMessage{
				ID:           msg.Id,
				Topic:        msg.Topic,
				PartitionKey: msg.PartitionKey,
				Data:         msg.Data,
				Offset:       msg.Offset,
				Timestamp:    time.Unix(msg.Timestamp, 0),
				Headers:      msg.Headers,
			}
			
			select {
			case msgChan <- streamMsg:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return msgChan, errChan, nil
}

// CommitGroupOffset commits the offset for a consumer group
func (s *StreamClient) CommitGroupOffset(ctx context.Context, topic, groupID string, partition int32, offset int64) error {
	return s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.CommitGroupOffsetRequest{
			Topic:     topic,
			GroupId:   groupID,
			Partition: partition,
			Offset:    offset,
		}
		
		resp, err := client.CommitGroupOffset(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("commit group offset failed: %s", resp.Status.Message)
		}
		
		return nil
	})
}

// GetGroupOffset gets the current offset for a consumer group
func (s *StreamClient) GetGroupOffset(ctx context.Context, topic, groupID string, partition int32) (int64, error) {
	var offset int64
	
	err := s.client.executeWithRetry(func(node string) error {
		client := s.client.streamClients[node]
		
		req := &streampb.GetGroupOffsetRequest{
			Topic:     topic,
			GroupId:   groupID,
			Partition: partition,
		}
		
		resp, err := client.GetGroupOffset(ctx, req)
		if err != nil {
			return err
		}
		
		if !resp.Status.Success {
			return fmt.Errorf("get group offset failed: %s", resp.Status.Message)
		}
		
		offset = resp.Offset
		return nil
	})
	
	return offset, err
}
