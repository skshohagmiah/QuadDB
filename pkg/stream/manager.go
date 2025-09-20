package stream

import (
	"context"
)

// Manager provides topic administration and helpers to build producers/consumers.
type Manager struct {
	st Storage
}

func NewManager(st Storage) *Manager { return &Manager{st: st} }

// CreateTopic creates a topic with N partitions.
func (m *Manager) CreateTopic(ctx context.Context, topic string, partitions int32) error {
	return m.st.StreamCreateTopic(ctx, topic, partitions)
}

// DeleteTopic deletes the topic and all its data.
func (m *Manager) DeleteTopic(ctx context.Context, topic string) error {
	return m.st.StreamDeleteTopic(ctx, topic)
}

// ListTopics lists all topics.
func (m *Manager) ListTopics(ctx context.Context) ([]string, error) {
	return m.st.StreamListTopics(ctx)
}

// TopicInfo returns metadata for a topic.
func (m *Manager) TopicInfo(ctx context.Context, topic string) (TopicInfo, error) {
	info, err := m.st.StreamGetTopicInfo(ctx, topic)
	if err != nil {
		return TopicInfo{}, err
	}
	return TopicInfo(info), nil
}

// NewProducer creates a high-throughput producer.
func (m *Manager) NewProducer(partitioner PartitionFunc, opts ProducerOptions) *Producer {
	return NewProducer(m.st, partitioner, opts)
}

// NewConsumer creates a consumer for a topic with the given consumerID (group).
func (m *Manager) NewConsumer(topic, consumerID string, h Handler, opts ConsumerOptions) *Consumer {
	return NewConsumer(m.st, topic, consumerID, h, opts)
}
