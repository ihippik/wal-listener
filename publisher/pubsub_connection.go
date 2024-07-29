package publisher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

const (
	KB = 1024
	MB = KB * KB
)

// PubSubConnection represent Pub/Sub connection.
type PubSubConnection struct {
	logger    *slog.Logger
	client    *pubsub.Client
	projectID string
	topics    map[string]*pubsub.Topic
	mu        sync.RWMutex
}

// NewPubSubConnection create new connection with specified project id.
func NewPubSubConnection(ctx context.Context, logger *slog.Logger, pubSubProjectID string) (*PubSubConnection, error) {
	if pubSubProjectID == "" {
		return nil, fmt.Errorf("project id is required for pub sub connection")
	}

	cli, err := pubsub.NewClient(ctx, pubSubProjectID)
	if err != nil {
		return nil, err
	}

	return &PubSubConnection{
		logger:    logger,
		client:    cli,
		projectID: pubSubProjectID,
		topics:    make(map[string]*pubsub.Topic),
	}, nil
}

func (c *PubSubConnection) getTopic(topic string) *pubsub.Topic {
	c.mu.Lock()
	defer c.mu.Unlock()

	if top, ok := c.topics[topic]; ok {
		return top
	}

	t := c.client.TopicInProject(topic, c.projectID)
	t.EnableMessageOrdering = true
	t.PublishSettings.ByteThreshold = 8 * MB
	t.PublishSettings.DelayThreshold = 500 * time.Millisecond
	t.PublishSettings.CountThreshold = 300

	c.topics[topic] = t

	return t
}

func (c *PubSubConnection) Publish(ctx context.Context, topic string, data []byte) PublishResult {
	t := c.getTopic(topic)

	return t.Publish(ctx, &pubsub.Message{
		Data: data,
	})
}

func (c *PubSubConnection) Flush(topic string) {
	t := c.getTopic(topic)
	t.Flush()
}

func (c *PubSubConnection) Close() error {
	return c.client.Close()
}
