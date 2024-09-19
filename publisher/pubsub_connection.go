package publisher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	t.PublishSettings.NumGoroutines = 1
	t.PublishSettings.CountThreshold = 1
	c.topics[topic] = t

	return t
}

func (c *PubSubConnection) Publish(ctx context.Context, topic string, data []byte) error {
	t := c.getTopic(topic)
	defer t.Flush()

	res := t.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	if _, err := res.Get(ctx); err != nil {
		c.logger.Error("Failed to publish message", "err", err)

		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("topic not found %w", err)
		}

		return fmt.Errorf("get: %w", err)
	}

	return nil
}

func (c *PubSubConnection) Close() error {
	return c.client.Close()
}
