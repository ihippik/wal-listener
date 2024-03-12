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

type PubSubConnection struct {
	logger    *slog.Logger
	client    *pubsub.Client
	projectID string
	topics    map[string]*pubsub.Topic
	mu        sync.RWMutex
}

func NewPubSubConnection(ctx context.Context, logger *slog.Logger, pubSubProjectId string) (*PubSubConnection, error) {
	if pubSubProjectId == "" {
		return nil, fmt.Errorf("project id is required for pub sub connection")
	}

	c, err := pubsub.NewClient(ctx, pubSubProjectId)
	if err != nil {
		return nil, err
	}
	return &PubSubConnection{
		logger:    logger,
		client:    c,
		projectID: pubSubProjectId,
		topics:    make(map[string]*pubsub.Topic),
		mu:        sync.RWMutex{},
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

	var res *pubsub.PublishResult
	var err error
	res = t.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	_, err = res.Get(ctx)
	if err != nil {
		c.logger.Error("Failed to publish message", "err", err)
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("topic not found %w", err)
		} else {
			return err
		}
	}

	return nil
}

func (c *PubSubConnection) Close() error {
	return c.client.Close()
}
