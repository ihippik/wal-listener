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
	logger         *slog.Logger
	client         *pubsub.Client
	projectID      string
	topics         map[string]*pubsub.Topic
	enableOrdering bool
	mu             sync.RWMutex
}

// NewPubSubConnection create new connection with specified project id.
func NewPubSubConnection(ctx context.Context, logger *slog.Logger, pubSubProjectID string, enableOrdering bool) (*PubSubConnection, error) {
	if pubSubProjectID == "" {
		return nil, fmt.Errorf("project id is required for pub sub connection")
	}

	cli, err := pubsub.NewClient(ctx, pubSubProjectID)
	if err != nil {
		return nil, err
	}

	return &PubSubConnection{
		logger:         logger,
		client:         cli,
		projectID:      pubSubProjectID,
		topics:         make(map[string]*pubsub.Topic),
		enableOrdering: enableOrdering,
	}, nil
}

func (c *PubSubConnection) getTopic(topic string) *pubsub.Topic {
	c.mu.Lock()
	defer c.mu.Unlock()

	if top, ok := c.topics[topic]; ok {
		return top
	}

	t := c.client.TopicInProject(topic, c.projectID)
	t.EnableMessageOrdering = c.enableOrdering
	t.PublishSettings.ByteThreshold = 8 * MB
	t.PublishSettings.DelayThreshold = 250 * time.Millisecond
	t.PublishSettings.CountThreshold = 150

	c.topics[topic] = t

	return t
}

func (c *PubSubConnection) Publish(ctx context.Context, topic string, data []byte, orderingKey string) PublishResult {
	t := c.getTopic(topic)

	result := t.Publish(ctx, &pubsub.Message{
		Data:        data,
		OrderingKey: orderingKey,
	})

	return &retryPausedResult{
		inner:       result,
		topic:       t,
		data:        data,
		orderingKey: orderingKey,
	}
}

func (c *PubSubConnection) Flush(topic string) {
	t := c.getTopic(topic)
	t.Flush()
}

func (c *PubSubConnection) Close() error {
	return c.client.Close()
}

type retryPausedResult struct {
	inner       *pubsub.PublishResult
	topic       *pubsub.Topic
	data        []byte
	orderingKey string
}

func (r *retryPausedResult) Get(ctx context.Context) (string, error) {
	serverID, err := r.inner.Get(ctx)

	if err != nil && errors.Is(err, pubsub.ErrPublishingPaused{}) {
		// if we failed to publish because publishing is paused, resume publishing and retry
		r.topic.ResumePublish(r.orderingKey)

		return r.topic.Publish(ctx, &pubsub.Message{
			Data:        r.data,
			OrderingKey: r.orderingKey,
		}).Get(ctx)
	}

	return serverID, err
}
