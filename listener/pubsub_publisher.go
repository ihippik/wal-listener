package listener

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/banked/wal-listener/v2/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// PubSubPublisher publishes to pub/sub.
type PubSubPublisher struct {
	topic *pubsub.Topic
}

// NewPubSubPublisher constructs a new pubsub publisher.
func NewPubSubPublisher(topic *pubsub.Topic) *PubSubPublisher {
	return &PubSubPublisher{
		topic: topic,
	}
}

// Publish publishes a message to the configured pubsub topic.
func (p *PubSubPublisher) Publish(ctx context.Context, _ *config.Config, log *logrus.Entry, event Event) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	result := p.topic.Publish(context.Background(), &pubsub.Message{
		Data: msg,
	})

	<-result.Ready()

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("pubsub publish: %w", err)
	}

	publishedPubSubEvents.With(prometheus.Labels{"topic": p.topic.String(), "table": event.Table}).Inc()

	log.WithFields(logrus.Fields{
		"topic":  p.topic.String(),
		"action": event.Action,
		"table":  event.Table,
	}).Infoln("event was sent")

	return nil
}
