package publisher

import (
	"context"
	"fmt"
	"github.com/ihippik/wal-listener/v2/apis"

	"github.com/goccy/go-json"
)

// GooglePubSubPublisher represent Pub/Sub publisher.
type GooglePubSubPublisher struct {
	pubSubConnection *PubSubConnection
}

// NewGooglePubSubPublisher create new instance of GooglePubSubPublisher.
func NewGooglePubSubPublisher(pubSubConnection *PubSubConnection) *GooglePubSubPublisher {
	return &GooglePubSubPublisher{
		pubSubConnection,
	}
}

// Publish send events, implements eventPublisher.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, topic string, event *apis.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	return p.pubSubConnection.Publish(ctx, topic, body)
}

func (p *GooglePubSubPublisher) Close() error {
	return p.pubSubConnection.Close()
}
