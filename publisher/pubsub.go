package publisher

import (
	"context"
	"github.com/goccy/go-json"
)

type GooglePubSubPublisher struct {
	pubSubConnection *PubSubConnection
}

func NewGooglePubSubPublisher(pubSubConnection *PubSubConnection) *GooglePubSubPublisher {
	return &GooglePubSubPublisher{
		pubSubConnection,
	}
}

// Publish send events, implements eventPublisher.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, topic string, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.pubSubConnection.Publish(ctx, topic, body)
}

func (p *GooglePubSubPublisher) Close() error {
	return p.pubSubConnection.Close()
}
