package publisher

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/goccy/go-json"
)

// GooglePubSubPublisher represent Pub/Sub publisher.
type GooglePubSubPublisher struct {
	pubSubConnection *PubSubConnection
	alive            atomic.Bool
}

// NewGooglePubSubPublisher create new instance of GooglePubSubPublisher.
func NewGooglePubSubPublisher(pubSubConnection *PubSubConnection) *GooglePubSubPublisher {
	p := &GooglePubSubPublisher{
		pubSubConnection: pubSubConnection,
	}
	p.alive.Store(true)

	return p
}

// Publish send events, implements eventPublisher.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, topic string, event *Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	err = p.pubSubConnection.Publish(ctx, topic, body)
	if err != nil {
		p.alive.Store(false)
		return err
	}

	p.alive.Store(true)
	return nil
}

// IsAlive returns the latest publisher health state.
func (p *GooglePubSubPublisher) IsAlive() bool {
	return p.alive.Load()
}

func (p *GooglePubSubPublisher) Close() error {
	return p.pubSubConnection.Close()
}
