package publisher

import (
	"context"
	"encoding/json"
	"fmt"
)

// Google Pub/Sub hard per-message limit is 10 MiB. The publish RPC also carries
// protobuf framing, attributes, and the ordering key, so keep safely under.
// Oversized events are dropped with a warning rather than passed to the client
// bundler (which rejects them with "item size exceeds bundle byte limit" and
// produces confusing, hard-to-action logs).
const maxPubSubMessageBytes = 9 * 1024 * 1024

// GooglePubSubPublisher represent Pub/Sub publisher.
type GooglePubSubPublisher struct {
	pubSubConnection *PubSubConnection
}

// NewGooglePubSubPublisher create new instance of GooglePubSubPublisher.
func NewGooglePubSubPublisher(pubSubConnection *PubSubConnection) *GooglePubSubPublisher {
	return &GooglePubSubPublisher{
		pubSubConnection: pubSubConnection,
	}
}

// Publish send events, implements eventPublisher.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, topic string, event *Event) PublishResult {
	body, err := json.Marshal(event)
	if err != nil {
		return NewPublishResult(fmt.Errorf("marshal: %w", err))
	}

	if len(body) > maxPubSubMessageBytes {
		p.pubSubConnection.logger.Warn("dropping oversized message",
			"topic", topic,
			"schema", event.Schema,
			"table", event.Table,
			"action", event.Action,
			"event_id", event.ID.String(),
			"size_bytes", len(body),
			"limit_bytes", maxPubSubMessageBytes,
		)
		return NewPublishResult(nil)
	}

	return p.pubSubConnection.Publish(ctx, topic, body)
}

func (p *GooglePubSubPublisher) Flush(topic string) {
	p.pubSubConnection.Flush(topic)
}

func (p *GooglePubSubPublisher) Close() error {
	return p.pubSubConnection.Close()
}
