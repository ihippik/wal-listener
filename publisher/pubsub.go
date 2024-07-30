package publisher

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"

	"github.com/goccy/go-json"
)

// GooglePubSubPublisher represent Pub/Sub publisher.
type GooglePubSubPublisher struct {
	pubSubConnection *PubSubConnection
	hasher           hash.Hash
}

// NewGooglePubSubPublisher create new instance of GooglePubSubPublisher.
func NewGooglePubSubPublisher(pubSubConnection *PubSubConnection) *GooglePubSubPublisher {
	return &GooglePubSubPublisher{
		pubSubConnection: pubSubConnection,
		hasher:           sha512.New(),
	}
}

// Publish send events, implements eventPublisher.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, topic string, event *Event) PublishResult {
	body, err := json.Marshal(event)
	if err != nil {
		return NewPublishResult(fmt.Errorf("marshal: %w", err))
	}

	p.hasher.Reset()
	p.hasher.Write([]byte(strings.Join(event.PrimaryKey, "-")))
	orderingKey := hex.EncodeToString(p.hasher.Sum(nil))

	return p.pubSubConnection.Publish(ctx, topic, body, orderingKey)
}

func (p *GooglePubSubPublisher) Flush(topic string) {
	p.pubSubConnection.Flush(topic)
}

func (p *GooglePubSubPublisher) Close() error {
	return p.pubSubConnection.Close()
}
