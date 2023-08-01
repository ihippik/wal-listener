package publisher

import (
	"fmt"
	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
)

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	js nats.JetStreamContext
}

// NewNatsPublisher return new NatsPublisher instance.
func NewNatsPublisher(js nats.JetStreamContext) *NatsPublisher {
	return &NatsPublisher{js: js}
}

// Publish serializes the event and publishes it on the bus.
func (n NatsPublisher) Publish(subject string, event Event) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}

	if _, err := n.js.Publish(subject, msg); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	return nil
}
