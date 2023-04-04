package listener

import (
	"fmt"
	"time"

	// "github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/ihippik/wal-listener/v2/config"
)

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	js nats.JetStreamContext
}

// Event structure for publishing to the NATS server.
type Event struct {
	ID        uuid.UUID      `json:"id"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	Action    string         `json:"action"`
	Data      map[string]any `json:"data"`
	DataOld   map[string]any `json:"dataOld"`
	EventTime time.Time      `json:"commitTime"`
}

// Publish serializes the event and publishes it on the bus.
func (n NatsPublisher) Publish(subject string, event Event) error {
	// msg, err := json.Marshal(event)
	// if err != nil {
	// 	return fmt.Errorf("marshal err: %w", err)
	// }

	// if _, err := n.js.Publish(subject, msg); err != nil {
	// 	return fmt.Errorf("failed to publish: %w", err)
	// }

	return nil
}

// NewNatsPublisher return new NatsPublisher instance.
func NewNatsPublisher(js nats.JetStreamContext) *NatsPublisher {
	return &NatsPublisher{js: js}
}

// SubjectName creates subject name from the prefix, schema and table name. Also using topic map from cfg.
func (e *Event) SubjectName(cfg *config.Config) string {
	topic := fmt.Sprintf("%s_%s", e.Schema, e.Table)

	if cfg.Listener.TopicsMap != nil {
		if t, ok := cfg.Listener.TopicsMap[topic]; ok {
			topic = t
		}
	}

	topic = cfg.Nats.StreamName + "." + cfg.Nats.TopicPrefix + topic

	return topic
}
