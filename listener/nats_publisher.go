package listener

import (
	"context"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/banked/wal-listener/v2/config"
)

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	js nats.JetStreamContext
}

// Event structure for publishing to the NATS server.
type Event struct {
	ID        uuid.UUID              `json:"id"`
	Schema    string                 `json:"schema"`
	Table     string                 `json:"table"`
	Action    string                 `json:"action"`
	Data      map[string]interface{} `json:"data"`
	EventTime time.Time              `json:"commitTime"`
}

// Publish serializes the event and publishes it on the bus.
func (n NatsPublisher) Publish(ctx context.Context, cfg *config.Config, log *logrus.Entry, event Event) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}

	subject := SubjectName(cfg, event)

	if _, err := n.js.Publish(subject, msg); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	publishedNatsEvents.With(prometheus.Labels{"subject": subject, "table": event.Table}).Inc()

	log.WithFields(logrus.Fields{
		"subject": subject,
		"action":  event.Action,
		"table":   event.Table,
	}).Infoln("event was sent")

	return nil
}

// NewNatsPublisher return new NatsPublisher instance.
func NewNatsPublisher(js nats.JetStreamContext) *NatsPublisher {
	return &NatsPublisher{js: js}
}

// SubjectName creates subject name from the prefix, schema and table name. Also using topic map from cfg.
func SubjectName(cfg *config.Config, event Event) string {
	topic := fmt.Sprintf("%s_%s", event.Schema, event.Table)

	if cfg.Listener.TopicsMap != nil {
		if t, ok := cfg.Listener.TopicsMap[topic]; ok {
			topic = t
		}
	}

	topic = cfg.Nats.StreamName + "." + cfg.Nats.TopicPrefix + topic

	return topic
}
