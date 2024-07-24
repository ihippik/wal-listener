package publisher

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
)

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	conn   *nats.Conn
	js     nats.JetStreamContext
	logger *slog.Logger
}

// NewNatsPublisher return new NatsPublisher instance.
func NewNatsPublisher(conn *nats.Conn, logger *slog.Logger) (*NatsPublisher, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jet stream: %w", err)
	}

	return &NatsPublisher{conn: conn, js: js, logger: logger}, nil
}

// Close connection.
func (n NatsPublisher) Close() error {
	n.conn.Close()
	return nil
}

// Publish serializes the event and publishes it on the bus.
func (n NatsPublisher) Publish(_ context.Context, subject string, event *Event) PublishResult {
	msg, err := json.Marshal(event)
	if err != nil {
		return NewPublishResult(fmt.Errorf("marshal err: %w", err))
	}

	if _, err := n.js.Publish(subject, msg); err != nil {
		return NewPublishResult(fmt.Errorf("failed to publish: %w", err))
	}

	return NewPublishResult(nil)
}

func (n NatsPublisher) Flush(subject string) {}

// CreateStream creates a stream by using JetStreamContext. We can do it manually.
func (n NatsPublisher) CreateStream(streamName string) error {
	stream, err := n.js.StreamInfo(streamName)
	if err != nil {
		n.logger.Warn("failed to get stream info", "err", err)
	}

	if stream == nil {
		streamSubjects := streamName + ".*"

		if _, err = n.js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		}); err != nil {
			return fmt.Errorf("add stream: %w", err)
		}

		n.logger.Info("stream not exists, created", slog.String("subjects", streamSubjects))
	}

	return nil
}
