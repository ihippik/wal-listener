package publisher

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
)

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	conn   *nats.Conn
	js     nats.JetStreamContext
	logger *slog.Logger
	alive  atomic.Bool
}

// NewNatsPublisher return new NatsPublisher instance.
func NewNatsPublisher(conn *nats.Conn, logger *slog.Logger) (*NatsPublisher, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jet stream: %w", err)
	}

	p := &NatsPublisher{conn: conn, js: js, logger: logger}
	p.alive.Store(true)

	return p, nil
}

// Close connection.
func (n *NatsPublisher) Close() error {
	n.conn.Close()
	return nil
}

// Publish serializes the event and publishes it on the bus.
func (n *NatsPublisher) Publish(_ context.Context, subject string, event *Event) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}

	if _, err := n.js.Publish(subject, msg); err != nil {
		n.alive.Store(false)
		return fmt.Errorf("failed to publish: %w", err)
	}

	n.alive.Store(true)
	return nil
}

// IsAlive returns the latest publisher health state.
func (n *NatsPublisher) IsAlive() bool {
	return n.alive.Load()
}

// CreateStream creates a stream by using JetStreamContext. We can do it manually.
func (n *NatsPublisher) CreateStream(streamName string) error {
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
