package listener

import (
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/nats-io/stan.go"
)

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	conn stan.Conn
}

// Close NATS connection.
func (n NatsPublisher) Close() error {
	return n.conn.Close()
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
func (n NatsPublisher) Publish(subject string, event Event) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}

	return n.conn.Publish(subject, msg)
}

// NewNatsPublisher return new NatsPublisher instance.
func NewNatsPublisher(conn stan.Conn) *NatsPublisher {
	return &NatsPublisher{conn: conn}
}

// SubjectName creates subject name from the prefix, schema and table name.
func (e *Event) SubjectName(prefix string) string {
	return fmt.Sprintf("%s%s_%s", prefix, e.Schema, e.Table)
}
