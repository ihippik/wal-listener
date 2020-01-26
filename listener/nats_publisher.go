package listener

import (
	"fmt"

	"github.com/nats-io/stan.go"
)

//go:generate  easyjson nats_publisher.go

type NatsPublisher struct {
	conn stan.Conn
}

func (n NatsPublisher) Close() error {
	return n.conn.Close()
}

// Event event structure for publishing to the NATS server.
//easyjson:json
type Event struct {
	Schema string                 `json:"schema"`
	Table  string                 `json:"table"`
	Action string                 `json:"action"`
	Data   map[string]interface{} `json:"data"`
}

func (n NatsPublisher) Publish(subject string, event Event) error {
	msg, err := event.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}
	return n.conn.Publish(subject, msg)
}

func NewNatsPublisher(conn stan.Conn) *NatsPublisher {
	return &NatsPublisher{conn: conn}
}

// GetSubjectName creates subject name from the prefix, schema and table name.
func (e Event) GetSubjectName(prefix string) string {
	return fmt.Sprintf("%s%s_%s", prefix, e.Schema, e.Table)
}
