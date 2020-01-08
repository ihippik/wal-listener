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
	Scheme string                 `json:"scheme"`
	Table  string                 `json:"table"`
	Action string                 `json:"action"`
	Data   map[string]interface{} `json:"data"`
}

func (n NatsPublisher) Publish(subject string, msg []byte) error {
	return n.conn.Publish(subject, msg)
}

func NewNatsPublisher(conn stan.Conn) *NatsPublisher {
	return &NatsPublisher{conn: conn}
}

// GetSubjectName creates subject name from the prefix, scheme and table name.
func (e Event) GetSubjectName(prefix string) string {
	return fmt.Sprintf("%s%s_%s", prefix, e.Scheme, e.Table)
}
