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
	TableName string                 `json:"tableName"`
	Action    string                 `json:"action"`
	Data      map[string]interface{} `json:"data"`
}

func (n NatsPublisher) Publish(subject string, msg []byte) error {
	return n.conn.Publish(subject, msg)
}

func NewNatsPublisher(conn stan.Conn) *NatsPublisher {
	return &NatsPublisher{conn: conn}
}

// GetSubjectName creates subject name from the prefix, db name and table name.
func (e *Event) GetSubjectName(prefix string, dbName string) string {
	subject := fmt.Sprintf("%s%s_%s", prefix, dbName, e.TableName)
	return subject
}
