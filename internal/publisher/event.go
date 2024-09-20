package publisher

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ihippik/wal-listener/v2/internal/config"
)

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

// SubjectName creates subject name from the prefix, schema and table name. Also using topic map from cfg.
func (e *Event) SubjectName(cfg *config.Config) string {
	topic := fmt.Sprintf("%s_%s", e.Schema, e.Table)

	if cfg.Listener.TopicsMap != nil {
		if t, ok := cfg.Listener.TopicsMap[topic]; ok {
			topic = t
		}
	}

	topic = cfg.Publisher.Topic + "." + cfg.Publisher.TopicPrefix + topic

	return topic
}
