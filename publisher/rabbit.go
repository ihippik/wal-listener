package publisher

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"
	"github.com/ihippik/wal-listener/v2/config"
	"github.com/wagslane/go-rabbitmq"
)

// RabbitPublisher represent event publisher for RabbitMQ.
type RabbitPublisher struct {
	pt        string
	conn      *rabbitmq.Conn
	publisher *rabbitmq.Publisher
}

// NewRabbitPublisher create new RabbitPublisher instance.
func NewRabbitPublisher(pubTopic string, conn *rabbitmq.Conn, publisher *rabbitmq.Publisher) (*RabbitPublisher, error) {
	return &RabbitPublisher{
		pubTopic,
		conn,
		publisher,
	}, nil
}

// Publish send events, implements eventPublisher.
func (p *RabbitPublisher) Publish(ctx context.Context, topic string, event *Event) PublishResult {
	const contentTypeJSON = "application/json"

	body, err := json.Marshal(event)
	if err != nil {
		return NewPublishResult(err)
	}

	return NewPublishResult(p.publisher.PublishWithContext(
		ctx,
		body,
		[]string{topic},
		rabbitmq.WithPublishOptionsContentType(contentTypeJSON),
		rabbitmq.WithPublishOptionsExchange(p.pt),
	))
}

func (p *RabbitPublisher) Flush(topic string) {}

// Close represent finalization for RabbitMQ publisher.
func (p *RabbitPublisher) Close() error {
	if err := p.conn.Close(); err != nil {
		return fmt.Errorf("connection close: %w", err)
	}

	p.publisher.Close()

	return nil
}

// NewConnection creates a new RabbitMQ connection manager.
func NewConnection(pCfg *config.PublisherCfg) (*rabbitmq.Conn, error) {
	conn, err := rabbitmq.NewConn(pCfg.Address)
	if err != nil {
		return nil, fmt.Errorf("new conn: %w", err)
	}

	return conn, nil
}

// NewPublisher represent constructor for RabbitMQ publisher.
func NewPublisher(topic string, conn *rabbitmq.Conn) (*rabbitmq.Publisher, error) {
	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(topic),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsExchangeKind("topic"),
		rabbitmq.WithPublisherOptionsExchangeDurable,
	)
	if err != nil {
		return nil, fmt.Errorf("publisher: %w", err)
	}

	return publisher, nil
}
