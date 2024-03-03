package publisher

import (
	"context"
	"encoding/json"
	"github.com/ihippik/wal-listener/v2/config"
	"github.com/wagslane/go-rabbitmq"
)

type RabbitPublisher struct {
	pt        string
	conn      *rabbitmq.Conn
	publisher *rabbitmq.Publisher
}

func NewRabbitPublisher(pubTopic string, conn *rabbitmq.Conn, publisher *rabbitmq.Publisher) (*RabbitPublisher, error) {
	return &RabbitPublisher{
		pubTopic,
		conn,
		publisher,
	}, nil
}

func (p *RabbitPublisher) Publish(topic string, event Event) error {

	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.publisher.PublishWithContext(
		context.TODO(),
		body,
		[]string{topic},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsExchange(p.pt),
	)
}

func (p *RabbitPublisher) Close() error {
	err := p.conn.Close()
	if err != nil {
		return err
	}

	p.publisher.Close()
	return nil
}

func NewConnection(pCfg *config.PublisherCfg) (*rabbitmq.Conn, error) {
	conn, err := rabbitmq.NewConn(pCfg.Address)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func NewPublisher(topic string, conn *rabbitmq.Conn) (*rabbitmq.Publisher, error) {

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(topic),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsExchangeKind("topic"),
		rabbitmq.WithPublisherOptionsExchangeDurable,
	)

	return publisher, err
}
