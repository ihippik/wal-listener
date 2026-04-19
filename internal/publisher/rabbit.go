package publisher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ihippik/wal-listener/v2/internal/config"

	"github.com/goccy/go-json"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitPublisher represent event publisher for RabbitMQ.
type RabbitPublisher struct {
	pt      string
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex
	alive   atomic.Bool
}

// NewRabbitPublisher create new RabbitPublisher instance.
func NewRabbitPublisher(
	pubTopic string,
	conn *amqp.Connection,
	channel *amqp.Channel,
) (*RabbitPublisher, error) {
	p := &RabbitPublisher{
		pt:      pubTopic,
		conn:    conn,
		channel: channel,
	}
	p.alive.Store(true)

	return p, nil
}

// Publish send events, implements eventPublisher.
func (p *RabbitPublisher) Publish(ctx context.Context, topic string, event *Event) error {
	const contentTypeJSON = "application/json"

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err = p.channel.PublishWithContext(
		ctx,
		p.pt,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType: contentTypeJSON,
			Body:        body,
		},
	); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

// IsAlive returns the latest publisher health state.
func (p *RabbitPublisher) IsAlive() bool {
	return p.alive.Load()
}

// CheckHealth verifies broker connectivity using the current AMQP connection/channel.
func (p *RabbitPublisher) CheckHealth(_ context.Context) error {
	if p.conn == nil || p.conn.IsClosed() {
		p.alive.Store(false)
		return fmt.Errorf("connection is closed")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel == nil || p.channel.IsClosed() {
		p.alive.Store(false)
		return fmt.Errorf("channel is closed")
	}

	// Passive declaration validates exchange availability over current channel.
	if err := p.channel.ExchangeDeclarePassive(
		p.pt,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		p.alive.Store(false)
		return fmt.Errorf("exchange passive declare: %w", err)
	}

	p.alive.Store(true)

	return nil
}

// Close represent finalization for RabbitMQ publisher.
func (p *RabbitPublisher) Close() error {
	var err error

	if p.channel != nil {
		err = p.channel.Close()
	}

	if p.conn != nil {
		err = errors.Join(err, p.conn.Close())
	}

	return err
}

// NewConnection creates a new RabbitMQ connection.
func NewConnection(pCfg *config.PublisherCfg) (*amqp.Connection, error) {
	conn, err := amqp.Dial(pCfg.Address)
	if err != nil {
		return nil, fmt.Errorf("new conn: %w", err)
	}

	return conn, nil
}

// NewPublisher creates a channel and declares the topic exchange.
func NewPublisher(topic string, conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	if err = channel.ExchangeDeclare(
		topic,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		_ = channel.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	return channel, nil
}
