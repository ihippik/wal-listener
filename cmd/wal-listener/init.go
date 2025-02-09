package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats.go"
	"log/slog"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
)

// initPgxConnections initialise db and replication connections.
func initPgxConnections(ctx context.Context, cfg *config.DatabaseCfg, logger *slog.Logger) (*pgx.Conn, *pgconn.PgConn, error) {
	connStringRepl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&replication=database", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)

	//todo: parse conn string, assign logger and ssl

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, nil, fmt.Errorf("db connection: %w", err)
	}

	replConn, err := pgconn.Connect(ctx, connStringRepl)
	if err != nil {
		return nil, nil, fmt.Errorf("replication connection: %w", err)
	}

	return conn, replConn, nil
}

type eventPublisher interface {
	Publish(context.Context, string, *publisher.Event) publisher.PublishResult
	Flush(string)
	Close() error
}

// factoryPublisher represents a factory function for creating a eventPublisher.
func factoryPublisher(ctx context.Context, cfg *config.PublisherCfg, logger *slog.Logger) (eventPublisher, error) {
	switch cfg.Type {
	case config.PublisherTypeStdout:
		return publisher.NewStdoutPublisher(), nil
	case config.PublisherTypeKafka:
		producer, err := publisher.NewProducer(cfg)
		if err != nil {
			return nil, fmt.Errorf("kafka producer: %w", err)
		}

		return publisher.NewKafkaPublisher(producer), nil
	case config.PublisherTypeNats:
		conn, err := nats.Connect(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("nats connection: %w", err)
		}

		pub, err := publisher.NewNatsPublisher(conn, logger)
		if err != nil {
			return nil, fmt.Errorf("new nats publisher: %w", err)
		}

		if err := pub.CreateStream(cfg.Topic); err != nil {
			return nil, fmt.Errorf("create stream: %w", err)
		}

		return pub, nil
	case config.PublisherTypeRabbitMQ:
		conn, err := publisher.NewConnection(cfg)
		if err != nil {
			return nil, fmt.Errorf("new connection: %w", err)
		}

		p, err := publisher.NewPublisher(cfg.Topic, conn)
		if err != nil {
			return nil, fmt.Errorf("new publisher: %w", err)
		}

		pub, err := publisher.NewRabbitPublisher(cfg.Topic, conn, p)
		if err != nil {
			return nil, fmt.Errorf("new rabbit publisher: %w", err)
		}

		return pub, nil
	case config.PublisherTypeGooglePubSub:
		pubSubConn, err := publisher.NewPubSubConnection(ctx, logger, cfg.PubSubProjectID, cfg.EnableOrdering)
		if err != nil {
			return nil, fmt.Errorf("could not create pubsub connection: %w", err)
		}

		return publisher.NewGooglePubSubPublisher(pubSubConn), nil
	default:
		return nil, fmt.Errorf("unknown publisher type: %s", cfg.Type)
	}
}
