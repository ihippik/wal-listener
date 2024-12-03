package main

import (
	"context"
	"fmt"
	"github.com/ihippik/wal-listener/v2/apis"
	"log/slog"

	"github.com/jackc/pgx"
	"github.com/nats-io/nats.go"

	"github.com/ihippik/wal-listener/v2/internal/publisher"
)

// initPgxConnections initialise db and replication connections.
func initPgxConnections(cfg *apis.DatabaseCfg, logger *slog.Logger) (*pgx.Conn, *pgx.ReplicationConn, error) {
	pgxConf := pgx.ConnConfig{
		LogLevel: pgx.LogLevelInfo,
		Logger:   pgxLogger{logger},
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Name,
		User:     cfg.User,
		Password: cfg.Password,
	}

	pgConn, err := pgx.Connect(pgxConf)
	if err != nil {
		return nil, nil, fmt.Errorf("db connection: %w", err)
	}

	rConnection, err := pgx.ReplicationConnect(pgxConf)
	if err != nil {
		return nil, nil, fmt.Errorf("replication connect: %w", err)
	}

	return pgConn, rConnection, nil
}

type pgxLogger struct {
	logger *slog.Logger
}

// Log DB message.
func (l pgxLogger) Log(_ pgx.LogLevel, msg string, _ map[string]any) {
	l.logger.Debug(msg)
}

type eventPublisher interface {
	Publish(context.Context, string, *apis.Event) error
	Close() error
}

// factoryPublisher represents a factory function for creating a eventPublisher.
func factoryPublisher(ctx context.Context, cfg *apis.PublisherCfg, logger *slog.Logger) (eventPublisher, error) {
	switch cfg.Type {
	case apis.PublisherTypeKafka:
		producer, err := publisher.NewProducer(cfg)
		if err != nil {
			return nil, fmt.Errorf("kafka producer: %w", err)
		}

		return publisher.NewKafkaPublisher(producer), nil
	case apis.PublisherTypeNats:
		// TODO: using direct credentials currently for testing purpose only
		conn, err := nats.Connect(cfg.Address, nats.UserCredentials("/home/user/go/src/go.bytebuilders.dev/launchpad/2021/gitea_setup/nats/admin.creds"))
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
	case apis.PublisherTypeRabbitMQ:
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
	case apis.PublisherTypeGooglePubSub:
		pubSubConn, err := publisher.NewPubSubConnection(ctx, logger, cfg.PubSubProjectID)
		if err != nil {
			return nil, fmt.Errorf("could not create pubsub connection: %w", err)
		}

		return publisher.NewGooglePubSubPublisher(pubSubConn), nil
	default:
		return nil, fmt.Errorf("unknown publisher type: %s", cfg.Type)
	}
}
