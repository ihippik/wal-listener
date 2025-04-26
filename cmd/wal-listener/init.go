package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx"
	"github.com/nats-io/nats.go"

	"github.com/ihippik/wal-listener/v2/internal/config"
	"github.com/ihippik/wal-listener/v2/internal/publisher"
)

// initPgxConnections initialise db and replication connections.
func initPgxConnections(cfg *config.DatabaseCfg, logger *slog.Logger) (*pgx.Conn, *pgx.ReplicationConn, error) {
	pgxConf := pgx.ConnConfig{
		LogLevel: pgx.LogLevelInfo,
		Logger:   pgxLogger{logger},
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Name,
		User:     cfg.User,
		Password: cfg.Password,
		OnNotice: func(c *pgx.Conn, n *pgx.Notice) {
			const codeFeatureNotSupported = "0A000"
			// ERROR: logical decoding cannot be used while in recovery (SQLSTATE 0A000)
			if n.Code == codeFeatureNotSupported {
				logger.Error("on notice handler: received error code 0A000, closing connection", "notice", n)
				// close connection to properly prepare for exit
				if err := c.Close(); err != nil {
					logger.Error("on notice handler: unable to close connection", "err", err)
				}

				panic("on notice handler: received error code 0A000")
			}

			logger.Debug("on notice handler: received notice message", "notice", n)
		},
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
	Publish(context.Context, string, *publisher.Event) error
	Close() error
}

// factoryPublisher represents a factory function for creating a eventPublisher.
func factoryPublisher(ctx context.Context, cfg *config.PublisherCfg, logger *slog.Logger) (eventPublisher, error) {
	switch cfg.Type {
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
		pubSubConn, err := publisher.NewPubSubConnection(ctx, logger, cfg.PubSubProjectID)
		if err != nil {
			return nil, fmt.Errorf("could not create pubsub connection: %w", err)
		}

		return publisher.NewGooglePubSubPublisher(pubSubConn), nil
	default:
		return nil, fmt.Errorf("unknown publisher type: %s", cfg.Type)
	}
}
