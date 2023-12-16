package main

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pgx"
	"github.com/nats-io/nats.go"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
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
	Publish(string, publisher.Event) error
	Close() error
}

// factoryPublisher represents a factory function for creating a eventPublisher.
func factoryPublisher(cfg *config.PublisherCfg, logger *slog.Logger) (eventPublisher, error) {
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
	default:
		return nil, fmt.Errorf("unknown publisher type: %s", cfg.Type)
	}
}
