package main

import (
	"fmt"
	"log/slog"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
	"github.com/jackc/pgx"
	"github.com/nats-io/nats.go"
)

// createStream creates a stream by using JetStreamContext. We can do it manually.
func createStream(logger *slog.Logger, js nats.JetStreamContext, streamName string) error {
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		logger.Warn("failed to get stream info", "err", err)
	}

	if stream == nil {
		var streamSubjects = streamName + ".*"

		if _, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		}); err != nil {
			return fmt.Errorf("add stream: %w", err)
		}

		logger.Info("stream not exists, created", slog.String("subjects", streamSubjects))
	}

	return nil
}

// initPgxConnections initialise db and replication connections.
func initPgxConnections(cfg *config.DatabaseCfg, logger *slog.Logger) (*pgx.Conn, *pgx.ReplicationConn, error) {
	pgxConf := pgx.ConnConfig{
		// TODO logger
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

func (l pgxLogger) Log(_ pgx.LogLevel, msg string, _ map[string]any) {
	l.logger.Debug(msg)
}

type eventPublisher interface {
	Publish(string, publisher.Event) error
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
		natsConn, err := nats.Connect(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("nats connection: %w", err)
		}
		defer natsConn.Close()

		js, err := natsConn.JetStream()
		if err != nil {
			return nil, fmt.Errorf("jet stream: %w", err)
		}

		if err := createStream(logger, js, cfg.Topic); err != nil {
			return nil, fmt.Errorf("create Nats stream: %w", err)
		}

		return publisher.NewNatsPublisher(js), nil
	default:
		return nil, fmt.Errorf("unknown publisher type: %s", cfg.Type)
	}
}
