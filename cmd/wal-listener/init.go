package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/nats-io/nats.go"
)

// initPgxConnections initialise db and replication connections.
func initPgxConnections(ctx context.Context, cfg *config.DatabaseCfg, logger *slog.Logger) (*pgx.Conn, *pgconn.PgConn, error) {
	sslMode := "require"
	if cfg.SSL == nil {
		sslMode = "prefer"
	}

	connStringRepl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&replication=database", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, sslMode)

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, sslMode)

	pgxConf, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing connection string: %w", err)
	}
	pgxConf.Tracer = NewTracerLogger(logger)

	if cfg.SSL != nil {
		pgxConf.TLSConfig = &tls.Config{
			ServerName:         cfg.SSL.ServerName,
			InsecureSkipVerify: cfg.SSL.SkipVerify,
		}
	}

	conn, err := pgx.ConnectConfig(ctx, pgxConf)
	if err != nil {
		return nil, nil, fmt.Errorf("db connection: %w", err)
	}

	replConn, err := pgconn.Connect(ctx, connStringRepl)
	if err != nil {
		return nil, nil, fmt.Errorf("replication connection: %w", err)
	}

	return conn, replConn, nil
}

type pgxLogger struct {
	logger *slog.Logger
}

func (l pgxLogger) Log(ctx context.Context, _ tracelog.LogLevel, msg string, data map[string]any) {
	var attrs []slog.Attr
	for k, v := range data {
		attrs = append(attrs, slog.Any(k, v))
	}
	l.logger.LogAttrs(ctx, slog.LevelDebug, msg, attrs...) // we always want debug level
}

func NewTracerLogger(l *slog.Logger) pgx.QueryTracer {
	return &tracelog.TraceLog{
		Logger:   &pgxLogger{logger: l},
		LogLevel: tracelog.LogLevelDebug,
	}
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
