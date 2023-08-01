package main

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/evalphobia/logrus_sentry"
	"github.com/jackc/pgx"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
)

// logger log levels.
const (
	warningLoggerLevel = "warning"
	errorLoggerLevel   = "error"
	fatalLoggerLevel   = "fatal"
	infoLoggerLevel    = "info"
)

func getVersion() string {
	var version = "unknown"

	info, ok := debug.ReadBuildInfo()
	if ok {
		for _, item := range info.Settings {
			if item.Key == "vcs.revision" {
				version = item.Value[:4]
			}
		}
	}

	return version
}

// getConf load config from file.
func getConf(path string) (*config.Config, error) {
	var cfg config.Config

	viper.SetConfigFile(path)

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode into config struct: %w", err)
	}

	return &cfg, nil
}

func initMetrics(addr string, logger *logrus.Entry) {
	if len(addr) == 0 {
		return
	}

	logger.WithField("addr", addr).Infoln("metrics handler")

	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.WithError(err).Errorln("init metrics handler")
		return
	}
}

// initLogger init Logrus preferences.
func initLogger(cfg *config.LoggerCfg, version string) *logrus.Entry {
	logger := logrus.New()

	logger.SetReportCaller(cfg.Caller)

	if cfg.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}

	var level logrus.Level

	switch cfg.Level {
	case warningLoggerLevel:
		level = logrus.WarnLevel
	case errorLoggerLevel:
		level = logrus.ErrorLevel
	case fatalLoggerLevel:
		level = logrus.FatalLevel
	case infoLoggerLevel:
		level = logrus.InfoLevel
	default:
		level = logrus.DebugLevel
	}

	logger.SetLevel(level)

	return logger.WithField("version", version)
}

// createStream creates a stream by using JetStreamContext. We can do it manually.
func createStream(logger *logrus.Entry, js nats.JetStreamContext, streamName string) error {
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		logger.WithError(err).Warnln("stream info")
	}

	if stream == nil {
		var streamSubjects = streamName + ".*"

		if _, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		}); err != nil {
			return err
		}

		logger.WithField("subjects", streamSubjects).Infoln("stream not exists, created..")
	}

	return nil
}

func initSentry(dsn string, logger *logrus.Entry) {
	if len(dsn) == 0 {
		logger.Warnln("empty Sentry DSN")
		return
	}

	hook, err := logrus_sentry.NewSentryHook(
		dsn,
		[]logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
		},
	)

	if err == nil {
		logger.Logger.AddHook(hook)
	}
}

// initPgxConnections initialise db and replication connections.
func initPgxConnections(cfg *config.DatabaseCfg) (*pgx.Conn, *pgx.ReplicationConn, error) {
	pgxConf := pgx.ConnConfig{
		// TODO logger
		LogLevel: pgx.LogLevelInfo,
		Logger:   pgxLogger{},
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

type pgxLogger struct{}

func (l pgxLogger) Log(_ pgx.LogLevel, msg string, _ map[string]any) {
	logrus.Debugln(msg)
}

type eventPublisher interface {
	Publish(string, publisher.Event) error
}

// factoryPublisher represents a factory function for creating a eventPublisher.
func factoryPublisher(cfg *config.PublisherCfg, logger *logrus.Entry) (eventPublisher, error) {
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
