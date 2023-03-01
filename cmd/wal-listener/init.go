package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"runtime/debug"

	"cloud.google.com/go/pubsub"
	"github.com/evalphobia/logrus_sentry"
	"github.com/jackc/pgx"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sethvargo/go-envconfig"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/banked/wal-listener/v2/config"
)

// logger log levels.
const (
	warningLoggerLevel = "warning"
	errorLoggerLevel   = "error"
	fatalLoggerLevel   = "fatal"
	infoLoggerLevel    = "info"
)

func getVersion() string {
	version := "unknown"

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

	if err := envconfig.Process(context.Background(), &cfg); err != nil {
		return nil, fmt.Errorf("unable to process environment variables into  config struct: %w", err)
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

// initLogger init logrus preferences.
func initLogger(cfg config.LoggerCfg, version string) *logrus.Entry {
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
		streamSubjects := streamName + ".*"

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

func initNats(cfg config.NatsCfg) (*nats.Conn, error) {
	opts := []nats.Option{}

	if mtls := cfg.MTLS; mtls.Enabled {
		if mtls.InsecureSkipVerify {
			opts = append(
				opts,
				nats.Secure(&tls.Config{InsecureSkipVerify: true}),
			)
		} else {
			opts = append(
				opts,
				nats.ClientCert(
					fmt.Sprintf("%s/%s", mtls.CertPath, mtls.CertFile),
					fmt.Sprintf("%s/%s", mtls.CertPath, mtls.KeyFile),
				),
				nats.RootCAs(fmt.Sprintf("%s/%s", mtls.CertPath, mtls.CAFile)),
			)
		}
	}

	return nats.Connect(cfg.Address, opts...)
}

func initPubSub(config config.PubSubCfg) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(context.Background(), config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub connect: %w", err)
	}

	return client, nil
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
func initPgxConnections(pgxConf pgx.ConnConfig) (*pgx.Conn, *pgx.ReplicationConn, error) {
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

func (l pgxLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	logrus.Debugln(msg)
}
