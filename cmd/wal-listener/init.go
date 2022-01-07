package main

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/ihippik/wal-listener/config"
	"github.com/ihippik/wal-listener/listener"
)

// logger log levels.
const (
	warningLoggerLevel = "warning"
	errorLoggerLevel   = "error"
	fatalLoggerLevel   = "fatal"
	infoLoggerLevel    = "info"
)

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

// initLogger init logrus preferences.
func initLogger(cfg config.LoggerCfg) {
	logrus.SetReportCaller(cfg.Caller)
	if !cfg.HumanReadable {
		logrus.SetFormatter(&logrus.JSONFormatter{})
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

	logrus.SetLevel(level)
}

// initPgxConnections initialise db and replication connections.
func initPgxConnections(cfg config.DatabaseCfg) (*pgx.Conn, *pgx.ReplicationConn, error) {
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
		return nil, nil, errors.Wrap(err, listener.ErrPostgresConnection)
	}

	rConnection, err := pgx.ReplicationConnect(pgxConf)
	if err != nil {
		return nil, nil, fmt.Errorf("%v: %w", listener.ErrReplicationConnection, err)
	}

	return pgConn, rConnection, nil
}

type pgxLogger struct{}

func (l pgxLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	logrus.Debugln(msg)
}
