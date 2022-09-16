package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/ihippik/wal-listener/listener"
)

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"v"},
		Usage:   "print only the version",
	}

	version := getVersion()

	app := &cli.App{
		Name:    "Wal-Listener",
		Usage:   "listen postgres events",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Value:   "config.yml",
				Aliases: []string{"c"},
				Usage:   "path to config file",
			},
		},
		Action: func(c *cli.Context) error {
			cfg, err := getConf(c.String("config"))
			if err != nil {
				return fmt.Errorf("get config: %w", err)
			}

			if err = cfg.Validate(); err != nil {
				return fmt.Errorf("validate config: %w", err)
			}

			logger := initLogger(cfg.Logger, version)

			initSentry(cfg.Monitoring.SentryDSN, logger)

			go initMetrics(logger)

			natsConn, err := nats.Connect(cfg.Nats.Address)
			if err != nil {
				return fmt.Errorf("nats connection: %w", err)
			}
			defer natsConn.Close()

			js, err := natsConn.JetStream()
			if err != nil {
				return fmt.Errorf("jet stream: %w", err)
			}

			if err := createStream(logger, js, cfg.Nats.StreamName); err != nil {
				return fmt.Errorf("create Nats stream: %w", err)
			}

			conn, rConn, err := initPgxConnections(cfg.Database)
			if err != nil {
				return fmt.Errorf("pgx connection: %w", err)
			}

			service := listener.NewWalListener(
				cfg,
				logger,
				listener.NewRepository(conn),
				rConn,
				listener.NewNatsPublisher(js),
				listener.NewBinaryParser(binary.BigEndian),
			)

			if err := service.Process(c.Context); err != nil {
				return fmt.Errorf("service process: %w", err)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}
