package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/nats-io/stan.go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/ihippik/wal-listener/listener"
)

// go build -ldflags "-X main.version=1.0.1" main.go
var version = "0.1.0"

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"v"},
		Usage:   "print only the version",
	}

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

			initLogger(cfg.Logger)

			sc, err := stan.Connect(cfg.Nats.ClusterID, cfg.Nats.ClientID, stan.NatsURL(cfg.Nats.Address))
			if err != nil {
				return fmt.Errorf("nats connection: %w", err)
			}

			conn, rConn, err := initPgxConnections(cfg.Database)
			if err != nil {
				return fmt.Errorf("pgx connection: %w", err)
			}

			service := listener.NewWalListener(
				cfg,
				listener.NewRepository(conn),
				rConn,
				listener.NewNatsPublisher(sc),
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
