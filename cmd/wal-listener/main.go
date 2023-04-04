package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/ihippik/wal-listener/v2/listener"
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

			conn, rConn, err := initPgxConnections(cfg.Database)
			if err != nil {
				return fmt.Errorf("pgx connection: %w", err)
			}

			service := listener.NewWalListener(
				cfg,
				logger,
				listener.NewRepository(conn),
				rConn,
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
