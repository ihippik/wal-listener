package main

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"

	scfg "github.com/ihippik/config"
	"github.com/urfave/cli/v2"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/listener"
)

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"v"},
		Usage:   "print only the version",
	}

	version := scfg.GetVersion()

	app := &cli.App{
		Name:    "WAL-Listener",
		Usage:   "listen PostgreSQL events",
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
			cfg, err := config.InitConfig(c.String("config"))
			if err != nil {
				return fmt.Errorf("get config: %w", err)
			}

			if err = cfg.Validate(); err != nil {
				return fmt.Errorf("validate config: %w", err)
			}

			if err := scfg.InitSentry(cfg.Monitoring.SentryDSN, version); err != nil {
				return fmt.Errorf("init sentry: %w", err)
			}

			logger := scfg.InitSlog(cfg.Logger, version, cfg.Monitoring.SentryDSN != "")

			go scfg.InitMetrics(cfg.Monitoring.PromAddr, logger)

			conn, rConn, err := initPgxConnections(cfg.Database, logger)
			if err != nil {
				return fmt.Errorf("pgx connection: %w", err)
			}

			pub, err := factoryPublisher(cfg.Publisher, logger)
			if err != nil {
				return fmt.Errorf("factory publisher: %w", err)
			}

			defer func() {
				if err := pub.Close(); err != nil {
					slog.Error("close publisher failed", "err", err.Error())
				}
			}()

			service := listener.NewWalListener(
				cfg,
				logger,
				listener.NewRepository(conn),
				rConn,
				pub,
				listener.NewBinaryParser(logger, binary.BigEndian),
				config.NewMetrics(),
			)

			if err := service.Process(c.Context); err != nil {
				return fmt.Errorf("service process: %w", err)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("service error", "error", err)
	}
}
