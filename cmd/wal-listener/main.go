package main

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

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
			ctx, cancel := signal.NotifyContext(c.Context, syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			cfg, err := config.InitConfig(c.String("config"))
			if err != nil {
				return fmt.Errorf("get config: %w", err)
			}

			if err = cfg.Validate(); err != nil {
				return fmt.Errorf("validate config: %w", err)
			}

			if err = scfg.InitSentry(cfg.Monitoring.SentryDSN, version); err != nil {
				return fmt.Errorf("init sentry: %w", err)
			}

			logger := scfg.InitSlog(cfg.Logger, version, cfg.Monitoring.SentryDSN != "")

			memSnapshotSignals := make(chan os.Signal, 1)
			signal.Notify(memSnapshotSignals, syscall.SIGUSR2)

			go func() {
				select {
				case <-ctx.Done():
					return
				case <-memSnapshotSignals:
					logger.Info("SIGUSR2 received, generating heap snapshot")

					memProfile, err := os.Create("mem.pb.gz")
					if err != nil {
						logger.Error("cannot create heap profile file", "err", err.Error())
					}

					runtime.GC()

					err = pprof.WriteHeapProfile(memProfile)
					if err != nil {
						logger.Error("cannot write heap profile", "err", err.Error())
					}

					memProfile.Close()
				}
			}()

			go scfg.InitMetrics(cfg.Monitoring.PromAddr, logger)

			conn, replConn, err := initPgxConnections(ctx, cfg.Database, logger)
			if err != nil {
				return fmt.Errorf("pgx connection: %w", err)
			}

			pub, err := factoryPublisher(ctx, cfg.Publisher, logger)
			if err != nil {
				return fmt.Errorf("factory publisher: %w", err)
			}

			defer func() {
				if err := pub.Close(); err != nil {
					slog.Error("close publisher failed", "err", err.Error())
				}
			}()

			replication := listener.NewReplicationWrapper(replConn, logger)

			service := listener.NewWalListener(
				cfg,
				logger,
				listener.NewRepository(conn),
				replication,
				pub,
				listener.NewBinaryParser(logger, binary.BigEndian),
				config.NewMetrics(),
			)

			go service.InitHandlers(ctx)

			if err := service.Process(ctx); err != nil {
				slog.Error("service process failed", "err", err.Error())
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("service error", "err", err)
	}
}
