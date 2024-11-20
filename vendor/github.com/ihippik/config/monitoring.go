package config

import (
	"log/slog"
	"net/http"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Monitoring represent configuration for any monitoring.
type Monitoring struct {
	SentryDSN string `yaml:"sentry_dsn" env:"SENTRY_DSN"`
	PromAddr  string `yaml:"prom_addr" env:"PROM_ADDR"`
}

// InitMetrics init metrics handler for Prometheus.
func InitMetrics(addr string, logger *slog.Logger) {
	if len(addr) == 0 {
		logger.Warn("metrics addr not set")
		return
	}

	logger.With("addr", addr).Info("metrics handler")

	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("init metrics handler", "error", err)
		return
	}
}

// InitSentry init Sentry client.
func InitSentry(dsn, ver string) error {
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:     dsn,
		Release: ver,
	}); err != nil {
		return err
	}

	return nil
}
