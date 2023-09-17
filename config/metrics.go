package config

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics Prometheus metrics.
type Metrics struct {
	filterSkippedEvents, publishedEvents *prometheus.CounterVec
}

// NewMetrics create and initialize new Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		publishedEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "published_events",
			Help: "The total number of published events",
		},
			[]string{"app", "subject", "table"},
		),
		filterSkippedEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "filter_skipped_events",
			Help: "The total number of skipped events",
		},
			[]string{"app", "table"},
		),
	}
}

const appName = "wal-listener"

// IncPublishedEvents increment published events counter.
func (m Metrics) IncPublishedEvents(subject, table string) {
	m.publishedEvents.With(prometheus.Labels{"app": appName, "subject": subject, "table": table}).Inc()
}

// IncFilterSkippedEvents increment skipped by filter events counter.
func (m Metrics) IncFilterSkippedEvents(table string) {
	m.filterSkippedEvents.With(prometheus.Labels{"app": appName, "table": table}).Inc()
}
