package config

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics Prometheus metrics.
type Metrics struct {
	filterSkippedEvents, publishedEvents, problematicEvents *prometheus.CounterVec
}

const (
	labelApp     = "app"
	labelTable   = "table"
	labelSubject = "subject"
	labelKind    = "kind"
)

// NewMetrics create and initialize new Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		publishedEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "published_events_total",
			Help: "The total number of published events",
		},
			[]string{labelApp, labelSubject, labelTable},
		),
		problematicEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "problematic_events_total",
			Help: "The total number of skipped problematic events",
		},
			[]string{labelApp, labelKind},
		),
		filterSkippedEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "filter_skipped_events_total",
			Help: "The total number of skipped events",
		},
			[]string{labelApp, labelTable},
		),
	}
}

const appName = "wal-listener"

// IncPublishedEvents increment published events counter.
func (m Metrics) IncPublishedEvents(subject, table string) {
	m.publishedEvents.With(prometheus.Labels{labelApp: appName, labelSubject: subject, labelTable: table}).Inc()
}

// IncFilterSkippedEvents increment skipped by filter events counter.
func (m Metrics) IncFilterSkippedEvents(table string) {
	m.filterSkippedEvents.With(prometheus.Labels{labelApp: appName, labelTable: table}).Inc()
}

// IncProblematicEvents increment skipped by filter events counter.
func (m Metrics) IncProblematicEvents(kind string) {
	m.problematicEvents.With(prometheus.Labels{labelApp: appName, labelKind: kind}).Inc()
}
