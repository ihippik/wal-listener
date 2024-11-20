package slogsentry

import (
	"context"
	"log/slog"
	"slices"

	"github.com/getsentry/sentry-go"
)

// SentryHandler is a Handler that writes log records to the Sentry.
type SentryHandler struct {
	slog.Handler
	levels []slog.Level
}

// NewSentryHandler creates a SentryHandler that writes to w,
// using the given options.
func NewSentryHandler(
	handler slog.Handler,
	levels []slog.Level,
) *SentryHandler {
	return &SentryHandler{
		Handler: handler,
		levels:  levels,
	}
}

// Enabled reports whether the handler handles records at the given level.
func (s *SentryHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return s.Handler.Enabled(ctx, level)
}

// Handle intercepts and processes logger messages.
// In our case, send a message to the Sentry.
func (s *SentryHandler) Handle(ctx context.Context, record slog.Record) error {
	const (
		shortErrKey = "err"
		longErrKey  = "error"
	)

	if slices.Contains(s.levels, record.Level) {
		switch record.Level {
		case slog.LevelError:
			record.Attrs(func(attr slog.Attr) bool {
				if attr.Key == shortErrKey || attr.Key == longErrKey {
					if err, ok := attr.Value.Any().(error); ok {
						sentry.CaptureException(err)
					}
				}

				return true
			})
		case slog.LevelDebug, slog.LevelInfo, slog.LevelWarn:
			sentry.CaptureMessage(record.Message)
		}
	}

	return s.Handler.Handle(ctx, record)
}

// WithAttrs returns a new SentryHandler whose attributes consists.
func (s *SentryHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return NewSentryHandler(s.Handler.WithAttrs(attrs), s.levels)
}

// WithGroup returns a new SentryHandler whose group consists.
func (s *SentryHandler) WithGroup(name string) slog.Handler {
	return NewSentryHandler(s.Handler.WithGroup(name), s.levels)
}
