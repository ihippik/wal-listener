# Slog-Sentry
Sentry hook for the standard Golang logger slog.

A minimalist hook for the standard Golang logger slog, which enables sending our logs to the [Sentry](https://sentry.io/).

Upon hook initialization, you need to provide it with a standard log handler (TextHandler, JSONHandler, etc.), 
as well as the log levels you want to send to Sentry.

Messages of the `Error` level expect the original error as one of the following arguments: `err` or `error`.

### Example
```go
package main

import (
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/ihippik/slog-sentry"
)

func main() {
	if err := sentry.Init(sentry.ClientOptions{
		Dsn: "myDSN",
	}); err != nil {
		slog.Error("init sentry", "err", err)
	}

	defer sentry.Flush(time.Second * 2)

	opt := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	handler := slog.NewTextHandler(os.Stdout, &opt)
	hook := slogsentry.NewSentryHandler(handler, []slog.Level{slog.LevelWarn, slog.LevelError})

	logger := slog.New(hook)
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message", "err", io.ErrNoProgress)
}
```


