# Config
Common configuration

## Logger
Represents a logger configuration and ability to create a slog logger instance.

```go
type Logger struct {
    Level  string // Logger level
    Format string // logger formatter
}
```

## Monitoring
Represents a monitoring configuration and ability to create a Sentry client and Prometheus handler.

```go
type Monitoring struct {
	SentryDSN string // Sentry DSN
	PromAddr  string // Prometheus address for metrics
}
```

## DB
Represents a DB connection configuration.

```go
type DB struct {
    Host        string // required
    Port        int    // required
    User        string // required
    Password    string
    DBName      string // required
    Schema      string
    MaxIdleConn int    // default=2
    MaxOpenConn int
}
```

## Kafka
Represents a Kafka consumer configuration.

```go
type KafkaInput struct {
    ClientID      string // required
    Brokers       string // required
    ConsumerGroup string // required
    OffsetOldest  bool  
    Verbose       bool   // logging
}
```

## Utility
- `GetVersion` - returns the version of the application (hash commit)

