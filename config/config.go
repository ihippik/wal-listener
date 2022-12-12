package config

import (
	"time"

	"github.com/asaskevich/govalidator"
)

// Config for wal-listener
type Config struct {
	Listener   ListenerCfg   `env:",prefix=LISTENER"`
	Database   DatabaseCfg   `env:",prefix=DB_"`
	Nats       NatsCfg       `env:",prefix=NATS"`
	Logger     LoggerCfg     `env:",prefix=LOG"`
	Monitoring MonitoringCfg `env:",prefix=MONITOR"`
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string        `valid:"required" env:"SLOT_NAME"`
	AckTimeout        time.Duration `env:"ACK_TIMEOUT"`
	RefreshConnection time.Duration `valid:"required" env:"REFRESH_TIMEOUT"`
	HeartbeatInterval time.Duration `valid:"required" env:"HEARTBEAT_INTERVAL"`
	Filter            FilterStruct  `env:"FILTER"`
	TopicsMap         map[string]string
}

// NatsCfg path of the NATS config.
type NatsCfg struct {
	Address     string      `valid:"required" env:"ADDRESS"`
	StreamName  string      `valid:"required" env:"STREAM_NAME"`
	TopicPrefix string      `env:"TOPIC_PREFIX"`
	MTLS        NatsMtlsCfg `env:"MTLS"`
}

// Nats TLS Coniguration
type NatsMtlsCfg struct {
	CertPath           string `env:"CERT_PATH,default=/etc/nats-certs/clients"`
	CertFile           string `env:"CERT_FILE,default=tls.crt"`
	KeyFile            string `env:"KEY_FILE,default=tls.key"`
	CAFile             string `env:"CA_FILE,default=ca.crt"`
	InsecureSkipVerify bool   `env:"INSECURE_SKIP_VERIFY,default=false"`
	Enabled            bool   `env:"ENABLED,default=false"`
}

// MonitoringCfg monitoring configuration.
type MonitoringCfg struct {
	SentryDSN string `env:"SENTRY_DSN`
	PromAddr  string `env:"PROMETHEUS_ADDRESS"`
}

// LoggerCfg path of the logger config.
type LoggerCfg struct {
	Caller bool   `env:"CALLER"`
	Level  string `env:"LEVEL"`
	Format string `env:"FORMAT"`
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	DSN string `valid:"required" env:"DSN,overwrite"`
}

// FilterStruct incoming WAL message filter.
type FilterStruct struct {
	Tables map[string][]string
}

// Validate config data.
func (c Config) Validate() error {
	_, err := govalidator.ValidateStruct(c)
	return err
}
