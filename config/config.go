package config

import (
	"time"

	"github.com/asaskevich/govalidator"
)

// Config for wal-listener
type Config struct {
	Listener   ListenerCfg   `env:",prefix=LISTENER_"`
	Database   DatabaseCfg   `env:",prefix=DB_"`
	Nats       NatsCfg       `env:",prefix=NATS_"`
	Logger     LoggerCfg     `env:",prefix=LOG_"`
	Monitoring MonitoringCfg `env:",prefix=MONITOR_"`
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string        `valid:"required" env:"SLOT_NAME,overwrite"`
	AckTimeout        time.Duration `env:"ACK_TIMEOUT,overwrite"`
	RefreshConnection time.Duration `valid:"required" env:"REFRESH_TIMEOUT,overwrite"`
	HeartbeatInterval time.Duration `valid:"required" env:"HEARTBEAT_INTERVAL,overwrite"`
	Filter            FilterStruct  `env:"FILTER,overwrite"`
	TopicsMap         map[string]string
}

// NatsCfg path of the NATS config.
type NatsCfg struct {
	Address     string      `valid:"required" env:"ADDRESS,overwrite"`
	StreamName  string      `valid:"required" env:"STREAM_NAME,overwrite"`
	TopicPrefix string      `env:"TOPIC_PREFIX,overwrite"`
	MTLS        NatsMtlsCfg `env:",prefix=MTLS_"`
}

// Nats TLS Coniguration
type NatsMtlsCfg struct {
	CertPath           string `env:"CERT_PATH,overwrite,default=/etc/nats-certs/clients"`
	CertFile           string `env:"CERT_FILE,overwrite,default=tls.crt"`
	KeyFile            string `env:"KEY_FILE,overwrite,default=tls.key"`
	CAFile             string `env:"CA_FILE,overwrite,default=ca.crt"`
	InsecureSkipVerify bool   `env:"INSECURE_SKIP_VERIFY,overwrite,default=false"`
	Enabled            bool   `env:"ENABLED,overwrite,default=false"`
}

// MonitoringCfg monitoring configuration.
type MonitoringCfg struct {
	SentryDSN string `env:"SENTRY_DSN,overwrite`
	PromAddr  string `env:"PROMETHEUS_ADDRESS,overwrite"`
}

// LoggerCfg path of the logger config.
type LoggerCfg struct {
	Caller bool   `env:"CALLER,overwrite"`
	Level  string `env:"LEVEL,overwrite"`
	Format string `env:"FORMAT,overwrite"`
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
