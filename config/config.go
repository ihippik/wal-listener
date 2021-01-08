package config

import (
	"time"

	"github.com/asaskevich/govalidator"
)

// Config for wal-listener/
type Config struct {
	Listener ListenerCfg
	Database DatabaseCfg
	Nats     NatsCfg
	Logger   LoggerCfg
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string `valid:"required"`
	AckTimeout        time.Duration
	RefreshConnection time.Duration `valid:"required"`
	HeartbeatInterval time.Duration `valid:"required"`
}

// NatsCfg path of the NATS config.
type NatsCfg struct {
	Address     string `valid:"required"`
	ClusterID   string `valid:"required"`
	ClientID    string `valid:"required"`
	TopicPrefix string `valid:"required"`
}

// LoggerCfg path of the logger config.
type LoggerCfg struct {
	Caller        bool
	Level         string
	HumanReadable bool
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	Host     string `valid:"required"`
	Port     uint16 `valid:"required"`
	Name     string `valid:"required"`
	User     string `valid:"required"`
	Password string `valid:"required"`
	Filter   FilterStruct
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
