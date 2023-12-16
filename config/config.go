package config

import (
	"fmt"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/spf13/viper"

	cfg "github.com/ihippik/config"
)

type PublisherType string

const (
	PublisherTypeNats  PublisherType = "nats"
	PublisherTypeKafka PublisherType = "kafka"
)

// Config for wal-listener.
type Config struct {
	Listener   *ListenerCfg  `valid:"required"`
	Database   *DatabaseCfg  `valid:"required"`
	Publisher  *PublisherCfg `valid:"required"`
	Logger     *cfg.Logger   `valid:"required"`
	Monitoring *cfg.Monitoring
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string `valid:"required"`
	AckTimeout        time.Duration
	RefreshConnection time.Duration `valid:"required"`
	HeartbeatInterval time.Duration `valid:"required"`
	Filter            FilterStruct
	TopicsMap         map[string]string
}

// PublisherCfg represent configuration for any types publisher.
type PublisherCfg struct {
	Type        PublisherType `valid:"required"`
	Address     string        `valid:"required"`
	Topic       string        `valid:"required"`
	TopicPrefix string
	EnableTLS   bool   `json:"enable_tls"`
	ClientCert  string `json:"client_cert"`
	ClientKey   string `json:"client_key"`
	CACert      string `json:"ca_cert"`
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	Host     string `valid:"required"`
	Port     uint16 `valid:"required"`
	Name     string `valid:"required"`
	User     string `valid:"required"`
	Password string `valid:"required"`
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

// InitConfig load config from file.
func InitConfig(path string) (*Config, error) {
	var conf Config

	viper.SetConfigFile(path)

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	if err := viper.Unmarshal(&conf); err != nil {
		return nil, fmt.Errorf("unable to decode into config struct: %w", err)
	}

	return &conf, nil
}
