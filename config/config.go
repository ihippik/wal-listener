package config

import (
	"time"

	"github.com/asaskevich/govalidator"
)

type Config struct {
	Listener ListenerCfg
	Database DatabaseCfg
	Nats     NatsCfg
	Logger   LoggerCfg
}

type ListenerCfg struct {
	SlotName          string        `valid:"required"`
	AckTimeout        time.Duration `valid:"required"`
	RefreshConnection time.Duration `valid:"required"`
	HeartbeatInterval time.Duration `valid:"required"`
}

type NatsCfg struct {
	Address     string `valid:"required"`
	ClusterID   string `valid:"required"`
	ClientID    string `valid:"required"`
	TopicPrefix string `valid:"required"`
}

type LoggerCfg struct {
	Caller        bool
	Level         string
	HumanReadable bool
}
type DatabaseCfg struct {
	Host     string `valid:"required"`
	Port     uint16 `valid:"required"`
	Name     string `valid:"required"`
	User     string `valid:"required"`
	Password string `valid:"required"`
	Filter   FilterStruct
}

type FilterStruct struct {
	Tables map[string][]string
}

func (c Config) Validate() error {
	_, err := govalidator.ValidateStruct(c)
	return err
}
