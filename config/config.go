package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/spf13/viper"

	cfg "github.com/ihippik/config"
)

type PublisherType string

const (
	PublisherTypeNats         PublisherType = "nats"
	PublisherTypeKafka        PublisherType = "kafka"
	PublisherTypeRabbitMQ     PublisherType = "rabbitmq"
	PublisherTypeGooglePubSub PublisherType = "google_pubsub"
)

// Config for wal-listener.
type Config struct {
	Listener   *ListenerCfg  `valid:"required"`
	Database   *DatabaseCfg  `valid:"required"`
	Publisher  *PublisherCfg `valid:"required"`
	Logger     *cfg.Logger   `valid:"required"`
	Monitoring cfg.Monitoring
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string `valid:"required"`
	ServerPort        int
	AckTimeout        time.Duration
	RefreshConnection time.Duration `valid:"required"`
	HeartbeatInterval time.Duration `valid:"required"`
	Include           IncludeStruct
	Exclude           ExcludeStruct
	TopicsMap         map[string]string

	ParsedTopicsMap map[*regexp.Regexp]string `valid:"-"`
}

// PublisherCfg represent configuration for any publisher types.
type PublisherCfg struct {
	Type            PublisherType `valid:"required"`
	Address         string
	Topic           string `valid:"required"`
	TopicPrefix     string
	EnableTLS       bool
	ClientCert      string
	ClientKey       string
	CACert          string
	PubSubProjectID string
	EnableOrdering  bool
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	Host     string `valid:"required"`
	Port     uint16 `valid:"required"`
	Name     string `valid:"required"`
	User     string `valid:"required"`
	Password string `valid:"required"`
	SSL      *SSLStruct
}

type IncludeStruct struct {
	Tables map[string][]string
}

type ExcludeStruct struct {
	Tables []string
}

type SSLStruct struct {
	SkipVerify bool
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

	dbPassword := os.Getenv("WAL_LISTENER_DB_PASSWORD")
	if dbPassword != "" {
		conf.Database.Password = dbPassword
	}

	conf.Listener.ParsedTopicsMap = make(map[*regexp.Regexp]string, len(conf.Listener.TopicsMap))
	for key, val := range conf.Listener.TopicsMap {
		var (
			re  *regexp.Regexp
			err error
		)

		if len(key) > 1 && key[0] == '/' && key[len(key)-1] == '/' {
			re, err = regexp.Compile(key[1 : len(key)-1])
			if err != nil {
				return nil, fmt.Errorf("invalid regexp topic map regexp %s: %w", key, err)
			}
		} else {
			re, err = regexp.Compile("^" + regexp.QuoteMeta(key) + "$")
			if err != nil {
				return nil, fmt.Errorf("invalid static topic map key %s: %w", key, err)
			}
		}

		conf.Listener.ParsedTopicsMap[re] = val
	}

	return &conf, nil
}
