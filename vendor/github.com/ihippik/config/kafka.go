package config

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

// KafkaInput represent consumer configuration.
type KafkaInput struct {
	ClientID      string `yaml:"client_id" env:"CLIENT_ID,required" valid:"required"`
	Brokers       string `yaml:"brokers" env:"BROKERS,required" valid:"required"`
	ConsumerGroup string `yaml:"consumer_group" env:"CONSUMER_GROUP,required" valid:"required"`
	OffsetOldest  bool   `yaml:"offset_latest" env:"OFFSET_OLDEST"`
	Verbose       bool   `yaml:"verbose" env:"VERBOSE"`
}

// KafkaOutput represent consumer configuration.
type KafkaOutput struct {
	Brokers string `yaml:"brokers" env:"BROKERS,required" valid:"required"`
}

// Consumer represent consumer group.
type Consumer struct {
	client sarama.ConsumerGroup
	ready  chan bool
	output chan []byte
	log    *slog.Logger
}

func newConsumer(log *slog.Logger, client sarama.ConsumerGroup) *Consumer {
	return &Consumer{
		client: client,
		log:    log,
		ready:  make(chan bool),
		output: make(chan []byte),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements ConsumerGroupHandler interface.
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				c.log.Debug("consume claim: message channel was closed")
				return nil
			}

			c.log.Debug("consume claim: message was consumed", slog.String("topic", message.Topic))

			c.output <- message.Value

			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// init represent main consumer worker.
func (c *Consumer) init(ctx context.Context, topics string) {
	defer func() {
		if err := c.client.Close(); err != nil {
			c.log.Error("close client", slog.Any("err", err))
		}
	}()

	for {
		if err := c.client.Consume(ctx, strings.Split(topics, ","), c); err != nil {
			c.log.Error("consume filed", slog.Any("err", err))
		}

		if err := ctx.Err(); err != nil {
			c.log.Debug("consumer: context canceled")
			return
		}

		c.ready = make(chan bool)
	}
}

// NewConsumerGroup constructor for creating consumer group.
func NewConsumerGroup(ctx context.Context, log *slog.Logger, cfg *KafkaInput, topics string) (*Consumer, error) {
	consumer, err := initConsumerGroup(cfg)
	if err != nil {
		return nil, fmt.Errorf("init consumer group: %w", err)
	}

	cns := newConsumer(log, consumer)

	go cns.init(ctx, topics)

	<-cns.ready

	log.Info("consumer was initialized", slog.String("topics", topics))

	return cns, nil
}

// Message representing a channel for processing messages.
func (c *Consumer) Message() <-chan []byte {
	return c.output
}

// initConsumerGroup initialised ConsumerGroup client.
func initConsumerGroup(cfg *KafkaInput) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}
	config.ClientID = cfg.ClientID
	config.Consumer.Return.Errors = true

	if cfg.OffsetOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if cfg.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	config.Version = sarama.DefaultVersion

	client, err := sarama.NewConsumerGroup(strings.Split(cfg.Brokers, ","), cfg.ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}

	return client, nil
}

// NewSyncProducer create new Sarama sync Producer.
func NewSyncProducer(cfg *KafkaOutput) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(cfg.Brokers, ","), config)

	return producer, err
}

// PrepareMessage prepare Kafka message for sending.
func PrepareMessage(topic string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(message),
	}

	return msg
}
