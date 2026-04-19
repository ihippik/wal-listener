package publisher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/IBM/sarama"
	"github.com/goccy/go-json"

	"github.com/ihippik/wal-listener/v2/internal/config"
)

// KafkaPublisher represent event publisher with Kafka broker.
type KafkaPublisher struct {
	cfg      *config.PublisherCfg
	log      *slog.Logger
	client   sarama.Client
	producer sarama.SyncProducer
	alive    atomic.Bool
}

// NewKafkaPublisher return new KafkaPublisher instance.
func NewKafkaPublisher(
	cfg *config.PublisherCfg,
	logger *slog.Logger,
	client sarama.Client,
	producer sarama.SyncProducer,
) *KafkaPublisher {
	p := &KafkaPublisher{
		cfg:      cfg,
		log:      logger,
		client:   client,
		producer: producer,
	}
	p.alive.Store(true)

	return p
}

func (p *KafkaPublisher) Publish(_ context.Context, topic string, event *Event) error {
	var key []byte

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if p.cfg.IsMessageKeyExists() {
		key = p.keyData(event)
	}

	partition, offset, err := p.producer.SendMessage(prepareMessage(topic, key, data))
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}

	p.log.Debug(
		"kafka producer: message was sent",
		slog.String("key", string(key)),
		slog.Int64("offset", offset),
		slog.Int("partition", int(partition)),
	)

	return nil
}

// IsAlive returns the latest publisher health state.
func (p *KafkaPublisher) IsAlive() bool {
	return p.alive.Load()
}

// CheckHealth verifies Kafka broker connectivity by refreshing metadata.
func (p *KafkaPublisher) CheckHealth(_ context.Context) error {
	if p.client == nil || p.client.Closed() {
		p.alive.Store(false)
		return errors.New("kafka client is closed")
	}

	if err := p.client.RefreshMetadata(); err != nil {
		p.alive.Store(false)
		return fmt.Errorf("refresh metadata: %w", err)
	}

	p.alive.Store(true)

	return nil
}

// Close connection close.
func (p *KafkaPublisher) Close() error {
	var err error

	if p.producer != nil {
		err = p.producer.Close()
	}

	if p.client != nil {
		err = errors.Join(err, p.client.Close())
	}

	return err
}

func (p *KafkaPublisher) keyData(e *Event) []byte {
	if data, ok := e.Data[p.cfg.MessageKeyFrom]; ok {
		key, _ := json.Marshal(data)
		return key
	}

	return []byte(e.Table)
}

// NewProducer return new Kafka producer instance.
func NewProducer(pCfg *config.PublisherCfg) (sarama.Client, sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	if pCfg.IsMessageKeyExists() {
		cfg.Producer.Partitioner = sarama.NewHashPartitioner
	}

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	if pCfg.EnableTLS {
		tlsCfg, err := newTLSCfg(pCfg.ClientCert, pCfg.ClientKey, pCfg.CACert)
		if err != nil {
			return nil, nil, fmt.Errorf("new TLS config: %w", err)
		}

		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsCfg
	}

	client, err := sarama.NewClient([]string{pCfg.Address}, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("new kafka client: %w", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("new sync producer from client: %w", err)
	}

	return client, producer, nil
}

// prepareMessage prepare sarama message for Kafka producer.
func prepareMessage(topic string, key, data []byte) *sarama.ProducerMessage {
	var msg = sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(data),
	}

	if key != nil {
		msg.Key = sarama.ByteEncoder(key)
	}

	return &msg
}

func newTLSCfg(certFile, keyFile, caCert string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load x509 key pair: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	ca, err := os.ReadFile(caCert)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(ca)
	cfg.RootCAs = caCertPool

	return cfg, nil
}
