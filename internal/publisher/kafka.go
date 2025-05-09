package publisher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"

	"github.com/IBM/sarama"
	"github.com/goccy/go-json"

	"github.com/ihippik/wal-listener/v2/internal/config"
)

// KafkaPublisher represent event publisher with Kafka broker.
type KafkaPublisher struct {
	cfg      *config.PublisherCfg
	log      *slog.Logger
	producer sarama.SyncProducer
}

// NewKafkaPublisher return new KafkaPublisher instance.
func NewKafkaPublisher(cfg *config.PublisherCfg, logger *slog.Logger, producer sarama.SyncProducer) *KafkaPublisher {
	return &KafkaPublisher{
		cfg:      cfg,
		log:      logger,
		producer: producer,
	}
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

// Close connection close.
func (p *KafkaPublisher) Close() error {
	return p.producer.Close()
}

func (p *KafkaPublisher) keyData(e *Event) []byte {
	if data, ok := e.Data[p.cfg.MessageKeyFrom]; ok {
		key, _ := json.Marshal(data)
		return key
	}

	return []byte(e.Table)
}

// NewProducer return new Kafka producer instance.
func NewProducer(pCfg *config.PublisherCfg) (sarama.SyncProducer, error) {
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
			return nil, fmt.Errorf("new TLS config: %w", err)
		}

		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsCfg
	}

	producer, err := sarama.NewSyncProducer([]string{pCfg.Address}, cfg)
	if err != nil {
		return nil, fmt.Errorf("new sync producer: %w", err)
	}

	return producer, nil
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
