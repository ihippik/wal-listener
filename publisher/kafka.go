package publisher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/goccy/go-json"

	"github.com/ihippik/wal-listener/v2/config"
)

// KafkaPublisher represent event publisher with Kafka broker.
type KafkaPublisher struct {
	producer sarama.SyncProducer
}

// NewKafkaPublisher return new KafkaPublisher instance.
func NewKafkaPublisher(producer sarama.SyncProducer) *KafkaPublisher {
	return &KafkaPublisher{producer: producer}
}

func (p *KafkaPublisher) Publish(_ context.Context, topic string, event *Event) PublishResult {
	data, err := json.Marshal(event)
	if err != nil {
		return NewPublishResult(fmt.Errorf("marshal: %w", err))
	}

	if _, _, err = p.producer.SendMessage(prepareMessage(topic, data)); err != nil {
		return NewPublishResult(fmt.Errorf("send message: %w", err))
	}

	return NewPublishResult(nil)
}

func (p *KafkaPublisher) Flush(topic string) {}

// Close connection close.
func (p *KafkaPublisher) Close() error {
	return p.producer.Close()
}

// NewProducer return new Kafka producer instance.
func NewProducer(pCfg *config.PublisherCfg) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
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

// prepareMessage prepare message for Kafka producer.
func prepareMessage(topic string, data []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(data),
	}
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
