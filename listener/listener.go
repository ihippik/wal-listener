package listener

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"

	"github.com/ihippik/wal-listener/config"
)

const errorBufferSize = 100

// Logical decoding plugin.
const (
	pgOutputPlugin = "pgoutput"
)

// Service info message.
const (
	StartServiceMessage = "service was started"
	StopServiceMessage  = "service was stopped"
)

type publisher interface {
	Publish(string, Event) error
	Close() error
}

type parser interface {
	ParseWalMessage([]byte, *WalTransaction) error
}

type replication interface {
	CreateReplicationSlotEx(slotName, outputPlugin string) (consistentPoint string, snapshotName string, err error)
	DropReplicationSlot(slotName string) (err error)
	StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error)
	WaitForReplicationMessage(ctx context.Context) (*pgx.ReplicationMessage, error)
	SendStandbyStatus(k *pgx.StandbyStatus) (err error)
	IsAlive() bool
	Close() error
}

type repository interface {
	CreatePublication(name string) error
	GetSlotLSN(slotName string) (string, error)
	IsAlive() bool
	Close() error
}

// Listener main service struct.
type Listener struct {
	mu         sync.RWMutex
	config     config.Config
	slotName   string
	publisher  publisher
	replicator replication
	repository repository
	parser     parser
	lsn        uint64
	errChannel chan error
}

// NewWalListener create and initialize new service instance.
func NewWalListener(
	cfg *config.Config,
	repo repository,
	repl replication,
	publ publisher,
	parser parser,
) *Listener {
	return &Listener{
		slotName:   fmt.Sprintf("%s_%s", cfg.Listener.SlotName, cfg.Database.Name),
		config:     *cfg,
		publisher:  publ,
		repository: repo,
		replicator: repl,
		parser:     parser,
		errChannel: make(chan error, errorBufferSize),
	}
}

// Process is main service entry point.
func (l *Listener) Process(ctx context.Context) error {
	var serviceErr *serviceErr

	logger := logrus.WithField("slot_name", l.slotName)

	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	logger.WithField("logger_level", l.config.Logger.Level).Infoln(StartServiceMessage)

	if err := l.repository.CreatePublication(publicationName); err != nil {
		logger.WithError(err).Warnln("skip create publication")
	}

	slotIsExists, err := l.slotIsExists()
	if err != nil {
		return fmt.Errorf("slot is exists: %w", err)
	}

	if !slotIsExists {
		consistentPoint, _, err := l.replicator.CreateReplicationSlotEx(l.slotName, pgOutputPlugin)
		if err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}

		lsn, err := pgx.ParseLSN(consistentPoint)
		if err != nil {
			return fmt.Errorf("parse lsn: %w", err)
		}

		l.setLSN(lsn)

		logger.Infoln("create new slot")
	} else {
		logger.Infoln("slot already exists, LSN updated")
	}

	go l.Stream(ctx)

	signalChan := make(chan os.Signal, 1)
	refresh := time.NewTicker(l.config.Listener.RefreshConnection)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

ProcessLoop:
	for {
		select {
		case <-refresh.C:
			if !l.replicator.IsAlive() {
				logrus.Fatalln(errReplConnectionIsLost)
			}

			if !l.repository.IsAlive() {
				logrus.Fatalln(errConnectionIsLost)
			}

		case err := <-l.errChannel:
			if errors.As(err, &serviceErr) {
				cancelFunc()

				logrus.Fatalln(err)
			} else {
				logrus.Errorln(err)
			}

		case <-signalChan:
			if err := l.Stop(); err != nil {
				logrus.WithError(err).Errorln("listener stop error")
			}

			break ProcessLoop
		}
	}

	return nil
}

// slotIsExists checks whether a slot has already been created and if it has been created uses it.
func (l *Listener) slotIsExists() (bool, error) {
	restartLSNStr, err := l.repository.GetSlotLSN(l.slotName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			logrus.
				WithField("slot", l.slotName).
				Warningln("restart_lsn for slot not found")

			return false, nil
		}

		return false, err
	}

	if len(restartLSNStr) == 0 {
		return false, nil
	}

	lsn, err := pgx.ParseLSN(restartLSNStr)
	if err != nil {
		return false, fmt.Errorf("parse lsn: %w", err)
	}

	l.setLSN(lsn)

	return true, nil
}

func publicationNames(publication string) string {
	return fmt.Sprintf(`publication_names '%s'`, publication)
}

const (
	protoVersion    = "proto_version '1'"
	publicationName = "wal-listener"
)

// Stream receive event from PostgreSQL.
// Accept message, apply filter and  publish it in NATS server.
func (l *Listener) Stream(ctx context.Context) {
	err := l.replicator.StartReplication(l.slotName, l.readLSN(), -1, protoVersion, publicationNames(publicationName))
	if err != nil {
		l.errChannel <- newListenerError("StartReplication()", err)

		return
	}

	go l.SendPeriodicHeartbeats(ctx)

	tx := NewWalTransaction()
	for {
		if ctx.Err() != nil {
			l.errChannel <- newListenerError("read msg", err)
			break
		}

		msg, err := l.replicator.WaitForReplicationMessage(ctx)
		if err != nil {
			l.errChannel <- newListenerError("WaitForReplicationMessage()", err)
			continue
		}

		if msg != nil {
			if msg.WalMessage != nil {
				logrus.WithField("wal", msg.WalMessage.WalStart).
					Debugln("receive wal message")

				if err := l.parser.ParseWalMessage(msg.WalMessage.WalData, tx); err != nil {
					logrus.WithError(err).Errorln("msg parse failed")
					l.errChannel <- fmt.Errorf("%v: %w", ErrUnmarshalMsg, err)

					continue
				}

				if tx.CommitTime != nil {
					natsEvents := tx.CreateEventsWithFilter(l.config.Database.Filter.Tables)
					for _, event := range natsEvents {
						subjectName := event.GetSubjectName(l.config.Nats.TopicPrefix)
						if err = l.publisher.Publish(subjectName, event); err != nil {
							l.errChannel <- fmt.Errorf("%v: %w", ErrPublishEvent, err)

							continue
						}

						logrus.
							WithField("subject", subjectName).
							WithField("action", event.Action).
							WithField("lsn", l.readLSN()).
							Infoln("event was send")

					}

					tx.Clear()
				}

				if msg.WalMessage.WalStart > l.readLSN() {
					if err = l.AckWalMessage(msg.WalMessage.WalStart); err != nil {
						l.errChannel <- fmt.Errorf("%v: %w", ErrAckWalMessage, err)

						continue
					}

					logrus.WithField("lsn", l.readLSN()).Debugln("ack wal msg")

				}
			}
			if msg.ServerHeartbeat != nil {
				//FIXME panic if there have been no messages for a long time.
				logrus.WithFields(logrus.Fields{
					"server_wal_end": msg.ServerHeartbeat.ServerWalEnd,
					"server_time":    msg.ServerHeartbeat.ServerTime,
				}).
					Debugln("received server heartbeat")

				if msg.ServerHeartbeat.ReplyRequested == 1 {
					logrus.Debugln("status requested")

					if err = l.SendStandbyStatus(); err != nil {
						l.errChannel <- fmt.Errorf("%v: %w", ErrSendStandbyStatus, err)
					}
				}
			}
		}
	}
}

// Stop is a finalizer function.
func (l *Listener) Stop() error {

	if err := l.publisher.Close(); err != nil {
		return fmt.Errorf("publisher close: %w", err)
	}

	if err := l.repository.Close(); err != nil {
		return fmt.Errorf("repository close: %w", err)
	}

	if err := l.replicator.Close(); err != nil {
		return fmt.Errorf("replicator close: %w", err)
	}

	logrus.Infoln(StopServiceMessage)

	return nil
}

// SendPeriodicHeartbeats send periodic keep alive heartbeats to the server.
func (l *Listener) SendPeriodicHeartbeats(ctx context.Context) {
	heart := time.NewTicker(l.config.Listener.HeartbeatInterval)
	defer heart.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.WithField("func", "SendPeriodicHeartbeats").
				Infoln("context was canceled, stop sending heartbeats")

			return
		case <-heart.C:
			{
				if err := l.SendStandbyStatus(); err != nil {
					logrus.WithError(err).Errorln("failed to send status heartbeat")

					continue
				}

				logrus.Debugln("sending periodic status heartbeat")
			}
		}
	}
}

// SendStandbyStatus sends a `StandbyStatus` object with the current RestartLSN value to the server.
func (l *Listener) SendStandbyStatus() error {
	standbyStatus, err := pgx.NewStandbyStatus(l.readLSN())
	if err != nil {
		return fmt.Errorf("unable to create StandbyStatus object: %w", err)
	}

	standbyStatus.ReplyRequested = 0

	if err := l.replicator.SendStandbyStatus(standbyStatus); err != nil {
		return fmt.Errorf("unable to send StandbyStatus object: %w", err)
	}

	return nil
}

// AckWalMessage acknowledge received wal message.
func (l *Listener) AckWalMessage(lsn uint64) error {
	l.setLSN(lsn)

	if err := l.SendStandbyStatus(); err != nil {
		return fmt.Errorf("send status: %w", err)
	}

	return nil
}

func (l *Listener) readLSN() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lsn
}

func (l *Listener) setLSN(lsn uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lsn = lsn
}
