package listener

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"

	"github.com/ihippik/wal-listener/config"
)

const errorBufferSize = 100

// Logical decoding plugin.
const (
	wal2JsonPlugin      = "wal2json"
	pluginArgIncludeLSN = `"include-lsn" 'on'`
)

// Service info message.
const (
	StartServiceMessage = "service was started"
	StopServiceMessage  = "service was stopped"
)

type publisher interface {
	Publish(subject string, msg []byte) error
	Close() error
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
	GetSlotLSN(slotName string) (string, error)
	IsAlive() bool
	Close() error
}

// Listener main service struct.
type Listener struct {
	config     config.Config
	slotName   string
	publisher  publisher
	replicator replication
	repository repository
	restartLSN uint64
	errChannel chan error
}

func NewWalListener(cfg *config.Config, repo repository, repl replication, publ publisher) *Listener {
	return &Listener{
		slotName:   fmt.Sprintf("%s_%s", cfg.Listener.SlotName, cfg.Database.Name),
		config:     *cfg,
		publisher:  publ,
		repository: repo,
		replicator: repl,
		errChannel: make(chan error, errorBufferSize),
	}
}

// Process is main service entry point.
func (w *Listener) Process() error {
	var serviceErr *serviceErr
	logger := logrus.WithField("slot_name", w.slotName)
	ctx, cancelFunc := context.WithCancel(context.Background())
	logrus.WithField("logger_level", w.config.Logger.Level).Infoln(StartServiceMessage)

	slotIsExists, err := w.slotIsExists()
	if err != nil {
		return err
	}

	if !slotIsExists {
		consistentPoint, _, err := w.replicator.CreateReplicationSlotEx(w.slotName, wal2JsonPlugin)
		if err != nil {
			return err
		}
		w.restartLSN, err = pgx.ParseLSN(consistentPoint)
		logger.Infoln("create new slot")
		if err != nil {
			return err
		}
	} else {
		logger.Infoln("slot already exists, restartLSN updated")
	}

	go w.Stream(ctx)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	refresh := time.Tick(w.config.Listener.RefreshConnection)
ProcessLoop:
	for {
		select {
		case <-refresh:
			if !w.replicator.IsAlive() {
				logrus.Fatalln(errReplConnectionIsLost)
			}
			if !w.repository.IsAlive() {
				logrus.Fatalln(errConnectionIsLost)
				w.errChannel <- errConnectionIsLost
			}
		case err := <-w.errChannel:
			if errors.As(err, &serviceErr) {
				cancelFunc()
				logrus.Fatalln(err)
			} else {
				logrus.Errorln(err)
			}

		case <-signalChan:
			cancelFunc()
			err := w.Stop()
			if err != nil {
				logrus.Errorln(err)
			}
			break ProcessLoop
		}
	}
	return nil
}

// slotIsExists checks whether a slot has already been created and if it has been created uses it.
func (w *Listener) slotIsExists() (bool, error) {
	restartLSNStr, err := w.repository.GetSlotLSN(w.slotName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if len(restartLSNStr) == 0 {
		return false, nil
	}
	w.restartLSN, err = pgx.ParseLSN(restartLSNStr)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Stream receive event from PostgreSQL.
// Accept message, apply filter and  publish it in NATS server.
func (w *Listener) Stream(ctx context.Context) {
	err := w.replicator.StartReplication(w.slotName, w.restartLSN, -1, pluginArgIncludeLSN)
	if err != nil {
		w.errChannel <- newListenerError("StartReplication()", err)
		return
	}

	go w.SendPeriodicHeartbeats(ctx)

	for {
		message, err := w.replicator.WaitForReplicationMessage(ctx)
		if err != nil {
			w.errChannel <- newListenerError("WaitForReplicationMessage()", err)
			continue
		}

		if message != nil {
			if message.WalMessage != nil {
				var m WalEvent
				walData := message.WalMessage.WalData
				err = m.UnmarshalJSON(walData)
				if err != nil {
					w.errChannel <- fmt.Errorf("%v: %w", ErrUnmarshalMsg, err)
					continue
				}
				if len(m.Change) == 0 {
					logrus.WithField("next_lsn", m.NextLSN).Infoln("skip empty WAL message")
					continue
				}
				err = m.Validate()
				if err != nil {
					logrus.WithError(err).Warningln(ErrValidateMessage)
					continue
				}
				logrus.WithFields(logrus.Fields{"next_lsn": m.NextLSN, "count_events": len(m.Change)}).
					Infoln("receive wal message")
				for _, event := range m.CreateEventsWithFilter(w.config.Database.Filter.Tables) {
					logrus.WithFields(logrus.Fields{"action": event.Action, "table": event.TableName}).
						Debugln("receive events")
					msg, err := event.MarshalJSON()
					if err != nil {
						w.errChannel <- fmt.Errorf("%v: %w", ErrMarshalMsg, err)
						continue
					}

					subjectName := event.GetSubjectName(w.config.Nats.TopicPrefix, w.config.Database.Name)

					// TODO tracer?!
					// TODO retry func for publish?!
					err = w.publisher.Publish(subjectName, msg)
					if err != nil {
						w.errChannel <- newListenerError("Publish()", err)
						continue
					} else {
						logrus.WithField("nxt_lsn", m.NextLSN).Infoln("event publish to NATS")
					}
				}

				err = w.AckWalMessage(m.NextLSN)
				if err != nil {
					w.errChannel <- fmt.Errorf("%v: %w", ErrAckWalMessage, err)
					continue
				} else {
					logrus.WithField("lsn", m.NextLSN).Debugln("ack wal message")
				}
			}
			if message.ServerHeartbeat != nil {
				//FIXME panic if there have been no messages for a long time.
				logrus.WithFields(logrus.Fields{
					"server_wal_end": message.ServerHeartbeat.ServerWalEnd,
					"server_time":    message.ServerHeartbeat.ServerTime,
				}).
					Debugln("received server heartbeat")
				if message.ServerHeartbeat.ReplyRequested == 1 {
					logrus.Debugln("status requested")
					err = w.SendStandbyStatus()
					if err != nil {
						w.errChannel <- fmt.Errorf("%v: %w", ErrSendStandbyStatus, err)
					}
				}
			}
		}
	}
}

// Stop is a finalizer function.
func (w *Listener) Stop() error {
	var err error
	err = w.publisher.Close()
	if err != nil {
		return err
	}
	err = w.repository.Close()
	if err != nil {
		return err
	}
	err = w.replicator.Close()
	if err != nil {
		return err
	}
	logrus.Infoln(StopServiceMessage)
	return nil
}

// SendPeriodicHeartbeats send periodic keep alive hearbeats to the server.
func (w *Listener) SendPeriodicHeartbeats(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logrus.WithField("func", "SendPeriodicHeartbeats").
				Infoln("context was canceled, stop sending heartbeats")
			return
		case <-time.Tick(w.config.Listener.HeartbeatInterval):
			{
				err := w.SendStandbyStatus()
				if err != nil {
					logrus.WithError(err).Errorln("failed to send status heartbeat")
					continue
				}
				logrus.Debugln("sending periodic status heartbeat")
			}
		}
	}
}

// SendStandbyStatus sends a `StandbyStatus` object with the current RestartLSN value to the server.
func (w *Listener) SendStandbyStatus() error {
	standbyStatus, err := pgx.NewStandbyStatus(w.restartLSN)
	if err != nil {
		return fmt.Errorf("unable to create StandbyStatus object: %w", err)
	}
	standbyStatus.ReplyRequested = 0
	err = w.replicator.SendStandbyStatus(standbyStatus)
	if err != nil {
		return fmt.Errorf("unable to send StandbyStatus object: %w", err)
	}
	return nil
}

// AckWalMessage acknowledge received wal message.
func (w *Listener) AckWalMessage(restartLSNStr string) error {
	restartLSN, err := pgx.ParseLSN(restartLSNStr)
	if err != nil {
		return err
	}
	w.restartLSN = restartLSN
	err = w.SendStandbyStatus()
	if err != nil {
		return err
	}
	return nil
}
