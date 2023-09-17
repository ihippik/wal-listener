package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
	"github.com/jackc/pgx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const errorBufferSize = 100

// Logical decoding plugin.
const pgOutputPlugin = "pgoutput"

type eventPublisher interface {
	Publish(string, publisher.Event) error
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
	cfg        *config.Config
	log        *slog.Logger
	mu         sync.RWMutex
	slotName   string
	publisher  eventPublisher
	replicator replication
	repository repository
	parser     parser
	lsn        uint64
	errChannel chan error
}

var (
	publishedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "published_events",
		Help: "The total number of published events",
	},
		[]string{"subject", "table"},
	)

	filterSkippedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "filter_skipped_events",
		Help: "The total number of skipped events",
	},
		[]string{"table"},
	)
)

// NewWalListener create and initialize new service instance.
func NewWalListener(
	cfg *config.Config,
	log *slog.Logger,
	repo repository,
	repl replication,
	publ eventPublisher,
	parser parser,
) *Listener {
	return &Listener{
		log:        log,
		slotName:   cfg.Listener.SlotName,
		cfg:        cfg,
		publisher:  publ,
		repository: repo,
		replicator: repl,
		parser:     parser,
		errChannel: make(chan error, errorBufferSize),
	}
}

// Process is main service entry point.
func (l *Listener) Process(ctx context.Context) error {
	logger := l.log.With("slot_name", l.slotName)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	logger.Info("service was started")

	if err := l.repository.CreatePublication(publicationName); err != nil {
		logger.Warn("publication creation was skipped", "err", err)
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

		logger.Info("new slot was created", slog.String("slot", l.slotName))
	} else {
		logger.Info("slot already exists, LSN updated")
	}

	go l.Stream(ctx)

	refresh := time.NewTicker(l.cfg.Listener.RefreshConnection)
	defer refresh.Stop()

	var svcErr *serviceErr

ProcessLoop:
	for {
		select {
		case <-refresh.C:
			if !l.replicator.IsAlive() {
				return fmt.Errorf("replicator: %w", errReplConnectionIsLost)
			}

			if !l.repository.IsAlive() {
				return fmt.Errorf("repository: %w", errConnectionIsLost)
			}
		case err := <-l.errChannel:
			if errors.As(err, &svcErr) {
				return err
			}

			logger.Error("listener: received error", "err", err)
		case <-ctx.Done():
			logger.Debug("listener: context was canceled")

			if err := l.Stop(); err != nil {
				logger.Error("listener: stop error", "err", err)
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
		return false, fmt.Errorf("get slot lsn: %w", err)
	}

	if len(restartLSNStr) == 0 {
		l.log.Warn("restart LSN not found", slog.String("slot_name", l.slotName))
		return false, nil
	}

	lsn, err := pgx.ParseLSN(restartLSNStr)
	if err != nil {
		return false, fmt.Errorf("parse lsn: %w", err)
	}

	l.setLSN(lsn)

	return true, nil
}

const (
	protoVersion    = "proto_version '1'"
	publicationName = "wal-listener"
)

// Stream receive event from PostgreSQL.
// Accept message, apply filter and  publish it in NATS server.
func (l *Listener) Stream(ctx context.Context) {
	if err := l.replicator.StartReplication(
		l.slotName,
		l.readLSN(),
		-1,
		protoVersion,
		publicationNames(publicationName),
	); err != nil {
		l.errChannel <- newListenerError("StartReplication()", err)
		return
	}

	go l.SendPeriodicHeartbeats(ctx)

	tx := NewWalTransaction(l.log)

	for {
		if err := ctx.Err(); err != nil {
			l.errChannel <- newListenerError("stream: context canceled", err)
			break
		}

		msg, err := l.replicator.WaitForReplicationMessage(ctx)
		if err != nil {
			l.errChannel <- newListenerError("WaitForReplicationMessage", err)
			continue
		}

		if msg != nil {
			if msg.WalMessage != nil {
				l.log.Debug("receive wal message", slog.Uint64("wal", msg.WalMessage.WalStart))

				if err := l.parser.ParseWalMessage(msg.WalMessage.WalData, tx); err != nil {
					l.log.Error("message parse failed", "err", err)
					l.errChannel <- fmt.Errorf("parse wal message: %w", err)

					continue
				}

				if tx.CommitTime != nil {
					natsEvents := tx.CreateEventsWithFilter(l.cfg.Listener.Filter.Tables)

					for _, event := range natsEvents {
						subjectName := event.SubjectName(l.cfg)

						if err = l.publisher.Publish(subjectName, event); err != nil {
							l.errChannel <- fmt.Errorf("publish message: %w", err)
							continue
						}

						publishedEvents.With(prometheus.Labels{"subject": subjectName, "table": event.Table}).Inc()

						l.log.Info(
							"event was sent",
							slog.String("subject", subjectName),
							slog.String("action", event.Action),
							slog.String("table", event.Table),
							slog.Uint64("lsn", l.readLSN()),
						)
					}

					tx.Clear()
				}

				if msg.WalMessage.WalStart > l.readLSN() {
					if err = l.AckWalMessage(msg.WalMessage.WalStart); err != nil {
						l.errChannel <- fmt.Errorf("acknowledge wal message: %w", err)
						continue
					}

					l.log.Debug("ack wal msg", slog.Uint64("lsn", l.readLSN()))
				}
			}

			if msg.ServerHeartbeat != nil {
				l.log.Debug(
					"received server heartbeat",
					slog.Uint64("server_wal_end", msg.ServerHeartbeat.ServerWalEnd),
					slog.Uint64("server_time", msg.ServerHeartbeat.ServerTime),
				)

				if msg.ServerHeartbeat.ReplyRequested == 1 {
					l.log.Debug("status requested")

					if err = l.SendStandbyStatus(); err != nil {
						l.errChannel <- fmt.Errorf("send standby status: %w", err)
					}
				}
			}
		}
	}
}

func publicationNames(publication string) string {
	return fmt.Sprintf(`publication_names '%s'`, publication)
}

// Stop is a finalizer function.
func (l *Listener) Stop() error {
	if err := l.repository.Close(); err != nil {
		return fmt.Errorf("repository close: %w", err)
	}

	if err := l.replicator.Close(); err != nil {
		return fmt.Errorf("replicator close: %w", err)
	}

	l.log.Info("service was stopped")

	return nil
}

// SendPeriodicHeartbeats send periodic keep alive heartbeats to the server.
func (l *Listener) SendPeriodicHeartbeats(ctx context.Context) {
	heart := time.NewTicker(l.cfg.Listener.HeartbeatInterval)
	defer heart.Stop()

	for {
		select {
		case <-ctx.Done():
			l.log.Info("periodic heartbeats: context was canceled")
			return
		case <-heart.C:
			if err := l.SendStandbyStatus(); err != nil {
				l.log.Error("failed to send status heartbeat", "err", err)
				continue
			}

			l.log.Debug("sending periodic status heartbeat")
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
