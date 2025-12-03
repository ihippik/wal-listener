package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	retry "github.com/avast/retry-go/v4"
	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/sync/errgroup"
)

// Logical decoding plugin.
const pgOutputPlugin = "pgoutput"

type eventPublisher interface {
	Publish(context.Context, string, *publisher.Event) publisher.PublishResult
	Flush(string)
}

type parser interface {
	ParseWalMessage([]byte, *WalTransaction) error
}

type replication interface {
	CreateReplicationSlotEx(slotName, outputPlugin string) error
	DropReplicationSlot(slotName string) (err error)
	IdentifySystem() (pglogrepl.IdentifySystemResult, error)
	StartReplication(slotName string, startLsn pglogrepl.LSN, pluginArguments ...string) (err error)
	WaitForReplicationMessage(ctx context.Context) (*pgproto3.CopyData, error)
	SendStandbyStatus(ctx context.Context, lsn pglogrepl.LSN, withReply bool) (err error)
	IsAlive() bool
	Close(ctx context.Context) error
}

type repository interface {
	CreatePublication(ctx context.Context, name string) error
	GetSlotLSN(ctx context.Context, slotName string) (*string, error)
	GetSlotRetainedWALBytes(ctx context.Context, slotName string) (*int64, error)
	IsAlive() bool
	Close(ctx context.Context) error
}

type monitor interface {
	IncPublishedEvents(subject, table string)
	IncFilterSkippedEvents(table string)
	IncProblematicEvents(kind string)
}

type messageAndEvents struct {
	xld    *pglogrepl.XLogData
	pkm    *pglogrepl.PrimaryKeepaliveMessage
	events []*publisher.Event
}

type eventAndPublishResult struct {
	subjectName string
	event       *publisher.Event
	result      publisher.PublishResult
	// Only include the message on the final event created from it
	// we only ACK after the last message has been successfully published
	xld *pglogrepl.XLogData
	pkm *pglogrepl.PrimaryKeepaliveMessage
}

// Listener main service struct.
type Listener struct {
	cfg        *config.Config
	log        *slog.Logger
	monitor    monitor
	mu         sync.RWMutex
	publisher  eventPublisher
	replicator replication
	repository repository
	parser     parser
	lsn        pglogrepl.LSN
	isAlive    atomic.Bool
}

// NewWalListener create and initialize new service instance.
func NewWalListener(
	cfg *config.Config,
	log *slog.Logger,
	repo repository,
	repl replication,
	pub eventPublisher,
	parser parser,
	monitor monitor,
) *Listener {
	return &Listener{
		log:        log,
		monitor:    monitor,
		cfg:        cfg,
		publisher:  pub,
		repository: repo,
		replicator: repl,
		parser:     parser,
		isAlive:    atomic.Bool{},
		lsn:        0,
	}
}

// InitHandlers init web handlers for liveness & readiness k8s probes.
func (l *Listener) InitHandlers(ctx context.Context) {
	const defaultTimeout = 500 * time.Millisecond

	if l.cfg.Listener.ServerPort == 0 {
		l.log.Debug("web server port for probes not specified, skip")
		return
	}

	handler := http.NewServeMux()
	handler.HandleFunc("GET /healthz", l.liveness)
	handler.HandleFunc("GET /ready", l.readiness)

	addr := ":" + strconv.Itoa(l.cfg.Listener.ServerPort)
	srv := http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  defaultTimeout,
		WriteTimeout: defaultTimeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			l.log.Error("error starting http listener", "err", err)
		}
	}()

	l.log.Debug("web handlers were initialised", slog.String("addr", addr))

	<-ctx.Done()
}

const contentTypeTextPlain = "text/plain"

func (l *Listener) liveness(w http.ResponseWriter, r *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if !l.replicator.IsAlive() || !l.repository.IsAlive() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("liveness probe failed", slog.String("error", errReplConnectionIsLost.Error()))
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("liveness: error writing response", "err", err)
	}
}

func (l *Listener) readiness(w http.ResponseWriter, r *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if !l.isAlive.Load() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("readiness probe failed", slog.String("error", errConnectionIsLost.Error()))
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("liveness: error writing response", "err", err)
	}
}

// Process is main service entry point.
func (l *Listener) Process(ctx context.Context) error {
	logger := l.log.With("slot_name", l.cfg.Listener.SlotName)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	logger.Info("service was started",
		slog.String("version", "v0.2.0"),
		slog.String("slot_name", l.cfg.Listener.SlotName),
		slog.String("publisher_type", string(l.cfg.Publisher.Type)),
		slog.String("db_host", l.cfg.Database.Host),
		slog.Int("db_port", int(l.cfg.Database.Port)),
		slog.String("db_name", l.cfg.Database.Name),
		slog.Bool("drop_foreign_origin", l.cfg.Listener.DropForeignOrigin),
		slog.Bool("skip_transaction_buffering", l.cfg.Listener.SkipTransactionBuffering),
		slog.Int("max_transaction_size", l.cfg.Listener.MaxTransactionSize),
	)

	if err := l.repository.CreatePublication(ctx, publicationName); err != nil {
		logger.Warn("publication creation was skipped", "err", err)
	}

	ident, err := l.replicator.IdentifySystem()
	if err != nil {
		return fmt.Errorf("identify system: %w", err)
	}

	slotIsExists, err := l.slotIsExists(ctx)
	if err != nil {
		return fmt.Errorf("slot is exists: %w", err)
	}

	if !slotIsExists {
		if err = l.replicator.CreateReplicationSlotEx(l.cfg.Listener.SlotName, pgOutputPlugin); err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}

		l.setLSN(ident.XLogPos)
		logger.Info("new slot was created", slog.String("slot", l.cfg.Listener.SlotName), slog.String("lsn", ident.XLogPos.String()), slog.Uint64("lsn_uint64", uint64(ident.XLogPos)))
	} else {
		logger.Info("slot already exists, LSN updated")
	}

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return l.Stream(ctx)
	})
	group.Go(func() error {
		return l.checkConnection(ctx)
	})

	if err = group.Wait(); err != nil {
		return err
	}

	return nil
}

// checkConnection periodically checks connections.
func (l *Listener) checkConnection(ctx context.Context) error {
	refresh := time.NewTicker(l.cfg.Listener.RefreshConnection)
	defer refresh.Stop()

	for {
		select {
		case <-refresh.C:
			if !l.replicator.IsAlive() {
				return fmt.Errorf("replicator: %w", errReplConnectionIsLost)
			}

			if !l.repository.IsAlive() {
				return fmt.Errorf("repository: %w", errConnectionIsLost)
			}
		case <-ctx.Done():
			l.log.Debug("check connection: context was canceled")

			if err := l.Stop(ctx); err != nil {
				l.log.Error("failed to stop service", "err", err)
			}

			return nil
		}
	}
}

// slotIsExists checks whether a slot has already been created and if it has been created uses it.
func (l *Listener) slotIsExists(ctx context.Context) (bool, error) {
	restartLSNStr, err := l.repository.GetSlotLSN(ctx, l.cfg.Listener.SlotName)
	if errors.Is(err, pgx.ErrNoRows) {
		l.log.Info("slot does not exist", slog.String("slot_name", l.cfg.Listener.SlotName))
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("get slot lsn: %w", err)
	}

	if restartLSNStr == nil {
		l.log.Error("restart LSN is NULL, dropping replication slot", slog.String("slot_name", l.cfg.Listener.SlotName))
		err = l.replicator.DropReplicationSlot(l.cfg.Listener.SlotName)
		if err != nil {
			return false, fmt.Errorf("drop replication slot: %w", err)
		}
		return false, nil
	}

	lsn, err := pglogrepl.ParseLSN(*restartLSNStr)
	if err != nil {
		return false, fmt.Errorf("parse lsn: %w", err)
	}

	l.setLSN(lsn)
	l.log.Info("LSN set from existing slot", slog.String("lsn", lsn.String()), slog.Uint64("lsn_uint64", uint64(lsn)))

	return true, nil
}

const (
	protoVersion    = "proto_version '1'" // todo: 2 not supported in pg 13
	publicationName = "wal-listener"
)

const (
	problemKindParse   = "parse"
	problemKindPublish = "publish"
	problemKindAck     = "ack"
)

// Stream receive event from PostgreSQL.
// Accept message, apply filter and  publish it in NATS server.
func (l *Listener) Stream(ctx context.Context) error {
	currentLSN := l.readLSN()
	l.log.Info("Starting replication", slog.String("lsn", currentLSN.String()), slog.Uint64("lsn_uint64", uint64(currentLSN)))

	// Build plugin arguments for pgoutput
	pluginArgs := []string{
		protoVersion,
		publicationNames(publicationName),
	}

	// When dropForeignOrigin is enabled, use PostgreSQL's built-in origin filtering
	// by setting origin='none' on the pgoutput plugin. This tells PostgreSQL to only
	// stream changes that originated locally (no foreign origin).
	// Requires PostgreSQL 16+.
	if l.cfg.Listener.DropForeignOrigin {
		pluginArgs = append(pluginArgs, "origin 'none'")
	}

	if err := l.replicator.StartReplication(
		l.cfg.Listener.SlotName,
		currentLSN,
		pluginArgs...,
	); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	go l.SendPeriodicHeartbeats(ctx)

	pool := &sync.Pool{
		New: func() any {
			return &publisher.Event{}
		},
	}

	topicSet := make(map[string]bool)
	defer func() {
		for topic := range topicSet {
			l.publisher.Flush(topic)
		}
	}()

	tx := NewWalTransaction(l.log, pool, l.monitor, l.cfg.Listener.Include.Tables, l.cfg.Listener.Exclude, l.cfg.Tags, l.cfg.Listener.MaxTransactionSize)

	group, ctx := errgroup.WithContext(ctx)
	messageChan := make(chan *pgproto3.CopyData, 20_000)
	eventsChan := make(chan *messageAndEvents, 20_000)
	resultChan := make(chan *eventAndPublishResult, 20_000)

	defer close(messageChan)
	defer close(eventsChan)
	defer close(resultChan)

	group.Go(func() error {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case <-ticker.C:
				l.log.Info(
					"channel status",
					slog.Int("messageChan", len(messageChan)),
					slog.Int("eventsChan", len(eventsChan)),
					slog.Int("resultChan", len(resultChan)),
					slog.String("lsn", l.readLSN().String()),
				)
			}
		}
	})

	group.Go(func() error {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case <-ticker.C:
				l.logRetainedWalBytes(ctx)
			}
		}
	})

	group.Go(func() error {
		for {
			if err := ctx.Err(); err != nil {
				l.log.Warn("stream: context canceled", "err", err)
				return nil
			}

			msg, err := l.replicator.WaitForReplicationMessage(ctx)
			if err != nil {
				return fmt.Errorf("wait for replication message: %w", err)
			}

			if msg == nil {
				l.log.Debug("msg was nil")
				continue
			}

			messageChan <- msg
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case msg := <-messageChan:

				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err) // todo
					}
					l.log.Debug("Primary Keepalive Message", "ServerWALEnd", pkm.ServerWALEnd, "ServerTime", pkm.ServerTime, "ReplyRequested", pkm.ReplyRequested)

					eventsChan <- &messageAndEvents{
						pkm: &pkm,
					}

					l.processHeartBeat(ctx, &pkm)

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						return fmt.Errorf("ParseXLogData failed (data): %w", err)
					}

					err = l.parser.ParseWalMessage(xld.WALData, tx)
					if err != nil {
						l.monitor.IncProblematicEvents(problemKindParse)
						if errors.Is(err, errEmptyWALMessage) {
							l.log.Debug("warning: empty WAL message")
							continue
						}
						return fmt.Errorf("parse: %w", err)
					}

					// once the transaction is committed, or if we're in the no buffering mode, retrieve all the events and emit them to the publisher
					// buffering will be necessary for streaming messages, but if we're only on protocol version 1 and using async replication, all messages have been committed on the primary, so they're safe to emit as they are seen.
					// See https://www.postgresql.org/docs/current/warm-standby.html#SYNCHRONOUS-REPLICATION
					if l.cfg.Listener.SkipTransactionBuffering || tx.CommitTime != nil {
						events := tx.CreateEventsWithFilter(ctx)
						eventsChan <- &messageAndEvents{
							xld:    &xld,
							events: events,
						}

						// On commit, reset the entire transaction struct for re-use. Otherwise, we're in the unbuffered mode, so just clear the actions which we've emitted above.
						if tx.CommitTime != nil {
							tx.Clear()
						} else {
							tx.ClearActions()
						}
					}
				}
			}
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case object := <-eventsChan:
				events, xld, pkm := object.events, object.xld, object.pkm

				if len(events) == 0 {
					resultChan <- &eventAndPublishResult{
						subjectName: "",
						event:       nil,
						result:      nil,
						xld:         xld,
						pkm:         pkm,
					}
				}

				for idx, event := range events {
					subjectName := event.SubjectName(l.cfg)
					topicSet[subjectName] = true

					result := l.publisher.Publish(ctx, subjectName, event)
					l.log.Debug(
						"sending event",
						slog.String("subject", subjectName),
						slog.String("action", event.Action),
						slog.String("schema", event.Schema),
						slog.String("table", event.Table),
						slog.String("lsn", l.readLSN().String()),
					)

					// Include the message on the last event of the batch
					if idx == len(events)-1 {
						resultChan <- &eventAndPublishResult{
							subjectName: subjectName,
							event:       event,
							result:      result,
							xld:         xld,
							pkm:         pkm,
						}
					} else {
						resultChan <- &eventAndPublishResult{
							subjectName: subjectName,
							event:       event,
							result:      result,
							pkm:         pkm,
						}
					}
				}
			}
		}
	})

	var latestWalStart atomic.Uint64

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case object := <-resultChan:
				subjectName, result, event, xld, pkm := object.subjectName, object.result, object.event, object.xld, object.pkm

				if result != nil {
					_, err := result.Get(ctx)
					if err != nil {
						// Only warn on publish failures, but continue to process and ack messages
						// this runs the risk of losing data, but this it is more important to keep processing WAL messages in the meantime
						l.monitor.IncProblematicEvents(problemKindPublish)

						l.log.Warn(
							"failed to publish message",
							slog.Any("error", err),
							slog.String("subjectName", subjectName),
							slog.String("table", event.Table),
							slog.String("action", event.Action),
						)
					} else {
						l.monitor.IncPublishedEvents(subjectName, event.Table)
						l.log.Debug(
							"event was sent",
							slog.String("subject", subjectName),
							slog.String("action", event.Action),
							slog.String("schema", event.Schema),
							slog.String("table", event.Table),
							slog.String("lsn", l.readLSN().String()),
						)
					}
					tx.pool.Put(event)
				}

				latest := latestWalStart.Load()
				if pkm != nil {
					walEnd := uint64(pkm.ServerWALEnd)
					if walEnd > latest {
						latestWalStart.Store(walEnd)
					}
				} else if xld != nil {
					// We do not need to compare and swap here as there's only one thread
					// writing to this value
					walStart := uint64(xld.WALStart)
					if xld.WALData != nil && walStart > latest {
						latestWalStart.Store(walStart)
					}
				}
			}
		}
	})

	group.Go(func() error {
		for {
			timer := time.NewTimer(500 * time.Millisecond)

			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
				latest := latestWalStart.Load()
				if latest > uint64(l.readLSN()) {
					if err := l.AckWalMessage(ctx, pglogrepl.LSN(latest)); err != nil {
						l.monitor.IncProblematicEvents(problemKindAck)
						return fmt.Errorf("ack: %w", err)
					}

					l.log.Debug("ack WAL message", slog.String("lsn", l.readLSN().String()))
				}
			}
		}
	})

	return group.Wait()
}

func (l *Listener) processHeartBeat(ctx context.Context, pkm *pglogrepl.PrimaryKeepaliveMessage) {
	if pkm.ReplyRequested {
		l.log.Debug("status requested")

		if err := l.SendStandbyStatus(ctx); err != nil {
			l.log.Warn("send standby status", slog.String("error", err.Error()))
		}
	}
}

func publicationNames(publication string) string {
	return fmt.Sprintf(`publication_names '%s'`, publication)
}

// Stop is a finalizer function.
func (l *Listener) Stop(ctx context.Context) error {
	if err := l.repository.Close(ctx); err != nil {
		return fmt.Errorf("repository close: %w", err)
	}

	if err := l.replicator.Close(ctx); err != nil {
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
			l.log.Warn("periodic heartbeats: context was canceled")
			return
		case <-heart.C:
			if err := l.SendStandbyStatus(ctx); err != nil {
				l.log.Error("failed to send heartbeat status", "err", err)
				l.isAlive.Store(false)

				continue
			}

			l.isAlive.Store(true)
			l.log.Debug("sending periodic heartbeat status")
		}
	}
}

// SendStandbyStatus sends a `StandbyStatus` object with the current RestartLSN value to the server.
func (l *Listener) SendStandbyStatus(ctx context.Context) error {
	lsn := l.readLSN()

	return retry.Do(func() error {
		if err := l.replicator.SendStandbyStatus(ctx, lsn, false); err != nil {
			return fmt.Errorf("unable to send StandbyStatus object: %w", err)
		}

		return nil
	}, retry.Attempts(3))
}

// AckWalMessage acknowledge received wal message.
func (l *Listener) AckWalMessage(ctx context.Context, lsn pglogrepl.LSN) error {
	l.setLSN(lsn)

	if err := l.SendStandbyStatus(ctx); err != nil {
		return fmt.Errorf("send status: %w", err)
	}

	return nil
}

func (l *Listener) logRetainedWalBytes(ctx context.Context) {
	retainedWalBytes, err := l.repository.GetSlotRetainedWALBytes(ctx, l.cfg.Listener.SlotName)
	if err != nil || retainedWalBytes == nil {
		l.log.Error("failed to get retained WAL bytes", "err", err, slog.String("slot_name", l.cfg.Listener.SlotName))
		return
	}

	l.log.Info(
		"slot retained WAL bytes",
		slog.String("lsn", l.readLSN().String()),
		slog.String("slot_name", l.cfg.Listener.SlotName),
		slog.Int64("retained_wal_bytes", *retainedWalBytes),
	)
}

func (l *Listener) readLSN() pglogrepl.LSN {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lsn
}

func (l *Listener) setLSN(lsn pglogrepl.LSN) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lsn = lsn
}
