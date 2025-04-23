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

	"github.com/jackc/pgx"
	"golang.org/x/sync/errgroup"

	"github.com/ihippik/wal-listener/v2/internal/config"
	tx "github.com/ihippik/wal-listener/v2/internal/listener/transaction"
	"github.com/ihippik/wal-listener/v2/internal/publisher"
)

// Logical decoding plugin.
const pgOutputPlugin = "pgoutput"

type eventPublisher interface {
	Publish(context.Context, string, *publisher.Event) error
}

type parser interface {
	ParseWalMessage([]byte, *tx.WAL) error
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
	CreatePublication(ctx context.Context, name string) error
	GetSlotLSN(ctx context.Context, slotName string) (string, error)
	NewStandbyStatus(walPositions ...uint64) (status *pgx.StandbyStatus, err error)
	IsReplicationActive(ctx context.Context, slotName string) (bool, error)
	IsAlive() bool
	Close() error
}

type monitor interface {
	IncPublishedEvents(subject, table string)
	IncFilterSkippedEvents(table string)
	IncProblematicEvents(kind string)
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
	lsn        uint64
	isAlive    atomic.Bool
}

var (
	errReplConnectionIsLost = errors.New("replication connection to postgres is lost")
	errConnectionIsLost     = errors.New("db connection to postgres is lost")
	errReplDidNotStart      = errors.New("replication did not start")
)

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
	}
}

// InitHandlers init web handlers for liveness and readiness k8s probes.
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

func (l *Listener) liveness(w http.ResponseWriter, _ *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if !l.replicator.IsAlive() || !l.repository.IsAlive() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("liveness probe failed")
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("liveness: error writing response", "err", err)
	}
}

func (l *Listener) readiness(w http.ResponseWriter, _ *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if !l.isAlive.Load() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("readiness probe failed")
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("liveness: error writing response", "err", err)
	}
}

// Process is the main service entry point.
func (l *Listener) Process(ctx context.Context) error {
	logger := l.log.With("slot_name", l.cfg.Listener.SlotName)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	logger.Info("service was started")

	if err := l.repository.CreatePublication(ctx, publicationName); err != nil {
		logger.Warn("publication creation was skipped", "err", err)
	}

	slotIsExists, err := l.slotIsExists(ctx)
	if err != nil {
		return fmt.Errorf("slot is exists: %w", err)
	}

	if !slotIsExists {
		consistentPoint, _, err := l.replicator.CreateReplicationSlotEx(l.cfg.Listener.SlotName, pgOutputPlugin)
		if err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}

		lsn, err := pgx.ParseLSN(consistentPoint)
		if err != nil {
			return fmt.Errorf("parse lsn: %w", err)
		}

		l.setLSN(lsn)

		logger.Info("new slot was created", slog.String("slot", l.cfg.Listener.SlotName))
	} else {
		logger.Info("slot already exists, LSN updated")
	}

	if replicationActive, err := l.repository.IsReplicationActive(ctx, l.cfg.Listener.SlotName); err != nil || replicationActive {
		l.log.Error(
			"replication seems to already be alive or unable to check it",
			slog.String("slot", l.cfg.Listener.SlotName),
		)

		return errReplDidNotStart
	}

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return l.Stream(ctx)
	})
	group.Go(func() error {
		return l.checkConnection(ctx)
	})

	if err = group.Wait(); err != nil {
		return fmt.Errorf("group: %w", err)
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

			if err := l.Stop(); err != nil {
				l.log.Error("failed to stop service", "err", err)
			}

			return nil
		}
	}
}

// slotIsExists checks whether a slot has already been created and if it has been created uses it.
func (l *Listener) slotIsExists(ctx context.Context) (bool, error) {
	restartLSNStr, err := l.repository.GetSlotLSN(ctx, l.cfg.Listener.SlotName)
	if err != nil {
		return false, fmt.Errorf("get slot lsn: %w", err)
	}

	if len(restartLSNStr) == 0 {
		l.log.Warn("restart LSN not found", slog.String("slot_name", l.cfg.Listener.SlotName))
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

const (
	problemKindParse   = "parse"
	problemKindPublish = "publish"
	problemKindAck     = "ack"
)

// Stream receives event from PostgreSQL.
// Accept message, apply filter and publish it in NATS server.
func (l *Listener) Stream(ctx context.Context) error {
	if err := l.replicator.StartReplication(
		l.cfg.Listener.SlotName,
		l.readLSN(),
		-1,
		protoVersion,
		publicationNames(publicationName),
	); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	go l.SendPeriodicHeartbeats(ctx)

	pool := &sync.Pool{
		New: func() any {
			return &publisher.Event{}
		},
	}

	txWAL := tx.NewWAL(l.log, pool, l.monitor)

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
			l.log.Debug("got empty message")
			continue
		}

		if err = l.processMessage(ctx, msg, txWAL); err != nil {
			return fmt.Errorf("process message: %w", err)
		}

		l.processHeartBeat(msg)
	}
}

func (l *Listener) processMessage(ctx context.Context, msg *pgx.ReplicationMessage, txWAL *tx.WAL) error {
	if msg.WalMessage == nil {
		l.log.Debug("empty wal-message")
		return nil
	}

	l.log.Debug("WAL message has been received", slog.Uint64("wal", msg.WalMessage.WalStart))

	if err := l.parser.ParseWalMessage(msg.WalMessage.WalData, txWAL); err != nil {
		l.monitor.IncProblematicEvents(problemKindParse)
		return fmt.Errorf("parse: %w", err)
	}

	if txWAL.CommitTime != nil {
		for event := range txWAL.CreateEventsWithFilter(ctx, l.cfg.Listener.Filter.Tables) {
			subjectName := event.SubjectName(l.cfg)

			if err := l.publisher.Publish(ctx, subjectName, event); err != nil {
				l.monitor.IncProblematicEvents(problemKindPublish)
				return fmt.Errorf("publish: %w", err)
			}

			l.monitor.IncPublishedEvents(subjectName, event.Table)

			l.log.Info(
				"event was sent",
				slog.String("subject", subjectName),
				slog.String("action", event.Action),
				slog.String("table", event.Table),
				slog.Uint64("lsn", l.readLSN()),
			)

			txWAL.RetrieveEvent(event)
		}

		txWAL.Clear()
	}

	if msg.WalMessage.WalStart > l.readLSN() {
		if err := l.AckWalMessage(msg.WalMessage.WalStart); err != nil {
			l.monitor.IncProblematicEvents(problemKindAck)
			return fmt.Errorf("ack: %w", err)
		}

		l.log.Debug("ack WAL message", slog.Uint64("lsn", l.readLSN()))
	}

	return nil
}

func (l *Listener) processHeartBeat(msg *pgx.ReplicationMessage) {
	if msg.ServerHeartbeat == nil {
		l.log.Debug("empty server heartbeat message")
		return
	}

	l.log.Debug(
		"received server heartbeat",
		slog.Uint64("server_wal_end", msg.ServerHeartbeat.ServerWalEnd),
		slog.Uint64("server_time", msg.ServerHeartbeat.ServerTime),
	)

	if msg.ServerHeartbeat.ServerWalEnd > l.readLSN() {
		l.setLSN(msg.ServerHeartbeat.ServerWalEnd)
	}

	if msg.ServerHeartbeat.ReplyRequested == 1 {
		l.log.Debug("status requested")

		if err := l.SendStandbyStatus(); err != nil {
			l.log.Warn("send standby status", "err", err)
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

// SendPeriodicHeartbeats send periodic keep living heartbeats to the server.
func (l *Listener) SendPeriodicHeartbeats(ctx context.Context) {
	heart := time.NewTicker(l.cfg.Listener.HeartbeatInterval)
	defer heart.Stop()

	for {
		select {
		case <-ctx.Done():
			l.log.Warn("periodic heartbeats: context was canceled")
			return
		case <-heart.C:
			if err := l.SendStandbyStatus(); err != nil {
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
func (l *Listener) SendStandbyStatus() error {
	lsn := l.readLSN()

	standbyStatus, err := l.repository.NewStandbyStatus(lsn)
	if err != nil {
		return fmt.Errorf("unable to create StandbyStatus object: %w", err)
	}

	standbyStatus.ReplyRequested = 0

	if err = l.replicator.SendStandbyStatus(standbyStatus); err != nil {
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
