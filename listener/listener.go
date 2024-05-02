package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
	"github.com/jackc/pgx"
	"golang.org/x/sync/errgroup"
)

// Logical decoding plugin.
const pgOutputPlugin = "pgoutput"

type eventPublisher interface {
	Publish(context.Context, string, *publisher.Event) error
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
	NewStandbyStatus(walPositions ...uint64) (status *pgx.StandbyStatus, err error)
	IsAlive() bool
	Close() error
}

type monitor interface {
	IncPublishedEvents(subject, table string)
	IncFilterSkippedEvents(table string)
	IncProblematicEvents(kind string)
}

type ListenerState string

const (
	StateStartingUp   ListenerState = "starting_up"
	StateReady        ListenerState = "ready"
	StateShuttingDown ListenerState = "shutting_down"
)

// Listener main service struct.
type Listener struct {
	cfg               *config.Config
	log               *slog.Logger
	monitor           monitor
	mu                sync.RWMutex
	publisher         eventPublisher
	replicator        replication
	repository        repository
	parser            parser
	lsn               uint64
	state             ListenerState
	stateLock         sync.Mutex
	stateChangedCond  *sync.Cond
	healthCheckServer *http.Server
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
		state:      StateStartingUp,
	}
}

// Process is main service entry point.
func (l *Listener) Process(orgCtx context.Context) error {
	logger := l.log.With("slot_name", l.cfg.Listener.SlotName)

	ctx, cancelCtxFunc := context.WithCancel(orgCtx)

	logger.Info("service was started")

	l.stateLock = sync.Mutex{}
	l.stateChangedCond = sync.NewCond(&l.stateLock)

	group := new(errgroup.Group)
	if l.cfg.HealthCheck != nil {
		group.Go(func() error {
			logger.Info("Starting health check server")
			l.healthCheckServer = l.createHealthCheckServer(logger)
			return l.healthCheckServer.ListenAndServe()
		})
	}
	group.Go(func() error {
		// Do startup activities asynchronously, so that the app can immediately answer to http health checks
		// and say that is it alive but not yet ready.

		if err := l.repository.CreatePublication(publicationName); err != nil {
			logger.Warn("publication creation was skipped", "err", err)
		}

		slotExists, err := l.slotExists()
		if err != nil {
			return fmt.Errorf("slot exists: %w", err)
		}

		if !slotExists {
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

		l.changeState(StateReady)
		logger.Info("Startup activities done, listener is ready.")

		return nil
	})

	group.Go(func() error {
		l.waitForState(StateReady)
		logger.Info("Starting stream")
		return l.Stream(ctx)
	})
	group.Go(func() error {
		l.waitForState(StateReady)
		logger.Info("Starting connection checker")
		return l.checkConnection(ctx)
	})
	group.Go(func() error {
		shutdownCh := make(chan os.Signal, 1)
		signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)
		<-shutdownCh

		signal.Reset()

		logger.Info("Received shutdown signal, listener is shutting down")
		l.changeState(StateShuttingDown)

		cancelCtxFunc()

		if l.healthCheckServer == nil {
			return nil
		}

		logger.Info("Shutting down health check server")
		shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownRelease()

		return l.healthCheckServer.Shutdown(shutdownCtx)
	})

	err := group.Wait()
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		// This is the expected way for the process to shut down.
		logger.Info("Listener processes is done")
		return nil
	}
	return err
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

// slotExists checks whether a slot has already been created and if it has been created uses it.
func (l *Listener) slotExists() (bool, error) {
	restartLSNStr, err := l.repository.GetSlotLSN(l.cfg.Listener.SlotName)
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

// Stream receive event from PostgreSQL.
// Accept message, apply filter and  publish it in NATS server.
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

	tx := NewWalTransaction(l.log, pool, l.monitor)

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

		if err = l.processMessage(ctx, msg, tx); err != nil {
			return fmt.Errorf("process message: %w", err)
		}

		l.processHeartBeat(msg)
	}
}

func (l *Listener) processMessage(ctx context.Context, msg *pgx.ReplicationMessage, tx *WalTransaction) error {
	if msg.WalMessage == nil {
		l.log.Debug("empty wal-message")
		return nil
	}

	l.log.Debug("WAL message has been received", slog.Uint64("wal", msg.WalMessage.WalStart))

	if err := l.parser.ParseWalMessage(msg.WalMessage.WalData, tx); err != nil {
		l.monitor.IncProblematicEvents(problemKindParse)
		return fmt.Errorf("parse: %w", err)
	}

	if tx.CommitTime != nil {
		for event := range tx.CreateEventsWithFilter(ctx, l.cfg.Listener.Filter.Tables) {
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

			tx.pool.Put(event)
		}

		tx.Clear()
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
		return
	}

	l.log.Debug(
		"received server heartbeat",
		slog.Uint64("server_wal_end", msg.ServerHeartbeat.ServerWalEnd),
		slog.Uint64("server_time", msg.ServerHeartbeat.ServerTime),
	)

	if msg.ServerHeartbeat.ReplyRequested == 1 {
		l.log.Debug("status requested")

		if err := l.SendStandbyStatus(); err != nil {
			l.log.Warn("send standby status: %w", err)
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
			l.log.Warn("periodic heartbeats: context was canceled")
			return
		case <-heart.C:
			if err := l.SendStandbyStatus(); err != nil {
				l.log.Error("failed to send heartbeat status", "err", err)
				continue
			}

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

func (l *Listener) createHealthCheckServer(logger *slog.Logger) *http.Server {
	healthCheckAddr := fmt.Sprintf(":%d", l.cfg.HealthCheck.Port)

	logger.Info("Creating health check http server", "addr", healthCheckAddr)

	http.HandleFunc("/-/alive", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "OK")
	})
	http.HandleFunc("/-/ready", func(w http.ResponseWriter, r *http.Request) {
		switch l.state {
		case StateReady:
			w.WriteHeader(200)
			fmt.Fprint(w, "OK")
		case StateStartingUp:
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprint(w, "Starting up")
		case StateShuttingDown:
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprint(w, "Shutting down")
		default:
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "Unknown listener state")
		}
	})
	return &http.Server{
		Addr:        healthCheckAddr,
		Handler:     http.DefaultServeMux,
		ReadTimeout: time.Second * 5,
	}
}

func (l *Listener) changeState(newState ListenerState) {
	l.stateLock.Lock()
	defer l.stateLock.Unlock()
	l.state = newState
	l.stateChangedCond.Broadcast()
}

func (l *Listener) waitForState(wantedState ListenerState) {
	l.stateLock.Lock()
	defer l.stateLock.Unlock()
	for l.state != wantedState {
		l.stateChangedCond.Wait()
	}
}
