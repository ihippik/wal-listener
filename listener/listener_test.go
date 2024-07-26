package listener

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
)

var (
	errSimple = errors.New("some err")
	epochNano = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
)

func TestListener_slotIsExists(t *testing.T) {
	type fields struct {
		slotName string
	}

	repo := new(repositoryMock)

	setGetSlotLSN := func(slotName, lsn string, err error) {
		repo.On("GetSlotLSN", slotName).
			Return(lsn, err).
			Once()
	}
	tests := []struct {
		name    string
		setup   func()
		fields  fields
		want    bool
		wantErr bool
	}{
		{
			name: "slot is exists",
			setup: func() {
				setGetSlotLSN("myslot", "0/17843B8", nil)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "empty lsn",
			setup: func() {
				setGetSlotLSN("myslot", "", nil)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "invalid lsn",
			setup: func() {
				setGetSlotLSN("myslot", "invalid", nil)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "repository error",
			setup: func() {
				setGetSlotLSN("myslot", "", errSimple)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: true,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			w := &Listener{
				log: logger,
				cfg: &config.Config{Listener: &config.ListenerCfg{
					SlotName: tt.fields.slotName,
				}},
				repository: repo,
			}

			got, err := w.slotIsExists()
			if (err != nil) != tt.wantErr {
				t.Errorf("slotIsExists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("slotIsExists() got = %v, want %v", got, tt.want)
			}

			repo.AssertExpectations(t)
		})
	}
}

func TestListener_Stop(t *testing.T) {
	repo := new(repositoryMock)
	publ := new(publisherMock)
	repl := new(replicatorMock)

	setRepoClose := func(err error) {
		repo.On("Close").
			Return(err).
			Once()
	}

	setReplClose := func(err error) {
		repl.On("Close").
			Return(err).
			Once()
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name    string
		setup   func()
		wantErr error
	}{
		{
			name: "success",
			setup: func() {
				setRepoClose(nil)
				setReplClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "repository error",
			setup: func() {
				setRepoClose(errors.New("repo err"))
			},
			wantErr: errors.New("repository close: repo err"),
		},
		{
			name: "replication error",
			setup: func() {
				setReplClose(errors.New("replication err"))
				setRepoClose(nil)
			},
			wantErr: errors.New("replicator close: replication err"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				log:        logger,
				publisher:  publ,
				replicator: repl,
				repository: repo,
			}
			err := w.Stop()
			if err != nil && assert.Error(t, tt.wantErr) {
				assert.EqualError(t, err, tt.wantErr.Error())
			}

			repo.AssertExpectations(t)
			repl.AssertExpectations(t)
			publ.AssertExpectations(t)
		})
	}
}

func nowInNano() uint64 {
	return uint64(time.Now().UnixNano()-epochNano) / uint64(1000)
}

func TestListener_SendStandbyStatus(t *testing.T) {
	type fields struct {
		restartLSN uint64
	}

	repl := new(replicatorMock)
	repo := new(repositoryMock)

	setNewStandbyStatus := func(walPositions []uint64, status *pgx.StandbyStatus, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(status, err).After(10 * time.Millisecond).Once()
	}

	setNewStandbyStatusRetry := func(walPositions []uint64, status *pgx.StandbyStatus, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(status, err).After(10 * time.Millisecond).Times(3)
	}

	setSendStandbyStatus := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(status),
		).
			Return(err).
			Once()
	}

	setSendStandbyStatusRetry := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(status),
		).
			Return(err).
			Times(3)
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name    string
		setup   func()
		fields  fields
		wantErr bool
	}{
		{
			name: "success",
			setup: func() {
				setNewStandbyStatus([]uint64{10}, &pgx.StandbyStatus{
					WalWritePosition: 10,
					WalFlushPosition: 10,
					WalApplyPosition: 10,
					ClientTime:       nowInNano(),
					ReplyRequested:   0,
				}, nil)

				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					nil,
				)
			},
			fields: fields{
				restartLSN: 10,
			},
			wantErr: false,
		},
		{
			name: "some replicator err",
			setup: func() {
				setNewStandbyStatusRetry([]uint64{10}, &pgx.StandbyStatus{
					WalWritePosition: 10,
					WalFlushPosition: 10,
					WalApplyPosition: 10,
					ClientTime:       nowInNano(),
					ReplyRequested:   0,
				}, nil)

				setSendStandbyStatusRetry(
					&pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					errSimple,
				)
			},
			fields: fields{
				restartLSN: 10,
			},
			wantErr: true,
		},
		{
			name: "some repo err",
			setup: func() {
				setNewStandbyStatusRetry([]uint64{10}, &pgx.StandbyStatus{
					WalWritePosition: 10,
					WalFlushPosition: 10,
					WalApplyPosition: 10,
					ClientTime:       nowInNano(),
					ReplyRequested:   0,
				}, errors.New("some err"))
			},
			fields: fields{
				restartLSN: 10,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer repl.AssertExpectations(t)
			defer repo.AssertExpectations(t)

			tt.setup()

			w := &Listener{
				log:        logger,
				replicator: repl,
				repository: repo,
				lsn:        tt.fields.restartLSN,
			}

			if err := w.SendStandbyStatus(); (err != nil) != tt.wantErr {
				t.Errorf("SendStandbyStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func abs(val int64) int64 {
	if val < 0 {
		return val * -1
	}
	return val
}

func standByStatusMatcher(want *pgx.StandbyStatus) any {
	return mock.MatchedBy(func(got *pgx.StandbyStatus) bool {
		return want.ReplyRequested == got.ReplyRequested &&
			want.WalApplyPosition == got.WalApplyPosition &&
			want.WalFlushPosition == got.WalFlushPosition &&
			want.WalWritePosition == got.WalWritePosition &&
			abs(int64(got.ClientTime)-int64(want.ClientTime)) < 1000
	})
}

func TestListener_AckWalMessage(t *testing.T) {
	type fields struct {
		restartLSN uint64
	}

	type args struct {
		LSN uint64
	}

	repl := new(replicatorMock)
	repo := new(repositoryMock)

	setNewStandbyStatus := func(walPositions []uint64, status *pgx.StandbyStatus, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(status, err).After(10 * time.Millisecond).Once()
	}

	setNewStandbyStatusRetry := func(walPositions []uint64, status *pgx.StandbyStatus, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(status, err).After(10 * time.Millisecond).Times(3)
	}

	setSendStandbyStatus := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(status),
		).
			Return(err).
			Once()
	}

	setSendStandbyStatusRetry := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(status),
		).
			Return(err).
			Times(3)
	}

	tests := []struct {
		name    string
		setup   func()
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "success",
			setup: func() {
				setNewStandbyStatus([]uint64{24658872}, &pgx.StandbyStatus{
					WalWritePosition: 24658872,
					WalFlushPosition: 24658872,
					WalApplyPosition: 24658872,
					ClientTime:       nowInNano(),
					ReplyRequested:   0,
				}, nil)

				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 24658872,
						WalFlushPosition: 24658872,
						WalApplyPosition: 24658872,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					nil,
				)
			},
			fields: fields{
				restartLSN: 0,
			},
			args: args{
				LSN: 24658872,
			},
			wantErr: false,
		},
		{
			name: "send status error",
			setup: func() {
				setNewStandbyStatusRetry([]uint64{24658872}, &pgx.StandbyStatus{
					WalWritePosition: 24658872,
					WalFlushPosition: 24658872,
					WalApplyPosition: 24658872,
					ClientTime:       nowInNano(),
					ReplyRequested:   0,
				}, nil)

				setSendStandbyStatusRetry(
					&pgx.StandbyStatus{
						WalWritePosition: 24658872,
						WalFlushPosition: 24658872,
						WalApplyPosition: 24658872,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					errSimple,
				)
			},
			fields: fields{
				restartLSN: 0,
			},
			args: args{
				LSN: 24658872,
			},
			wantErr: true,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				log:        logger,
				replicator: repl,
				repository: repo,
				lsn:        tt.fields.restartLSN,
			}
			if err := w.AckWalMessage(tt.args.LSN); (err != nil) != tt.wantErr {
				t.Errorf("AckWalMessage() error = %v, wantErr %v", err, tt.wantErr)
			}

			repl.AssertExpectations(t)
		})
	}
}

func TestListener_Stream(t *testing.T) {
	t.Skip() // FIXME

	repo := new(repositoryMock)
	publ := new(publisherMock)
	repl := new(replicatorMock)
	prs := new(parserMock)

	type fields struct {
		config     *config.Config
		slotName   string
		restartLSN uint64
	}

	type args struct {
		timeout time.Duration
	}

	setNewStandbyStatus := func(walPositions []uint64, status *pgx.StandbyStatus, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(status, err).After(10 * time.Millisecond)
	}

	setParseWalMessageOnce := func(msg []byte, tx *WalTransaction, err error) {
		prs.On("ParseWalMessage", msg, tx).Return(err)
	}

	setStartReplication := func(err error, slotName string, startLsn uint64, timeline int64, pluginArguments ...string) {
		repl.On(
			"StartReplication",
			slotName,
			startLsn,
			timeline,
			pluginArguments,
		).Return(err)
	}

	setWaitForReplicationMessage := func(msg *pgx.ReplicationMessage, err error) {
		repl.On(
			"WaitForReplicationMessage",
			mock.Anything,
		).Return(msg, err).After(10 * time.Millisecond)
	}

	setSendStandbyStatus := func(want *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			mock.MatchedBy(func(got *pgx.StandbyStatus) bool {
				return want.ReplyRequested == got.ReplyRequested &&
					want.WalFlushPosition == got.WalFlushPosition &&
					want.WalWritePosition == got.WalWritePosition &&
					abs(int64(want.ClientTime)-int64(got.ClientTime)) < 100000
			}),
		).Return(err).After(10 * time.Millisecond)
	}

	setPublish := func(subject string, want publisher.Event, err error) {
		publ.On("Publish", mock.Anything, subject, mock.MatchedBy(func(got publisher.Event) bool {
			ok := want.Action == got.Action &&
				reflect.DeepEqual(want.Data, got.Data) &&
				want.ID == got.ID &&
				want.Schema == got.Schema &&
				want.Table == got.Table &&
				want.EventTime.Sub(got.EventTime).Milliseconds() < 1000
			if !ok {
				t.Errorf("- want + got\n- %#+v\n+ %#+v", want, got)
			}
			return ok
		})).Return(err)
	}

	uuid.SetRand(bytes.NewReader(make([]byte, 512)))

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	metrics := new(monitorMock)

	tests := []struct {
		name    string
		setup   func()
		fields  fields
		args    args
		wantErr error
	}{
		{
			name: "success",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					protoVersion,
					"publication_names 'wal-listener'",
				)

				setNewStandbyStatus([]uint64{10}, &pgx.StandbyStatus{
					WalWritePosition: 10,
					WalFlushPosition: 10,
					WalApplyPosition: 10,
					ClientTime:       nowInNano(),
					ReplyRequested:   0,
				}, nil)

				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					nil,
				)

				setParseWalMessageOnce(
					[]byte(`some bytes`),
					&WalTransaction{
						monitor:       metrics,
						log:           logger,
						LSN:           0,
						BeginTime:     nil,
						CommitTime:    nil,
						RelationStore: make(map[int32]RelationData),
						Actions:       nil,
					},
					nil,
				)

				setPublish(
					"STREAM.pre_public_users",
					publisher.Event{
						ID:        uuid.MustParse("00000000-0000-4000-8000-000000000000"),
						Schema:    "public",
						Table:     "users",
						Action:    "INSERT",
						Data:      map[string]any{"id": 1},
						EventTime: time.Now(),
					},
					nil,
				)

				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{
						WalMessage: &pgx.WalMessage{
							WalStart:     10,
							ServerWalEnd: 0,
							ServerTime:   0,
							WalData:      []byte(`some bytes`),
						},
						ServerHeartbeat: &pgx.ServerHeartbeat{
							ServerWalEnd:   0,
							ServerTime:     0,
							ReplyRequested: 1,
						},
					},
					nil,
				)
			},
			fields: fields{
				config: &config.Config{
					Listener: &config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 5 * time.Millisecond,
						Include: config.IncludeStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &config.PublisherCfg{
						Topic:       "STREAM",
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 5 * time.Millisecond,
			},
		},
		{
			name: "start replication err",
			setup: func() {
				setStartReplication(
					errSimple,
					"myslot",
					uint64(0),
					int64(-1),
					protoVersion,
					"publication_names 'wal-listener'",
				)
			},
			fields: fields{
				config: &config.Config{
					Listener: &config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 1,
						Include: config.IncludeStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &config.PublisherCfg{
						Topic:       "stream",
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 100 * time.Microsecond,
			},
			wantErr: errors.New("start replication: some err"),
		},
		{
			name: "wait for replication message err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					protoVersion,
					"publication_names 'wal-listener'",
				)

				setNewStandbyStatus([]uint64{0}, &pgx.StandbyStatus{
					WalWritePosition: 10,
					WalFlushPosition: 10,
					WalApplyPosition: 10,
					ClientTime:       nowInNano(),
					ReplyRequested:   10,
				}, nil)

				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					nil,
				)

				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{
						WalMessage: &pgx.WalMessage{
							WalStart:     10,
							ServerWalEnd: 0,
							ServerTime:   0,
							WalData:      []byte(`some bytes`),
						},
						ServerHeartbeat: &pgx.ServerHeartbeat{
							ServerWalEnd:   0,
							ServerTime:     0,
							ReplyRequested: 1,
						},
					},
					errSimple,
				)
			},
			fields: fields{
				config: &config.Config{
					Listener: &config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 1,
						Include: config.IncludeStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &config.PublisherCfg{
						Topic:       "stream",
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 100 * time.Microsecond,
			},
			wantErr: errors.New("wait for replication message: some err"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer repl.AssertExpectations(t)

			tt.setup()

			ctx, _ := context.WithTimeout(context.Background(), tt.args.timeout)

			w := &Listener{
				log:        logger,
				monitor:    metrics,
				cfg:        tt.fields.config,
				publisher:  publ,
				replicator: repl,
				repository: repo,
				parser:     prs,
				lsn:        tt.fields.restartLSN,
			}

			if err := w.Stream(ctx); err != nil && assert.Error(t, tt.wantErr, err.Error()) {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}

			repl.ExpectedCalls = nil
		})
	}
}

func TestListener_Process(t *testing.T) {
	ctx := context.Background()
	monitor := new(monitorMock)
	parser := new(parserMock)
	repo := new(repositoryMock)
	repl := new(replicatorMock)
	pub := new(publisherMock)

	setCreatePublication := func(name string, err error) {
		repo.On("CreatePublication", name).Return(err).Once()
	}

	setGetSlotLSN := func(slotName string, lsn string, err error) {
		repo.On("GetSlotLSN", slotName).Return(lsn, err).Once()
	}

	setStartReplication := func(
		err error,
		slotName string,
		startLsn uint64,
		timeline int64,
		pluginArguments ...string) {
		repl.On("StartReplication", slotName, startLsn, timeline, pluginArguments).Return(err).Once()
	}

	setIsAlive := func(res bool) {
		repl.On("IsAlive").Return(res)
	}

	setClose := func(err error) {
		repl.On("Close").Return(err).Maybe()
	}

	setRepoClose := func(err error) {
		repo.On("Close").Return(err)
	}

	setRepoIsAlive := func(res bool) {
		repo.On("IsAlive").Return(res)
	}

	setWaitForReplicationMessage := func(mess *pgx.ReplicationMessage, err error) {
		repl.On("WaitForReplicationMessage", mock.Anything).Return(mess, err)
	}

	setSendStandbyStatus := func(err error) {
		repl.On("SendStandbyStatus", mock.Anything).Return(err)
	}

	setCreateReplicationSlotEx := func(slotName, outputPlugin, consistentPoint, snapshotName string, err error) {
		repl.On("CreateReplicationSlotEx", slotName, outputPlugin).Return(consistentPoint, snapshotName, err)
	}

	setNewStandbyStatus := func(walPositions []uint64, status *pgx.StandbyStatus, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(status, err).After(10 * time.Millisecond)
	}

	tests := []struct {
		name    string
		cfg     *config.Config
		setup   func()
		wantErr error
	}{
		{
			name: "success",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(ctx, time.Millisecond*200)

				setNewStandbyStatus([]uint64{1099511628288}, &pgx.StandbyStatus{
					WalWritePosition: 10,
					WalFlushPosition: 10,
					WalApplyPosition: 10,
					ClientTime:       nowInNano(),
					ReplyRequested:   0,
				}, nil)

				setCreatePublication("wal-listener", nil)
				setGetSlotLSN("slot1", "100/200", nil)
				setStartReplication(
					nil,
					"slot1",
					1099511628288,
					-1,
					"proto_version '1'",
					"publication_names 'wal-listener'",
				)
				setIsAlive(true)
				setRepoIsAlive(true)
				setWaitForReplicationMessage(nil, nil)
				setSendStandbyStatus(nil)
				setClose(nil)
				setRepoClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "skip create publication",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(ctx, time.Millisecond*20)
				setCreatePublication("wal-listener", errors.New("some err"))
				setGetSlotLSN("slot1", "100/200", nil)
				setStartReplication(
					nil,
					"slot1",
					1099511628288,
					-1,
					"proto_version '1'",
					"publication_names 'wal-listener'",
				)
				setIsAlive(true)
				setRepoIsAlive(true)
				setWaitForReplicationMessage(nil, nil)
				setSendStandbyStatus(nil)
				setClose(nil)
				setRepoClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "get slot error",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(ctx, time.Millisecond*20)
				setCreatePublication("wal-listener", nil)
				setGetSlotLSN("slot1", "100/200", errors.New("some err"))
			},
			wantErr: errors.New("slot is exists: get slot lsn: some err"),
		},
		{
			name: "slot does not exists",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(ctx, time.Millisecond*20)
				setCreatePublication("wal-listener", nil)
				setGetSlotLSN("slot1", "", nil)
				setCreateReplicationSlotEx(
					"slot1",
					"pgoutput",
					"100/200",
					"",
					nil,
				)
				setStartReplication(
					nil,
					"slot1",
					1099511628288,
					-1,
					"proto_version '1'",
					"publication_names 'wal-listener'",
				)
				setIsAlive(true)
				setRepoIsAlive(true)
				setWaitForReplicationMessage(nil, nil)
				setSendStandbyStatus(nil)
				setClose(nil)
				setRepoClose(nil)
			},
			wantErr: nil,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer repo.AssertExpectations(t)
			defer repl.AssertExpectations(t)

			tt.setup()

			l := NewWalListener(
				tt.cfg,
				logger,
				repo,
				repl,
				pub,
				parser,
				monitor,
			)

			err := l.Process(ctx)
			if err != nil && assert.Error(t, tt.wantErr, err.Error()) {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, tt.wantErr)
			}
		})
	}
}
