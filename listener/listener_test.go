package listener

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	ctx := context.Background()

	setGetSlotLSN := func(slotName, lsn string, err error) {
		repo.On("GetSlotLSN", slotName).
			Return(&lsn, err).
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
			wantErr: true,
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

			got, err := w.slotIsExists(ctx)
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

	ctx := context.Background()

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
			err := w.Stop(ctx)
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
		restartLSN pglogrepl.LSN
	}

	repl := new(replicatorMock)
	repo := new(repositoryMock)
	ctx := context.Background()

	setSendStandbyStatus := func(lsn pglogrepl.LSN, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(lsn),
		).
			Return(err).
			Once()
	}

	setSendStandbyStatusRetry := func(lsn pglogrepl.LSN, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(lsn),
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
				setSendStandbyStatus(
					pglogrepl.LSN(10),
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
				setSendStandbyStatusRetry(
					pglogrepl.LSN(10),
					errSimple,
				)
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

			if err := w.SendStandbyStatus(ctx); (err != nil) != tt.wantErr {
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

//func standByStatusMatcher(want *pgx.StandbyStatus) any {
//	return mock.MatchedBy(func(got *pgx.StandbyStatus) bool {
//		return want.ReplyRequested == got.ReplyRequested &&
//			want.WalApplyPosition == got.WalApplyPosition &&
//			want.WalFlushPosition == got.WalFlushPosition &&
//			want.WalWritePosition == got.WalWritePosition &&
//			abs(int64(got.ClientTime)-int64(want.ClientTime)) < 1000
//	})
//}

func standByStatusMatcher(want pglogrepl.LSN) any {
	return mock.MatchedBy(func(got pglogrepl.LSN) bool {
		return want == got
	})
}

func TestListener_AckWalMessage(t *testing.T) {
	type fields struct {
		restartLSN pglogrepl.LSN
	}

	type args struct {
		LSN pglogrepl.LSN
	}

	repl := new(replicatorMock)
	repo := new(repositoryMock)

	ctx := context.Background()

	setNewStandbyStatus := func(walPositions []uint64, lsn pglogrepl.LSN, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(lsn, err).After(10 * time.Millisecond).Once()
	}

	setNewStandbyStatusRetry := func(walPositions []uint64, lsn pglogrepl.LSN, err error) {
		repo.On("NewStandbyStatus", walPositions).Return(lsn, err).After(10 * time.Millisecond).Times(3)
	}

	setSendStandbyStatus := func(lsn pglogrepl.LSN, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(lsn),
		).
			Return(err).
			Once()
	}

	setSendStandbyStatusRetry := func(lsn pglogrepl.LSN, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(lsn),
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
				setNewStandbyStatus([]uint64{24658872}, pglogrepl.LSN(24658872), nil)

				setSendStandbyStatus(
					pglogrepl.LSN(24658872),
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
				setNewStandbyStatusRetry([]uint64{24658872}, pglogrepl.LSN(24658872), nil)

				setSendStandbyStatusRetry(
					pglogrepl.LSN(24658872),
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
			if err := w.AckWalMessage(ctx, tt.args.LSN); (err != nil) != tt.wantErr {
				t.Errorf("AckWalMessage() error = %v, wantErr %v", err, tt.wantErr)
			}

			repl.AssertExpectations(t)
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
		repo.On("GetSlotLSN", slotName).Return(&lsn, err).Once()
	}

	setStartReplication := func(
		err error,
		slotName string,
		startLsn pglogrepl.LSN,
		pluginArguments ...string,
	) {
		repl.On("StartReplication", slotName, startLsn, pluginArguments).Return(err).Once()
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

	setWaitForReplicationMessage := func(mess *pgproto3.CopyData, err error) {
		repl.On("WaitForReplicationMessage", mock.Anything).Return(mess, err)
	}

	setSendStandbyStatus := func(err error) {
		repl.On("SendStandbyStatus", mock.Anything).Return(err)
	}

	setCreateReplicationSlotEx := func(slotName, outputPlugin, consistentPoint, snapshotName string, err error) {
		repl.On("CreateReplicationSlotEx", slotName, outputPlugin).Return(consistentPoint, snapshotName, err)
	}

	setIdentifySystem := func(result pglogrepl.IdentifySystemResult, err error) {
		repl.On("IdentifySystem").Return(result, err)
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

				setIdentifySystem(pglogrepl.IdentifySystemResult{}, nil)
				setCreatePublication("wal-listener", nil)
				setGetSlotLSN("slot1", "100/200", nil)
				setStartReplication(
					nil,
					"slot1",
					1099511628288,
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
				setIdentifySystem(pglogrepl.IdentifySystemResult{}, nil)
				setCreatePublication("wal-listener", errors.New("some err"))
				setGetSlotLSN("slot1", "100/200", nil)
				setStartReplication(
					nil,
					"slot1",
					1099511628288,
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
				setIdentifySystem(pglogrepl.IdentifySystemResult{}, nil)
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
				setIdentifySystem(pglogrepl.IdentifySystemResult{XLogPos: 1099511628288}, nil)
				setCreatePublication("wal-listener", nil)
				setGetSlotLSN("slot1", "", pgx.ErrNoRows)
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
