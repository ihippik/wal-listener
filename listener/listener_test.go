package listener

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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
	tests := []struct {
		name            string
		cfg             *config.Config
		setup           func(repo *repositoryMock, repl *replicatorMock)
		wantErr         error
		useTimeout      bool
		timeoutDuration time.Duration
	}{
		{
			name: "success",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					DropForeignOrigin: false,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func(repo *repositoryMock, repl *replicatorMock) {
				repl.On("IdentifySystem").Return(pglogrepl.IdentifySystemResult{}, nil)
				repo.On("CreatePublication", "wal-listener").Return(nil).Once()
				lsn := "100/200"
				repo.On("GetSlotLSN", "slot1").Return(&lsn, nil).Once()
				repl.On("StartReplication", "slot1", pglogrepl.LSN(1099511628288), []string{"proto_version '1'", "publication_names 'wal-listener'"}).Return(nil).Once()
				repl.On("IsAlive").Return(true).Maybe()
				repo.On("IsAlive").Return(true).Maybe()
				repl.On("WaitForReplicationMessage", mock.Anything).Return((*pgproto3.CopyData)(nil), nil).Maybe()
				repl.On("SendStandbyStatus", mock.Anything).Return(nil).Maybe()
				repl.On("Close").Return(nil).Maybe()
				repo.On("Close").Return(nil)
			},
			wantErr:         nil,
			useTimeout:      true,
			timeoutDuration: time.Second * 2,
		},
		{
			name: "skip create publication",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					DropForeignOrigin: false,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func(repo *repositoryMock, repl *replicatorMock) {
				repl.On("IdentifySystem").Return(pglogrepl.IdentifySystemResult{}, nil)
				repo.On("CreatePublication", "wal-listener").Return(errors.New("some err")).Once()
				lsn := "100/200"
				repo.On("GetSlotLSN", "slot1").Return(&lsn, nil).Once()
				repl.On("StartReplication", "slot1", pglogrepl.LSN(1099511628288), []string{"proto_version '1'", "publication_names 'wal-listener'"}).Return(nil).Once()
				repl.On("IsAlive").Return(true).Maybe()
				repo.On("IsAlive").Return(true).Maybe()
				repl.On("WaitForReplicationMessage", mock.Anything).Return((*pgproto3.CopyData)(nil), context.Canceled).Maybe()
				repl.On("SendStandbyStatus", mock.Anything).Return(nil).Maybe()
				repl.On("Close").Return(nil).Maybe()
				repo.On("Close").Return(nil)
			},
			wantErr:         nil,
			useTimeout:      true,
			timeoutDuration: time.Second * 2,
		},
		{
			name: "get slot error",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					DropForeignOrigin: false,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func(repo *repositoryMock, repl *replicatorMock) {
				repl.On("IdentifySystem").Return(pglogrepl.IdentifySystemResult{}, nil)
				repo.On("CreatePublication", "wal-listener").Return(nil).Once()
				lsn := "100/200"
				repo.On("GetSlotLSN", "slot1").Return(&lsn, errors.New("some err")).Once()
			},
			wantErr:    errors.New("slot is exists: get slot lsn: some err"),
			useTimeout: false,
		},
		{
			name: "slot does not exists",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					DropForeignOrigin: false,
					Include: config.IncludeStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func(repo *repositoryMock, repl *replicatorMock) {
				repl.On("IdentifySystem").Return(pglogrepl.IdentifySystemResult{XLogPos: 1099511628288}, nil)
				repo.On("CreatePublication", "wal-listener").Return(nil).Once()
				lsn := ""
				repo.On("GetSlotLSN", "slot1").Return(&lsn, pgx.ErrNoRows).Once()
				repl.On("CreateReplicationSlotEx", "slot1", "pgoutput").Return(nil)
				repl.On("StartReplication", "slot1", pglogrepl.LSN(1099511628288), []string{"proto_version '1'", "publication_names 'wal-listener'"}).Return(nil).Once()
				repl.On("IsAlive").Return(true).Maybe()
				repo.On("IsAlive").Return(true).Maybe()
				repl.On("WaitForReplicationMessage", mock.Anything).Return((*pgproto3.CopyData)(nil), context.Canceled).Maybe()
				repl.On("SendStandbyStatus", mock.Anything).Return(nil).Maybe()
				repl.On("Close").Return(nil).Maybe()
				repo.On("Close").Return(nil)
			},
			wantErr:         nil,
			useTimeout:      true,
			timeoutDuration: time.Second * 2,
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := new(repositoryMock)
			repl := new(replicatorMock)
			pub := new(publisherMock)
			parser := new(parserMock)
			monitor := new(monitorMock)
			
			defer repo.AssertExpectations(t)
			defer repl.AssertExpectations(t)

			tt.setup(repo, repl)

			l := NewWalListener(
				tt.cfg,
				logger,
				repo,
				repl,
				pub,
				parser,
				monitor,
			)

			var ctx context.Context
			var cancel context.CancelFunc

			if tt.useTimeout {
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeoutDuration)
				defer cancel()
			} else {
				ctx = context.Background()
			}

			err := l.Process(ctx)
			if tt.wantErr != nil {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				if tt.useTimeout && err != nil && strings.Contains(err.Error(), "context canceled") {
					assert.NoError(t, nil)
				} else {
					assert.NoError(t, err)
				}
			}
		})
	}
}
