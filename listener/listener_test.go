package listener

import (
	"context"
	"errors"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/jackc/pgx"
	"github.com/stretchr/testify/assert"

	"github.com/ihippik/wal-listener/config"
)

var someErr = errors.New("some err")

func TestListener_slotIsExists(t *testing.T) {
	repo := new(repositoryMock)
	type fields struct {
		slotName string
	}

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
			name: "slot not exists (no rows)",
			setup: func() {
				setGetSlotLSN("myslot", "", pgx.ErrNoRows)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "repository error",
			setup: func() {
				setGetSlotLSN("myslot", "", someErr)
			},
			fields: fields{
				slotName: "myslot",
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				slotName:   tt.fields.slotName,
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
	setPublClose := func(err error) {
		publ.On("Close").
			Return(err).
			Once()
	}
	setReplClose := func(err error) {
		repl.On("Close").
			Return(err).
			Once()
	}

	tests := []struct {
		name    string
		setup   func()
		wantErr error
	}{
		{
			name: "success",
			setup: func() {
				setRepoClose(nil)
				setPublClose(nil)
				setReplClose(nil)
			},
			wantErr: nil,
		},
		{
			name: "repository error",
			setup: func() {
				setPublClose(nil)
				setRepoClose(errors.New("repo err"))
			},
			wantErr: errors.New("repo err"),
		},
		{
			name: "publication error",
			setup: func() {
				setPublClose(errors.New("publication err"))
			},
			wantErr: errors.New("publication err"),
		},
		{
			name: "replication error",
			setup: func() {
				setReplClose(errors.New("replication err"))
				setRepoClose(nil)
				setPublClose(nil)
			},
			wantErr: errors.New("replication err"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				publisher:  publ,
				replicator: repl,
				repository: repo,
			}
			err := w.Stop()
			assert.Equal(t, tt.wantErr, err)

			repo.AssertExpectations(t)
			repl.AssertExpectations(t)
			publ.AssertExpectations(t)
		})
	}
}

func TestListener_SendStandbyStatus(t *testing.T) {
	repl := new(replicatorMock)
	type fields struct {
		status     *pgx.StandbyStatus
		restartLSN uint64
	}

	setSendStandbyStatus := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			status,
		).
			Return(err).
			Once()
	}
	wayback := time.Date(1974, time.May, 19, 1, 2, 3, 4, time.UTC)
	patch := monkey.Patch(time.Now, func() time.Time { return wayback })
	defer patch.Unpatch()

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
					&pgx.StandbyStatus{
						WalWritePosition: 0,
						WalFlushPosition: 0,
						WalApplyPosition: 0,
						ClientTime:       18445935546232551617,
						ReplyRequested:   0,
					},
					nil,
				)
			},
			fields: fields{
				restartLSN: 0,
			},
			wantErr: false,
		},
		{
			name: "success",
			setup: func() {
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 0,
						WalFlushPosition: 0,
						WalApplyPosition: 0,
						ClientTime:       18445935546232551617,
						ReplyRequested:   0,
					},
					someErr,
				)
			},
			fields: fields{
				restartLSN: 0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				replicator: repl,
				restartLSN: tt.fields.restartLSN,
			}
			if err := w.SendStandbyStatus(); (err != nil) != tt.wantErr {
				t.Errorf("SendStandbyStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
			repl.AssertExpectations(t)
		})
	}
}

func TestListener_AckWalMessage(t *testing.T) {
	repl := new(replicatorMock)
	type fields struct {
		restartLSN uint64
	}
	type args struct {
		restartLSNStr string
	}

	setSendStandbyStatus := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			status,
		).
			Return(err).
			Once()
	}
	wayback := time.Date(1974, time.May, 19, 1, 2, 3, 4, time.UTC)
	patch := monkey.Patch(time.Now, func() time.Time { return wayback })
	defer patch.Unpatch()

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
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 24658872,
						WalFlushPosition: 24658872,
						WalApplyPosition: 24658872,
						ClientTime:       18445935546232551617,
						ReplyRequested:   0,
					},
					nil,
				)
			},
			fields: fields{
				restartLSN: 0,
			},
			args: args{
				restartLSNStr: "0/17843B8",
			},
			wantErr: false,
		},
		{
			name: "send status error",
			setup: func() {
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 24658872,
						WalFlushPosition: 24658872,
						WalApplyPosition: 24658872,
						ClientTime:       18445935546232551617,
						ReplyRequested:   0,
					},
					someErr,
				)
			},
			fields: fields{
				restartLSN: 0,
			},
			args: args{
				restartLSNStr: "0/17843B8",
			},
			wantErr: true,
		},
		{
			name:  "invalid lsn",
			setup: func() {},
			fields: fields{
				restartLSN: 0,
			},
			args: args{
				restartLSNStr: "invalid",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				replicator: repl,
				restartLSN: tt.fields.restartLSN,
			}
			if err := w.AckWalMessage(tt.args.restartLSNStr); (err != nil) != tt.wantErr {
				t.Errorf("AckWalMessage() error = %v, wantErr %v", err, tt.wantErr)
			}

			repl.AssertExpectations(t)
		})
	}
}

func TestListener_Stream(t *testing.T) {
	repo := new(repositoryMock)
	publ := new(publisherMock)
	repl := new(replicatorMock)
	type fields struct {
		config     config.Config
		slotName   string
		restartLSN uint64
	}
	type args struct {
		timeout time.Duration
	}

	wayback := time.Date(1974, time.May, 19, 1, 2, 3, 4, time.UTC)
	patch := monkey.Patch(time.Now, func() time.Time { return wayback })
	defer patch.Unpatch()

	setSendStandbyStatus := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			status,
		).
			Return(err)
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
		).Return(msg, err)
	}

	setPublish := func(subject string, msg []byte, err error) {
		publ.On("Publish", subject, msg).Return(err)
	}

	tests := []struct {
		name   string
		setup  func()
		fields fields
		args   args
	}{
		{
			name: "success",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					pluginArgIncludeLSN,
				)
				setPublish(
					"pre_user_service_users",
					[]byte(`{"schema":"user_service","table":"users","action":"insert","data":{"k1":"v1"}}`),
					nil,
				)
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 24658872,
						WalFlushPosition: 24658872,
						WalApplyPosition: 24658872,
						ClientTime:       18445935546232551617,
						ReplyRequested:   0,
					},
					nil,
				)

				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{
						WalMessage: &pgx.WalMessage{
							WalStart:     0,
							ServerWalEnd: 0,
							ServerTime:   0,
							WalData:      []byte(`{"nextlsn":"0/17843B8","change":[{"kind":"insert","schema":"user_service","table":"users","columnnames":["k1"],"columnvalues":["v1"]}]}`),
						},
						ServerHeartbeat: &pgx.ServerHeartbeat{
							ServerWalEnd:   0,
							ServerTime:     0,
							ReplyRequested: 0,
						},
					},
					nil,
				)
			},
			fields: fields{
				config: config.Config{
					Listener: config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 0,
					},
					Database: config.DatabaseCfg{
						Filter: config.FilterStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Nats: config.NatsCfg{
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 10 * time.Millisecond,
			},
		},
		{
			name: "publish err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					pluginArgIncludeLSN,
				)
				setPublish(
					"pre_users",
					[]byte(`{"tableName":"users","action":"insert","data":{"k1":"v1"}}`),
					someErr,
				)

				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{
						WalMessage: &pgx.WalMessage{
							WalStart:     0,
							ServerWalEnd: 0,
							ServerTime:   0,
							WalData:      []byte(`{"nextLsn":"0","change":[{"kind":"insert","schema":"user_service","table":"users","columnnames":["k1"],"columnvalues":["v1"]}]}`),
						},
						ServerHeartbeat: &pgx.ServerHeartbeat{
							ServerWalEnd:   0,
							ServerTime:     0,
							ReplyRequested: 0,
						},
					},
					nil,
				)
			},
			fields: fields{
				config: config.Config{
					Listener: config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 0,
					},
					Database: config.DatabaseCfg{
						Filter: config.FilterStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Nats: config.NatsCfg{
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 10 * time.Millisecond,
			},
		},
		{
			name: "validate err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					pluginArgIncludeLSN,
				)

				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{
						WalMessage: &pgx.WalMessage{
							WalStart:     0,
							ServerWalEnd: 0,
							ServerTime:   0,
							WalData:      []byte(`{"nextLsn":"0","change":[{"columnnames":["v1","v2"]}]}`),
						},
						ServerHeartbeat: &pgx.ServerHeartbeat{
							ServerWalEnd:   0,
							ServerTime:     0,
							ReplyRequested: 0,
						},
					},
					nil,
				)
			},
			fields: fields{
				config: config.Config{
					Listener: config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						RefreshConnection: 0,
						HeartbeatInterval: 0,
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 10 * time.Millisecond,
			},
		},
		{
			name: "skip empty WAL",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					pluginArgIncludeLSN,
				)

				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{
						WalMessage: &pgx.WalMessage{
							WalStart:     0,
							ServerWalEnd: 0,
							ServerTime:   0,
							WalData:      []byte(`{"nextLsn":"0","change":[]}`),
						},
						ServerHeartbeat: &pgx.ServerHeartbeat{
							ServerWalEnd:   0,
							ServerTime:     0,
							ReplyRequested: 0,
						},
					},
					nil,
				)
			},
			fields: fields{
				config: config.Config{
					Listener: config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						RefreshConnection: 0,
						HeartbeatInterval: 0,
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 10 * time.Millisecond,
			},
		},
		{
			name: "message unmarshal err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					pluginArgIncludeLSN,
				)

				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{
						WalMessage: &pgx.WalMessage{
							WalStart:     0,
							ServerWalEnd: 0,
							ServerTime:   0,
							WalData:      nil,
						},
						ServerHeartbeat: &pgx.ServerHeartbeat{
							ServerWalEnd:   0,
							ServerTime:     0,
							ReplyRequested: 0,
						},
					},
					nil,
				)
			},
			fields: fields{
				config: config.Config{
					Listener: config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						RefreshConnection: 0,
						HeartbeatInterval: 0,
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 10 * time.Millisecond,
			},
		},
		{
			name: "start replication err",
			setup: func() {
				setStartReplication(
					someErr,
					"myslot",
					uint64(0),
					int64(-1),
					pluginArgIncludeLSN,
				)
			},
			fields: fields{
				config: config.Config{
					Listener: config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						RefreshConnection: 0,
						HeartbeatInterval: 0,
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 1 * time.Second,
			},
		},
		{
			name: "wait message err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					pluginArgIncludeLSN,
				)
				setWaitForReplicationMessage(
					&pgx.ReplicationMessage{},
					someErr,
				)
			},
			fields: fields{
				config: config.Config{
					Listener: config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						RefreshConnection: 0,
						HeartbeatInterval: 0,
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 1 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			ctx, cancel := context.WithTimeout(context.Background(), tt.args.timeout)
			w := &Listener{
				config:     tt.fields.config,
				slotName:   tt.fields.slotName,
				publisher:  publ,
				replicator: repl,
				repository: repo,
				restartLSN: tt.fields.restartLSN,
				errChannel: make(chan error, errorBufferSize),
			}
			go func() {
				<-w.errChannel
				cancel()

			}()
			w.Stream(ctx)
			repl.AssertExpectations(t)
			repl.ExpectedCalls = nil
		})
	}
}
