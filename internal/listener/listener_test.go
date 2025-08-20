package listener

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	tx "github.com/ihippik/wal-listener/v2/internal/listener/transaction"
	"github.com/jackc/pgx"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ihippik/wal-listener/v2/internal/config"
)

var epochNano = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

var logger = slog.New(slog.NewJSONHandler(io.Discard, nil))

func TestListener_liveness(t *testing.T) {
	type fields struct {
		repo func(*testing.T) repository
		repl func(*testing.T) replication
	}

	tests := []struct {
		name   string
		fields fields

		expectedStatus int
		expectedBody   string
	}{
		{
			name: "success",
			fields: fields{
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("IsAlive").Return(true).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("IsAlive").Return(true).Once()
					return r
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "ok",
		},
		{
			name: "failed repo",
			fields: fields{
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("IsAlive").Return(false).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("IsAlive").Return(true).Once()
					return r
				},
			},
			expectedStatus: http.StatusInternalServerError,
			expectedBody:   "failed",
		},
		{
			name: "failed replication",
			fields: fields{
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("IsAlive").Return(false).Once()
					return r
				},
				repo: func(t *testing.T) repository {
					return newMockrepository(t)
				},
			},
			expectedStatus: http.StatusInternalServerError,
			expectedBody:   "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				log:        logger,
				repository: tt.fields.repo(t),
				replicator: tt.fields.repl(t),
			}

			req := httptest.NewRequest(http.MethodGet, "/live", nil)
			w := httptest.NewRecorder()

			l.liveness(w, req)

			res := w.Result()
			defer res.Body.Close()

			if res.StatusCode != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, res.StatusCode)
			}

			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}

			if string(body) != tt.expectedBody {
				t.Errorf("expected body %s, got %s", tt.expectedBody, string(body))
			}

			if got := res.Header.Get("Content-Type"); got != contentTypeTextPlain {
				t.Errorf("expected Content-Type %s, got %s", contentTypeTextPlain, got)
			}
		})
	}
}

func TestListener_readiness(t *testing.T) {
	tests := []struct {
		name           string
		isAlive        bool
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "success",
			isAlive:        true,
			expectedStatus: http.StatusOK,
			expectedBody:   "ok",
		},
		{
			name:           "failed",
			isAlive:        false,
			expectedStatus: http.StatusInternalServerError,
			expectedBody:   "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				log: logger,
			}

			l.isAlive.Store(tt.isAlive)

			req := httptest.NewRequest(http.MethodGet, "/ready", nil)
			w := httptest.NewRecorder()

			l.readiness(w, req)

			res := w.Result()
			defer res.Body.Close()

			if res.StatusCode != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, res.StatusCode)
			}

			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}

			if string(body) != tt.expectedBody {
				t.Errorf("expected body %s, got %s", tt.expectedBody, string(body))
			}

			if got := res.Header.Get("Content-Type"); got != contentTypeTextPlain {
				t.Errorf("expected Content-Type %s, got %s", contentTypeTextPlain, got)
			}
		})
	}
}

func TestListener_slotIsExists(t *testing.T) {
	type fields struct {
		slotName string
		repo     func(*testing.T) repository
	}

	tests := []struct {
		name    string
		fields  fields
		want    uint64
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "slot is exists",
			fields: fields{
				slotName: "myslot",
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("GetSlotLSN", mock.Anything, "myslot").Return("0/17843B8", nil).Once()
					return r
				},
			},
			want:    24658872,
			wantErr: require.NoError,
		},
		{
			name: "empty lsn",
			fields: fields{
				slotName: "myslot",
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("GetSlotLSN", mock.Anything, "myslot").Return("", nil).Once()
					return r
				},
			},
			want:    0,
			wantErr: require.NoError,
		},
		{
			name: "invalid lsn",
			fields: fields{
				slotName: "myslot",
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("GetSlotLSN", mock.Anything, "myslot").Return("invalid", nil).Once()
					return r
				},
			},
			want:    0,
			wantErr: require.Error,
		},
		{
			name: "repository error",
			fields: fields{
				slotName: "myslot",
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("GetSlotLSN", mock.Anything, "myslot").
						Return("", errors.New("some err")).
						Once()
					return r
				},
			},
			want:    0,
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				log: logger,
				cfg: &config.Config{Listener: &config.ListenerCfg{
					SlotName: tt.fields.slotName,
				}},
				repository: tt.fields.repo(t),
			}

			got, err := l.slotIsExists(context.Background())
			tt.wantErr(t, err)

			if got != tt.want {
				t.Errorf("slotIsExists() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestListener_Stop(t *testing.T) {
	type fields struct {
		monitor   func(*testing.T) monitor
		publisher func(*testing.T) eventPublisher
		repl      func(*testing.T) replication
		repo      func(*testing.T) repository
	}

	tests := []struct {
		name    string
		fields  fields
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "success",
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("Close").Return(nil).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("Close").Return(nil).Once()
					return r
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "repository error",
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("Close").Return(errors.New("repo err")).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					return newMockreplication(t)
				},
			},
			wantErr: func(t require.TestingT, err error, i ...any) {
				require.ErrorContains(t, err, "repository close: repo err")
			},
		},
		{
			name: "replication error",
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("Close").Return(nil).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("Close").Return(errors.New("replication err")).Once()
					return r
				},
			},
			wantErr: func(t require.TestingT, err error, i ...any) {
				require.ErrorContains(t, err, "replicator close: replication err")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				log:        logger,
				monitor:    tt.fields.monitor(t),
				publisher:  tt.fields.publisher(t),
				replicator: tt.fields.repl(t),
				repository: tt.fields.repo(t),
			}

			err := l.Stop()
			tt.wantErr(t, err)
		})
	}
}

func TestListener_SendStandbyStatus(t *testing.T) {
	type fields struct {
		restartLSN uint64
		repl       func(*testing.T) replication
		repo       func(*testing.T) repository
	}

	now := nowInNano()

	tests := []struct {
		name    string
		setup   func()
		fields  fields
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "success",
			fields: fields{
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("NewStandbyStatus", []uint64{10}).Return(
						&pgx.StandbyStatus{
							WalWritePosition: 10,
							WalFlushPosition: 10,
							WalApplyPosition: 10,
							ClientTime:       now,
							ReplyRequested:   0,
						},
						nil,
					).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("SendStandbyStatus", &pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       now,
						ReplyRequested:   0,
					}).Return(nil).Once()
					return r
				},
				restartLSN: 10,
			},
			wantErr: require.NoError,
		},
		{
			name: "some repo err",
			fields: fields{
				restartLSN: 10,
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("NewStandbyStatus", []uint64{10}).Return(
						&pgx.StandbyStatus{
							WalWritePosition: 10,
							WalFlushPosition: 10,
							WalApplyPosition: 10,
							ClientTime:       now,
							ReplyRequested:   0,
						},
						errors.New("repo err"),
					).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					return newMockreplication(t)
				},
			},
			wantErr: func(t require.TestingT, err error, i ...any) {
				require.ErrorContains(t, err, "unable to create StandbyStatus object: repo err")
			},
		},
		{
			name: "some replicator err",
			fields: fields{
				restartLSN: 10,
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("NewStandbyStatus", []uint64{10}).Return(
						&pgx.StandbyStatus{
							WalWritePosition: 10,
							WalFlushPosition: 10,
							WalApplyPosition: 10,
							ClientTime:       now,
							ReplyRequested:   0,
						},
						nil,
					).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("SendStandbyStatus", &pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       now,
						ReplyRequested:   0,
					}).Return(errors.New("replication err")).Once()
					return r
				},
			},
			wantErr: func(t require.TestingT, err error, i ...any) {
				require.ErrorContains(t, err, "unable to send StandbyStatus object: replication err")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				log:        logger,
				replicator: tt.fields.repl(t),
				repository: tt.fields.repo(t),
				lsn:        tt.fields.restartLSN,
			}

			err := l.SendStandbyStatus()
			tt.wantErr(t, err)
		})
	}
}

func TestListener_AckWalMessage(t *testing.T) {
	type fields struct {
		restartLSN uint64
		repl       func(*testing.T) replication
		repo       func(*testing.T) repository
	}

	type args struct {
		LSN uint64
	}

	now := nowInNano()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "success",
			fields: fields{
				restartLSN: 0,
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("NewStandbyStatus", []uint64{24658872}).Return(
						&pgx.StandbyStatus{
							WalWritePosition: 24658872,
							WalFlushPosition: 24658872,
							WalApplyPosition: 24658872,
							ClientTime:       now,
							ReplyRequested:   0,
						},
						nil,
					).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("SendStandbyStatus", &pgx.StandbyStatus{
						WalWritePosition: 24658872,
						WalFlushPosition: 24658872,
						WalApplyPosition: 24658872,
						ClientTime:       now,
						ReplyRequested:   0,
					}).Return(nil).Once()
					return r
				},
			},
			args: args{
				LSN: 24658872,
			},
			wantErr: require.NoError,
		},
		{
			name: "send status error",
			fields: fields{
				restartLSN: 0,
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("NewStandbyStatus", []uint64{24658872}).Return(
						&pgx.StandbyStatus{
							WalWritePosition: 24658872,
							WalFlushPosition: 24658872,
							WalApplyPosition: 24658872,
							ClientTime:       now,
							ReplyRequested:   0,
						},
						nil,
					).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("SendStandbyStatus", &pgx.StandbyStatus{
						WalWritePosition: 24658872,
						WalFlushPosition: 24658872,
						WalApplyPosition: 24658872,
						ClientTime:       now,
						ReplyRequested:   0,
					}).Return(errors.New("some err")).Once()
					return r
				},
			},
			args: args{
				LSN: 24658872,
			},
			wantErr: func(t require.TestingT, err error, i ...any) {
				require.ErrorContains(t, err, "send status: unable to send StandbyStatus object: some err")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Listener{
				log:        logger,
				replicator: tt.fields.repl(t),
				repository: tt.fields.repo(t),
				lsn:        tt.fields.restartLSN,
			}
			err := w.AckWalMessage(tt.args.LSN)
			tt.wantErr(t, err)
		})
	}
}

func TestListener_Process(t *testing.T) {
	type fields struct {
		monitor   func(*testing.T) monitor
		publisher func(*testing.T) eventPublisher
		repl      func(*testing.T) replication
		repo      func(*testing.T) repository
		parser    func(*testing.T) parser
	}

	ctx := context.Background()
	now := nowInNano()

	tests := []struct {
		name    string
		fields  fields
		cfg     *config.Config
		setup   func()
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "success: empty wal msg",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("IsReplicationActive", mock.Anything, "slot1").Return(false, nil).Once()
					r.On("NewStandbyStatus", []uint64{1099511628288}).Return(
						&pgx.StandbyStatus{
							WalWritePosition: 10,
							WalFlushPosition: 10,
							WalApplyPosition: 10,
							ClientTime:       now,
							ReplyRequested:   0,
						},
						nil,
					)
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(nil).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("100/200", nil).Once()
					r.On("IsAlive").Return(true)
					r.On("Close").Return(nil).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("SendStandbyStatus", &pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       now,
						ReplyRequested:   0,
					}).Return(errors.New("some err"))
					r.On(
						"StartReplication",
						"slot1",
						uint64(1099511628288),
						int64(-1),
						[]string{"proto_version '1'", "publication_names 'wal-listener'"},
					).Return(nil).Once()
					r.On("WaitForReplicationMessage", mock.Anything).Return(nil, nil)
					r.On("IsAlive").Return(true)
					r.On("Close").Return(nil).Once()

					return r
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "start replication failed",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: time.Second,
					HeartbeatInterval: time.Second,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("IsReplicationActive", mock.Anything, "slot1").Return(false, nil).Once()
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(nil).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("100/200", nil).Once()
					r.On("Close").Return(nil).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On(
						"StartReplication",
						"slot1",
						uint64(1099511628288),
						int64(-1),
						[]string{"proto_version '1'", "publication_names 'wal-listener'"},
					).Return(errors.New("some err")).Once()
					r.On("Close").Return(nil).Once()

					return r
				},
			},
			wantErr: require.Error,
		},
		{
			name: "wait for replication message failed",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: time.Second,
					HeartbeatInterval: time.Second,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("IsReplicationActive", mock.Anything, "slot1").Return(false, nil).Once()
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(nil).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("100/200", nil).Once()
					r.On("Close").Return(nil).Once()
					return r
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On(
						"StartReplication",
						"slot1",
						uint64(1099511628288),
						int64(-1),
						[]string{"proto_version '1'", "publication_names 'wal-listener'"},
					).Return(nil).Once()
					r.On("WaitForReplicationMessage", mock.Anything).
						Return(nil, errors.New("some err")).Once()
					r.On("Close").Return(nil).Once()

					return r
				},
			},
			wantErr: require.Error,
		},
		{
			name: "skip create publication",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: time.Second,
					HeartbeatInterval: 1,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On(
						"StartReplication",
						"slot1",
						uint64(1099511628288),
						int64(-1),
						[]string{"proto_version '1'", "publication_names 'wal-listener'"},
					).Return(nil).Once()
					r.On("WaitForReplicationMessage", mock.Anything).Return(nil, nil)
					r.On("SendStandbyStatus", &pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       now,
						ReplyRequested:   0,
					}).Return(nil)
					r.On("Close").Return(nil)

					return r
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(errors.New("some err")).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("100/200", nil).Once()
					r.On("IsReplicationActive", mock.Anything, "slot1").Return(false, nil).Once()
					r.On("NewStandbyStatus", []uint64{1099511628288}).Return(
						&pgx.StandbyStatus{
							WalWritePosition: 10,
							WalFlushPosition: 10,
							WalApplyPosition: 10,
							ClientTime:       now,
							ReplyRequested:   0,
						},
						nil,
					)
					r.On("Close").Return(nil).Once()

					return r
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "get slot error",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: 1,
					HeartbeatInterval: 2,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repl: func(t *testing.T) replication {
					return newMockreplication(t)
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(nil).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("", errors.New("some err")).Once()
					return r
				},
			},
			wantErr: func(t require.TestingT, err error, i ...any) {
				require.ErrorContains(t, err, "slot is exists: get slot lsn: some err")
			},
		},
		{
			name: "slot does not exists",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: time.Second,
					HeartbeatInterval: time.Second,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On(
						"StartReplication",
						"slot1",
						uint64(1099511628288),
						int64(-1),
						[]string{"proto_version '1'", "publication_names 'wal-listener'"},
					).Return(nil).Once()
					r.On("WaitForReplicationMessage", mock.Anything).Return(nil, nil)
					r.On("CreateReplicationSlotEx", "slot1", "pgoutput").Return("100/200", "", nil).Once()
					r.On("Close").Return(nil).Once()

					return r
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(nil).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("", nil).Once()
					r.On("IsReplicationActive", mock.Anything, "slot1").Return(false, nil).Once()
					r.On("Close").Return(nil).Once()

					return r
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			wantErr: require.NoError,
		},
		{
			name: "create slot failed",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: time.Second,
					HeartbeatInterval: time.Second,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("CreateReplicationSlotEx", "slot1", "pgoutput").
						Return("", "", errors.New("some err")).Once()

					return r
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(nil).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("", nil).Once()

					return r
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			wantErr: require.Error,
		},
		{
			name: "check replication active failed",
			cfg: &config.Config{
				Listener: &config.ListenerCfg{
					SlotName:          "slot1",
					AckTimeout:        0,
					RefreshConnection: time.Second,
					HeartbeatInterval: time.Second,
					Filter: config.FilterStruct{
						Tables: nil,
					},
					TopicsMap: nil,
				},
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("CreateReplicationSlotEx", "slot1", "pgoutput").
						Return("100/200", "", nil).Once()

					return r
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("CreatePublication", mock.Anything, "wal-listener").Return(nil).Once()
					r.On("GetSlotLSN", mock.Anything, "slot1").Return("", nil).Once()
					r.On("IsReplicationActive", mock.Anything, "slot1").
						Return(false, errors.New("some err")).Once()

					return r
				},
			},
			setup: func() {
				ctx, _ = context.WithTimeout(context.Background(), time.Millisecond*100)
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			l := NewWalListener(
				tt.cfg,
				logger,
				tt.fields.repo(t),
				tt.fields.repl(t),
				tt.fields.publisher(t),
				tt.fields.parser(t),
				tt.fields.monitor(t),
			)

			err := l.Process(ctx)
			tt.wantErr(t, err)
		})
	}
}

func nowInNano() uint64 {
	return uint64(time.Now().UnixNano()-epochNano) / uint64(1000)
}

func TestListener_processMessage(t *testing.T) {
	type fields struct {
		cfg       *config.Config
		monitor   func(*testing.T) monitor
		publisher func(*testing.T) eventPublisher
		repl      func(*testing.T) replication
		repo      func(*testing.T) repository
		parser    func(*testing.T) parser
	}

	type args struct {
		msg *pgx.ReplicationMessage
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "empty message",
			args: args{
				msg: &pgx.ReplicationMessage{
					WalMessage:      nil,
					ServerHeartbeat: nil,
				},
			},
			fields: fields{
				monitor: func(t *testing.T) monitor {
					return newMockmonitor(t)
				},
				repl: func(t *testing.T) replication {
					return newMockreplication(t)
				},
				repo: func(t *testing.T) repository {
					return newMockrepository(t)
				},
				parser: func(t *testing.T) parser {
					return newMockparser(t)
				},
				publisher: func(t *testing.T) eventPublisher {
					return newMockeventPublisher(t)
				},
				cfg: &config.Config{
					Listener: &config.ListenerCfg{
						SlotName:          "slot1",
						AckTimeout:        0,
						RefreshConnection: time.Second,
						HeartbeatInterval: time.Second,
						Filter: config.FilterStruct{
							Tables: nil,
						},
					},
				},
			},
			wantErr: require.NoError,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txWAL := tx.NewWAL(logger, nil, tt.fields.monitor(t))

			l := &Listener{
				cfg:        tt.fields.cfg,
				log:        logger,
				monitor:    tt.fields.monitor(t),
				publisher:  tt.fields.publisher(t),
				replicator: tt.fields.repl(t),
				repository: tt.fields.repo(t),
				parser:     tt.fields.parser(t),
			}

			err := l.processMessage(ctx, tt.args.msg, txWAL)
			tt.wantErr(t, err)
		})
	}
}

func TestListener_processHeartBeat(t *testing.T) {
	type fields struct {
		cfg  *config.Config
		repl func(*testing.T) replication
		repo func(*testing.T) repository
	}

	type args struct {
		msg *pgx.ReplicationMessage
	}

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "empty msg",
			args: args{
				msg: &pgx.ReplicationMessage{
					WalMessage:      nil,
					ServerHeartbeat: nil,
				},
			},
			fields: fields{
				repl: func(t *testing.T) replication {
					return newMockreplication(t)
				},
				repo: func(t *testing.T) repository {
					return newMockrepository(t)
				},
			},
		},
		{
			name: "reply",
			args: args{
				msg: &pgx.ReplicationMessage{
					WalMessage: &pgx.WalMessage{},
					ServerHeartbeat: &pgx.ServerHeartbeat{
						ServerWalEnd:   10,
						ServerTime:     20,
						ReplyRequested: 1,
					},
				},
			},
			fields: fields{
				repl: func(t *testing.T) replication {
					r := newMockreplication(t)
					r.On("SendStandbyStatus", &pgx.StandbyStatus{
						WalWritePosition: 10,
						WalFlushPosition: 10,
						WalApplyPosition: 10,
						ClientTime:       200,
						ReplyRequested:   0,
					}).Return(nil).Once()
					return r
				},
				repo: func(t *testing.T) repository {
					r := newMockrepository(t)
					r.On("NewStandbyStatus", []uint64{10}).
						Return(
							&pgx.StandbyStatus{
								WalWritePosition: 10,
								WalFlushPosition: 10,
								WalApplyPosition: 10,
								ClientTime:       200,
								ReplyRequested:   0,
							},
							nil,
						).Once()
					return r
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{
				cfg:        tt.fields.cfg,
				log:        logger,
				replicator: tt.fields.repl(t),
				repository: tt.fields.repo(t),
			}

			l.processHeartBeat(tt.args.msg)
		})
	}
}
