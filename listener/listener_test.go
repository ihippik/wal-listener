package listener

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
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

	logger := logrus.New()
	logger.Out = io.Discard

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			w := &Listener{
				log:        logrus.NewEntry(logger),
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

	setReplClose := func(err error) {
		repl.On("Close").
			Return(err).
			Once()
	}

	logger := logrus.New()
	logger.Out = io.Discard

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
				log:        logrus.NewEntry(logger),
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
	repl := new(replicatorMock)
	type fields struct {
		restartLSN uint64
	}

	setSendStandbyStatus := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(status),
		).
			Return(err).
			Once()
	}

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
						ClientTime:       nowInNano(),
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
			name: "some err",
			setup: func() {
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 0,
						WalFlushPosition: 0,
						WalApplyPosition: 0,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					errSimple,
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
				lsn:        tt.fields.restartLSN,
			}
			if err := w.SendStandbyStatus(); (err != nil) != tt.wantErr {
				t.Errorf("SendStandbyStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
			repl.AssertExpectations(t)
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
	repl := new(replicatorMock)
	type fields struct {
		restartLSN uint64
	}
	type args struct {
		LSN uint64
	}

	setSendStandbyStatus := func(status *pgx.StandbyStatus, err error) {
		repl.On(
			"SendStandbyStatus",
			standByStatusMatcher(status),
		).
			Return(err).
			Once()
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
				setSendStandbyStatus(
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			w := &Listener{
				replicator: repl,
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

	setParseWalMessageOnce := func(msg []byte, tx *WalTransaction, err error) {
		prs.On("ParseWalMessage", msg, tx).Return(err).Once().
			After(10 * time.Millisecond)
	}

	setStartReplication := func(err error, slotName string, startLsn uint64, timeline int64, pluginArguments ...string) {
		repl.On(
			"StartReplication",
			slotName,
			startLsn,
			timeline,
			pluginArguments,
		).Return(err).Once().After(10 * time.Millisecond)
	}

	setWaitForReplicationMessage := func(msg *pgx.ReplicationMessage, err error) {
		repl.On(
			"WaitForReplicationMessage",
			mock.Anything,
		).Return(msg, err).Once().After(10 * time.Millisecond)
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
		).
			Return(err).After(10 * time.Millisecond)
	}

	setPublish := func(subject string, want publisher.Event, err error) {
		publ.On("Publish", subject, mock.MatchedBy(func(got publisher.Event) bool {
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
		})).Return(err).
			Once().
			After(10 * time.Millisecond)
	}

	uuid.SetRand(bytes.NewReader(make([]byte, 512)))

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
					protoVersion,
					"publication_names 'wal-listener'",
				)
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 0,
						WalFlushPosition: 0,
						WalApplyPosition: 0,
						ClientTime:       nowInNano(),
						ReplyRequested:   0,
					},
					nil,
				)
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
						HeartbeatInterval: 1,
						Filter: config.FilterStruct{
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
				timeout: 40 * time.Millisecond,
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
						HeartbeatInterval: 1, Filter: config.FilterStruct{
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
		},
		{
			name: "wait replication err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					protoVersion,
					"publication_names 'wal-listener'",
				)
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 0,
						WalFlushPosition: 0,
						WalApplyPosition: 0,
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
						HeartbeatInterval: 1, Filter: config.FilterStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &config.PublisherCfg{
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 20 * time.Millisecond,
			},
		},
		{
			name: "parse err",
			setup: func() {
				setStartReplication(
					nil,
					"myslot",
					uint64(0),
					int64(-1),
					protoVersion,
					"publication_names 'wal-listener'",
				)
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 0,
						WalFlushPosition: 0,
						WalApplyPosition: 0,
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
					nil,
				)
				setParseWalMessageOnce(
					[]byte(`some bytes`),
					&WalTransaction{
						LSN:           0,
						BeginTime:     nil,
						CommitTime:    nil,
						RelationStore: make(map[int32]RelationData),
						Actions:       nil,
					},
					errSimple,
				)
			},
			fields: fields{
				config: &config.Config{
					Listener: &config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 1, Filter: config.FilterStruct{
							Tables: map[string][]string{"users": {"insert"}},
						},
					},
					Publisher: &config.PublisherCfg{
						TopicPrefix: "pre_",
					},
				},
				slotName:   "myslot",
				restartLSN: 0,
			},
			args: args{
				timeout: 30 * time.Millisecond,
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
					protoVersion,
					"publication_names 'wal-listener'",
				)
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
				setSendStandbyStatus(
					&pgx.StandbyStatus{
						WalWritePosition: 0,
						WalFlushPosition: 0,
						WalApplyPosition: 0,
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
					nil,
				)
				setParseWalMessageOnce(
					[]byte(`some bytes`),
					&WalTransaction{
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
					errSimple,
				)
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
				config: &config.Config{
					Listener: &config.ListenerCfg{
						SlotName:          "myslot",
						AckTimeout:        0,
						HeartbeatInterval: 1, Filter: config.FilterStruct{
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
				timeout: 50 * time.Millisecond,
			},
		},
	}

	logger := logrus.New()
	logger.Out = io.Discard

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			ctx, cancel := context.WithTimeout(context.Background(), tt.args.timeout)
			w := &Listener{
				log:        logrus.NewEntry(logger),
				cfg:        tt.fields.config,
				slotName:   tt.fields.slotName,
				publisher:  publ,
				replicator: repl,
				repository: repo,
				parser:     prs,
				lsn:        tt.fields.restartLSN,
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
