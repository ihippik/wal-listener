package listener

import (
	"context"
	"github.com/jackc/pgx"
	"time"

	"github.com/stretchr/testify/mock"

	trx "github.com/ihippik/wal-listener/v2/internal/listener/transaction"
	"github.com/ihippik/wal-listener/v2/internal/publisher"
)

type monitorMock struct{}

func (m *monitorMock) IncPublishedEvents(subject, table string) {}

func (m *monitorMock) IncFilterSkippedEvents(table string) {}

func (m *monitorMock) IncProblematicEvents(kind string) {}

type parserMock struct {
	mock.Mock
}

func (p *parserMock) ParseWalMessage(msg []byte, tx *trx.WAL) error {
	args := p.Called(msg, tx)
	now := time.Now()

	tx.BeginTime = &now
	tx.CommitTime = &now
	tx.Actions = []trx.ActionData{
		{
			Schema: "public",
			Table:  "users",
			Kind:   "INSERT",
			NewColumns: []trx.Column{
				trx.InitColumn(nil, "id", 1, 23, true),
			},
		},
	}

	return args.Error(0)
}

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(ctx context.Context, subject string, event *publisher.Event) error {
	args := p.Called(ctx, subject, event)
	return args.Error(0)
}

type replicatorMock struct {
	mock.Mock
}

func (r *replicatorMock) CreateReplicationSlotEx(slotName, outputPlugin string) (
	consistentPoint string,
	snapshotName string,
	err error,
) {
	args := r.Called(slotName, outputPlugin)
	return args.Get(0).(string), args.Get(1).(string), args.Error(2)
}

func (r *replicatorMock) DropReplicationSlot(slotName string) (err error) {
	args := r.Called(slotName)
	return args.Error(1)
}

func (r *replicatorMock) StartReplication(
	slotName string,
	startLsn uint64,
	timeline int64,
	pluginArguments ...string,
) (err error) {
	args := r.Called(slotName, startLsn, timeline, pluginArguments)
	return args.Error(0)
}

func (r *replicatorMock) WaitForReplicationMessage(ctx context.Context) (mess *pgx.ReplicationMessage, err error) {
	args := r.Called(ctx)
	return args.Get(0).(*pgx.ReplicationMessage), args.Error(1)
}

func (r *replicatorMock) SendStandbyStatus(status *pgx.StandbyStatus) (err error) {
	return r.Called(status).Error(0)
}

func (r *replicatorMock) IsAlive() bool {
	return r.Called().Bool(0)
}

func (r *replicatorMock) Close() error {
	return r.Called().Error(0)
}
