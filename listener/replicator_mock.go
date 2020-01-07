package listener

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/stretchr/testify/mock"
)

type replicatorMock struct {
	mock.Mock
}

func (r *replicatorMock) CreateReplicationSlotEx(slotName, outputPlugin string) (consistentPoint string, snapshotName string, err error) {
	panic("implement me")
}

func (r *replicatorMock) DropReplicationSlot(slotName string) (err error) {
	panic("implement me")
}

func (r *replicatorMock) StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error) {
	panic("implement me")
}

func (r *replicatorMock) WaitForReplicationMessage(ctx context.Context) (*pgx.ReplicationMessage, error) {
	panic("implement me")
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
