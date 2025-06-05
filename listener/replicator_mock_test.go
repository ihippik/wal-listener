package listener

import (
	"context"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/stretchr/testify/mock"
)

type replicatorMock struct {
	mock.Mock
}

func (r *replicatorMock) IdentifySystem() (pglogrepl.IdentifySystemResult, error) {
	args := r.Called()
	return args.Get(0).(pglogrepl.IdentifySystemResult), args.Error(1)
}

func (r *replicatorMock) CreateReplicationSlotEx(slotName, outputPlugin string) error {
	args := r.Called(slotName, outputPlugin)
	return args.Error(0)
}

func (r *replicatorMock) DropReplicationSlot(slotName string) (err error) {
	args := r.Called(slotName)
	return args.Error(1)
}

func (r *replicatorMock) StartReplication(
	slotName string,
	startLsn pglogrepl.LSN,
	pluginArguments ...string,
) error {
	args := r.Called(slotName, startLsn, pluginArguments)
	return args.Error(0)
}

func (r *replicatorMock) WaitForReplicationMessage(ctx context.Context) (*pgproto3.CopyData, error) {
	args := r.Called(ctx)
	return args.Get(0).(*pgproto3.CopyData), args.Error(1)
}

func (r *replicatorMock) SendStandbyStatus(ctx context.Context, lsn pglogrepl.LSN, withReply bool) (err error) {
	return r.Called(lsn).Error(0)
}

func (r *replicatorMock) IsAlive() bool {
	return r.Called().Bool(0)
}

func (r *replicatorMock) Close(ctx context.Context) error {
	return r.Called().Error(0)
}
