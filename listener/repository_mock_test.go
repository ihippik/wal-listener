package listener

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/stretchr/testify/mock"
)

type repositoryMock struct {
	mock.Mock
}

func (r *repositoryMock) GetSlotLSN(ctx context.Context, slotName string) (string, error) {
	args := r.Called(ctx, slotName)
	return args.Get(0).(string), args.Error(1)
}

func (r *repositoryMock) IsAlive() bool {
	return r.Called().Bool(0)
}

func (r *repositoryMock) Close() error {
	return r.Called().Error(0)
}

func (r *repositoryMock) CreatePublication(ctx context.Context, name string) (err error) {
	args := r.Called(ctx, name)
	return args.Error(0)
}

func (r *repositoryMock) NewStandbyStatus(walPositions ...uint64) (status *pgx.StandbyStatus, err error) {
	args := r.Called(walPositions)
	return args.Get(0).(*pgx.StandbyStatus), args.Error(1)
}

func (r *repositoryMock) IsReplicationActive(ctx context.Context, slotName string) (bool, error) {
	args := r.Called(ctx, slotName)
	return args.Bool(0), args.Error(1)
}
