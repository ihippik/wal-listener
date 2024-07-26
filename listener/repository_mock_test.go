package listener

import (
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
)

type repositoryMock struct {
	mock.Mock
}

func (r *repositoryMock) GetSlotLSN(slotName string) (string, error) {
	args := r.Called(slotName)
	return args.Get(0).(string), args.Error(1)
}

func (r *repositoryMock) IsAlive() bool {
	return r.Called().Bool(0)
}

func (r *repositoryMock) Close() error {
	return r.Called().Error(0)
}

func (r *repositoryMock) CreatePublication(name string) (err error) {
	args := r.Called(name)
	return args.Error(0)
}

func (r *repositoryMock) NewStandbyStatus(walPositions ...uint64) (status *pgx.StandbyStatus, err error) {
	args := r.Called(walPositions)
	return args.Get(0).(*pgx.StandbyStatus), args.Error(1)
}
