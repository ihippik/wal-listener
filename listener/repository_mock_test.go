package listener

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type repositoryMock struct {
	mock.Mock
}

func (r *repositoryMock) GetSlotLSN(_ context.Context, slotName string) (*string, error) {
	args := r.Called(slotName)
	return args.Get(0).(*string), args.Error(1)
}

func (r *repositoryMock) GetSlotRetainedWALBytes(_ context.Context, slotName string) (*int64, error) {
	args := r.Called(slotName)
	return args.Get(0).(*int64), args.Error(1)
}

func (r *repositoryMock) IsAlive() bool {
	return r.Called().Bool(0)
}

func (r *repositoryMock) Close(_ context.Context) error {
	return r.Called().Error(0)
}

func (r *repositoryMock) CreatePublication(ctx context.Context, name string) error {
	args := r.Called(name)
	return args.Error(0)
}
