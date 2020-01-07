package listener

import "github.com/stretchr/testify/mock"

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
