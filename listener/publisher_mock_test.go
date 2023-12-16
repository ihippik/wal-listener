package listener

import (
	"github.com/stretchr/testify/mock"

	"github.com/ihippik/wal-listener/v2/publisher"
)

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(subject string, event publisher.Event) error {
	args := p.Called(subject, event)
	return args.Error(0)
}
