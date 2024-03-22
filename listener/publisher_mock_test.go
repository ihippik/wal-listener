package listener

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/ihippik/wal-listener/v2/publisher"
)

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(ctx context.Context, subject string, event *publisher.Event) error {
	args := p.Called(ctx, subject, event)
	return args.Error(0)
}
