package listener

import (
	"context"
	"github.com/ihippik/wal-listener/v2/internal/publisher"

	"github.com/stretchr/testify/mock"
)

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(ctx context.Context, subject string, event *publisher.Event) error {
	args := p.Called(ctx, subject, event)
	return args.Error(0)
}
