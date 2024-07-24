package listener

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/ihippik/wal-listener/v2/publisher"
)

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(ctx context.Context, subject string, event *publisher.Event) publisher.PublishResult {
	args := p.Called(ctx, subject, event)
	return publisher.NewPublishResult(args.Error(0))
}

func (p *publisherMock) Flush(subject string) {}
