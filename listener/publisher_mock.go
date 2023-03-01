package listener

import (
	"context"

	"github.com/banked/wal-listener/v2/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
)

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(ctx context.Context, cfg *config.Config, log *logrus.Entry, event Event) error {
	args := p.Called(ctx, cfg, log, event)
	return args.Error(0)
}
