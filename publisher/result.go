package publisher

import "context"

type PublishResult interface {
	Get(ctx context.Context) (serverID string, err error)
}

type result struct {
	serverID string
	err      error
}

func NewPublishResult(err error) PublishResult {
	return &result{serverID: "na", err: err}
}

func (r *result) Get(ctx context.Context) (string, error) {
	return r.serverID, r.err
}
