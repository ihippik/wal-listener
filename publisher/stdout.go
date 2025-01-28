package publisher

import (
	"context"
	"encoding/json"
	"fmt"
)

type StdoutPublisher struct{}

func NewStdoutPublisher() *StdoutPublisher {
	return &StdoutPublisher{}
}

func (p *StdoutPublisher) Publish(ctx context.Context, topic string, event *Event) PublishResult {
	body, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return NewPublishResult(err)
	}

	fmt.Println(string(body))
	return NewPublishResult(nil)
}

func (p *StdoutPublisher) Flush(topic string) {}

func (p *StdoutPublisher) Close() error { return nil }
