package publisher

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestGooglePubSubPublisher_DropsOversizedMessage(t *testing.T) {
	var logBuf bytes.Buffer
	conn := &PubSubConnection{
		logger: slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	p := NewGooglePubSubPublisher(conn)

	// A string payload larger than the limit guarantees the marshaled event
	// exceeds maxPubSubMessageBytes.
	big := strings.Repeat("x", maxPubSubMessageBytes+1)
	eventID := uuid.New()
	event := &Event{
		ID:     eventID,
		Schema: "public",
		Table:  "big_table",
		Action: "update",
		Data:   map[string]any{"blob": big},
	}

	result := p.Publish(context.Background(), "some-topic", event)

	serverID, err := result.Get(context.Background())
	if err != nil {
		t.Fatalf("expected nil error for dropped oversized event, got %v", err)
	}
	if serverID == "" {
		t.Fatalf("expected non-empty serverID placeholder, got empty string")
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "dropping oversized message") {
		t.Errorf("expected warning log, got: %q", logs)
	}
	if !strings.Contains(logs, "big_table") {
		t.Errorf("expected table name in log, got: %q", logs)
	}
	if !strings.Contains(logs, eventID.String()) {
		t.Errorf("expected event id in log, got: %q", logs)
	}
}

func TestGooglePubSubPublisher_UnderLimitIsNotDropped(t *testing.T) {
	// A small event should not take the drop path. We can't safely exercise the
	// downstream publish path here (it requires a real pubsub client), so we
	// verify the size check by confirming the drop-path warning is NOT emitted
	// for an under-limit event. The test then recovers from the expected nil
	// client panic when the real publish is attempted.
	var logBuf bytes.Buffer
	conn := &PubSubConnection{
		logger: slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	p := NewGooglePubSubPublisher(conn)

	event := &Event{
		ID:     uuid.New(),
		Schema: "public",
		Table:  "small_table",
		Action: "insert",
		Data:   map[string]any{"v": "hello"},
	}

	defer func() {
		// Recover from the expected panic when Publish hits the nil pubsub client.
		_ = recover()
		if strings.Contains(logBuf.String(), "dropping oversized message") {
			t.Errorf("did not expect drop-path warning for under-limit event, got logs: %q", logBuf.String())
		}
	}()

	_ = p.Publish(context.Background(), "some-topic", event)
}
