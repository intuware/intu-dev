package connector

import (
	"context"
	"log/slog"

	"github.com/intuware/intu/internal/message"
)

type LogDest struct {
	name   string
	logger *slog.Logger
}

func NewLogDest(name string, logger *slog.Logger) *LogDest {
	return &LogDest{name: name, logger: logger}
}

func (l *LogDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	l.logger.Info("destination send",
		"destination", l.name,
		"messageId", msg.ID,
		"contentLength", len(msg.Raw),
	)
	return &message.Response{StatusCode: 200, Body: []byte(`{"status":"logged"}`)}, nil
}

func (l *LogDest) Stop(ctx context.Context) error {
	return nil
}

func (l *LogDest) Type() string {
	return "log"
}
