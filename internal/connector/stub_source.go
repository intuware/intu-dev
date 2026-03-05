package connector

import (
	"context"
	"log/slog"
)

type StubSource struct {
	connType string
	logger   *slog.Logger
}

func NewStubSource(connType string, logger *slog.Logger) *StubSource {
	return &StubSource{connType: connType, logger: logger}
}

func (s *StubSource) Start(ctx context.Context, handler MessageHandler) error {
	s.logger.Warn("stub source started — no messages will be received", "type", s.connType)
	return nil
}

func (s *StubSource) Stop(ctx context.Context) error {
	return nil
}

func (s *StubSource) Type() string {
	return s.connType
}
