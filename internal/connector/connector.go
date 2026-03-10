package connector

import (
	"context"

	"github.com/intuware/intu-dev/internal/message"
)

type MessageHandler func(ctx context.Context, msg *message.Message) error

type SourceConnector interface {
	Start(ctx context.Context, handler MessageHandler) error
	Stop(ctx context.Context) error
	Type() string
}

type DestinationConnector interface {
	Send(ctx context.Context, msg *message.Message) (*message.Response, error)
	Stop(ctx context.Context) error
	Type() string
}
