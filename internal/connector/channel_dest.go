package connector

import (
	"context"
	"log/slog"

	"github.com/intuware/intu/internal/message"
)

type ChannelDest struct {
	name            string
	targetChannelID string
	logger          *slog.Logger
}

func NewChannelDest(name, targetChannelID string, logger *slog.Logger) *ChannelDest {
	return &ChannelDest{name: name, targetChannelID: targetChannelID, logger: logger}
}

func (c *ChannelDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	clone := &message.Message{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		ChannelID:     c.targetChannelID,
		Raw:           append([]byte{}, msg.Raw...),
		ContentType:   msg.ContentType,
		Headers:       make(map[string]string),
		Metadata:      make(map[string]any),
		Timestamp:     msg.Timestamp,
	}
	for k, v := range msg.Headers {
		clone.Headers[k] = v
	}
	for k, v := range msg.Metadata {
		clone.Metadata[k] = v
	}

	channelBus.Publish(c.targetChannelID, clone)
	c.logger.Debug("message published to channel", "target", c.targetChannelID, "messageId", msg.ID)

	return &message.Response{StatusCode: 200}, nil
}

func (c *ChannelDest) Stop(ctx context.Context) error {
	return nil
}

func (c *ChannelDest) Type() string {
	return "channel"
}
