package connector

import (
	"context"
	"log/slog"

	"github.com/intuware/intu-dev/internal/message"
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
	// Clone preserves original transport metadata for the target channel.
	clone := &message.Message{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		ChannelID:     c.targetChannelID,
		Raw:           append([]byte{}, msg.Raw...),
		Transport:     msg.Transport,
		ContentType:   msg.ContentType,
		HTTP:          msg.HTTP,
		File:          msg.File,
		FTP:           msg.FTP,
		Kafka:         msg.Kafka,
		TCP:           msg.TCP,
		SMTP:          msg.SMTP,
		DICOM:         msg.DICOM,
		Database:      msg.Database,
		Metadata:      make(map[string]any),
		Timestamp:     msg.Timestamp,
	}
	for k, v := range msg.Metadata {
		clone.Metadata[k] = v
	}

	// Stamp msg so the "sent" record reflects the channel destination.
	msg.ClearTransportMeta()
	msg.Transport = "channel"

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
