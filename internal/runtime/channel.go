package runtime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/intuware/intu/internal/connector"
	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

type ChannelRuntime struct {
	ID           string
	Config       *config.ChannelConfig
	Source       connector.SourceConnector
	Destinations []connector.DestinationConnector
	Pipeline     *Pipeline
	Logger       *slog.Logger
}

func (cr *ChannelRuntime) Start(ctx context.Context) error {
	cr.Logger.Info("starting channel", "id", cr.ID)
	return cr.Source.Start(ctx, cr.handleMessage)
}

func (cr *ChannelRuntime) Stop(ctx context.Context) error {
	cr.Logger.Info("stopping channel", "id", cr.ID)

	if err := cr.Source.Stop(ctx); err != nil {
		cr.Logger.Error("error stopping source", "channel", cr.ID, "error", err)
	}

	for _, dest := range cr.Destinations {
		if err := dest.Stop(ctx); err != nil {
			cr.Logger.Error("error stopping destination", "channel", cr.ID, "type", dest.Type(), "error", err)
		}
	}

	return nil
}

func (cr *ChannelRuntime) handleMessage(ctx context.Context, msg *message.Message) error {
	cr.Logger.Debug("processing message", "channel", cr.ID, "messageId", msg.ID)

	result, err := cr.Pipeline.Execute(ctx, msg)
	if err != nil {
		return fmt.Errorf("pipeline execute: %w", err)
	}

	if result.Filtered {
		return nil
	}

	var outBytes []byte
	switch v := result.Output.(type) {
	case []byte:
		outBytes = v
	case string:
		outBytes = []byte(v)
	default:
		outBytes = msg.Raw
	}

	outMsg := &message.Message{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		ChannelID:     msg.ChannelID,
		Raw:           outBytes,
		ContentType:   msg.ContentType,
		Headers:       msg.Headers,
		Metadata:      msg.Metadata,
		Timestamp:     msg.Timestamp,
	}

	for _, dest := range cr.Destinations {
		resp, err := dest.Send(ctx, outMsg)
		if err != nil {
			cr.Logger.Error("destination send failed",
				"channel", cr.ID,
				"destination", dest.Type(),
				"messageId", msg.ID,
				"error", err,
			)
			continue
		}
		if resp != nil && resp.Error != nil {
			cr.Logger.Warn("destination returned error",
				"channel", cr.ID,
				"destination", dest.Type(),
				"messageId", msg.ID,
				"statusCode", resp.StatusCode,
			)
		}
	}

	return nil
}
