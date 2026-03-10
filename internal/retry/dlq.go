package retry

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type DeadLetterQueue struct {
	cfg    *config.DeadLetterConfig
	send   SendFunc
	logger *slog.Logger
}

func NewDeadLetterQueue(cfg *config.DeadLetterConfig, send SendFunc, logger *slog.Logger) *DeadLetterQueue {
	return &DeadLetterQueue{cfg: cfg, send: send, logger: logger}
}

type DLQEntry struct {
	OriginalMessageID string         `json:"original_message_id"`
	CorrelationID     string         `json:"correlation_id"`
	ChannelID         string         `json:"channel_id"`
	OriginalPayload   string         `json:"original_payload,omitempty"`
	Error             string         `json:"error"`
	Destination       string         `json:"destination"`
	Timestamp         time.Time      `json:"timestamp"`
	Metadata          map[string]any `json:"metadata,omitempty"`
}

func (dlq *DeadLetterQueue) Send(ctx context.Context, msg *message.Message, destination string, err error) {
	if dlq.cfg == nil || !dlq.cfg.Enabled {
		return
	}

	entry := DLQEntry{
		OriginalMessageID: msg.ID,
		CorrelationID:     msg.CorrelationID,
		ChannelID:         msg.ChannelID,
		Destination:       destination,
		Timestamp:         time.Now(),
		Metadata:          msg.Metadata,
	}

	if err != nil {
		entry.Error = err.Error()
	}

	if dlq.cfg.IncludeOriginal {
		entry.OriginalPayload = string(msg.Raw)
	}

	data, jsonErr := json.Marshal(entry)
	if jsonErr != nil {
		dlq.logger.Error("failed to marshal DLQ entry", "error", jsonErr)
		return
	}

	dlqMsg := &message.Message{
		ID:            msg.ID + "-dlq",
		CorrelationID: msg.CorrelationID,
		ChannelID:     msg.ChannelID,
		Raw:           data,
		ContentType:   "json",
		Headers:       map[string]string{"X-DLQ": "true"},
		Metadata:      msg.Metadata,
		Timestamp:     time.Now(),
	}

	if dlq.send != nil {
		if _, sendErr := dlq.send(ctx, dlqMsg); sendErr != nil {
			dlq.logger.Error("DLQ send failed", "error", sendErr)
		} else {
			dlq.logger.Info("message sent to DLQ",
				"messageId", msg.ID,
				"destination", destination,
				"error", entry.Error,
			)
		}
	}
}
