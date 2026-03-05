package observability

import (
	"log/slog"
	"os"
	"strings"

	"github.com/intuware/intu/pkg/config"
)

type ChannelLogger struct {
	logger     *slog.Logger
	channelID  string
	payloadCfg *config.PayloadLogging
	truncateAt int
}

func NewChannelLogger(channelID string, logCfg *config.ChannelLogging, globalLevel string) *ChannelLogger {
	level := globalLevel
	if logCfg != nil && logCfg.Level != "" {
		level = logCfg.Level
	}

	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	case "silent":
		slogLevel = slog.Level(100)
	default:
		slogLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel})
	logger := slog.New(handler).With("channel", channelID)

	var payloadCfg *config.PayloadLogging
	truncateAt := 0
	if logCfg != nil {
		payloadCfg = logCfg.Payloads
		truncateAt = logCfg.TruncateAt
	}

	return &ChannelLogger{
		logger:     logger,
		channelID:  channelID,
		payloadCfg: payloadCfg,
		truncateAt: truncateAt,
	}
}

func (cl *ChannelLogger) Logger() *slog.Logger {
	return cl.logger
}

func (cl *ChannelLogger) LogSourcePayload(raw []byte) {
	if cl.payloadCfg == nil || !cl.payloadCfg.Source {
		return
	}
	cl.logger.Debug("source payload", "payload", cl.truncate(string(raw)))
}

func (cl *ChannelLogger) LogTransformedPayload(data []byte) {
	if cl.payloadCfg == nil || !cl.payloadCfg.Transformed {
		return
	}
	cl.logger.Debug("transformed payload", "payload", cl.truncate(string(data)))
}

func (cl *ChannelLogger) LogSentPayload(destination string, data []byte) {
	if cl.payloadCfg == nil || !cl.payloadCfg.Sent {
		return
	}
	cl.logger.Debug("sent payload", "destination", destination, "payload", cl.truncate(string(data)))
}

func (cl *ChannelLogger) LogResponsePayload(destination string, data []byte) {
	if cl.payloadCfg == nil || !cl.payloadCfg.Response {
		return
	}
	cl.logger.Debug("response payload", "destination", destination, "payload", cl.truncate(string(data)))
}

func (cl *ChannelLogger) LogFilteredMessage(messageID string) {
	if cl.payloadCfg == nil || !cl.payloadCfg.Filtered {
		return
	}
	cl.logger.Debug("message filtered", "messageId", messageID)
}

func (cl *ChannelLogger) truncate(s string) string {
	if cl.truncateAt > 0 && len(s) > cl.truncateAt {
		return s[:cl.truncateAt] + "...(truncated)"
	}
	return s
}
