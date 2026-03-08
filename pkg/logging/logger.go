package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/intuware/intu/pkg/config"
)

func New(level string, cfg *config.LoggingConfig) *slog.Logger {
	var logLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	var writer io.Writer
	if cfg != nil && len(cfg.Transports) > 0 {
		transport, err := NewTransportFromConfig(cfg)
		if err != nil {
			handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
			logger := slog.New(handler)
			logger.Error("failed to initialize log transport, falling back to stdout", "error", err)
			return logger
		}
		writer = transport
	} else {
		writer = os.Stdout
	}

	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: logLevel})
	return slog.New(handler)
}

func WriterFromConfig(cfg *config.LoggingConfig) io.Writer {
	if cfg == nil || len(cfg.Transports) == 0 {
		return os.Stdout
	}
	transport, err := NewTransportFromConfig(cfg)
	if err != nil {
		return os.Stdout
	}
	return transport
}
