package connector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

type FileDest struct {
	name   string
	cfg    *config.FileDestMapConfig
	logger *slog.Logger
}

func NewFileDest(name string, cfg *config.FileDestMapConfig, logger *slog.Logger) *FileDest {
	return &FileDest{name: name, cfg: cfg, logger: logger}
}

func (f *FileDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	dir := f.cfg.Directory
	if dir == "" {
		dir = "."
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create directory %s: %w", dir, err)
	}

	filename := f.cfg.FilenamePattern
	if filename == "" {
		filename = fmt.Sprintf("%s_%d.dat", msg.ChannelID, time.Now().UnixNano())
	} else {
		filename = strings.ReplaceAll(filename, "{{channelId}}", msg.ChannelID)
		filename = strings.ReplaceAll(filename, "{{correlationId}}", msg.CorrelationID)
		filename = strings.ReplaceAll(filename, "{{messageId}}", msg.ID)
		filename = strings.ReplaceAll(filename, "{{timestamp}}", time.Now().Format("20060102T150405"))
	}

	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, msg.Raw, 0o644); err != nil {
		return nil, fmt.Errorf("write file %s: %w", path, err)
	}

	f.logger.Debug("file written", "path", path, "bytes", len(msg.Raw))
	return &message.Response{StatusCode: 200, Body: []byte(path)}, nil
}

func (f *FileDest) Stop(ctx context.Context) error {
	return nil
}

func (f *FileDest) Type() string {
	return "file"
}
