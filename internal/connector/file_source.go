package connector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type FileSource struct {
	cfg    *config.FileListener
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewFileSource(cfg *config.FileListener, logger *slog.Logger) *FileSource {
	return &FileSource{cfg: cfg, logger: logger}
}

func (f *FileSource) Start(ctx context.Context, handler MessageHandler) error {
	if f.cfg.Scheme != "" && f.cfg.Scheme != "local" {
		f.logger.Warn("file source only supports local scheme currently", "scheme", f.cfg.Scheme)
	}

	interval := 10 * time.Second
	if f.cfg.PollInterval != "" {
		d, err := time.ParseDuration(f.cfg.PollInterval)
		if err == nil {
			interval = d
		}
	}

	ctx, f.cancel = context.WithCancel(ctx)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		f.poll(ctx, handler)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				f.poll(ctx, handler)
			}
		}
	}()

	f.logger.Info("file source started", "dir", f.cfg.Directory, "pattern", f.cfg.FilePattern)
	return nil
}

func (f *FileSource) poll(ctx context.Context, handler MessageHandler) {
	dir := f.cfg.Directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		f.logger.Error("read directory failed", "dir", dir, "error", err)
		return
	}

	pattern := f.cfg.FilePattern
	var matched []os.DirEntry

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if pattern != "" {
			ok, _ := filepath.Match(pattern, e.Name())
			if !ok {
				continue
			}
		}
		matched = append(matched, e)
	}

	if f.cfg.SortBy == "name" {
		sort.Slice(matched, func(i, j int) bool {
			return matched[i].Name() < matched[j].Name()
		})
	} else if f.cfg.SortBy == "modified" {
		sort.Slice(matched, func(i, j int) bool {
			ii, _ := matched[i].Info()
			jj, _ := matched[j].Info()
			if ii == nil || jj == nil {
				return false
			}
			return ii.ModTime().Before(jj.ModTime())
		})
	}

	for _, e := range matched {
		select {
		case <-ctx.Done():
			return
		default:
		}

		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			f.logger.Error("read file failed", "path", path, "error", err)
			if f.cfg.ErrorDir != "" {
				f.moveFile(path, filepath.Join(f.cfg.ErrorDir, e.Name()))
			}
			continue
		}

		msg := message.New("", data)
		msg.Metadata["filename"] = e.Name()
		msg.Metadata["filepath"] = path

		if err := handler(ctx, msg); err != nil {
			f.logger.Error("handler error", "file", e.Name(), "error", err)
			if f.cfg.ErrorDir != "" {
				f.moveFile(path, filepath.Join(f.cfg.ErrorDir, e.Name()))
			}
			continue
		}

		if f.cfg.MoveTo != "" {
			f.moveFile(path, filepath.Join(f.cfg.MoveTo, e.Name()))
		} else {
			os.Remove(path)
		}
	}
}

func (f *FileSource) moveFile(src, dst string) {
	dir := filepath.Dir(dst)
	os.MkdirAll(dir, 0o755)
	if err := os.Rename(src, dst); err != nil {
		if data, readErr := os.ReadFile(src); readErr == nil {
			if writeErr := os.WriteFile(dst, data, 0o644); writeErr == nil {
				os.Remove(src)
				return
			}
		}
		f.logger.Error("move file failed", "src", src, "dst", dst, "error", err)
	}
}

func (f *FileSource) Stop(ctx context.Context) error {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
	return nil
}

func (f *FileSource) Type() string {
	return fmt.Sprintf("file/%s", strings.ToLower(f.cfg.Scheme))
}
