package logging

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu/pkg/config"
)

const defaultMaxSizeMB = 100

type FileTransport struct {
	mu         sync.Mutex
	path       string
	maxBytes   int64
	maxFiles   int
	compress   bool
	file       *os.File
	written    int64
}

func NewFileTransport(cfg *config.FileLogConfig) (*FileTransport, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("file transport path is required")
	}

	maxSize := cfg.MaxSizeMB
	if maxSize <= 0 {
		maxSize = defaultMaxSizeMB
	}
	maxFiles := cfg.MaxFiles
	if maxFiles <= 0 {
		maxFiles = 5
	}

	dir := filepath.Dir(cfg.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log directory %s: %w", dir, err)
	}

	f, err := os.OpenFile(cfg.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", cfg.Path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}

	return &FileTransport{
		path:     cfg.Path,
		maxBytes: int64(maxSize) * 1024 * 1024,
		maxFiles: maxFiles,
		compress: cfg.Compress,
		file:     f,
		written:  info.Size(),
	}, nil
}

func (t *FileTransport) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.written+int64(len(p)) > t.maxBytes {
		if err := t.rotate(); err != nil {
			return 0, fmt.Errorf("rotate: %w", err)
		}
	}

	n, err := t.file.Write(p)
	t.written += int64(n)
	return n, err
}

func (t *FileTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.file != nil {
		return t.file.Close()
	}
	return nil
}

func (t *FileTransport) rotate() error {
	if t.file != nil {
		t.file.Close()
	}

	ts := time.Now().Format("20060102T150405")
	ext := filepath.Ext(t.path)
	base := strings.TrimSuffix(t.path, ext)
	rotatedName := fmt.Sprintf("%s.%s%s", base, ts, ext)

	if err := os.Rename(t.path, rotatedName); err != nil {
		return fmt.Errorf("rename %s to %s: %w", t.path, rotatedName, err)
	}

	if t.compress {
		go compressFile(rotatedName)
	}

	f, err := os.OpenFile(t.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open new log file: %w", err)
	}
	t.file = f
	t.written = 0

	go t.pruneOldFiles()
	return nil
}

func (t *FileTransport) pruneOldFiles() {
	ext := filepath.Ext(t.path)
	base := strings.TrimSuffix(t.path, ext)
	dir := filepath.Dir(t.path)
	prefix := filepath.Base(base) + "."

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	var rotated []string
	for _, e := range entries {
		name := e.Name()
		if name == filepath.Base(t.path) {
			continue
		}
		if strings.HasPrefix(name, prefix) {
			rotated = append(rotated, filepath.Join(dir, name))
		}
	}

	sort.Strings(rotated)

	for len(rotated) > t.maxFiles {
		os.Remove(rotated[0])
		rotated = rotated[1:]
	}
}

func compressFile(path string) {
	src, err := os.Open(path)
	if err != nil {
		return
	}
	defer src.Close()

	dst, err := os.Create(path + ".gz")
	if err != nil {
		return
	}

	gz := gzip.NewWriter(dst)
	if _, err := io.Copy(gz, src); err != nil {
		gz.Close()
		dst.Close()
		os.Remove(path + ".gz")
		return
	}
	gz.Close()
	dst.Close()
	src.Close()
	os.Remove(path)
}
