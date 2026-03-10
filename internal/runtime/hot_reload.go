package runtime

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/intuware/intu/pkg/config"
)

type HotReloader struct {
	engine      *DefaultEngine
	channelsDir string
	watcher     *fsnotify.Watcher
	logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	debounce    map[string]time.Time
	debounceMu  sync.Mutex
	buildMu     sync.Mutex
	lastBuild   time.Time
}

func NewHotReloader(engine *DefaultEngine, channelsDir string, logger *slog.Logger) (*HotReloader, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &HotReloader{
		engine:      engine,
		channelsDir: channelsDir,
		watcher:     watcher,
		logger:      logger,
		debounce:    make(map[string]time.Time),
	}, nil
}

func (hr *HotReloader) Start(ctx context.Context) error {
	hr.ctx, hr.cancel = context.WithCancel(ctx)

	if err := hr.watcher.Add(hr.channelsDir); err != nil {
		return err
	}

	entries, err := os.ReadDir(hr.channelsDir)
	if err == nil {
		for _, e := range entries {
			if e.IsDir() {
				channelDir := filepath.Join(hr.channelsDir, e.Name())
				hr.watchChannelDir(channelDir)
			}
		}
	}

	hr.wg.Add(1)
	go hr.watchLoop()

	hr.logger.Info("channel hot-reload enabled", "dir", hr.channelsDir)
	return nil
}

func (hr *HotReloader) watchChannelDir(channelDir string) {
	if err := hr.watcher.Add(channelDir); err != nil {
		hr.logger.Debug("failed to watch channel dir", "dir", channelDir, "error", err)
		return
	}

	files, err := os.ReadDir(channelDir)
	if err != nil {
		return
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if name == "channel.yaml" || strings.HasSuffix(name, ".ts") {
			filePath := filepath.Join(channelDir, name)
			if err := hr.watcher.Add(filePath); err != nil {
				hr.logger.Debug("failed to watch file", "file", filePath, "error", err)
			}
		}
	}
}

func (hr *HotReloader) Stop() {
	if hr.cancel != nil {
		hr.cancel()
	}
	hr.wg.Wait()
	hr.watcher.Close()
	hr.logger.Info("channel hot-reload stopped")
}

func (hr *HotReloader) watchLoop() {
	defer hr.wg.Done()

	for {
		select {
		case <-hr.ctx.Done():
			return

		case event, ok := <-hr.watcher.Events:
			if !ok {
				return
			}
			hr.handleEvent(event)

		case err, ok := <-hr.watcher.Errors:
			if !ok {
				return
			}
			hr.logger.Error("file watcher error", "error", err)
		}
	}
}

func (hr *HotReloader) handleEvent(event fsnotify.Event) {
	name := filepath.Base(event.Name)
	dir := filepath.Dir(event.Name)

	if name == "channel.yaml" {
		channelID := filepath.Base(dir)
		if hr.shouldDebounce(channelID) {
			return
		}

		if event.Op&fsnotify.Remove != 0 || event.Op&fsnotify.Rename != 0 {
			hr.logger.Info("channel config removed, stopping", "channel", channelID)
			hr.stopChannel(channelID)
			return
		}

		// Re-add file watch (editors may delete+recreate on save)
		_ = hr.watcher.Add(event.Name)
		hr.logger.Info("channel config changed, reloading", "channel", channelID)
		hr.reloadChannel(channelID, dir)
		return
	}

	if strings.HasSuffix(name, ".ts") {
		channelID := filepath.Base(dir)
		if _, exists := hr.engine.channels[channelID]; exists {
			if hr.shouldDebounce(channelID) {
				return
			}
			// Re-add file watch (editors may delete+recreate on save)
			_ = hr.watcher.Add(event.Name)
			hr.logger.Info("TypeScript changed, rebuilding and reloading", "file", name, "channel", channelID)
			if err := hr.rebuildTS(); err != nil {
				hr.logger.Error("TypeScript rebuild failed", "error", err)
				return
			}
			hr.reloadChannel(channelID, dir)
		}
		return
	}

	parentDir := filepath.Dir(event.Name)
	if parentDir == hr.channelsDir {
		info, err := os.Stat(event.Name)
		if err != nil {
			if event.Op&fsnotify.Remove != 0 {
				channelID := filepath.Base(event.Name)
				hr.logger.Info("channel directory removed", "channel", channelID)
				hr.stopChannel(channelID)
			}
			return
		}

		if info.IsDir() && event.Op&fsnotify.Create != 0 {
			channelID := filepath.Base(event.Name)
			hr.watchChannelDir(event.Name)

			channelYAML := filepath.Join(event.Name, "channel.yaml")
			if _, err := os.Stat(channelYAML); err == nil {
				hr.logger.Info("new channel directory detected", "channel", channelID)
				hr.startChannel(channelID, event.Name)
			}
		}
	}
}

func (hr *HotReloader) shouldDebounce(channelID string) bool {
	hr.debounceMu.Lock()
	defer hr.debounceMu.Unlock()

	now := time.Now()
	if last, ok := hr.debounce[channelID]; ok {
		if now.Sub(last) < 2*time.Second {
			return true
		}
	}
	hr.debounce[channelID] = now
	return false
}

func (hr *HotReloader) reloadChannel(channelID, channelDir string) {
	hr.stopChannel(channelID)

	time.Sleep(100 * time.Millisecond)

	hr.startChannel(channelID, channelDir)
}

func (hr *HotReloader) startChannel(channelID, channelDir string) {
	chCfg, err := config.LoadChannelConfig(channelDir)
	if err != nil {
		hr.logger.Error("failed to load channel config for hot-reload", "channel", channelID, "error", err)
		return
	}

	if !chCfg.Enabled {
		hr.logger.Info("channel disabled, not starting", "channel", channelID)
		return
	}

	cr, err := hr.engine.buildChannelRuntime(channelDir, chCfg)
	if err != nil {
		hr.logger.Error("failed to build channel runtime for hot-reload", "channel", channelID, "error", err)
		return
	}

	if err := cr.Start(hr.ctx); err != nil {
		hr.logger.Error("failed to start hot-reloaded channel", "channel", channelID, "error", err)
		return
	}

	hr.engine.channels[channelID] = cr
	hr.logger.Info("channel hot-reloaded", "channel", channelID)
}

func (hr *HotReloader) rebuildTS() error {
	hr.buildMu.Lock()
	defer hr.buildMu.Unlock()

	if time.Since(hr.lastBuild) < time.Second {
		return nil
	}

	npm := exec.Command("npm", "run", "build")
	npm.Dir = hr.engine.rootDir
	out, err := npm.CombinedOutput()
	if err != nil {
		hr.logger.Error("tsc compilation output", "output", string(out))
		return err
	}
	hr.lastBuild = time.Now()
	hr.logger.Info("TypeScript recompiled")
	return nil
}

func (hr *HotReloader) stopChannel(channelID string) {
	cr, exists := hr.engine.channels[channelID]
	if !exists {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cr.Stop(ctx); err != nil {
		hr.logger.Error("error stopping channel during hot-reload", "channel", channelID, "error", err)
	}

	if hr.engine.clusterMode && hr.engine.coordinator != nil {
		_ = hr.engine.coordinator.ReleaseChannel(ctx, channelID)
	}

	delete(hr.engine.channels, channelID)
	hr.logger.Info("channel stopped for hot-reload", "channel", channelID)
}
