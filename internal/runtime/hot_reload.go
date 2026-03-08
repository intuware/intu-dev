package runtime

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/intuware/intu/pkg/config"
)

type HotReloader struct {
	engine     *DefaultEngine
	channelsDir string
	watcher    *fsnotify.Watcher
	logger     *slog.Logger
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	debounce   map[string]time.Time
	debounceMu sync.Mutex
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
				if err := hr.watcher.Add(channelDir); err != nil {
					hr.logger.Debug("failed to watch channel dir", "dir", channelDir, "error", err)
				}
			}
		}
	}

	hr.wg.Add(1)
	go hr.watchLoop()

	hr.logger.Info("channel hot-reload enabled", "dir", hr.channelsDir)
	return nil
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

		switch {
		case event.Op&fsnotify.Write != 0 || event.Op&fsnotify.Create != 0:
			hr.logger.Info("channel config changed, reloading", "channel", channelID)
			hr.reloadChannel(channelID, dir)

		case event.Op&fsnotify.Remove != 0:
			hr.logger.Info("channel config removed, stopping", "channel", channelID)
			hr.stopChannel(channelID)
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

		if info.IsDir() {
			channelID := filepath.Base(event.Name)

			if err := hr.watcher.Add(event.Name); err != nil {
				hr.logger.Debug("failed to watch new channel dir", "dir", event.Name, "error", err)
			}

			if event.Op&fsnotify.Create != 0 {
				channelYAML := filepath.Join(event.Name, "channel.yaml")
				if _, err := os.Stat(channelYAML); err == nil {
					hr.logger.Info("new channel directory detected", "channel", channelID)
					hr.startChannel(channelID, event.Name)
				}
			}
		}
	}

	if strings.HasSuffix(name, ".ts") || strings.HasSuffix(name, ".js") {
		channelID := filepath.Base(dir)
		if _, exists := hr.engine.channels[channelID]; exists {
			hr.logger.Debug("script file changed", "file", name, "channel", channelID)
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
