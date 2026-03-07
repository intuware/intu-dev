package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/intuware/intu/internal/alerting"
	"github.com/intuware/intu/internal/connector"
	"github.com/intuware/intu/internal/observability"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
)

type Engine interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type ConnectorFactory interface {
	CreateSource(listenerCfg config.ListenerConfig) (connector.SourceConnector, error)
	CreateDestination(name string, dest config.Destination) (connector.DestinationConnector, error)
}

type DefaultEngine struct {
	rootDir      string
	cfg          *config.Config
	channels     map[string]*ChannelRuntime
	factory      ConnectorFactory
	logger       *slog.Logger
	metrics      *observability.Metrics
	store        storage.MessageStore
	alertMgr     *alerting.AlertManager
	maps         *MapVariables
	codeTemplates *CodeTemplateLoader
}

func NewDefaultEngine(rootDir string, cfg *config.Config, factory ConnectorFactory, logger *slog.Logger) *DefaultEngine {
	return &DefaultEngine{
		rootDir:  rootDir,
		cfg:      cfg,
		channels: make(map[string]*ChannelRuntime),
		factory:  factory,
		logger:   logger,
		metrics:  observability.Global(),
		maps:     NewMapVariables(),
	}
}

// SetMessageStore injects the message store for pipeline persistence.
func (e *DefaultEngine) SetMessageStore(store storage.MessageStore) {
	e.store = store
}

// SetAlertManager injects the alert manager.
func (e *DefaultEngine) SetAlertManager(am *alerting.AlertManager) {
	e.alertMgr = am
}

func (e *DefaultEngine) Start(ctx context.Context) error {
	e.logger.Info("starting engine", "name", e.cfg.Runtime.Name)

	e.codeTemplates = NewCodeTemplateLoader(e.rootDir, e.logger)
	if e.cfg.CodeTemplates != nil {
		for _, lib := range e.cfg.CodeTemplates {
			if err := e.codeTemplates.LoadLibrary(lib.Name, lib.Directory); err != nil {
				e.logger.Warn("failed to load code template library", "name", lib.Name, "error", err)
			}
		}
	}

	if e.cfg.Global != nil && e.cfg.Global.Hooks != nil && e.cfg.Global.Hooks.OnStartup != "" {
		runner := NewGojaRunner()
		hookPath := filepath.Join(e.rootDir, "dist", e.cfg.Global.Hooks.OnStartup)
		hookPath = strings.TrimSuffix(hookPath, ".ts") + ".js"
		if _, err := runner.Call("onStartup", hookPath, map[string]any{}); err != nil {
			e.logger.Warn("global startup hook failed", "error", err)
		} else {
			e.logger.Info("global startup hook executed")
		}
	}

	if e.alertMgr != nil {
		e.alertMgr.Start(ctx)
	}

	channelsDir := filepath.Join(e.rootDir, e.cfg.ChannelsDir)
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		return fmt.Errorf("read channels dir: %w", err)
	}

	type channelEntry struct {
		dir   string
		cfg   *config.ChannelConfig
		order int
	}
	var channelEntries []channelEntry

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		channelDir := filepath.Join(channelsDir, entry.Name())
		chCfg, err := config.LoadChannelConfig(channelDir)
		if err != nil {
			e.logger.Warn("skipping channel", "name", entry.Name(), "error", err)
			continue
		}

		if !chCfg.Enabled {
			e.logger.Info("channel disabled, skipping", "id", chCfg.ID)
			continue
		}

		channelEntries = append(channelEntries, channelEntry{
			dir:   channelDir,
			cfg:   chCfg,
			order: chCfg.StartupOrder,
		})
	}

	sort.Slice(channelEntries, func(i, j int) bool {
		return channelEntries[i].order < channelEntries[j].order
	})

	started := make(map[string]bool)
	for _, ce := range channelEntries {
		if !e.dependenciesMet(ce.cfg, started) {
			e.logger.Error("channel dependencies not met, skipping",
				"id", ce.cfg.ID,
				"depends_on", ce.cfg.DependsOn,
			)
			continue
		}

		cr, err := e.buildChannelRuntime(ce.dir, ce.cfg)
		if err != nil {
			e.logger.Error("failed to build channel runtime", "id", ce.cfg.ID, "error", err)
			continue
		}

		if err := cr.Start(ctx); err != nil {
			e.logger.Error("failed to start channel", "id", ce.cfg.ID, "error", err)
			continue
		}

		e.channels[ce.cfg.ID] = cr
		started[ce.cfg.ID] = true
		e.logger.Info("channel started", "id", ce.cfg.ID)
	}

	e.logger.Info("engine started", "channels", len(e.channels))
	return nil
}

func (e *DefaultEngine) Stop(ctx context.Context) error {
	e.logger.Info("stopping engine")

	for id, cr := range e.channels {
		if err := cr.Stop(ctx); err != nil {
			e.logger.Error("error stopping channel", "id", id, "error", err)
		}
	}

	if e.alertMgr != nil {
		e.alertMgr.Stop()
	}

	if e.cfg.Global != nil && e.cfg.Global.Hooks != nil && e.cfg.Global.Hooks.OnShutdown != "" {
		runner := NewGojaRunner()
		hookPath := filepath.Join(e.rootDir, "dist", e.cfg.Global.Hooks.OnShutdown)
		hookPath = strings.TrimSuffix(hookPath, ".ts") + ".js"
		if _, err := runner.Call("onShutdown", hookPath, map[string]any{}); err != nil {
			e.logger.Warn("global shutdown hook failed", "error", err)
		} else {
			e.logger.Info("global shutdown hook executed")
		}
	}

	e.logger.Info("engine stopped")
	return nil
}

func (e *DefaultEngine) dependenciesMet(chCfg *config.ChannelConfig, started map[string]bool) bool {
	for _, dep := range chCfg.DependsOn {
		if !started[dep] {
			return false
		}
	}
	return true
}

func (e *DefaultEngine) buildChannelRuntime(channelDir string, chCfg *config.ChannelConfig) (*ChannelRuntime, error) {
	source, err := e.factory.CreateSource(chCfg.Listener)
	if err != nil {
		return nil, fmt.Errorf("create source for %s: %w", chCfg.ID, err)
	}

	dests := make(map[string]connector.DestinationConnector)
	for _, d := range chCfg.Destinations {
		name := d.Name
		if name == "" {
			name = d.Ref
		}
		if name == "" {
			continue
		}

		ref := d.Ref
		if ref == "" {
			ref = d.Name
		}

		rootDest, ok := e.cfg.Destinations[ref]
		if !ok {
			e.logger.Warn("destination not found in root config", "ref", ref, "channel", chCfg.ID)
			continue
		}
		dest, err := e.factory.CreateDestination(name, rootDest)
		if err != nil {
			return nil, fmt.Errorf("create destination %s: %w", name, err)
		}
		dests[name] = dest
	}

	runner := NewGojaRunner()
	pipeline := NewPipeline(channelDir, e.rootDir, chCfg.ID, chCfg, runner, e.logger)

	if e.store != nil {
		pipeline.SetMessageStore(e.store)
	}

	cr := &ChannelRuntime{
		ID:           chCfg.ID,
		Config:       chCfg,
		Source:       source,
		Destinations: dests,
		DestConfigs:  chCfg.Destinations,
		Pipeline:     pipeline,
		Logger:       e.logger,
		Metrics:      e.metrics,
		Store:        e.store,
		Maps:         e.maps,
	}

	cr.initRetryAndQueue(e.cfg)

	return cr, nil
}
