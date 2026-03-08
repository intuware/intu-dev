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
	rootDir       string
	cfg           *config.Config
	channels      map[string]*ChannelRuntime
	factory       ConnectorFactory
	logger        *slog.Logger
	metrics       *observability.Metrics
	store         storage.MessageStore
	alertMgr      *alerting.AlertManager
	maps          *MapVariables
	codeTemplates *CodeTemplateLoader
	jsRunner      JSRunner
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

	if err := e.initJSRunner(); err != nil {
		return fmt.Errorf("init JS runner: %w", err)
	}

	if e.cfg.Global != nil && e.cfg.Global.Hooks != nil && e.cfg.Global.Hooks.OnStartup != "" {
		hookPath := filepath.Join(e.rootDir, "dist", e.cfg.Global.Hooks.OnStartup)
		hookPath = strings.TrimSuffix(hookPath, ".ts") + ".js"
		if _, err := e.jsRunner.Call("onStartup", hookPath, map[string]any{}); err != nil {
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

	if nr, ok := e.jsRunner.(*NodeRunner); ok {
		for _, ce := range channelEntries {
			e.preloadChannelScripts(ce.dir, ce.cfg, nr)
		}
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
		hookPath := filepath.Join(e.rootDir, "dist", e.cfg.Global.Hooks.OnShutdown)
		hookPath = strings.TrimSuffix(hookPath, ".ts") + ".js"
		if _, err := e.jsRunner.Call("onShutdown", hookPath, map[string]any{}); err != nil {
			e.logger.Warn("global shutdown hook failed", "error", err)
		} else {
			e.logger.Info("global shutdown hook executed")
		}
	}

	if e.jsRunner != nil {
		if err := e.jsRunner.Close(); err != nil {
			e.logger.Error("error closing JS runner", "error", err)
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

	pipeline := NewPipeline(channelDir, e.rootDir, chCfg.ID, chCfg, e.jsRunner, e.logger)

	channelStore := e.resolveChannelStore(chCfg)
	if channelStore != nil {
		pipeline.SetMessageStore(channelStore)
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
		Store:        channelStore,
		Maps:         e.maps,
	}

	cr.initRetryAndQueue(e.cfg)

	return cr, nil
}

func (e *DefaultEngine) resolveChannelStore(chCfg *config.ChannelConfig) storage.MessageStore {
	if e.store == nil {
		return nil
	}

	if chCfg.MessageStorage == nil {
		return e.store
	}

	chStorage := chCfg.MessageStorage

	if chStorage.Mode == "" && len(chStorage.Stages) == 0 {
		if chStorage.Enabled {
			return e.store
		}
		return e.store
	}

	mode := chStorage.Mode
	if mode == "" {
		mode = "full"
	}

	return storage.NewCompositeStore(e.store, mode, chStorage.Stages)
}

func (e *DefaultEngine) initJSRunner() error {
	jsRuntime := e.cfg.Runtime.JSRuntime
	if jsRuntime == "" {
		jsRuntime = "node"
	}

	switch jsRuntime {
	case "goja":
		e.jsRunner = NewGojaRunner()
		e.logger.Info("using Goja JS runtime")
	default:
		poolSize := e.cfg.Runtime.WorkerPool
		nr, err := NewNodeRunner(poolSize, e.logger)
		if err != nil {
			e.logger.Warn("failed to start Node.js worker pool, falling back to Goja", "error", err)
			e.jsRunner = NewGojaRunner()
			return nil
		}
		e.jsRunner = nr
	}
	return nil
}

func (e *DefaultEngine) preloadChannelScripts(channelDir string, cfg *config.ChannelConfig, nr *NodeRunner) {
	preload := func(file string) {
		if file == "" {
			return
		}
		var entrypoint string
		if strings.HasSuffix(file, ".ts") {
			jsFile := strings.TrimSuffix(file, ".ts") + ".js"
			rel, _ := filepath.Rel(e.rootDir, channelDir)
			entrypoint = filepath.Join(e.rootDir, "dist", rel, jsFile)
		} else {
			entrypoint = filepath.Join(channelDir, file)
		}
		if err := nr.PreloadModule(entrypoint); err != nil {
			e.logger.Debug("preload skipped", "path", entrypoint, "error", err)
		}
	}

	if cfg.Pipeline != nil {
		preload(cfg.Pipeline.Validator)
		preload(cfg.Pipeline.Transformer)
		preload(cfg.Pipeline.Preprocessor)
		preload(cfg.Pipeline.Postprocessor)
		preload(cfg.Pipeline.SourceFilter)
	}
	if cfg.Validator != nil {
		preload(cfg.Validator.Entrypoint)
	}
	if cfg.Transformer != nil {
		preload(cfg.Transformer.Entrypoint)
	}
	for _, d := range cfg.Destinations {
		preload(d.TransformerFile)
		preload(d.Filter)
		preload(d.ResponseTransformer)
	}
}
