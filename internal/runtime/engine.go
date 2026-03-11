package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/alerting"
	"github.com/intuware/intu-dev/internal/cluster"
	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/observability"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/redis/go-redis/v9"
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
	jsRunner      *NodeRunner

	coordinator    cluster.ChannelCoordinator
	dedup          cluster.MessageDeduplicator
	redisClient    *redis.Client
	redisKeyPrefix string
	clusterMode    bool
	cancelAcq      context.CancelFunc
	acqWg          *sync.WaitGroup

	hotReloader     *HotReloader
	pendingChannels []pendingChannel
}

type pendingChannel struct {
	dir string
	cfg *config.ChannelConfig
}

func NewDefaultEngine(rootDir string, cfg *config.Config, factory ConnectorFactory, logger *slog.Logger) *DefaultEngine {
	mode := cfg.Runtime.Mode
	if mode == "" {
		mode = "standalone"
	}

	return &DefaultEngine{
		rootDir:     rootDir,
		cfg:         cfg,
		channels:    make(map[string]*ChannelRuntime),
		factory:     factory,
		logger:      logger,
		metrics:     observability.Global(),
		maps:        NewMapVariables(),
		clusterMode: mode == "cluster",
		acqWg:       &sync.WaitGroup{},
	}
}

func (e *DefaultEngine) SetMessageStore(store storage.MessageStore) {
	e.store = store
}

func (e *DefaultEngine) SetAlertManager(am *alerting.AlertManager) {
	e.alertMgr = am
}

func (e *DefaultEngine) SetCoordinator(coord cluster.ChannelCoordinator) {
	e.coordinator = coord
}

func (e *DefaultEngine) SetDeduplicator(dedup cluster.MessageDeduplicator) {
	e.dedup = dedup
}

func (e *DefaultEngine) SetRedisClient(client *redis.Client, keyPrefix string) {
	e.redisClient = client
	e.redisKeyPrefix = keyPrefix
}

func (e *DefaultEngine) Metrics() *observability.Metrics {
	return e.metrics
}

func (e *DefaultEngine) MessageStore() storage.MessageStore {
	return e.store
}

func (e *DefaultEngine) RootDir() string {
	return e.rootDir
}

func (e *DefaultEngine) Config() *config.Config {
	return e.cfg
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

	if err := e.initNodeRunner(); err != nil {
		return fmt.Errorf("init Node.js runner: %w", err)
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
	channelDirs, err := config.DiscoverChannelDirs(channelsDir)
	if err != nil {
		return fmt.Errorf("discover channels: %w", err)
	}

	type channelEntry struct {
		dir   string
		cfg   *config.ChannelConfig
		order int
	}
	var channelEntries []channelEntry

	for _, channelDir := range channelDirs {
		chCfg, err := config.LoadChannelConfig(channelDir)
		if err != nil {
			e.logger.Warn("skipping channel", "dir", channelDir, "error", err)
			continue
		}

		if !chCfg.Enabled {
			e.logger.Info("channel disabled, skipping", "id", chCfg.ID)
			continue
		}

		if !chCfg.MatchesProfile(e.cfg.Runtime.Profile) {
			e.logger.Info("channel not in active profile, skipping",
				"id", chCfg.ID, "profiles", chCfg.Profiles, "active", e.cfg.Runtime.Profile)
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

		if e.clusterMode && e.coordinator != nil {
			if !e.coordinator.ShouldAcquireChannel(ce.cfg.ID, ce.cfg.Tags) {
				e.logger.Info("channel not eligible for this instance (tag affinity)",
					"id", ce.cfg.ID,
					"instance", e.coordinator.InstanceID(),
				)
				e.pendingChannels = append(e.pendingChannels, pendingChannel{dir: ce.dir, cfg: ce.cfg})
				continue
			}

			acquired, err := e.coordinator.AcquireChannel(ctx, ce.cfg.ID)
			if err != nil {
				e.logger.Error("failed to acquire channel", "id", ce.cfg.ID, "error", err)
				e.pendingChannels = append(e.pendingChannels, pendingChannel{dir: ce.dir, cfg: ce.cfg})
				continue
			}
			if !acquired {
				e.logger.Info("channel owned by another instance, skipping",
					"id", ce.cfg.ID,
				)
				e.pendingChannels = append(e.pendingChannels, pendingChannel{dir: ce.dir, cfg: ce.cfg})
				continue
			}
		}

		cr, err := e.buildChannelRuntime(ce.dir, ce.cfg)
		if err != nil {
			e.logger.Error("failed to build channel runtime", "id", ce.cfg.ID, "error", err)
			if e.clusterMode && e.coordinator != nil {
				_ = e.coordinator.ReleaseChannel(ctx, ce.cfg.ID)
			}
			continue
		}

		if err := cr.Start(ctx); err != nil {
			e.logger.Error("failed to start channel", "id", ce.cfg.ID, "error", err)
			if e.clusterMode && e.coordinator != nil {
				_ = e.coordinator.ReleaseChannel(ctx, ce.cfg.ID)
			}
			continue
		}

		e.channels[ce.cfg.ID] = cr
		started[ce.cfg.ID] = true
		e.logger.Info("channel started", "id", ce.cfg.ID)
	}

	for _, ce := range channelEntries {
		e.preloadChannelScripts(ce.dir, ce.cfg)
	}

	if e.clusterMode && e.coordinator != nil && len(e.pendingChannels) > 0 {
		acqCtx, acqCancel := context.WithCancel(ctx)
		e.cancelAcq = acqCancel
		e.acqWg.Add(1)
		go e.channelAcquisitionLoop(acqCtx)
	}

	e.logger.Info("engine started",
		"channels", len(e.channels),
		"mode", e.cfg.Runtime.Mode,
	)
	return nil
}

func (e *DefaultEngine) Stop(ctx context.Context) error {
	e.logger.Info("stopping engine")

	if e.cancelAcq != nil {
		e.cancelAcq()
	}
	e.acqWg.Wait()

	for id, cr := range e.channels {
		if err := cr.Stop(ctx); err != nil {
			e.logger.Error("error stopping channel", "id", id, "error", err)
		}
	}

	if e.clusterMode && e.coordinator != nil {
		for id := range e.channels {
			if err := e.coordinator.ReleaseChannel(ctx, id); err != nil {
				e.logger.Warn("failed to release channel lease", "id", id, "error", err)
			}
		}
		e.coordinator.Stop()
	}

	if e.hotReloader != nil {
		e.hotReloader.Stop()
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
	resolvedDestCfgs := make(map[string]config.Destination)
	for _, d := range chCfg.Destinations {
		name := d.Name
		if name == "" {
			name = d.Ref
		}
		if name == "" {
			continue
		}

		var destCfg config.Destination

		if d.Type != "" {
			destCfg = d.ToDestination()
		} else {
			ref := d.Ref
			if ref == "" {
				ref = d.Name
			}

			rootDest, ok := e.cfg.Destinations[ref]
			if !ok {
				e.logger.Warn("destination not found in root config", "ref", ref, "channel", chCfg.ID)
				continue
			}
			destCfg = rootDest
		}

		resolvedDestCfgs[name] = destCfg

		dest, err := e.factory.CreateDestination(name, destCfg)
		if err != nil {
			return nil, fmt.Errorf("create destination %s: %w", name, err)
		}
		dests[name] = dest
	}

	pipeline := NewPipeline(channelDir, e.rootDir, chCfg.ID, chCfg, e.jsRunner, e.logger)
	pipeline.SetResolvedDestinations(resolvedDestCfgs)

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
		Dedup:        e.dedup,
	}

	cr.initRetryAndQueue(e.cfg, e.redisClient, e.clusterMode, e.redisKeyPrefix)

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

func (e *DefaultEngine) initNodeRunner() error {
	poolSize := e.cfg.Runtime.WorkerPool
	nr, err := NewNodeRunner(poolSize, e.logger)
	if err != nil {
		return fmt.Errorf("start Node.js worker pool: %w", err)
	}
	e.jsRunner = nr
	return nil
}

func (e *DefaultEngine) preloadChannelScripts(channelDir string, cfg *config.ChannelConfig) {
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
		if err := e.jsRunner.PreloadModule(entrypoint); err != nil {
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
		if d.Transformer != nil {
			preload(d.Transformer.Entrypoint)
		}
		preload(d.Filter)
		if d.ResponseTransformer != nil {
			preload(d.ResponseTransformer.Entrypoint)
		}
	}
}

func (e *DefaultEngine) InitRuntime(ctx context.Context) error {
	e.codeTemplates = NewCodeTemplateLoader(e.rootDir, e.logger)
	if e.cfg.CodeTemplates != nil {
		for _, lib := range e.cfg.CodeTemplates {
			if err := e.codeTemplates.LoadLibrary(lib.Name, lib.Directory); err != nil {
				e.logger.Warn("failed to load code template library", "name", lib.Name, "error", err)
			}
		}
	}
	return e.initNodeRunner()
}

func (e *DefaultEngine) CloseRuntime() error {
	if e.jsRunner != nil {
		return e.jsRunner.Close()
	}
	return nil
}

func (e *DefaultEngine) ReprocessMessage(ctx context.Context, channelID string, msg *message.Message) error {
	if cr, ok := e.channels[channelID]; ok {
		return cr.handleMessage(ctx, msg)
	}

	channelDir := e.findChannelDir(channelID)
	if channelDir == "" {
		return fmt.Errorf("channel %q not found", channelID)
	}

	chCfg, err := config.LoadChannelConfig(channelDir)
	if err != nil {
		return fmt.Errorf("load channel config %s: %w", channelID, err)
	}

	cr, err := e.buildChannelRuntime(channelDir, chCfg)
	if err != nil {
		return fmt.Errorf("build channel runtime %s: %w", channelID, err)
	}

	return cr.handleMessage(ctx, msg)
}

func (e *DefaultEngine) findChannelDir(channelID string) string {
	channelsDir := filepath.Join(e.rootDir, e.cfg.ChannelsDir)
	dir, err := config.FindChannelDir(channelsDir, channelID)
	if err != nil {
		return ""
	}
	return dir
}

func (e *DefaultEngine) WatchChannels(ctx context.Context) error {
	channelsDir := filepath.Join(e.rootDir, e.cfg.ChannelsDir)
	hr, err := NewHotReloader(e, channelsDir, e.logger)
	if err != nil {
		return fmt.Errorf("init hot-reloader: %w", err)
	}
	e.hotReloader = hr
	return hr.Start(ctx)
}

func (e *DefaultEngine) UndeployChannel(ctx context.Context, channelID string) error {
	cr, ok := e.channels[channelID]
	if !ok {
		return nil
	}

	if err := cr.Stop(ctx); err != nil {
		e.logger.Error("error stopping channel", "id", channelID, "error", err)
		return fmt.Errorf("stop channel %s: %w", channelID, err)
	}

	if e.clusterMode && e.coordinator != nil {
		_ = e.coordinator.ReleaseChannel(ctx, channelID)
	}

	delete(e.channels, channelID)
	e.logger.Info("channel undeployed", "id", channelID)
	return nil
}

func (e *DefaultEngine) DeployChannel(ctx context.Context, channelID string) error {
	if _, ok := e.channels[channelID]; ok {
		return nil
	}

	channelDir := e.findChannelDir(channelID)
	if channelDir == "" {
		return fmt.Errorf("channel %q not found", channelID)
	}

	chCfg, err := config.LoadChannelConfig(channelDir)
	if err != nil {
		return fmt.Errorf("load channel config %s: %w", channelID, err)
	}

	if !chCfg.Enabled {
		return fmt.Errorf("channel %s is disabled in config", channelID)
	}

	cr, err := e.buildChannelRuntime(channelDir, chCfg)
	if err != nil {
		return fmt.Errorf("build channel runtime %s: %w", channelID, err)
	}

	if err := cr.Start(ctx); err != nil {
		return fmt.Errorf("start channel %s: %w", channelID, err)
	}

	e.channels[channelID] = cr
	e.logger.Info("channel deployed", "id", channelID)
	return nil
}

func (e *DefaultEngine) RestartChannel(ctx context.Context, channelID string) error {
	if err := e.UndeployChannel(ctx, channelID); err != nil {
		return err
	}
	return e.DeployChannel(ctx, channelID)
}

func (e *DefaultEngine) GetChannelRuntime(channelID string) (*ChannelRuntime, bool) {
	cr, ok := e.channels[channelID]
	return cr, ok
}

func (e *DefaultEngine) ListChannelIDs() []string {
	var ids []string
	for id := range e.channels {
		ids = append(ids, id)
	}
	return ids
}

func (e *DefaultEngine) channelAcquisitionLoop(ctx context.Context) {
	defer e.acqWg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tryAcquirePendingChannels(ctx)
		}
	}
}

func (e *DefaultEngine) tryAcquirePendingChannels(ctx context.Context) {
	var remaining []pendingChannel

	for _, pc := range e.pendingChannels {
		if _, exists := e.channels[pc.cfg.ID]; exists {
			continue
		}

		if !e.coordinator.ShouldAcquireChannel(pc.cfg.ID, pc.cfg.Tags) {
			remaining = append(remaining, pc)
			continue
		}

		acquired, err := e.coordinator.AcquireChannel(ctx, pc.cfg.ID)
		if err != nil {
			e.logger.Debug("failed to acquire pending channel", "id", pc.cfg.ID, "error", err)
			remaining = append(remaining, pc)
			continue
		}
		if !acquired {
			remaining = append(remaining, pc)
			continue
		}

		cr, err := e.buildChannelRuntime(pc.dir, pc.cfg)
		if err != nil {
			e.logger.Error("failed to build acquired channel runtime", "id", pc.cfg.ID, "error", err)
			_ = e.coordinator.ReleaseChannel(ctx, pc.cfg.ID)
			remaining = append(remaining, pc)
			continue
		}

		if err := cr.Start(ctx); err != nil {
			e.logger.Error("failed to start acquired channel", "id", pc.cfg.ID, "error", err)
			_ = e.coordinator.ReleaseChannel(ctx, pc.cfg.ID)
			remaining = append(remaining, pc)
			continue
		}

		e.channels[pc.cfg.ID] = cr
		e.logger.Info("acquired and started pending channel", "id", pc.cfg.ID)
	}

	e.pendingChannels = remaining
}
