package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// Phase represents a point in the pipeline where plugins can execute.
type Phase string

const (
	PhaseBeforeValidation  Phase = "before_validation"
	PhaseAfterValidation   Phase = "after_validation"
	PhaseBeforeTransform   Phase = "before_transform"
	PhaseAfterTransform    Phase = "after_transform"
	PhaseBeforeDestination Phase = "before_destination"
	PhaseAfterDestination  Phase = "after_destination"
)

// PipelineStage is the Go interface for custom pipeline plugins. Each stage
// declares its Name, the Phase at which it runs, and a Process function that
// receives and returns a message.
type PipelineStage interface {
	Name() string
	Phase() Phase
	Process(ctx context.Context, msg *message.Message) (*message.Message, error)
}

// ScriptPlugin implements PipelineStage by delegating to a TypeScript/JS
// function via the Node.js runner. This is the standard plugin type registered
// through channel YAML.
type ScriptPlugin struct {
	name       string
	phase      Phase
	entrypoint string
	channelDir string
	projectDir string
	runner     *NodeRunner
	logger     *slog.Logger
}

func NewScriptPlugin(cfg config.PluginConfig, channelDir, projectDir string, runner *NodeRunner, logger *slog.Logger) (*ScriptPlugin, error) {
	p := Phase(cfg.Phase)
	switch p {
	case PhaseBeforeValidation, PhaseAfterValidation,
		PhaseBeforeTransform, PhaseAfterTransform,
		PhaseBeforeDestination, PhaseAfterDestination:
	default:
		return nil, fmt.Errorf("invalid plugin phase %q for plugin %q", cfg.Phase, cfg.Name)
	}

	return &ScriptPlugin{
		name:       cfg.Name,
		phase:      p,
		entrypoint: cfg.Entrypoint,
		channelDir: channelDir,
		projectDir: projectDir,
		runner:     runner,
		logger:     logger,
	}, nil
}

func (sp *ScriptPlugin) Name() string  { return sp.name }
func (sp *ScriptPlugin) Phase() Phase  { return sp.phase }

func (sp *ScriptPlugin) Process(ctx context.Context, msg *message.Message) (*message.Message, error) {
	entrypoint := sp.resolveScriptPath(sp.entrypoint)

	intuMsg := map[string]any{
		"body":        string(msg.Raw),
		"transport":   msg.Transport,
		"contentType": string(msg.ContentType),
	}
	if msg.SourceCharset != "" {
		intuMsg["sourceCharset"] = msg.SourceCharset
	}
	if len(msg.Metadata) > 0 {
		intuMsg["metadata"] = msg.Metadata
	}

	pipelineCtx := map[string]any{
		"channelId":     msg.ChannelID,
		"correlationId": msg.CorrelationID,
		"messageId":     msg.ID,
		"stage":         string(sp.phase),
	}

	result, err := sp.runner.Call("process", entrypoint, intuMsg, pipelineCtx)
	if err != nil {
		return nil, fmt.Errorf("plugin %s: %w", sp.name, err)
	}

	if result == nil {
		return msg, nil
	}

	m, ok := result.(map[string]any)
	if !ok {
		return msg, nil
	}

	if body, hasBody := m["body"]; hasBody {
		switch v := body.(type) {
		case string:
			msg.Raw = []byte(v)
		case []byte:
			msg.Raw = v
		}
	}
	if ct, ok := m["contentType"].(string); ok && ct != "" {
		msg.ContentType = message.ContentType(ct)
	}
	if t, ok := m["transport"].(string); ok && t != "" {
		msg.Transport = t
	}
	if md, ok := m["metadata"].(map[string]any); ok {
		if msg.Metadata == nil {
			msg.Metadata = make(map[string]any)
		}
		for k, v := range md {
			msg.Metadata[k] = v
		}
	}

	return msg, nil
}

func (sp *ScriptPlugin) resolveScriptPath(file string) string {
	if strings.HasSuffix(file, ".ts") {
		jsFile := strings.TrimSuffix(file, ".ts") + ".js"
		rel, _ := filepath.Rel(sp.projectDir, sp.channelDir)
		return filepath.Join(sp.projectDir, "dist", rel, jsFile)
	}
	return filepath.Join(sp.channelDir, file)
}

// PluginRegistry holds the ordered set of plugins for a channel, indexed by
// phase for fast lookup during pipeline execution.
type PluginRegistry struct {
	plugins map[Phase][]PipelineStage
}

func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{
		plugins: make(map[Phase][]PipelineStage),
	}
}

func (r *PluginRegistry) Register(stage PipelineStage) {
	r.plugins[stage.Phase()] = append(r.plugins[stage.Phase()], stage)
}

// Execute runs all plugins registered for the given phase in order. Returns
// the (possibly modified) message or the first error encountered.
func (r *PluginRegistry) Execute(ctx context.Context, phase Phase, msg *message.Message, logger *slog.Logger) (*message.Message, error) {
	stages := r.plugins[phase]
	if len(stages) == 0 {
		return msg, nil
	}

	current := msg
	for _, stage := range stages {
		result, err := stage.Process(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("plugin %s (%s): %w", stage.Name(), phase, err)
		}
		if result != nil {
			current = result
		}
		logger.Debug("plugin executed", "plugin", stage.Name(), "phase", string(phase), "messageId", msg.ID)
	}
	return current, nil
}

func (r *PluginRegistry) HasPlugins(phase Phase) bool {
	return len(r.plugins[phase]) > 0
}
