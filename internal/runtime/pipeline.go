package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

type Pipeline struct {
	channelDir string
	projectDir string
	channelID  string
	config     *config.ChannelConfig
	runner     JSRunner
	logger     *slog.Logger
}

func NewPipeline(channelDir, projectDir, channelID string, cfg *config.ChannelConfig, runner JSRunner, logger *slog.Logger) *Pipeline {
	return &Pipeline{
		channelDir: channelDir,
		projectDir: projectDir,
		channelID:  channelID,
		config:     cfg,
		runner:     runner,
		logger:     logger,
	}
}

type PipelineResult struct {
	Filtered bool
	Output   any
}

func (p *Pipeline) Execute(ctx context.Context, msg *message.Message) (*PipelineResult, error) {
	var current any = msg.Raw

	if p.config.Pipeline != nil && p.config.Pipeline.Preprocessor != "" {
		out, err := p.callScript("preprocess", p.config.Pipeline.Preprocessor, current)
		if err != nil {
			return nil, fmt.Errorf("preprocessor: %w", err)
		}
		current = out
	}

	if p.config.Pipeline != nil && p.config.Pipeline.SourceFilter != "" {
		out, err := p.callScript("filter", p.config.Pipeline.SourceFilter, current, p.buildPipelineCtx(msg))
		if err != nil {
			return nil, fmt.Errorf("source filter: %w", err)
		}
		if keep, ok := out.(bool); ok && !keep {
			p.logger.Info("message filtered", "channel", p.channelID, "messageId", msg.ID)
			return &PipelineResult{Filtered: true}, nil
		}
	}

	transformerFile := p.resolveTransformer()
	if transformerFile != "" {
		out, err := p.callScript("transform", transformerFile, current, p.buildTransformCtx(msg))
		if err != nil {
			return nil, fmt.Errorf("transformer: %w", err)
		}
		current = out
	}

	return &PipelineResult{Output: current}, nil
}

func (p *Pipeline) resolveTransformer() string {
	if p.config.Pipeline != nil && p.config.Pipeline.Transformer != "" {
		return p.config.Pipeline.Transformer
	}
	if p.config.Transformer != nil && p.config.Transformer.Entrypoint != "" {
		return p.config.Transformer.Entrypoint
	}
	return ""
}

func (p *Pipeline) callScript(fn, file string, args ...any) (any, error) {
	entrypoint := p.resolveScriptPath(file)
	return p.runner.Call(fn, entrypoint, args...)
}

func (p *Pipeline) resolveScriptPath(file string) string {
	if strings.HasSuffix(file, ".ts") {
		jsFile := strings.TrimSuffix(file, ".ts") + ".js"
		rel, _ := filepath.Rel(p.projectDir, p.channelDir)
		compiled := filepath.Join(p.projectDir, "dist", rel, jsFile)
		return compiled
	}
	return filepath.Join(p.channelDir, file)
}

func (p *Pipeline) buildPipelineCtx(msg *message.Message) map[string]any {
	return map[string]any{
		"channelId":     p.channelID,
		"correlationId": msg.CorrelationID,
		"messageId":     msg.ID,
		"timestamp":     msg.Timestamp.Format("2006-01-02T15:04:05.000Z"),
	}
}

func (p *Pipeline) buildTransformCtx(msg *message.Message) map[string]any {
	ctx := p.buildPipelineCtx(msg)
	if p.config.DataTypes != nil {
		ctx["inboundDataType"] = p.config.DataTypes.Inbound
		ctx["outboundDataType"] = p.config.DataTypes.Outbound
	}
	return ctx
}
