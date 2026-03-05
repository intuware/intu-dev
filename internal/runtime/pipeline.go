package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/intuware/intu/internal/datatype"
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
	parser     datatype.Parser
}

func NewPipeline(channelDir, projectDir, channelID string, cfg *config.ChannelConfig, runner JSRunner, logger *slog.Logger) *Pipeline {
	inboundType := ""
	if cfg.DataTypes != nil {
		inboundType = cfg.DataTypes.Inbound
	}
	parser, err := datatype.NewParser(inboundType)
	if err != nil {
		logger.Warn("unsupported inbound data type, using raw", "type", inboundType, "error", err)
		parser, _ = datatype.NewParser("raw")
	}

	return &Pipeline{
		channelDir: channelDir,
		projectDir: projectDir,
		channelID:  channelID,
		config:     cfg,
		runner:     runner,
		logger:     logger,
		parser:     parser,
	}
}

type DestinationResult struct {
	Name     string
	Success  bool
	Response *message.Response
	Error    string
}

type PipelineResult struct {
	Filtered     bool
	Output       any
	OutputBytes  []byte
	RouteTo      []string
	DestResults  []DestinationResult
}

func (p *Pipeline) Execute(ctx context.Context, msg *message.Message) (*PipelineResult, error) {
	var current any = msg.Raw

	if p.config.Pipeline != nil && p.config.Pipeline.Preprocessor != "" {
		out, err := p.callScript("preprocess", p.config.Pipeline.Preprocessor, current)
		if err != nil {
			return nil, fmt.Errorf("preprocessor: %w", err)
		}
		if b, ok := out.([]byte); ok {
			msg.Raw = b
			current = b
		} else if s, ok := out.(string); ok {
			msg.Raw = []byte(s)
			current = []byte(s)
		}
	}

	parsed, err := p.parser.Parse(msg.Raw)
	if err != nil {
		p.logger.Warn("data type parsing failed, using raw", "error", err)
		parsed = string(msg.Raw)
	}
	current = parsed

	if p.config.Pipeline != nil && p.config.Pipeline.SourceFilter != "" {
		out, err := p.callScript("filter", p.config.Pipeline.SourceFilter, current, p.buildPipelineCtx(msg))
		if err != nil {
			return nil, fmt.Errorf("source filter: %w", err)
		}
		if keep, ok := out.(bool); ok && !keep {
			p.logger.Info("message filtered at source", "channel", p.channelID, "messageId", msg.ID)
			return &PipelineResult{Filtered: true}, nil
		}
	}

	routeTo := []string{}
	transformerFile := p.resolveTransformer()
	if transformerFile != "" {
		tctx := p.buildTransformCtx(msg)
		out, err := p.callScript("transform", transformerFile, current, tctx)
		if err != nil {
			return nil, fmt.Errorf("transformer: %w", err)
		}
		current = out

		if routes, ok := tctx["_routeTo"].([]string); ok && len(routes) > 0 {
			routeTo = routes
		}
	}

	outputBytes := p.toBytes(current)

	return &PipelineResult{
		Output:      current,
		OutputBytes: outputBytes,
		RouteTo:     routeTo,
	}, nil
}

func (p *Pipeline) ExecuteDestinationPipeline(ctx context.Context, msg *message.Message, transformed any, dest config.ChannelDestination) (*message.Message, bool, error) {
	current := transformed

	if dest.Filter != "" {
		out, err := p.callScript("filter", dest.Filter, current, p.buildDestCtx(msg, dest.Name))
		if err != nil {
			return nil, false, fmt.Errorf("destination filter %s: %w", dest.Name, err)
		}
		if keep, ok := out.(bool); ok && !keep {
			p.logger.Debug("message filtered at destination", "destination", dest.Name, "messageId", msg.ID)
			return nil, true, nil
		}
	}

	if dest.TransformerFile != "" {
		out, err := p.callScript("transform", dest.TransformerFile, current, p.buildDestCtx(msg, dest.Name))
		if err != nil {
			return nil, false, fmt.Errorf("destination transformer %s: %w", dest.Name, err)
		}
		current = out
	}

	outBytes := p.toBytes(current)
	outMsg := &message.Message{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		ChannelID:     msg.ChannelID,
		Raw:           outBytes,
		ContentType:   msg.ContentType,
		Headers:       msg.Headers,
		Metadata:      msg.Metadata,
		Timestamp:     msg.Timestamp,
	}

	return outMsg, false, nil
}

func (p *Pipeline) ExecuteResponseTransformer(ctx context.Context, msg *message.Message, dest config.ChannelDestination, resp *message.Response) error {
	if dest.ResponseTransformer == "" {
		return nil
	}

	respData := map[string]any{
		"statusCode": resp.StatusCode,
		"headers":    resp.Headers,
	}
	if resp.Body != nil {
		respData["body"] = string(resp.Body)
	}
	if resp.Error != nil {
		respData["error"] = resp.Error.Error()
	}

	_, err := p.callScript("transformResponse", dest.ResponseTransformer, respData, p.buildDestCtx(msg, dest.Name))
	return err
}

func (p *Pipeline) ExecutePostprocessor(ctx context.Context, msg *message.Message, transformed any, results []DestinationResult) error {
	if p.config.Pipeline == nil || p.config.Pipeline.Postprocessor == "" {
		return nil
	}

	var resultsData []map[string]any
	for _, r := range results {
		rd := map[string]any{
			"destinationName": r.Name,
			"success":         r.Success,
		}
		if r.Error != "" {
			rd["error"] = r.Error
		}
		resultsData = append(resultsData, rd)
	}

	_, err := p.callScript("postprocess", p.config.Pipeline.Postprocessor, transformed, resultsData, p.buildPipelineCtx(msg))
	return err
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
	ctx["sourceType"] = p.config.Listener.Type
	return ctx
}

func (p *Pipeline) buildDestCtx(msg *message.Message, destName string) map[string]any {
	ctx := p.buildPipelineCtx(msg)
	ctx["destinationName"] = destName
	return ctx
}

func (p *Pipeline) toBytes(data any) []byte {
	switch v := data.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return []byte(fmt.Sprintf("%v", v))
		}
		return b
	}
}
