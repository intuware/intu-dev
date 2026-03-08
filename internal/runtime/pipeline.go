package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/intuware/intu/internal/datatype"
	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Pipeline struct {
	channelDir   string
	projectDir   string
	channelID    string
	config       *config.ChannelConfig
	runner       JSRunner
	logger       *slog.Logger
	parser       datatype.Parser
	store        storage.MessageStore
	maps         *MapVariables
	connectorMap *SyncMap
	splitter     datatype.BatchSplitter
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

	var splitter datatype.BatchSplitter
	if cfg.Batch != nil && cfg.Batch.Enabled && cfg.Batch.SplitOn != "" {
		s, err := datatype.NewBatchSplitter(cfg.Batch.SplitOn)
		if err != nil {
			logger.Warn("unsupported batch splitter, batch disabled", "splitOn", cfg.Batch.SplitOn, "error", err)
		} else {
			splitter = s
		}
	}

	return &Pipeline{
		channelDir: channelDir,
		projectDir: projectDir,
		channelID:  channelID,
		config:     cfg,
		runner:     runner,
		logger:     logger,
		parser:     parser,
		splitter:   splitter,
	}
}

func (p *Pipeline) SetMessageStore(store storage.MessageStore) {
	p.store = store
}

func (p *Pipeline) SetMapContext(maps *MapVariables, connectorMap *SyncMap) {
	p.maps = maps
	p.connectorMap = connectorMap
}

type DestinationResult struct {
	Name     string
	Success  bool
	Response *message.Response
	Error    string
}

type PipelineResult struct {
	Filtered    bool
	Output      any
	OutputBytes []byte
	RouteTo     []string
	DestResults []DestinationResult
	BatchItems  []BatchItem
}

type BatchItem struct {
	Raw    []byte
	Parsed any
	Output any
}

func (p *Pipeline) Execute(ctx context.Context, msg *message.Message) (*PipelineResult, error) {
	tracer := otel.Tracer("intu.pipeline")
	ctx, span := tracer.Start(ctx, "pipeline.execute",
		trace.WithAttributes(
			attribute.String("channel.id", p.channelID),
			attribute.String("message.id", msg.ID),
		),
	)
	defer span.End()

	var current any = msg.Raw

	if p.config.Pipeline != nil && p.config.Pipeline.Preprocessor != "" {
		_, preprocessSpan := tracer.Start(ctx, "pipeline.preprocess",
			trace.WithAttributes(attribute.String("script", p.config.Pipeline.Preprocessor)),
		)
		out, err := p.callScript("preprocess", p.config.Pipeline.Preprocessor, current)
		if err != nil {
			preprocessSpan.SetStatus(codes.Error, err.Error())
			preprocessSpan.End()
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("preprocessor: %w", err)
		}
		preprocessSpan.End()
		if b, ok := out.([]byte); ok {
			msg.Raw = b
			current = b
		} else if s, ok := out.(string); ok {
			msg.Raw = []byte(s)
			current = []byte(s)
		}
	}

	if p.splitter != nil {
		return p.executeBatch(ctx, msg)
	}

	return p.executeSingle(ctx, msg, msg.Raw)
}

func (p *Pipeline) executeBatch(ctx context.Context, msg *message.Message) (*PipelineResult, error) {
	parts, err := p.splitter.Split(msg.Raw)
	if err != nil {
		p.logger.Warn("batch split failed, processing as single message", "error", err)
		return p.executeSingle(ctx, msg, msg.Raw)
	}

	if len(parts) <= 1 {
		return p.executeSingle(ctx, msg, msg.Raw)
	}

	p.logger.Debug("batch split", "channel", p.channelID, "parts", len(parts))

	var allOutputs []any
	var allBytes []byte
	routeTo := []string{}

	maxBatch := len(parts)
	if p.config.Batch != nil && p.config.Batch.MaxBatchSize > 0 && maxBatch > p.config.Batch.MaxBatchSize {
		maxBatch = p.config.Batch.MaxBatchSize
	}

	for i := 0; i < maxBatch; i++ {
		part := parts[i]
		result, err := p.executeSingle(ctx, msg, part)
		if err != nil {
			p.logger.Warn("batch item error", "index", i, "error", err)
			continue
		}
		if result.Filtered {
			continue
		}
		allOutputs = append(allOutputs, result.Output)
		allBytes = append(allBytes, result.OutputBytes...)
		allBytes = append(allBytes, '\n')
		if len(result.RouteTo) > 0 {
			routeTo = append(routeTo, result.RouteTo...)
		}
	}

	outputBytes, _ := json.Marshal(allOutputs)

	return &PipelineResult{
		Output:      allOutputs,
		OutputBytes: outputBytes,
		RouteTo:     routeTo,
	}, nil
}

func (p *Pipeline) executeSingle(ctx context.Context, msg *message.Message, raw []byte) (*PipelineResult, error) {
	tracer := otel.Tracer("intu.pipeline")

	parsed, err := p.parser.Parse(raw)
	if err != nil {
		p.logger.Warn("data type parsing failed, using raw", "error", err)
		parsed = string(raw)
	}
	current := parsed

	if validatorFile := p.resolveValidator(); validatorFile != "" {
		_, validatorSpan := tracer.Start(ctx, "pipeline.validate",
			trace.WithAttributes(attribute.String("script", validatorFile)),
		)
		out, err := p.callScript("validate", validatorFile, current, p.buildPipelineCtx(msg))
		if err != nil {
			validatorSpan.SetStatus(codes.Error, err.Error())
			validatorSpan.End()
			return nil, fmt.Errorf("validator: %w", err)
		}
		validatorSpan.End()
		if valid, ok := out.(bool); ok && !valid {
			p.logger.Info("message rejected by validator", "channel", p.channelID, "messageId", msg.ID)
			return &PipelineResult{Filtered: true}, nil
		}
	}

	if p.config.Pipeline != nil && p.config.Pipeline.SourceFilter != "" {
		_, filterSpan := tracer.Start(ctx, "pipeline.source_filter",
			trace.WithAttributes(attribute.String("script", p.config.Pipeline.SourceFilter)),
		)
		out, err := p.callScript("filter", p.config.Pipeline.SourceFilter, current, p.buildPipelineCtx(msg))
		if err != nil {
			filterSpan.SetStatus(codes.Error, err.Error())
			filterSpan.End()
			return nil, fmt.Errorf("source filter: %w", err)
		}
		filterSpan.End()
		if keep, ok := out.(bool); ok && !keep {
			p.logger.Info("message filtered at source", "channel", p.channelID, "messageId", msg.ID)
			return &PipelineResult{Filtered: true}, nil
		}
	}

	routeTo := []string{}
	transformerFile := p.resolveTransformer()
	if transformerFile != "" {
		_, transformSpan := tracer.Start(ctx, "pipeline.transform",
			trace.WithAttributes(attribute.String("script", transformerFile)),
		)
		tctx := p.buildTransformCtx(msg)
		out, err := p.callScript("transform", transformerFile, current, tctx)
		if err != nil {
			transformSpan.SetStatus(codes.Error, err.Error())
			transformSpan.End()
			return nil, fmt.Errorf("transformer: %w", err)
		}
		transformSpan.End()
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
	tracer := otel.Tracer("intu.pipeline")
	destName := dest.Name
	if destName == "" {
		destName = dest.Ref
	}
	ctx, span := tracer.Start(ctx, "pipeline.destination",
		trace.WithAttributes(
			attribute.String("destination", destName),
			attribute.String("message.id", msg.ID),
		),
	)
	defer span.End()

	current := transformed

	if dest.Filter != "" {
		_, filterSpan := tracer.Start(ctx, "pipeline.destination.filter",
			trace.WithAttributes(attribute.String("script", dest.Filter)),
		)
		out, err := p.callScript("filter", dest.Filter, current, p.buildDestCtx(msg, dest.Name))
		if err != nil {
			filterSpan.SetStatus(codes.Error, err.Error())
			filterSpan.End()
			return nil, false, fmt.Errorf("destination filter %s: %w", dest.Name, err)
		}
		filterSpan.End()
		if keep, ok := out.(bool); ok && !keep {
			p.logger.Debug("message filtered at destination", "destination", dest.Name, "messageId", msg.ID)
			span.SetAttributes(attribute.Bool("filtered", true))
			return nil, true, nil
		}
	}

	if dest.TransformerFile != "" {
		_, transformSpan := tracer.Start(ctx, "pipeline.destination.transform",
			trace.WithAttributes(attribute.String("script", dest.TransformerFile)),
		)
		out, err := p.callScript("transform", dest.TransformerFile, current, p.buildDestCtx(msg, dest.Name))
		if err != nil {
			transformSpan.SetStatus(codes.Error, err.Error())
			transformSpan.End()
			return nil, false, fmt.Errorf("destination transformer %s: %w", dest.Name, err)
		}
		transformSpan.End()
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

func (p *Pipeline) resolveValidator() string {
	if p.config.Pipeline != nil && p.config.Pipeline.Validator != "" {
		return p.config.Pipeline.Validator
	}
	if p.config.Validator != nil && p.config.Validator.Entrypoint != "" {
		return p.config.Validator.Entrypoint
	}
	return ""
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
	start := time.Now()
	result, err := p.runner.Call(fn, entrypoint, args...)
	elapsed := time.Since(start)
	p.logger.Info("script executed",
		"channel", p.channelID,
		"function", fn,
		"file", file,
		"duration_ms", float64(elapsed.Microseconds())/1000.0,
	)
	return result, err
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
	ctx := map[string]any{
		"channelId":     p.channelID,
		"correlationId": msg.CorrelationID,
		"messageId":     msg.ID,
		"timestamp":     msg.Timestamp.Format(time.RFC3339Nano),
	}

	if p.maps != nil {
		ctx["globalMap"] = p.maps.GlobalMap().Snapshot()
		ctx["channelMap"] = p.maps.ChannelMap(p.channelID).Snapshot()
		ctx["responseMap"] = p.maps.ResponseMap(p.channelID).Snapshot()
	}
	if p.connectorMap != nil {
		ctx["connectorMap"] = p.connectorMap.Snapshot()
	}

	return ctx
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
