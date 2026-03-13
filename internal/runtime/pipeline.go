package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/intuware/intu-dev/internal/datatype"
	iencoding "github.com/intuware/intu-dev/internal/encoding"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Pipeline struct {
	channelDir    string
	projectDir    string
	channelID     string
	config        *config.ChannelConfig
	runner        *NodeRunner
	logger        *slog.Logger
	parser        datatype.Parser
	store         storage.MessageStore
	maps          *MapVariables
	connectorMap  *SyncMap
	splitter      datatype.BatchSplitter
	resolvedDests map[string]config.Destination
}

func NewPipeline(channelDir, projectDir, channelID string, cfg *config.ChannelConfig, runner *NodeRunner, logger *slog.Logger) *Pipeline {
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

func (p *Pipeline) SetResolvedDestinations(dests map[string]config.Destination) {
	p.resolvedDests = dests
}

type DestinationResult struct {
	Name     string
	Success  bool
	Response *message.Response
	Error    string
}

type PipelineResult struct {
	Filtered      bool
	Output        any
	OutputBytes   []byte
	OutputMsg     *message.Message
	RouteTo       []string
	DestResults   []DestinationResult
	BatchItems    []BatchItem
	SourceIntuMsg map[string]any
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

	// Transcode from source charset to UTF-8 so parsers and JSON
	// serialization (to Node.js) receive valid UTF-8 text.
	if msg.SourceCharset != "" || !isValidUTF8(msg.Raw) {
		transcoded, err := iencoding.ToUTF8(msg.Raw, msg.SourceCharset)
		if err != nil {
			p.logger.Warn("charset transcoding failed, using raw bytes", "charset", msg.SourceCharset, "error", err)
		} else {
			msg.Raw = transcoded
			msg.SourceCharset = "" // now UTF-8
		}
	}

	var current any = string(msg.Raw)

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
	intuMsg := p.buildIntuMessage(msg, current)

	if validatorFile := p.resolveValidator(); validatorFile != "" {
		_, validatorSpan := tracer.Start(ctx, "pipeline.validate",
			trace.WithAttributes(attribute.String("script", validatorFile)),
		)
		out, err := p.callScript("validate", validatorFile, intuMsg, p.buildPipelineCtx(msg))
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
		out, err := p.callScript("filter", p.config.Pipeline.SourceFilter, intuMsg, p.buildPipelineCtx(msg))
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
		out, err := p.callScript("transform", transformerFile, intuMsg, tctx)
		if err != nil {
			transformSpan.SetStatus(codes.Error, err.Error())
			transformSpan.End()
			return nil, fmt.Errorf("transformer: %w", err)
		}
		transformSpan.End()

		outMsg := p.parseIntuResult(out, msg)
		current = outMsg.Output

		if routes, ok := tctx["_routeTo"].([]string); ok && len(routes) > 0 {
			routeTo = routes
		}
		if len(routeTo) == 0 {
			if m, ok := out.(map[string]any); ok {
				if r, exists := m["_routeTo"]; exists {
					routeTo = toStringSlice(r)
				}
			}
		}

		outputBytes := p.toBytes(outMsg.Output)
		return &PipelineResult{
			Output:        outMsg.Output,
			OutputBytes:   outputBytes,
			OutputMsg:     outMsg.Msg,
			RouteTo:       routeTo,
			SourceIntuMsg: intuMsg,
		}, nil
	}

	outputBytes := p.toBytes(current)

	return &PipelineResult{
		Output:        current,
		OutputBytes:   outputBytes,
		RouteTo:       routeTo,
		SourceIntuMsg: intuMsg,
	}, nil
}

func (p *Pipeline) ExecuteDestinationPipeline(ctx context.Context, msg *message.Message, transformed any, sourceIntuMsg map[string]any, dest config.ChannelDestination) (*message.Message, bool, error) {
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
	intuMsg := p.buildDestIntuMessage(current, destName, dest)
	destCtx := p.buildDestCtx(msg, destName, sourceIntuMsg)

	if dest.Filter != "" {
		_, filterSpan := tracer.Start(ctx, "pipeline.destination.filter",
			trace.WithAttributes(attribute.String("script", dest.Filter)),
		)
		out, err := p.callScript("filter", dest.Filter, intuMsg, destCtx)
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

	destType := p.resolveDestType(destName, dest)

	destTransformer := resolveDestTransformer(dest)
	if destTransformer != "" {
		_, transformSpan := tracer.Start(ctx, "pipeline.destination.transform",
			trace.WithAttributes(attribute.String("script", destTransformer)),
		)
		out, err := p.callScript("transform", destTransformer, intuMsg, destCtx)
		if err != nil {
			transformSpan.SetStatus(codes.Error, err.Error())
			transformSpan.End()
			return nil, false, fmt.Errorf("destination transformer %s: %w", dest.Name, err)
		}
		transformSpan.End()

		outResult := p.parseIntuResult(out, msg)
		current = outResult.Output
		outMsg := outResult.Msg
		outMsg.Transport = destType
		outMsg.Raw = p.toBytes(current)
		return outMsg, false, nil
	}

	outBytes := p.toBytes(current)
	outMsg := cloneMessageShell(msg)
	outMsg.Raw = outBytes
	outMsg.Transport = destType

	return outMsg, false, nil
}

func (p *Pipeline) ExecuteResponseTransformer(ctx context.Context, msg *message.Message, dest config.ChannelDestination, resp *message.Response) error {
	respTransformer := resolveDestResponseTransformer(dest)
	if respTransformer == "" {
		return nil
	}

	respData := map[string]any{
		"statusCode": resp.StatusCode,
		"headers":    resp.Headers,
	}
	if resp.Body != nil {
		if utf8.Valid(resp.Body) {
			respData["body"] = string(resp.Body)
		} else {
			respData["body"] = string(ensureValidUTF8(resp.Body))
		}
	}
	if resp.Error != nil {
		respData["error"] = resp.Error.Error()
	}

	_, err := p.callScript("transformResponse", respTransformer, respData, p.buildDestCtx(msg, dest.Name, nil))
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

func resolveDestTransformer(dest config.ChannelDestination) string {
	if dest.Transformer != nil && dest.Transformer.Entrypoint != "" {
		return dest.Transformer.Entrypoint
	}
	return ""
}

func resolveDestResponseTransformer(dest config.ChannelDestination) string {
	if dest.ResponseTransformer != nil && dest.ResponseTransformer.Entrypoint != "" {
		return dest.ResponseTransformer.Entrypoint
	}
	return ""
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
	return ctx
}

func (p *Pipeline) buildDestCtx(msg *message.Message, destName string, sourceIntuMsg map[string]any) map[string]any {
	ctx := p.buildPipelineCtx(msg)
	ctx["destinationName"] = destName
	if sourceIntuMsg != nil {
		ctx["sourceMessage"] = sourceIntuMsg
	}
	return ctx
}

// buildIntuMessage constructs the IntuMessage map passed to transformers.
func (p *Pipeline) buildIntuMessage(msg *message.Message, parsed any) map[string]any {
	im := map[string]any{
		"body":        parsed,
		"transport":   msg.Transport,
		"contentType": string(msg.ContentType),
	}

	if msg.HTTP != nil {
		im["http"] = map[string]any{
			"headers":     nonNilMap(msg.HTTP.Headers),
			"queryParams": nonNilMap(msg.HTTP.QueryParams),
			"pathParams":  nonNilMap(msg.HTTP.PathParams),
			"method":      msg.HTTP.Method,
			"statusCode":  msg.HTTP.StatusCode,
		}
	}
	if msg.File != nil {
		im["file"] = map[string]any{
			"filename":  msg.File.Filename,
			"directory": msg.File.Directory,
		}
	}
	if msg.FTP != nil {
		im["ftp"] = map[string]any{
			"filename":  msg.FTP.Filename,
			"directory": msg.FTP.Directory,
		}
	}
	if msg.Kafka != nil {
		im["kafka"] = map[string]any{
			"headers":   nonNilMap(msg.Kafka.Headers),
			"topic":     msg.Kafka.Topic,
			"key":       msg.Kafka.Key,
			"partition": msg.Kafka.Partition,
			"offset":    msg.Kafka.Offset,
		}
	}
	if msg.TCP != nil {
		im["tcp"] = map[string]any{
			"remoteAddr": msg.TCP.RemoteAddr,
		}
	}
	if msg.SMTP != nil {
		im["smtp"] = map[string]any{
			"from":    msg.SMTP.From,
			"to":      msg.SMTP.To,
			"subject": msg.SMTP.Subject,
			"cc":      msg.SMTP.CC,
			"bcc":     msg.SMTP.BCC,
		}
	}
	if msg.DICOM != nil {
		im["dicom"] = map[string]any{
			"callingAE": msg.DICOM.CallingAE,
			"calledAE":  msg.DICOM.CalledAE,
		}
	}
	if msg.Database != nil {
		im["database"] = map[string]any{
			"query":  msg.Database.Query,
			"params": msg.Database.Params,
		}
	}

	return im
}

// resolveDestType determines the transport type for a destination by checking
// the inline config, the resolved root-level config, or inferring from which
// protocol config block is populated.
func (p *Pipeline) resolveDestType(destName string, destCfg config.ChannelDestination) string {
	if destCfg.Type != "" {
		return destCfg.Type
	}
	if p.resolvedDests != nil {
		if rd, ok := p.resolvedDests[destName]; ok && rd.Type != "" {
			return rd.Type
		}
	}
	switch {
	case destCfg.HTTP != nil:
		return "http"
	case destCfg.File != nil:
		return "file"
	case destCfg.Kafka != nil:
		return "kafka"
	case destCfg.TCP != nil:
		return "tcp"
	case destCfg.SMTP != nil:
		return "smtp"
	case destCfg.Database != nil:
		return "database"
	case destCfg.DICOM != nil:
		return "dicom"
	case destCfg.ChannelDest != nil:
		return "channel"
	case destCfg.FHIR != nil:
		return "fhir"
	case destCfg.JMS != nil:
		return "jms"
	case destCfg.Direct != nil:
		return "direct"
	}
	return ""
}

// getResolvedDest returns the fully resolved Destination config. It first
// checks the engine-resolved map, then falls back to converting the inline
// ChannelDestination.
func (p *Pipeline) getResolvedDest(destName string, destCfg config.ChannelDestination) config.Destination {
	if p.resolvedDests != nil {
		if rd, ok := p.resolvedDests[destName]; ok {
			return rd
		}
	}
	return destCfg.ToDestination()
}

// buildDestIntuMessage constructs a destination-native IntuMessage. Unlike
// buildIntuMessage (which uses source transport metadata), this method
// populates protocol fields from the destination config so that destination
// transformers see the outbound transport context instead of the inbound one.
func (p *Pipeline) buildDestIntuMessage(transformed any, destName string, destCfg config.ChannelDestination) map[string]any {
	destType := p.resolveDestType(destName, destCfg)
	rd := p.getResolvedDest(destName, destCfg)

	contentType := ""
	if p.config.DataTypes != nil && p.config.DataTypes.Outbound != "" {
		contentType = p.config.DataTypes.Outbound
	}

	im := map[string]any{
		"body":        transformed,
		"transport":   destType,
		"contentType": contentType,
	}

	switch destType {
	case "http":
		if rd.HTTP != nil {
			im["http"] = map[string]any{
				"headers":     nonNilStrMap(rd.HTTP.Headers),
				"queryParams": nonNilStrMap(rd.HTTP.QueryParams),
				"pathParams":  nonNilStrMap(rd.HTTP.PathParams),
				"method":      rd.HTTP.Method,
			}
		}
	case "fhir":
		if rd.FHIR != nil {
			im["http"] = map[string]any{
				"headers":     make(map[string]string),
				"queryParams": make(map[string]string),
				"pathParams":  make(map[string]string),
				"method":      "POST",
			}
		}
	case "file":
		if rd.File != nil {
			im["file"] = map[string]any{
				"filename":  rd.File.FilenamePattern,
				"directory": rd.File.Directory,
			}
		}
	case "sftp":
		if rd.SFTP != nil {
			im["file"] = map[string]any{
				"filename":  rd.SFTP.FilenamePattern,
				"directory": rd.SFTP.Directory,
			}
		}
	case "kafka":
		if rd.Kafka != nil {
			im["kafka"] = map[string]any{
				"headers": make(map[string]string),
				"topic":   rd.Kafka.Topic,
				"key":     "",
			}
		}
	case "tcp":
		if rd.TCP != nil {
			im["tcp"] = map[string]any{
				"remoteAddr": fmt.Sprintf("%s:%d", rd.TCP.Host, rd.TCP.Port),
			}
		}
	case "smtp":
		if rd.SMTP != nil {
			im["smtp"] = map[string]any{
				"from":    rd.SMTP.From,
				"to":      rd.SMTP.To,
				"subject": rd.SMTP.Subject,
			}
		}
	case "dicom":
		if rd.DICOM != nil {
			im["dicom"] = map[string]any{
				"callingAE": rd.DICOM.AETitle,
				"calledAE":  rd.DICOM.CalledAETitle,
			}
		}
	case "database":
		if rd.Database != nil {
			im["database"] = map[string]any{
				"query":  rd.Database.Statement,
				"params": make(map[string]any),
			}
		}
	}

	return im
}

func nonNilStrMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	return m
}

type intuResult struct {
	Output any
	Msg    *message.Message
}

// parseIntuResult extracts body and protocol metadata from a transformer return value.
func (p *Pipeline) parseIntuResult(result any, original *message.Message) *intuResult {
	m, ok := result.(map[string]any)
	if !ok {
		return &intuResult{
			Output: result,
			Msg:    cloneMessageShell(original),
		}
	}

	body, hasBody := m["body"]
	if !hasBody {
		return &intuResult{
			Output: result,
			Msg:    cloneMessageShell(original),
		}
	}

	out := cloneMessageShell(original)

	if ct, ok := m["contentType"].(string); ok && ct != "" {
		out.ContentType = message.ContentType(ct)
	}
	if t, ok := m["transport"].(string); ok && t != "" {
		out.Transport = t
	}

	if httpData, ok := m["http"].(map[string]any); ok {
		out.HTTP = parseHTTPMeta(httpData)
	}
	if fileData, ok := m["file"].(map[string]any); ok {
		out.File = parseFileMeta(fileData)
	}
	if ftpData, ok := m["ftp"].(map[string]any); ok {
		out.FTP = parseFTPMeta(ftpData)
	}
	if kafkaData, ok := m["kafka"].(map[string]any); ok {
		out.Kafka = parseKafkaMeta(kafkaData)
	}
	if tcpData, ok := m["tcp"].(map[string]any); ok {
		out.TCP = parseTCPMeta(tcpData)
	}
	if smtpData, ok := m["smtp"].(map[string]any); ok {
		out.SMTP = parseSMTPMeta(smtpData)
	}
	if dicomData, ok := m["dicom"].(map[string]any); ok {
		out.DICOM = parseDICOMMeta(dicomData)
	}
	if dbData, ok := m["database"].(map[string]any); ok {
		out.Database = parseDatabaseMeta(dbData)
	}

	return &intuResult{Output: body, Msg: out}
}

func cloneMessageShell(m *message.Message) *message.Message {
	return &message.Message{
		ID:            m.ID,
		CorrelationID: m.CorrelationID,
		ChannelID:     m.ChannelID,
		Transport:     m.Transport,
		ContentType:   m.ContentType,
		HTTP:          m.HTTP,
		File:          m.File,
		FTP:           m.FTP,
		Kafka:         m.Kafka,
		TCP:           m.TCP,
		SMTP:          m.SMTP,
		DICOM:         m.DICOM,
		Database:      m.Database,
		Metadata:      m.Metadata,
		Timestamp:     m.Timestamp,
	}
}

func parseHTTPMeta(data map[string]any) *message.HTTPMeta {
	meta := &message.HTTPMeta{
		Headers:     toStringMap(data["headers"]),
		QueryParams: toStringMap(data["queryParams"]),
		PathParams:  toStringMap(data["pathParams"]),
	}
	if v, ok := data["method"].(string); ok {
		meta.Method = v
	}
	if v, ok := data["statusCode"].(float64); ok {
		meta.StatusCode = int(v)
	}
	return meta
}

func parseFileMeta(data map[string]any) *message.FileMeta {
	meta := &message.FileMeta{}
	if v, ok := data["filename"].(string); ok {
		meta.Filename = v
	}
	if v, ok := data["directory"].(string); ok {
		meta.Directory = v
	}
	return meta
}

func parseFTPMeta(data map[string]any) *message.FTPMeta {
	meta := &message.FTPMeta{}
	if v, ok := data["filename"].(string); ok {
		meta.Filename = v
	}
	if v, ok := data["directory"].(string); ok {
		meta.Directory = v
	}
	return meta
}

func parseKafkaMeta(data map[string]any) *message.KafkaMeta {
	meta := &message.KafkaMeta{
		Headers: toStringMap(data["headers"]),
	}
	if v, ok := data["topic"].(string); ok {
		meta.Topic = v
	}
	if v, ok := data["key"].(string); ok {
		meta.Key = v
	}
	if v, ok := data["partition"].(float64); ok {
		meta.Partition = int(v)
	}
	if v, ok := data["offset"].(float64); ok {
		meta.Offset = int64(v)
	}
	return meta
}

func parseTCPMeta(data map[string]any) *message.TCPMeta {
	meta := &message.TCPMeta{}
	if v, ok := data["remoteAddr"].(string); ok {
		meta.RemoteAddr = v
	}
	return meta
}

func parseSMTPMeta(data map[string]any) *message.SMTPMeta {
	meta := &message.SMTPMeta{}
	if v, ok := data["from"].(string); ok {
		meta.From = v
	}
	if v, ok := data["to"]; ok {
		meta.To = toStringSlice(v)
	}
	if v, ok := data["subject"].(string); ok {
		meta.Subject = v
	}
	if v, ok := data["cc"]; ok {
		meta.CC = toStringSlice(v)
	}
	if v, ok := data["bcc"]; ok {
		meta.BCC = toStringSlice(v)
	}
	return meta
}

func parseDICOMMeta(data map[string]any) *message.DICOMMeta {
	meta := &message.DICOMMeta{}
	if v, ok := data["callingAE"].(string); ok {
		meta.CallingAE = v
	}
	if v, ok := data["calledAE"].(string); ok {
		meta.CalledAE = v
	}
	return meta
}

func parseDatabaseMeta(data map[string]any) *message.DatabaseMeta {
	meta := &message.DatabaseMeta{}
	if v, ok := data["query"].(string); ok {
		meta.Query = v
	}
	if v, ok := data["params"].(map[string]any); ok {
		meta.Params = v
	}
	return meta
}

func toStringMap(v any) map[string]string {
	result := make(map[string]string)
	m, ok := v.(map[string]any)
	if !ok {
		return result
	}
	for k, val := range m {
		if s, ok := val.(string); ok {
			result[k] = s
		} else {
			result[k] = fmt.Sprintf("%v", val)
		}
	}
	return result
}

func toStringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	return m
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

func isValidUTF8(data []byte) bool {
	return utf8.Valid(data)
}

// ensureValidUTF8 replaces invalid UTF-8 sequences with U+FFFD. Used as a
// last-resort safeguard when transcoding didn't happen upstream.
func ensureValidUTF8(data []byte) []byte {
	if utf8.Valid(data) {
		return data
	}
	out := make([]byte, 0, len(data))
	for len(data) > 0 {
		r, size := utf8.DecodeRune(data)
		if r == utf8.RuneError && size <= 1 {
			out = append(out, []byte(string(utf8.RuneError))...)
			data = data[1:]
		} else {
			out = append(out, data[:size]...)
			data = data[size:]
		}
	}
	return out
}
