package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/intuware/intu/internal/connector"
	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/internal/observability"
	"github.com/intuware/intu/internal/retry"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
)

type ChannelRuntime struct {
	ID           string
	Config       *config.ChannelConfig
	Source       connector.SourceConnector
	Destinations map[string]connector.DestinationConnector
	DestConfigs  []config.ChannelDestination
	Pipeline     *Pipeline
	Logger       *slog.Logger
	Metrics      *observability.Metrics
	Store        storage.MessageStore
	Maps         *MapVariables

	retryers map[string]*retry.Retryer
	dlq      *retry.DeadLetterQueue
	queues   map[string]*retry.DestinationQueue
}

func (cr *ChannelRuntime) initRetryAndQueue(rootCfg *config.Config) {
	cr.retryers = make(map[string]*retry.Retryer)
	cr.queues = make(map[string]*retry.DestinationQueue)

	for _, destCfg := range cr.DestConfigs {
		destName := destCfg.Name
		if destName == "" {
			destName = destCfg.Ref
		}

		if destCfg.Retry != nil && destCfg.Retry.MaxAttempts > 0 {
			cr.retryers[destName] = retry.NewRetryer(destCfg.Retry, cr.Logger)
		} else if destCfg.Ref != "" {
			if rootDest, ok := rootCfg.Destinations[destCfg.Ref]; ok && rootDest.Retry != nil {
				mapped := &config.RetryConfig{
					MaxAttempts:    rootDest.Retry.MaxAttempts,
					Backoff:        rootDest.Retry.Backoff,
					InitialDelayMs: rootDest.Retry.InitialDelayMs,
					MaxDelayMs:     rootDest.Retry.MaxDelayMs,
					Jitter:         rootDest.Retry.Jitter,
					RetryOn:        rootDest.Retry.RetryOn,
					NoRetryOn:      rootDest.Retry.NoRetryOn,
				}
				cr.retryers[destName] = retry.NewRetryer(mapped, cr.Logger)
			}
		}

		if destCfg.Queue != nil && destCfg.Queue.Enabled {
			dest := cr.Destinations[destName]
			if dest != nil {
				sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
					return dest.Send(ctx, msg)
				}
				cr.queues[destName] = retry.NewDestinationQueue(
					destName,
					destCfg.Queue.MaxSize,
					destCfg.Queue.Overflow,
					destCfg.Queue.Threads,
					sendFn,
					cr.Logger,
				)
			}
		}
	}

	if rootCfg.DeadLetter != nil && rootCfg.DeadLetter.Enabled {
		dlqDestName := rootCfg.DeadLetter.Destination
		if dlqDest, ok := cr.Destinations[dlqDestName]; ok {
			sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
				return dlqDest.Send(ctx, msg)
			}
			cr.dlq = retry.NewDeadLetterQueue(rootCfg.DeadLetter, sendFn, cr.Logger)
		} else {
			cr.dlq = retry.NewDeadLetterQueue(rootCfg.DeadLetter, nil, cr.Logger)
		}
	}
}

func (cr *ChannelRuntime) Start(ctx context.Context) error {
	cr.Logger.Info("starting channel", "id", cr.ID)
	return cr.Source.Start(ctx, cr.handleMessage)
}

func (cr *ChannelRuntime) Stop(ctx context.Context) error {
	cr.Logger.Info("stopping channel", "id", cr.ID)

	if err := cr.Source.Stop(ctx); err != nil {
		cr.Logger.Error("error stopping source", "channel", cr.ID, "error", err)
	}

	for name, q := range cr.queues {
		cr.Logger.Debug("stopping destination queue", "channel", cr.ID, "destination", name)
		q.Stop()
	}

	for name, dest := range cr.Destinations {
		if err := dest.Stop(ctx); err != nil {
			cr.Logger.Error("error stopping destination", "channel", cr.ID, "name", name, "error", err)
		}
	}

	return nil
}

func (cr *ChannelRuntime) handleMessage(ctx context.Context, msg *message.Message) error {
	startTime := time.Now()
	msg.ChannelID = cr.ID

	if cr.Metrics != nil {
		cr.Metrics.IncrReceived(cr.ID)
	}

	connectorMap := NewConnectorMap()
	if cr.Maps != nil {
		cr.Pipeline.SetMapContext(cr.Maps, connectorMap)
	}

	cr.Logger.Debug("processing message", "channel", cr.ID, "messageId", msg.ID)

	cr.storeMessage(msg, "received", "RECEIVED")

	result, err := cr.Pipeline.Execute(ctx, msg)
	if err != nil {
		if cr.Metrics != nil {
			cr.Metrics.IncrErrored(cr.ID, "pipeline")
		}
		cr.storeMessage(msg, "error", "ERROR")
		return fmt.Errorf("pipeline execute: %w", err)
	}

	if result.Filtered {
		if cr.Metrics != nil {
			cr.Metrics.IncrFiltered(cr.ID)
		}
		cr.storeMessage(msg, "filtered", "FILTERED")
		return nil
	}

	cr.storeMessageWithContent(msg, "transformed", "TRANSFORMED", result.OutputBytes)

	activeDests := cr.resolveActiveDestinations(result.RouteTo)

	var destResults []DestinationResult

	for _, destCfg := range activeDests {
		destName := destCfg.Name
		if destName == "" {
			destName = destCfg.Ref
		}

		dest, ok := cr.Destinations[destName]
		if !ok {
			cr.Logger.Warn("destination not found", "name", destName, "channel", cr.ID)
			destResults = append(destResults, DestinationResult{
				Name:    destName,
				Success: false,
				Error:   "destination not found",
			})
			continue
		}

		outMsg, filtered, err := cr.Pipeline.ExecuteDestinationPipeline(ctx, msg, result.Output, destCfg)
		if err != nil {
			cr.Logger.Error("destination pipeline error", "destination", destName, "error", err)
			if cr.Metrics != nil {
				cr.Metrics.IncrErrored(cr.ID, destName)
			}
			destResults = append(destResults, DestinationResult{
				Name:    destName,
				Success: false,
				Error:   err.Error(),
			})
			continue
		}
		if filtered {
			destResults = append(destResults, DestinationResult{
				Name:    destName,
				Success: true,
			})
			continue
		}

		destStart := time.Now()
		resp, sendErr := cr.sendToDestination(ctx, destName, dest, outMsg)
		if cr.Metrics != nil {
			cr.Metrics.RecordDestLatency(cr.ID, destName, time.Since(destStart))
		}

		if sendErr != nil {
			cr.Logger.Error("destination send failed",
				"channel", cr.ID,
				"destination", destName,
				"messageId", msg.ID,
				"error", sendErr,
			)
			if cr.Metrics != nil {
				cr.Metrics.IncrErrored(cr.ID, destName)
			}
			if cr.dlq != nil {
				cr.dlq.Send(ctx, msg, destName, sendErr)
			}
			destResults = append(destResults, DestinationResult{
				Name:    destName,
				Success: false,
				Error:   sendErr.Error(),
			})
			continue
		}

		if resp != nil {
			_ = cr.Pipeline.ExecuteResponseTransformer(ctx, msg, destCfg, resp)
		}

		success := resp == nil || resp.Error == nil
		dr := DestinationResult{
			Name:    destName,
			Success: success,
		}
		if resp != nil {
			dr.Response = resp
			if resp.Error != nil {
				dr.Error = resp.Error.Error()
				if cr.Metrics != nil {
					cr.Metrics.IncrErrored(cr.ID, destName)
				}
				if cr.dlq != nil {
					cr.dlq.Send(ctx, msg, destName, resp.Error)
				}
			}
		}
		destResults = append(destResults, dr)
	}

	if err := cr.Pipeline.ExecutePostprocessor(ctx, msg, result.Output, destResults); err != nil {
		cr.Logger.Error("postprocessor error", "channel", cr.ID, "error", err)
	}

	if cr.Metrics != nil {
		cr.Metrics.IncrProcessed(cr.ID)
		cr.Metrics.RecordLatency(cr.ID, "total", time.Since(startTime))
	}

	cr.storeMessage(msg, "sent", "SENT")

	return nil
}

func (cr *ChannelRuntime) sendToDestination(ctx context.Context, destName string, dest connector.DestinationConnector, msg *message.Message) (*message.Response, error) {
	if q, ok := cr.queues[destName]; ok {
		return nil, q.Enqueue(ctx, msg)
	}

	if r, ok := cr.retryers[destName]; ok {
		sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
			return dest.Send(ctx, msg)
		}
		return r.Execute(ctx, msg, sendFn)
	}

	return dest.Send(ctx, msg)
}

func (cr *ChannelRuntime) storeMessage(msg *message.Message, stage, status string) {
	if cr.Store == nil {
		return
	}
	record := &storage.MessageRecord{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		ChannelID:     cr.ID,
		Stage:         stage,
		Content:       msg.Raw,
		Status:        status,
		Timestamp:     time.Now(),
		Metadata:      msg.Metadata,
	}
	if err := cr.Store.Save(record); err != nil {
		cr.Logger.Warn("failed to store message", "stage", stage, "error", err)
	}
}

func (cr *ChannelRuntime) storeMessageWithContent(msg *message.Message, stage, status string, content []byte) {
	if cr.Store == nil {
		return
	}
	record := &storage.MessageRecord{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		ChannelID:     cr.ID,
		Stage:         stage,
		Content:       content,
		Status:        status,
		Timestamp:     time.Now(),
		Metadata:      msg.Metadata,
	}
	if err := cr.Store.Save(record); err != nil {
		cr.Logger.Warn("failed to store message", "stage", stage, "error", err)
	}
}

func (cr *ChannelRuntime) resolveActiveDestinations(routeTo []string) []config.ChannelDestination {
	if len(routeTo) == 0 {
		return cr.DestConfigs
	}

	routeSet := make(map[string]bool)
	for _, r := range routeTo {
		routeSet[r] = true
	}

	var active []config.ChannelDestination
	for _, d := range cr.DestConfigs {
		name := d.Name
		if name == "" {
			name = d.Ref
		}
		if routeSet[name] {
			active = append(active, d)
		}
	}
	return active
}
