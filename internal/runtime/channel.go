package runtime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/intuware/intu/internal/connector"
	"github.com/intuware/intu/internal/message"
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

	for name, dest := range cr.Destinations {
		if err := dest.Stop(ctx); err != nil {
			cr.Logger.Error("error stopping destination", "channel", cr.ID, "name", name, "error", err)
		}
	}

	return nil
}

func (cr *ChannelRuntime) handleMessage(ctx context.Context, msg *message.Message) error {
	msg.ChannelID = cr.ID
	cr.Logger.Debug("processing message", "channel", cr.ID, "messageId", msg.ID)

	result, err := cr.Pipeline.Execute(ctx, msg)
	if err != nil {
		return fmt.Errorf("pipeline execute: %w", err)
	}

	if result.Filtered {
		return nil
	}

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

		resp, err := dest.Send(ctx, outMsg)
		if err != nil {
			cr.Logger.Error("destination send failed",
				"channel", cr.ID,
				"destination", destName,
				"messageId", msg.ID,
				"error", err,
			)
			destResults = append(destResults, DestinationResult{
				Name:    destName,
				Success: false,
				Error:   err.Error(),
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
			}
		}
		destResults = append(destResults, dr)
	}

	if err := cr.Pipeline.ExecutePostprocessor(ctx, msg, result.Output, destResults); err != nil {
		cr.Logger.Error("postprocessor error", "channel", cr.ID, "error", err)
	}

	return nil
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
