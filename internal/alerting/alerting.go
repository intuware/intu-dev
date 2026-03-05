package alerting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/intuware/intu/internal/observability"
	"github.com/intuware/intu/pkg/config"
)

type AlertManager struct {
	alerts  []config.AlertConfig
	metrics *observability.Metrics
	send    AlertSendFunc
	logger  *slog.Logger
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type AlertSendFunc func(ctx context.Context, destination string, payload []byte) error

type AlertEvent struct {
	Name      string    `json:"name"`
	Trigger   string    `json:"trigger"`
	Channel   string    `json:"channel"`
	Value     int64     `json:"value"`
	Threshold int       `json:"threshold"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

func NewAlertManager(alerts []config.AlertConfig, metrics *observability.Metrics, send AlertSendFunc, logger *slog.Logger) *AlertManager {
	return &AlertManager{
		alerts:  alerts,
		metrics: metrics,
		send:    send,
		logger:  logger,
	}
}

func (am *AlertManager) Start(ctx context.Context) {
	ctx, am.cancel = context.WithCancel(ctx)
	am.wg.Add(1)
	go func() {
		defer am.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				am.evaluate(ctx)
			}
		}
	}()
	am.logger.Info("alert manager started", "rules", len(am.alerts))
}

func (am *AlertManager) Stop() {
	if am.cancel != nil {
		am.cancel()
	}
	am.wg.Wait()
}

func (am *AlertManager) evaluate(ctx context.Context) {
	for _, alert := range am.alerts {
		triggered, value := am.checkTrigger(alert)
		if !triggered {
			continue
		}

		event := AlertEvent{
			Name:      alert.Name,
			Trigger:   alert.Trigger.Type,
			Channel:   alert.Trigger.Channel,
			Value:     value,
			Threshold: alert.Trigger.Threshold,
			Timestamp: time.Now(),
			Message:   fmt.Sprintf("Alert %q triggered: %s threshold %d exceeded (current: %d)", alert.Name, alert.Trigger.Type, alert.Trigger.Threshold, value),
		}

		data, err := json.Marshal(event)
		if err != nil {
			am.logger.Error("marshal alert event failed", "error", err)
			continue
		}

		for _, dest := range alert.Destinations {
			if am.send != nil {
				if err := am.send(ctx, dest, data); err != nil {
					am.logger.Error("send alert failed", "destination", dest, "error", err)
				} else {
					am.logger.Info("alert sent", "name", alert.Name, "destination", dest)
				}
			}
		}
	}
}

func (am *AlertManager) checkTrigger(alert config.AlertConfig) (bool, int64) {
	switch alert.Trigger.Type {
	case "error_count":
		key := "messages_errored_total." + alert.Trigger.Channel
		if alert.Trigger.Channel == "*" {
			key = "messages_errored_total"
		}
		val := am.metrics.Counter(key).Load()
		return val >= int64(alert.Trigger.Threshold), val

	case "queue_depth":
		key := "queue_depth." + alert.Trigger.Channel
		val := am.metrics.Gauge(key).Load()
		return val >= int64(alert.Trigger.Threshold), val

	default:
		return false, 0
	}
}
