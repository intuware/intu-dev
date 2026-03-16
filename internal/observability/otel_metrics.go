package observability

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
)

type OTelMetrics struct {
	receivedCounter  otelmetric.Int64Counter
	processedCounter otelmetric.Int64Counter
	erroredCounter   otelmetric.Int64Counter
	filteredCounter  otelmetric.Int64Counter
	durationHist     otelmetric.Float64Histogram
	destLatencyHist  otelmetric.Float64Histogram
	queueDepthGauge  otelmetric.Int64UpDownCounter
}

var (
	globalOTelMetrics *OTelMetrics
	otelMetricsOnce   sync.Once
)

func initOTelMetricsInstruments(mp *metric.MeterProvider) {
	otelMetricsOnce.Do(func() {
		meter := mp.Meter("intu")
		om := &OTelMetrics{}

		om.receivedCounter, _ = meter.Int64Counter("intu_messages_received_total",
			otelmetric.WithDescription("Total messages received"),
		)
		om.processedCounter, _ = meter.Int64Counter("intu_messages_processed_total",
			otelmetric.WithDescription("Total messages processed"),
		)
		om.erroredCounter, _ = meter.Int64Counter("intu_messages_errored_total",
			otelmetric.WithDescription("Total messages errored"),
		)
		om.filteredCounter, _ = meter.Int64Counter("intu_messages_filtered_total",
			otelmetric.WithDescription("Total messages filtered"),
		)
		om.durationHist, _ = meter.Float64Histogram("intu_message_duration_ms",
			otelmetric.WithDescription("Message processing duration in milliseconds"),
			otelmetric.WithExplicitBucketBoundaries(0.1, 0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000),
		)
		om.destLatencyHist, _ = meter.Float64Histogram("intu_destination_latency_ms",
			otelmetric.WithDescription("Destination send latency in milliseconds"),
			otelmetric.WithExplicitBucketBoundaries(0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000),
		)
		om.queueDepthGauge, _ = meter.Int64UpDownCounter("intu_queue_depth",
			otelmetric.WithDescription("Current depth of destination queues"),
		)

		globalOTelMetrics = om
	})
}

func GetOTelMetrics() *OTelMetrics {
	return globalOTelMetrics
}

func (om *OTelMetrics) IncrReceived(channel string) {
	if om == nil {
		return
	}
	om.receivedCounter.Add(context.Background(), 1,
		otelmetric.WithAttributes(attribute.String("channel", channel)),
	)
}

func (om *OTelMetrics) IncrProcessed(channel string) {
	if om == nil {
		return
	}
	om.processedCounter.Add(context.Background(), 1,
		otelmetric.WithAttributes(attribute.String("channel", channel)),
	)
}

func (om *OTelMetrics) IncrErrored(channel, destination string) {
	if om == nil {
		return
	}
	om.erroredCounter.Add(context.Background(), 1,
		otelmetric.WithAttributes(
			attribute.String("channel", channel),
			attribute.String("destination", destination),
		),
	)
}

func (om *OTelMetrics) IncrFiltered(channel string) {
	if om == nil {
		return
	}
	om.filteredCounter.Add(context.Background(), 1,
		otelmetric.WithAttributes(attribute.String("channel", channel)),
	)
}

func (om *OTelMetrics) RecordLatency(channel, stage string, d time.Duration) {
	if om == nil {
		return
	}
	ms := float64(d.Microseconds()) / 1000.0
	om.durationHist.Record(context.Background(), ms,
		otelmetric.WithAttributes(
			attribute.String("channel", channel),
			attribute.String("stage", stage),
		),
	)
}

func (om *OTelMetrics) RecordDestLatency(channel, destination string, d time.Duration) {
	if om == nil {
		return
	}
	ms := float64(d.Microseconds()) / 1000.0
	om.destLatencyHist.Record(context.Background(), ms,
		otelmetric.WithAttributes(
			attribute.String("channel", channel),
			attribute.String("destination", destination),
		),
	)
}

func (om *OTelMetrics) SetQueueDepth(channel, destination string, depth int64) {
	if om == nil {
		return
	}
	om.queueDepthGauge.Add(context.Background(), depth,
		otelmetric.WithAttributes(
			attribute.String("channel", channel),
			attribute.String("destination", destination),
		),
	)
}
