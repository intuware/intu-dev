package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/intuware/intu/pkg/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type OTelShutdown struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *metric.MeterProvider
}

func (s *OTelShutdown) Shutdown(ctx context.Context) error {
	var errs []error
	if s.tracerProvider != nil {
		if err := s.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("tracer provider shutdown: %w", err))
		}
	}
	if s.meterProvider != nil {
		if err := s.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("meter provider shutdown: %w", err))
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func InitOTel(cfg *config.OTelConfig) (*OTelShutdown, error) {
	if cfg == nil || !cfg.Enabled {
		return &OTelShutdown{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "intu"
	}

	attrs := []resource.Option{
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	}
	for k, v := range cfg.ResourceAttributes {
		attrs = append(attrs, resource.WithAttributes(semconv.ServiceVersionKey.String(k+"="+v)))
	}
	res, err := resource.New(ctx, attrs...)
	if err != nil {
		return nil, fmt.Errorf("create OTel resource: %w", err)
	}

	shutdown := &OTelShutdown{}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.Traces {
		tp, err := initTracerProvider(ctx, cfg, res)
		if err != nil {
			return nil, fmt.Errorf("init tracer provider: %w", err)
		}
		shutdown.tracerProvider = tp
		otel.SetTracerProvider(tp)
	}

	if cfg.Metrics {
		mp, err := initMeterProvider(ctx, cfg, res)
		if err != nil {
			return nil, fmt.Errorf("init meter provider: %w", err)
		}
		shutdown.meterProvider = mp
		otel.SetMeterProvider(mp)
	}

	return shutdown, nil
}

func initTracerProvider(ctx context.Context, cfg *config.OTelConfig, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	var opts []otlptracegrpc.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
	}
	if cfg.Protocol == "grpc-insecure" {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
	)

	return tp, nil
}

func initMeterProvider(ctx context.Context, cfg *config.OTelConfig, res *resource.Resource) (*metric.MeterProvider, error) {
	var opts []otlpmetricgrpc.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlpmetricgrpc.WithEndpoint(cfg.Endpoint))
	}
	if cfg.Protocol == "grpc-insecure" {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}

	exporter, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter,
			metric.WithInterval(10*time.Second),
		)),
		metric.WithResource(res),
	)

	return mp, nil
}

func MeterProvider() *metric.MeterProvider {
	mp, ok := otel.GetMeterProvider().(*metric.MeterProvider)
	if !ok {
		return nil
	}
	return mp
}
