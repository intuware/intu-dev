package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/intuware/intu/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

type PrometheusServer struct {
	server *http.Server
	logger *slog.Logger
}

func NewPrometheusServer(cfg *config.PrometheusConfig, logger *slog.Logger) (*PrometheusServer, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	port := cfg.Port
	if port == 0 {
		port = 9090
	}
	path := cfg.Path
	if path == "" {
		path = "/metrics"
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	exporter, err := otelprometheus.New(
		otelprometheus.WithRegisterer(registry),
	)
	if err != nil {
		return nil, fmt.Errorf("create Prometheus OTel exporter: %w", err)
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(exporter),
	)

	initOTelMetricsInstruments(mp)

	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return &PrometheusServer{
		server: server,
		logger: logger,
	}, nil
}

func (ps *PrometheusServer) Start() error {
	ps.logger.Info("starting Prometheus metrics server", "addr", ps.server.Addr)
	go func() {
		if err := ps.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			ps.logger.Error("Prometheus server error", "error", err)
		}
	}()
	return nil
}

func (ps *PrometheusServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ps.server.Shutdown(ctx)
}
