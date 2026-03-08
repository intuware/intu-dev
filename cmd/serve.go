package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/intuware/intu/internal/alerting"
	"github.com/intuware/intu/internal/auth"
	"github.com/intuware/intu/internal/cluster"
	"github.com/intuware/intu/internal/connector"
	"github.com/intuware/intu/internal/observability"
	"github.com/intuware/intu/internal/runtime"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the intu runtime engine",
		Long:  "Loads configuration, boots all enabled channels, and processes messages.",
		RunE: func(cmd *cobra.Command, args []string) error {
			buildLogger := logging.New(rootOpts.logLevel, nil)

			if _, err := os.Stat(filepath.Join(dir, "package.json")); err == nil {
				buildLogger.Info("building TypeScript channels")
				npm := exec.Command("npm", "run", "build")
				npm.Dir = dir
				npm.Stdout = cmd.OutOrStdout()
				npm.Stderr = cmd.ErrOrStderr()
				if err := npm.Run(); err != nil {
					return fmt.Errorf("build failed (npm run build): %w", err)
				}
				buildLogger.Info("build complete")
			}

			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			logger := logging.New(rootOpts.logLevel, cfg.Logging)
			logger.Info("config loaded", "name", cfg.Runtime.Name, "profile", profile)

			secretsProvider, err := auth.NewSecretsProvider(cfg.Secrets)
			if err != nil {
				logger.Warn("secrets provider init failed, using env fallback", "error", err)
				secretsProvider = &auth.EnvSecretsProvider{}
			} else {
				providerName := "env"
				if cfg.Secrets != nil && cfg.Secrets.Provider != "" {
					providerName = cfg.Secrets.Provider
				}
				logger.Info("secrets provider initialized", "provider", providerName)
			}
			_ = secretsProvider

			var otelShutdown *observability.OTelShutdown
			if cfg.Observability != nil && cfg.Observability.OpenTelemetry != nil && cfg.Observability.OpenTelemetry.Enabled {
				shutdown, err := observability.InitOTel(cfg.Observability.OpenTelemetry)
				if err != nil {
					logger.Warn("OpenTelemetry init failed", "error", err)
				} else {
					otelShutdown = shutdown
					logger.Info("OpenTelemetry initialized",
						"endpoint", cfg.Observability.OpenTelemetry.Endpoint,
						"traces", cfg.Observability.OpenTelemetry.Traces,
						"metrics", cfg.Observability.OpenTelemetry.Metrics,
					)
				}
			}

			var promServer *observability.PrometheusServer
			if cfg.Observability != nil && cfg.Observability.Prometheus != nil && cfg.Observability.Prometheus.Enabled {
				ps, err := observability.NewPrometheusServer(cfg.Observability.Prometheus, logger)
				if err != nil {
					logger.Warn("Prometheus server init failed", "error", err)
				} else if ps != nil {
					promServer = ps
					if err := promServer.Start(); err != nil {
						logger.Warn("Prometheus server start failed", "error", err)
					} else {
						logger.Info("Prometheus metrics server started",
							"port", cfg.Observability.Prometheus.Port,
							"path", cfg.Observability.Prometheus.Path,
						)
					}
				}
			}

			if cfg.Runtime.Health != nil {
				hc := cluster.NewHealthChecker(cfg.Runtime.Health, logger)
				if err := hc.Start(); err != nil {
					logger.Warn("health check server failed to start", "error", err)
				}
			}

			store, err := storage.NewMessageStore(cfg.MessageStorage)
			if err != nil {
				logger.Warn("message store init failed, using memory store", "error", err)
				store = storage.NewMemoryStore()
			}
			storeDriver := "memory"
			storeMode := "full"
			if cfg.MessageStorage != nil {
				if cfg.MessageStorage.Driver != "" {
					storeDriver = cfg.MessageStorage.Driver
				}
				if cfg.MessageStorage.Mode != "" {
					storeMode = cfg.MessageStorage.Mode
				}
			}
			logger.Info("message store initialized", "driver", storeDriver, "mode", storeMode)

			factory := connector.NewFactory(logger)
			engine := runtime.NewDefaultEngine(dir, cfg, factory, logger)
			engine.SetMessageStore(store)

			runtimeMode := cfg.Runtime.Mode
			if runtimeMode == "" {
				runtimeMode = "standalone"
			}

			var redisCluster *cluster.RedisClient
			if runtimeMode == "cluster" && cfg.Cluster != nil && cfg.Cluster.Enabled {
				if cfg.Cluster.Coordination != nil && cfg.Cluster.Coordination.Redis != nil {
					rc, err := cluster.NewRedisClient(cfg.Cluster.Coordination.Redis)
					if err != nil {
						return fmt.Errorf("redis client init: %w", err)
					}
					redisCluster = rc
					defer rc.Close()

					coord := cluster.NewRedisCoordinator(rc, cfg.Cluster, logger)
					engine.SetCoordinator(coord)
					engine.SetRedisClient(rc.Client(), rc.Key())

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					if err := coord.Start(ctx); err != nil {
						return fmt.Errorf("coordinator start: %w", err)
					}

					if cfg.Cluster.Deduplication != nil && cfg.Cluster.Deduplication.Enabled {
						window := 5 * time.Minute
						if cfg.Cluster.Deduplication.Window != "" {
							if d, err := time.ParseDuration(cfg.Cluster.Deduplication.Window); err == nil {
								window = d
							}
						}

						if cfg.Cluster.Deduplication.Store == "redis" {
							dedup := cluster.NewRedisDeduplicator(rc, window)
							engine.SetDeduplicator(dedup)
						} else {
							dedup := cluster.NewDeduplicator(window)
							engine.SetDeduplicator(dedup)
						}
					}

					logger.Info("cluster mode enabled",
						"instanceID", cfg.Cluster.InstanceID,
						"redis", cfg.Cluster.Coordination.Redis.Address,
					)
				}
			} else {
				coord := cluster.NewCoordinator(cfg.Cluster, logger)
				engine.SetCoordinator(coord)
			}

			_ = redisCluster

			if len(cfg.Alerts) > 0 {
				metrics := engine.Metrics()
				alertSend := func(ctx context.Context, destination string, payload []byte) error {
					logger.Info("alert fired", "destination", destination, "payload", string(payload))
					return nil
				}
				am := alerting.NewAlertManager(cfg.Alerts, metrics, alertSend, logger)
				engine.SetAlertManager(am)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := engine.Start(ctx); err != nil {
				return fmt.Errorf("engine start: %w", err)
			}

			fmt.Fprintln(cmd.OutOrStdout(), "intu engine running. Press Ctrl+C to stop.")

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh

			logger.Info("shutdown signal received")
			cancel()

			if err := engine.Stop(context.Background()); err != nil {
				logger.Error("engine stop error", "error", err)
			}

			if promServer != nil {
				if err := promServer.Stop(); err != nil {
					logger.Error("Prometheus server stop error", "error", err)
				}
			}

			if otelShutdown != nil {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer shutdownCancel()
				if err := otelShutdown.Shutdown(shutdownCtx); err != nil {
					logger.Error("OpenTelemetry shutdown error", "error", err)
				}
			}

			fmt.Fprintln(cmd.OutOrStdout(), "intu engine stopped.")
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}
