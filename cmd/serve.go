package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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
	"github.com/intuware/intu/internal/dashboard"
	"github.com/intuware/intu/internal/message"
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

			// --- Audit ---
			var auditLogger *auth.AuditLogger
			if cfg.Audit != nil && cfg.Audit.Enabled {
				auditLogger = auth.NewAuditLogger(cfg.Audit, logger)

				var auditStore auth.AuditStore
				switch {
				case cfg.Audit.Destination == "postgres" && cfg.Runtime.Storage.PostgresDSN != "":
					pgStore, pgErr := auth.NewPostgresAuditStore(cfg.Runtime.Storage.PostgresDSN, "intu_")
					if pgErr != nil {
						logger.Warn("postgres audit store init failed, using memory", "error", pgErr)
						auditStore = auth.NewMemoryAuditStore()
					} else {
						auditStore = pgStore
						defer pgStore.Close()
					}
				default:
					auditStore = auth.NewMemoryAuditStore()
				}

				auditLogger.SetStore(auditStore)
				logger.Info("audit logger initialized", "destination", cfg.Audit.Destination)
			}

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

			if err := engine.WatchChannels(ctx); err != nil {
				logger.Warn("channel hot-reload not available", "error", err)
			}

			// --- Dashboard (embedded in serve) ---
			var dashSrv *dashboard.Server
			dashCfg := cfg.Dashboard
			if dashCfg == nil {
				dashCfg = &config.DashboardConfig{
					Enabled: true,
					Port:    3000,
					Auth: &config.DashboardAuthConfig{
						Provider: "basic",
						Username: "admin",
						Password: "admin",
					},
				}
			}

			if dashCfg.Enabled {
				port := dashCfg.Port
				if port == 0 {
					port = 3000
				}

				authMw := buildDashboardAuth(dashCfg, cfg, logger)

				channelsDir := filepath.Join(dir, cfg.ChannelsDir)

				reprocessFn := func(ctx context.Context, channelID string, rawContent []byte) error {
					msg := message.New(channelID, rawContent)
					msg.Metadata["reprocessed"] = true
					return engine.ReprocessMessage(ctx, channelID, msg)
				}

				var rbac *auth.RBACManager
				if len(cfg.Roles) > 0 {
					rbac = auth.NewRBACManager(cfg.Roles)
				}

				dashSrv = dashboard.NewServer(&dashboard.ServerConfig{
					Config:         cfg,
					ChannelsDir:    channelsDir,
					Store:          store,
					Metrics:        observability.Global(),
					Logger:         logger,
					RBAC:           rbac,
					AuditLogger:    auditLogger,
					AuthMiddleware: authMw,
					ReprocessFunc:  reprocessFn,
					Port:           port,
				})

				dashErrCh := make(chan error, 1)
				go func() {
					addr := fmt.Sprintf(":%d", port)
					if err := dashSrv.Start(addr); err != nil && err != http.ErrServerClosed {
						dashErrCh <- err
					}
				}()

				select {
				case dashErr := <-dashErrCh:
					logger.Error("dashboard failed to start", "error", dashErr)
					dashSrv = nil
				case <-time.After(500 * time.Millisecond):
					authProvider := "basic"
					if dashCfg.Auth != nil && dashCfg.Auth.Provider != "" {
						authProvider = dashCfg.Auth.Provider
					}
					fmt.Fprintf(cmd.OutOrStdout(), "Dashboard running on http://localhost:%d (auth: %s)\n", port, authProvider)
				}
			}

			fmt.Fprintln(cmd.OutOrStdout(), "intu engine running. Press Ctrl+C to stop.")

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh

			logger.Info("shutdown signal received")
			cancel()

			if dashSrv != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := dashSrv.Stop(shutCtx); err != nil {
					logger.Error("dashboard stop error", "error", err)
				}
				shutCancel()
			}

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

			if auditLogger != nil {
				if err := auditLogger.Close(); err != nil {
					logger.Error("audit logger close error", "error", err)
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

func buildDashboardAuth(dashCfg *config.DashboardConfig, cfg *config.Config, logger *slog.Logger) func(http.Handler) http.Handler {
	if dashCfg.Auth == nil {
		return dashboard.BasicAuthMiddleware("admin", "admin")
	}

	switch dashCfg.Auth.Provider {
	case "basic":
		user := dashCfg.Auth.Username
		pass := dashCfg.Auth.Password
		if user == "" {
			user = "admin"
		}
		if pass == "" {
			pass = "admin"
		}
		return dashboard.BasicAuthMiddleware(user, pass)

	case "ldap":
		if cfg.AccessControl != nil && cfg.AccessControl.LDAP != nil {
			var rbac *auth.RBACManager
			if len(cfg.Roles) > 0 {
				rbac = auth.NewRBACManager(cfg.Roles)
			}
			ldapProvider := auth.NewLDAPProvider(cfg.AccessControl.LDAP, rbac, logger)
			return auth.NewLDAPAuthMiddleware(ldapProvider)
		}
		logger.Warn("dashboard auth set to ldap but no LDAP config found, falling back to basic auth")
		return dashboard.BasicAuthMiddleware("admin", "admin")

	case "oidc":
		if cfg.AccessControl != nil && cfg.AccessControl.OIDC != nil {
			var rbac *auth.RBACManager
			if len(cfg.Roles) > 0 {
				rbac = auth.NewRBACManager(cfg.Roles)
			}
			oidcProvider, err := auth.NewOIDCProvider(cfg.AccessControl.OIDC, rbac, logger)
			if err != nil {
				logger.Warn("OIDC provider init failed, falling back to basic auth", "error", err)
				return dashboard.BasicAuthMiddleware("admin", "admin")
			}
			disableLogin := dashCfg.Auth != nil && dashCfg.Auth.DisableLoginPage
			return auth.NewOIDCAuthMiddleware(oidcProvider, disableLogin)
		}
		logger.Warn("dashboard auth set to oidc but no OIDC config found, falling back to basic auth")
		return dashboard.BasicAuthMiddleware("admin", "admin")

	case "none":
		return nil

	default:
		return dashboard.BasicAuthMiddleware("admin", "admin")
	}
}
