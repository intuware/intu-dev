package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/intuware/intu/internal/alerting"
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
			logger := logging.New(rootOpts.logLevel)

			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			logger.Info("config loaded", "name", cfg.Runtime.Name, "profile", profile)

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

			metrics := observability.Global()

			factory := connector.NewFactory(logger)
			engine := runtime.NewDefaultEngine(dir, cfg, factory, logger)
			engine.SetMessageStore(store)

			if len(cfg.Alerts) > 0 {
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

			fmt.Fprintln(cmd.OutOrStdout(), "intu engine stopped.")
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}
