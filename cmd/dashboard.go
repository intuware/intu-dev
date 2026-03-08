package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/intuware/intu/internal/auth"
	"github.com/intuware/intu/internal/dashboard"
	"github.com/intuware/intu/internal/observability"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

func newDashboardCmd() *cobra.Command {
	var dir, profile string
	var port int

	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Start the web dashboard",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.New(rootOpts.logLevel, nil)
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)

			var store storage.MessageStore
			if cfg.MessageStorage != nil {
				store, err = storage.NewMessageStore(cfg.MessageStorage)
				if err != nil {
					logger.Warn("message store init failed", "error", err)
				}
			}

			var rbac *auth.RBACManager
			if len(cfg.Roles) > 0 {
				rbac = auth.NewRBACManager(cfg.Roles)
			}

			var auditLogger *auth.AuditLogger
			if cfg.Audit != nil {
				auditLogger = auth.NewAuditLogger(cfg.Audit, logger)
			}

			var authMiddleware = buildAuthMiddleware(cfg, logger)

			srv := dashboard.NewServer(&dashboard.ServerConfig{
				Config:         cfg,
				ChannelsDir:    channelsDir,
				Store:          store,
				Metrics:        observability.Global(),
				Logger:         logger,
				RBAC:           rbac,
				AuditLogger:    auditLogger,
				AuthMiddleware: authMiddleware,
				Port:           port,
			})

			addr := fmt.Sprintf(":%d", port)
			fmt.Fprintf(cmd.OutOrStdout(), "Dashboard running at http://localhost:%d\n", port)
			return srv.Start(addr)
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().IntVar(&port, "port", 3000, "Dashboard port")
	return cmd
}
