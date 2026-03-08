package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/intuware/intu/internal/observability"
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

			mux := http.NewServeMux()

			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				fmt.Fprint(w, dashboardHTML)
			})

			mux.HandleFunc("/api/channels", func(w http.ResponseWriter, r *http.Request) {
				channels := listChannelsAPI(channelsDir)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(channels)
			})

			mux.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
				snap := observability.Global().Snapshot()
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(snap)
			})

			addr := fmt.Sprintf(":%d", port)
			logger.Info("dashboard starting", "addr", addr)
			fmt.Fprintf(cmd.OutOrStdout(), "Dashboard running at http://localhost:%d\n", port)

			return http.ListenAndServe(addr, mux)
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().IntVar(&port, "port", 3000, "Dashboard port")
	return cmd
}

func listChannelsAPI(channelsDir string) []map[string]any {
	var channels []map[string]any
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		return channels
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		chCfg, err := config.LoadChannelConfig(filepath.Join(channelsDir, e.Name()))
		if err != nil {
			continue
		}
		ch := map[string]any{
			"id":       chCfg.ID,
			"enabled":  chCfg.Enabled,
			"listener": chCfg.Listener.Type,
		}
		if len(chCfg.Tags) > 0 {
			ch["tags"] = chCfg.Tags
		}
		if chCfg.Group != "" {
			ch["group"] = chCfg.Group
		}
		destNames := []string{}
		for _, d := range chCfg.Destinations {
			n := d.Name
			if n == "" {
				n = d.Ref
			}
			destNames = append(destNames, n)
		}
		ch["destinations"] = destNames
		channels = append(channels, ch)
	}
	return channels
}

const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
  <title>intu Dashboard</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; }
    .header { background: #1e293b; padding: 20px 32px; border-bottom: 1px solid #334155; }
    .header h1 { font-size: 1.5rem; color: #38bdf8; }
    .container { max-width: 1200px; margin: 32px auto; padding: 0 32px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 16px; }
    .card { background: #1e293b; border-radius: 12px; padding: 24px; border: 1px solid #334155; }
    .card h3 { color: #38bdf8; margin-bottom: 12px; font-size: 1.1rem; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    .badge-enabled { background: #065f46; color: #6ee7b7; }
    .badge-disabled { background: #7f1d1d; color: #fca5a5; }
    .detail { color: #94a3b8; font-size: 0.875rem; margin-top: 8px; }
    .section { margin-top: 32px; }
    .section h2 { color: #f1f5f9; margin-bottom: 16px; }
    pre { background: #0f172a; padding: 16px; border-radius: 8px; overflow-x: auto; font-size: 0.8rem; color: #94a3b8; }
  </style>
</head>
<body>
  <div class="header"><h1>intu Dashboard</h1></div>
  <div class="container">
    <div class="section"><h2>Channels</h2><div class="cards" id="channels"></div></div>
    <div class="section"><h2>Metrics</h2><pre id="metrics">Loading...</pre></div>
  </div>
  <script>
    fetch('/api/channels').then(r=>r.json()).then(chs=>{
      const el=document.getElementById('channels');
      if(!chs||!chs.length){el.innerHTML='<p>No channels found.</p>';return;}
      el.innerHTML=chs.map(c=>'<div class="card"><h3>'+c.id+' <span class="badge '+(c.enabled?'badge-enabled':'badge-disabled')+'">'+(c.enabled?'enabled':'disabled')+'</span></h3><div class="detail">Listener: '+c.listener+'</div><div class="detail">Destinations: '+(c.destinations||[]).join(', ')+'</div>'+(c.tags?'<div class="detail">Tags: '+c.tags.join(', ')+'</div>':'')+'</div>').join('');
    });
    fetch('/api/metrics').then(r=>r.json()).then(m=>{
      document.getElementById('metrics').textContent=JSON.stringify(m,null,2);
    });
  </script>
</body>
</html>`
