package dashboard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu/internal/auth"
	"github.com/intuware/intu/internal/observability"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
)

type ReprocessFunc func(ctx context.Context, channelID string, rawContent []byte) error

type Server struct {
	cfg         *config.Config
	channelsDir string
	store       storage.MessageStore
	metrics     *observability.Metrics
	logger      *slog.Logger
	rbac        *auth.RBACManager
	auditLogger *auth.AuditLogger
	authMw      func(http.Handler) http.Handler
	reprocessFn ReprocessFunc
	server      *http.Server
	mu          sync.RWMutex
}

type ServerConfig struct {
	Config         *config.Config
	ChannelsDir    string
	Store          storage.MessageStore
	Metrics        *observability.Metrics
	Logger         *slog.Logger
	RBAC           *auth.RBACManager
	AuditLogger    *auth.AuditLogger
	AuthMiddleware func(http.Handler) http.Handler
	ReprocessFunc  ReprocessFunc
	Port           int
}

func NewServer(scfg *ServerConfig) *Server {
	s := &Server{
		cfg:         scfg.Config,
		channelsDir: scfg.ChannelsDir,
		store:       scfg.Store,
		metrics:     scfg.Metrics,
		logger:      scfg.Logger,
		rbac:        scfg.RBAC,
		auditLogger: scfg.AuditLogger,
		authMw:      scfg.AuthMiddleware,
		reprocessFn: scfg.ReprocessFunc,
	}

	if s.metrics == nil {
		s.metrics = observability.Global()
	}

	return s
}

func (s *Server) BuildHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/channels", s.handleChannels)
	mux.HandleFunc("/api/metrics", s.handleMetrics)
	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/messages/", s.handleMessageByID)
	mux.HandleFunc("/api/channels/", s.handleChannelAction)

	var handler http.Handler = mux
	if s.authMw != nil {
		handler = s.authMw(handler)
	}

	return handler
}

func (s *Server) Start(addr string) error {
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.BuildHandler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("dashboard listen %s: %w", addr, err)
	}
	s.logger.Info("dashboard listening", "addr", ln.Addr().String())
	return s.server.Serve(ln)
}

func (s *Server) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, dashboardSPA)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	channels := s.listChannels()
	totalChannels := len(channels)
	activeChannels := 0
	for _, ch := range channels {
		if enabled, ok := ch["enabled"].(bool); ok && enabled {
			activeChannels++
		}
	}

	result := map[string]any{
		"total_channels":  totalChannels,
		"active_channels": activeChannels,
	}

	msgCounts := map[string]int{}
	if s.store != nil {
		now := time.Now()
		windows := []struct {
			key string
			dur time.Duration
		}{
			{"last_60s", 60 * time.Second},
			{"last_5m", 5 * time.Minute},
			{"last_1h", time.Hour},
			{"last_24h", 24 * time.Hour},
		}
		for _, w := range windows {
			records, err := s.store.Query(storage.QueryOpts{
				Since: now.Add(-w.dur),
				Limit: 100000,
			})
			if err == nil {
				msgCounts[w.key] = len(records)
			}
		}
	}
	result["message_counts"] = msgCounts

	snap := s.metrics.Snapshot()
	var volume []map[string]any
	if counters, ok := snap["counters"].(map[string]int64); ok {
		receivedPrefix := "messages_received_total."
		processedPrefix := "messages_processed_total."
		erroredPrefix := "messages_errored_total."

		channelReceived := map[string]int64{}
		channelProcessed := map[string]int64{}
		channelErrored := map[string]int64{}

		for k, v := range counters {
			if strings.HasPrefix(k, receivedPrefix) {
				ch := strings.TrimPrefix(k, receivedPrefix)
				channelReceived[ch] = v
			} else if strings.HasPrefix(k, processedPrefix) {
				ch := strings.TrimPrefix(k, processedPrefix)
				channelProcessed[ch] = v
			} else if strings.HasPrefix(k, erroredPrefix) {
				rest := strings.TrimPrefix(k, erroredPrefix)
				parts := strings.SplitN(rest, ".", 2)
				ch := parts[0]
				channelErrored[ch] += v
			}
		}

		allCh := map[string]bool{}
		for ch := range channelReceived {
			allCh[ch] = true
		}
		for ch := range channelProcessed {
			allCh[ch] = true
		}

		for ch := range allCh {
			volume = append(volume, map[string]any{
				"channel":   ch,
				"received":  channelReceived[ch],
				"processed": channelProcessed[ch],
				"errored":   channelErrored[ch],
			})
		}
	}
	result["channel_volume"] = volume

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	channels := s.listChannels()
	writeJSON(w, http.StatusOK, channels)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snap := s.metrics.Snapshot()
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.store == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}

	q := r.URL.Query()
	opts := storage.QueryOpts{
		ChannelID: q.Get("channel"),
		Status:    q.Get("status"),
		Limit:     50,
	}

	if lim := q.Get("limit"); lim != "" {
		var l int
		if _, err := fmt.Sscanf(lim, "%d", &l); err == nil && l > 0 {
			opts.Limit = l
		}
	}

	if off := q.Get("offset"); off != "" {
		var o int
		if _, err := fmt.Sscanf(off, "%d", &o); err == nil && o >= 0 {
			opts.Offset = o
		}
	}

	if since := q.Get("since"); since != "" {
		if t, err := time.Parse(time.RFC3339, since); err == nil {
			opts.Since = t
		} else if t, err := time.Parse("2006-01-02", since); err == nil {
			opts.Since = t
		}
	}

	if before := q.Get("before"); before != "" {
		if t, err := time.Parse(time.RFC3339, before); err == nil {
			opts.Before = t
		} else if t, err := time.Parse("2006-01-02", before); err == nil {
			opts.Before = t
		}
	}

	records, err := s.store.Query(opts)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, records)
}

func (s *Server) handleMessageByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/messages/")
	if id == "" {
		http.Error(w, "message ID required", http.StatusBadRequest)
		return
	}

	parts := strings.Split(id, "/")
	msgID := parts[0]

	if len(parts) == 2 && parts[1] == "reprocess" {
		s.handleReprocess(w, r, msgID)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.store == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "store not configured"})
		return
	}

	record, err := s.store.Get(msgID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "message not found"})
		return
	}

	allStages, err := s.store.Query(storage.QueryOpts{
		Limit: 100,
	})
	if err != nil {
		writeJSON(w, http.StatusOK, record)
		return
	}

	var stages []*storage.MessageRecord
	for _, rec := range allStages {
		if rec.ID == msgID || rec.CorrelationID == record.CorrelationID {
			stages = append(stages, rec)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message": record,
		"stages":  stages,
	})
}

func (s *Server) handleReprocess(w http.ResponseWriter, r *http.Request, msgID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.store == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message store not configured"})
		return
	}

	record, err := s.store.Get(msgID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "message not found"})
		return
	}

	if s.reprocessFn == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "reprocessing not available (engine not running)"})
		return
	}

	if err := s.reprocessFn(r.Context(), record.ChannelID, record.Content); err != nil {
		s.logger.Error("reprocess failed via dashboard",
			"originalID", msgID,
			"channel", record.ChannelID,
			"error", err,
		)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if s.auditLogger != nil {
		s.auditLogger.Log("message.reprocess", "dashboard", map[string]any{
			"original_id": msgID,
			"channel":     record.ChannelID,
		})
	}

	s.logger.Info("message reprocessed via dashboard",
		"originalID", msgID,
		"channel", record.ChannelID,
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"reprocessed":         true,
		"original_message_id": msgID,
		"channel":             record.ChannelID,
	})
}

func (s *Server) handleChannelAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/channels/")
	parts := strings.SplitN(path, "/", 2)

	if len(parts) < 2 {
		if r.Method == http.MethodGet {
			s.handleChannelDetail(w, r, parts[0])
			return
		}
		http.Error(w, "action required (deploy/undeploy/restart)", http.StatusBadRequest)
		return
	}

	channelID := parts[0]
	action := parts[1]

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	switch action {
	case "deploy":
		s.setChannelState(w, channelID, true, "deployed")
	case "undeploy":
		s.setChannelState(w, channelID, false, "undeployed")
	case "restart":
		s.setChannelState(w, channelID, false, "restarting")
		s.setChannelState(w, channelID, true, "restarted")
	default:
		http.Error(w, "unknown action: "+action, http.StatusBadRequest)
	}
}

func (s *Server) handleChannelDetail(w http.ResponseWriter, r *http.Request, channelID string) {
	channelDir := filepath.Join(s.channelsDir, channelID)
	chCfg, err := config.LoadChannelConfig(channelDir)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "channel not found"})
		return
	}

	result := map[string]any{
		"id":      chCfg.ID,
		"enabled": chCfg.Enabled,
	}

	result["listener"] = listenerConfigMap(chCfg.Listener)

	var dests []map[string]any
	for _, d := range chCfg.Destinations {
		dm := map[string]any{}
		name := d.Name
		if name == "" {
			name = d.Ref
		}
		dm["name"] = name
		if d.Type != "" {
			dm["type"] = d.Type
		} else if d.Ref != "" {
			dm["type"] = "ref"
		}
		dm["config"] = destinationConfigMap(d)
		dests = append(dests, dm)
	}
	result["destinations"] = dests

	if len(chCfg.Tags) > 0 {
		result["tags"] = chCfg.Tags
	}
	if chCfg.Group != "" {
		result["group"] = chCfg.Group
	}
	if chCfg.Priority != "" {
		result["priority"] = chCfg.Priority
	}
	if chCfg.Pipeline != nil {
		pipe := map[string]any{}
		if chCfg.Pipeline.Preprocessor != "" {
			pipe["preprocessor"] = chCfg.Pipeline.Preprocessor
		}
		if chCfg.Pipeline.Validator != "" {
			pipe["validator"] = chCfg.Pipeline.Validator
		}
		if chCfg.Pipeline.SourceFilter != "" {
			pipe["source_filter"] = chCfg.Pipeline.SourceFilter
		}
		if chCfg.Pipeline.Transformer != "" {
			pipe["transformer"] = chCfg.Pipeline.Transformer
		}
		if chCfg.Pipeline.Postprocessor != "" {
			pipe["postprocessor"] = chCfg.Pipeline.Postprocessor
		}
		result["pipeline"] = pipe
	}
	if chCfg.DataTypes != nil {
		dt := map[string]any{}
		if chCfg.DataTypes.Inbound != "" {
			dt["inbound"] = chCfg.DataTypes.Inbound
		}
		if chCfg.DataTypes.Outbound != "" {
			dt["outbound"] = chCfg.DataTypes.Outbound
		}
		result["data_types"] = dt
	}

	snap := s.metrics.Snapshot()
	channelMetrics := map[string]any{}
	if counters, ok := snap["counters"].(map[string]int64); ok {
		for key, v := range counters {
			if strings.Contains(key, channelID) {
				channelMetrics[key] = v
			}
		}
	}
	if timings, ok := snap["timings"].(map[string]map[string]any); ok {
		for key, v := range timings {
			if strings.Contains(key, channelID) {
				channelMetrics[key] = v
			}
		}
	}
	if len(channelMetrics) > 0 {
		result["metrics"] = channelMetrics
	}

	writeJSON(w, http.StatusOK, result)
}

func listenerConfigMap(l config.ListenerConfig) map[string]any {
	m := map[string]any{"type": l.Type}
	var cfg map[string]any

	switch l.Type {
	case "http":
		if h := l.HTTP; h != nil {
			cfg = map[string]any{"port": h.Port}
			if h.Path != "" {
				cfg["path"] = h.Path
			}
			if len(h.Methods) > 0 {
				cfg["methods"] = h.Methods
			}
		}
	case "tcp":
		if t := l.TCP; t != nil {
			cfg = map[string]any{"port": t.Port}
			if t.Mode != "" {
				cfg["mode"] = t.Mode
			}
			if t.MaxConnections > 0 {
				cfg["max_connections"] = t.MaxConnections
			}
			if t.TimeoutMs > 0 {
				cfg["timeout_ms"] = t.TimeoutMs
			}
		}
	case "sftp":
		if s := l.SFTP; s != nil {
			cfg = map[string]any{}
			if s.Host != "" {
				cfg["host"] = s.Host
			}
			if s.Port > 0 {
				cfg["port"] = s.Port
			}
			if s.Directory != "" {
				cfg["directory"] = s.Directory
			}
			if s.PollInterval != "" {
				cfg["poll_interval"] = s.PollInterval
			}
			if s.FilePattern != "" {
				cfg["file_pattern"] = s.FilePattern
			}
			if s.MoveTo != "" {
				cfg["move_to"] = s.MoveTo
			}
		}
	case "file":
		if f := l.File; f != nil {
			cfg = map[string]any{}
			if f.Directory != "" {
				cfg["directory"] = f.Directory
			}
			if f.PollInterval != "" {
				cfg["poll_interval"] = f.PollInterval
			}
			if f.FilePattern != "" {
				cfg["file_pattern"] = f.FilePattern
			}
			if f.Scheme != "" {
				cfg["scheme"] = f.Scheme
			}
			if f.MoveTo != "" {
				cfg["move_to"] = f.MoveTo
			}
		}
	case "kafka":
		if k := l.Kafka; k != nil {
			cfg = map[string]any{}
			if k.Topic != "" {
				cfg["topic"] = k.Topic
			}
			if k.GroupID != "" {
				cfg["group_id"] = k.GroupID
			}
			if len(k.Brokers) > 0 {
				cfg["brokers"] = k.Brokers
			}
		}
	case "database":
		if d := l.Database; d != nil {
			cfg = map[string]any{}
			if d.Driver != "" {
				cfg["driver"] = d.Driver
			}
			if d.PollInterval != "" {
				cfg["poll_interval"] = d.PollInterval
			}
		}
	case "channel":
		if c := l.Channel; c != nil {
			cfg = map[string]any{}
			if c.SourceChannelID != "" {
				cfg["source_channel_id"] = c.SourceChannelID
			}
		}
	case "dicom":
		if d := l.DICOM; d != nil {
			cfg = map[string]any{"port": d.Port}
			if d.AETitle != "" {
				cfg["ae_title"] = d.AETitle
			}
		}
	case "fhir":
		if f := l.FHIR; f != nil {
			cfg = map[string]any{"port": f.Port}
			if f.BasePath != "" {
				cfg["base_path"] = f.BasePath
			}
			if f.Version != "" {
				cfg["version"] = f.Version
			}
		}
	case "email":
		if e := l.Email; e != nil {
			cfg = map[string]any{}
			if e.Host != "" {
				cfg["host"] = e.Host
			}
			if e.Port > 0 {
				cfg["port"] = e.Port
			}
			if e.Protocol != "" {
				cfg["protocol"] = e.Protocol
			}
			if e.PollInterval != "" {
				cfg["poll_interval"] = e.PollInterval
			}
			if e.Folder != "" {
				cfg["folder"] = e.Folder
			}
		}
	case "soap":
		if s := l.SOAP; s != nil {
			cfg = map[string]any{"port": s.Port}
			if s.ServiceName != "" {
				cfg["service_name"] = s.ServiceName
			}
		}
	case "ihe":
		if i := l.IHE; i != nil {
			cfg = map[string]any{"port": i.Port}
			if i.Profile != "" {
				cfg["profile"] = i.Profile
			}
		}
	}

	if cfg != nil {
		m["config"] = cfg
	}
	return m
}

func destinationConfigMap(d config.ChannelDestination) map[string]any {
	cfg := map[string]any{}
	if d.HTTP != nil {
		if d.HTTP.URL != "" {
			cfg["url"] = d.HTTP.URL
		}
		if d.HTTP.Method != "" {
			cfg["method"] = d.HTTP.Method
		}
		if d.HTTP.TimeoutMs > 0 {
			cfg["timeout_ms"] = d.HTTP.TimeoutMs
		}
	}
	if d.TCP != nil {
		if d.TCP.Host != "" {
			cfg["host"] = d.TCP.Host
		}
		if d.TCP.Port > 0 {
			cfg["port"] = d.TCP.Port
		}
	}
	if d.File != nil {
		if d.File.Directory != "" {
			cfg["directory"] = d.File.Directory
		}
		if d.File.FilenamePattern != "" {
			cfg["filename_pattern"] = d.File.FilenamePattern
		}
	}
	if d.Kafka != nil {
		if d.Kafka.Topic != "" {
			cfg["topic"] = d.Kafka.Topic
		}
	}
	if d.Database != nil {
		if d.Database.Driver != "" {
			cfg["driver"] = d.Database.Driver
		}
	}
	if d.ChannelDest != nil {
		if d.ChannelDest.TargetChannelID != "" {
			cfg["target_channel_id"] = d.ChannelDest.TargetChannelID
		}
	}
	if d.FHIR != nil {
		if d.FHIR.BaseURL != "" {
			cfg["base_url"] = d.FHIR.BaseURL
		}
		if d.FHIR.Version != "" {
			cfg["version"] = d.FHIR.Version
		}
	}
	if d.SMTP != nil {
		if d.SMTP.Host != "" {
			cfg["smtp_host"] = d.SMTP.Host
		}
		if len(d.SMTP.To) > 0 {
			cfg["to"] = d.SMTP.To
		}
	}
	if d.Filter != "" {
		cfg["filter"] = d.Filter
	}
	if d.TransformerFile != "" {
		cfg["transformer"] = d.TransformerFile
	}
	return cfg
}

func (s *Server) setChannelState(w http.ResponseWriter, channelID string, enabled bool, action string) {
	channelPath := filepath.Join(s.channelsDir, channelID, "channel.yaml")
	if _, err := os.Stat(channelPath); os.IsNotExist(err) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "channel not found"})
		return
	}

	if err := setChannelEnabledDashboard(s.channelsDir, channelID, enabled); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if s.auditLogger != nil {
		s.auditLogger.Log("channel."+action, "dashboard", map[string]any{
			"channel": channelID,
			"enabled": enabled,
		})
	}

	s.logger.Info("channel state changed via dashboard",
		"channel", channelID,
		"action", action,
		"enabled", enabled,
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"channel": channelID,
		"action":  action,
		"enabled": enabled,
	})
}

func (s *Server) listChannels() []map[string]any {
	var channels []map[string]any
	entries, err := os.ReadDir(s.channelsDir)
	if err != nil {
		return channels
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		chCfg, err := config.LoadChannelConfig(filepath.Join(s.channelsDir, e.Name()))
		if err != nil {
			continue
		}
		ch := map[string]any{
			"id":            chCfg.ID,
			"enabled":       chCfg.Enabled,
			"listener":      chCfg.Listener.Type,
			"listener_type": chCfg.Listener.Type,
		}
		if len(chCfg.Tags) > 0 {
			ch["tags"] = chCfg.Tags
		}
		if chCfg.Group != "" {
			ch["group"] = chCfg.Group
		}
		destNames := []string{}
		destTypes := []string{}
		for _, d := range chCfg.Destinations {
			n := d.Name
			if n == "" {
				n = d.Ref
			}
			destNames = append(destNames, n)
			if d.Type != "" {
				destTypes = append(destTypes, d.Type)
			} else if d.Ref != "" {
				destTypes = append(destTypes, "ref")
			}
		}
		ch["destinations"] = destNames
		ch["destination_types"] = destTypes
		channels = append(channels, ch)
	}
	return channels
}

func setChannelEnabledDashboard(channelsDir, channelID string, enabled bool) error {
	path := filepath.Join(channelsDir, channelID, "channel.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read channel %s: %w", channelID, err)
	}

	content := string(data)
	if enabled {
		content = strings.Replace(content, "enabled: false", "enabled: true", 1)
	} else {
		content = strings.Replace(content, "enabled: true", "enabled: false", 1)
	}

	return os.WriteFile(path, []byte(content), 0o644)
}

type FormAuth struct {
	username string
	password string
	sessions sync.Map
}

func NewFormAuth(username, password string) *FormAuth {
	return &FormAuth{username: username, password: password}
}

func BasicAuthMiddleware(username, password string) func(http.Handler) http.Handler {
	fa := NewFormAuth(username, password)
	return fa.Middleware()
}

func (fa *FormAuth) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				switch r.Method {
				case http.MethodGet:
					fa.serveLoginPage(w, r, "")
				case http.MethodPost:
					fa.handleLogin(w, r)
				default:
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				}
				return
			}

			if r.URL.Path == "/logout" {
				fa.handleLogout(w, r)
				return
			}

			if strings.HasPrefix(r.URL.Path, "/api/") {
				u, p, ok := r.BasicAuth()
				if ok && u == fa.username && p == fa.password {
					r.Header.Set("X-Auth-User", u)
					next.ServeHTTP(w, r)
					return
				}

				cookie, err := r.Cookie("intu_session")
				if err == nil {
					if user, ok := fa.sessions.Load(cookie.Value); ok {
						r.Header.Set("X-Auth-User", user.(string))
						next.ServeHTTP(w, r)
						return
					}
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]string{"error": "authentication required"})
				return
			}

			cookie, err := r.Cookie("intu_session")
			if err == nil {
				if user, ok := fa.sessions.Load(cookie.Value); ok {
					r.Header.Set("X-Auth-User", user.(string))
					next.ServeHTTP(w, r)
					return
				}
			}

			http.Redirect(w, r, "/login", http.StatusFound)
		})
	}
}

func (fa *FormAuth) handleLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		fa.serveLoginPage(w, r, "Invalid form submission")
		return
	}

	user := r.FormValue("username")
	pass := r.FormValue("password")

	if user != fa.username || pass != fa.password {
		fa.serveLoginPage(w, r, "Invalid username or password")
		return
	}

	token := generateSessionToken()
	fa.sessions.Store(token, user)

	http.SetCookie(w, &http.Cookie{
		Name:     "intu_session",
		Value:    token,
		Path:     "/",
		MaxAge:   28800,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	http.Redirect(w, r, "/", http.StatusFound)
}

func (fa *FormAuth) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("intu_session"); err == nil {
		fa.sessions.Delete(cookie.Value)
	}

	http.SetCookie(w, &http.Cookie{
		Name:   "intu_session",
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	})

	http.Redirect(w, r, "/login", http.StatusFound)
}

func (fa *FormAuth) serveLoginPage(w http.ResponseWriter, _ *http.Request, errMsg string) {
	errorHTML := ""
	if errMsg != "" {
		errorHTML = `<div class="bg-red-500/10 border border-red-500/30 text-red-400 px-4 py-3 rounded-xl text-sm mb-6 text-center">` + errMsg + `</div>`
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, loginPageHTML, errorHTML)
}

func generateSessionToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

const loginPageHTML = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>intu - Sign In</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%%3Csvg width='32' height='32' viewBox='0 0 100 100' fill='none' xmlns='http://www.w3.org/2000/svg'%%3E%%3Crect width='100' height='100' rx='24' fill='%%230ea5e9'/%%3E%%3Cpath d='M50 15C50 15 52 30 65 32C52 34 50 49 50 49C50 49 48 34 35 32C48 30 50 15 50 15Z' fill='white'/%%3E%%3Crect x='42' y='55' width='16' height='30' rx='5' fill='white'/%%3E%%3C/svg%%3E">
  <script>(function(){var t=localStorage.getItem('intu-theme');if(t==='light')document.documentElement.classList.remove('dark');})()</script>
  <script src="https://cdn.tailwindcss.com"></script>
  <script>tailwind.config={darkMode:'class'}</script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    @keyframes pulse-line { 0%%,100%% { opacity: 0.3; } 50%% { opacity: 1; } }
    .animate-pulse-line { animation: pulse-line 2s ease-in-out infinite; }
  </style>
</head>
<body class="bg-gray-50 dark:bg-slate-900 text-gray-800 dark:text-slate-100 min-h-screen flex items-center justify-center p-4 font-[Inter,system-ui,sans-serif] transition-colors duration-200">
  <div class="w-full max-w-md">
    <div class="text-center mb-10">
      <div class="inline-flex items-center justify-center mb-4">
        <svg width="56" height="56" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
          <rect width="100" height="100" rx="24" fill="#0ea5e9"/>
          <path d="M50 15C50 15 52 30 65 32C52 34 50 49 50 49C50 49 48 34 35 32C48 30 50 15 50 15Z" fill="white"/>
          <rect x="42" y="55" width="16" height="30" rx="5" fill="white"/>
        </svg>
      </div>
      <h1 class="text-3xl font-extrabold text-gray-900 dark:text-white tracking-tight">intu<span class="text-sky-500">.dev</span></h1>
      <p class="text-gray-400 dark:text-slate-500 mt-1 text-sm">Healthcare Interoperability Engine</p>
    </div>
    <div class="bg-white dark:bg-slate-800/80 backdrop-blur border border-gray-200 dark:border-slate-700/50 rounded-2xl p-8 shadow-xl dark:shadow-2xl dark:shadow-black/20 transition-colors duration-200">
      %s
      <form method="POST" action="/login" class="space-y-5">
        <div>
          <label for="username" class="block text-sm font-medium text-gray-500 dark:text-slate-400 mb-1.5">Username</label>
          <input type="text" id="username" name="username" autocomplete="username" autofocus required
            class="w-full px-4 py-2.5 bg-gray-50 dark:bg-slate-900/60 border border-gray-300 dark:border-slate-600/50 rounded-xl text-gray-900 dark:text-white placeholder-gray-400 dark:placeholder-slate-500 focus:outline-none focus:border-sky-400/60 focus:ring-1 focus:ring-sky-400/30 transition-all text-sm">
        </div>
        <div>
          <label for="password" class="block text-sm font-medium text-gray-500 dark:text-slate-400 mb-1.5">Password</label>
          <input type="password" id="password" name="password" autocomplete="current-password" required
            class="w-full px-4 py-2.5 bg-gray-50 dark:bg-slate-900/60 border border-gray-300 dark:border-slate-600/50 rounded-xl text-gray-900 dark:text-white placeholder-gray-400 dark:placeholder-slate-500 focus:outline-none focus:border-sky-400/60 focus:ring-1 focus:ring-sky-400/30 transition-all text-sm">
        </div>
        <button type="submit"
          class="w-full py-2.5 bg-sky-500 hover:bg-sky-400 text-white dark:text-slate-900 font-semibold rounded-xl transition-all duration-200 text-sm focus:outline-none focus:ring-2 focus:ring-sky-400/50 focus:ring-offset-2 focus:ring-offset-white dark:focus:ring-offset-slate-800">
          Sign In
        </button>
      </form>
    </div>
    <p class="text-center text-gray-300 dark:text-slate-600 text-xs mt-6">Powered by intu engine</p>
  </div>
</body>
</html>`

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
