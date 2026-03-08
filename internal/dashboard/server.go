package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

type Server struct {
	cfg         *config.Config
	channelsDir string
	store       storage.MessageStore
	metrics     *observability.Metrics
	logger      *slog.Logger
	rbac        *auth.RBACManager
	auditLogger *auth.AuditLogger
	authMw      func(http.Handler) http.Handler
	server      *http.Server
	mu          sync.RWMutex
}

type ServerConfig struct {
	Config      *config.Config
	ChannelsDir string
	Store       storage.MessageStore
	Metrics     *observability.Metrics
	Logger      *slog.Logger
	RBAC        *auth.RBACManager
	AuditLogger *auth.AuditLogger
	AuthMiddleware func(http.Handler) http.Handler
	Port        int
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
	}

	if s.metrics == nil {
		s.metrics = observability.Global()
	}

	return s
}

func (s *Server) BuildHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", s.handleIndex)
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

	s.logger.Info("dashboard server starting", "addr", addr)
	return s.server.ListenAndServe()
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

	reprocessRecord := &storage.MessageRecord{
		ID:            fmt.Sprintf("rp-%s-%d", msgID, time.Now().UnixMilli()),
		CorrelationID: record.CorrelationID,
		ChannelID:     record.ChannelID,
		Stage:         "reprocessed",
		Content:       record.Content,
		Status:        "REPROCESSED",
		Timestamp:     time.Now(),
		Metadata: map[string]any{
			"original_message_id": record.ID,
			"original_status":     record.Status,
			"reprocessed_at":      time.Now().Format(time.RFC3339),
		},
	}

	if err := s.store.Save(reprocessRecord); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if s.auditLogger != nil {
		s.auditLogger.Log("message.reprocess", "dashboard", map[string]any{
			"original_id": msgID,
			"new_id":      reprocessRecord.ID,
			"channel":     record.ChannelID,
		})
	}

	s.logger.Info("message reprocessed via dashboard",
		"originalID", msgID,
		"newID", reprocessRecord.ID,
		"channel", record.ChannelID,
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"reprocessed":         true,
		"original_message_id": msgID,
		"new_message_id":      reprocessRecord.ID,
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

	if chCfg.Listener.Type != "" {
		result["listener_type"] = chCfg.Listener.Type
	}

	destNames := []string{}
	for _, d := range chCfg.Destinations {
		n := d.Name
		if n == "" {
			n = d.Ref
		}
		destNames = append(destNames, n)
	}
	result["destinations"] = destNames

	if len(chCfg.Tags) > 0 {
		result["tags"] = chCfg.Tags
	}
	if chCfg.Group != "" {
		result["group"] = chCfg.Group
	}
	if chCfg.Pipeline != nil {
		result["pipeline"] = chCfg.Pipeline
	}

	snap := s.metrics.Snapshot()
	channelMetrics := map[string]any{}
	for key, v := range snap {
		if strings.Contains(key, channelID) {
			channelMetrics[key] = v
		}
	}
	if len(channelMetrics) > 0 {
		result["metrics"] = channelMetrics
	}

	writeJSON(w, http.StatusOK, result)
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

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
