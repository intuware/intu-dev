package connector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

type HTTPSource struct {
	cfg      *config.HTTPListener
	server   *http.Server
	listener net.Listener
	logger   *slog.Logger
}

func NewHTTPSource(cfg *config.HTTPListener, logger *slog.Logger) *HTTPSource {
	return &HTTPSource{cfg: cfg, logger: logger}
}

func (h *HTTPSource) Start(ctx context.Context, handler MessageHandler) error {
	path := h.cfg.Path
	if path == "" {
		path = "/"
	}
	methods := h.cfg.Methods
	if len(methods) == 0 {
		methods = []string{"POST"}
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		allowed := false
		for _, m := range methods {
			if strings.EqualFold(r.Method, m) {
				allowed = true
				break
			}
		}
		if !allowed {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if h.cfg.Auth != nil {
			if !h.authenticate(r) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		msg := message.New("", body)
		for k, v := range r.Header {
			if len(v) > 0 {
				msg.Headers[k] = v[0]
			}
		}
		if cid := r.Header.Get("X-Correlation-Id"); cid != "" {
			msg.CorrelationID = cid
		}

		if err := handler(r.Context(), msg); err != nil {
			h.logger.Error("message handler error", "error", err)
			http.Error(w, "Processing failed", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"accepted"}`)
	})

	addr := ":" + strconv.Itoa(h.cfg.Port)
	h.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	h.listener = ln

	h.logger.Info("HTTP listener started", "addr", addr, "path", path)

	go func() {
		if err := h.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			h.logger.Error("HTTP server error", "error", err)
		}
	}()

	return nil
}

func (h *HTTPSource) authenticate(r *http.Request) bool {
	if h.cfg.Auth == nil {
		return true
	}

	switch h.cfg.Auth.Type {
	case "bearer":
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		return token == h.cfg.Auth.Token
	case "basic":
		user, pass, ok := r.BasicAuth()
		return ok && user == h.cfg.Auth.Username && pass == h.cfg.Auth.Password
	case "api_key":
		if h.cfg.Auth.Header != "" {
			return r.Header.Get(h.cfg.Auth.Header) == h.cfg.Auth.Key
		}
		if h.cfg.Auth.QueryParam != "" {
			return r.URL.Query().Get(h.cfg.Auth.QueryParam) == h.cfg.Auth.Key
		}
		return false
	case "none", "":
		return true
	default:
		return true
	}
}

func (h *HTTPSource) Stop(ctx context.Context) error {
	if h.server != nil {
		return h.server.Shutdown(ctx)
	}
	return nil
}

func (h *HTTPSource) Type() string {
	return "http"
}

func (h *HTTPSource) Addr() string {
	if h.listener != nil {
		return h.listener.Addr().String()
	}
	return ""
}
