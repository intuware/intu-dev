package connector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	iencoding "github.com/intuware/intu-dev/internal/encoding"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type HTTPSource struct {
	cfg     *config.HTTPListener
	cfgPort int // port value used as key in shared listener map (may be 0)
	path    string
	started bool
	logger  *slog.Logger
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

	h.cfgPort = h.cfg.Port
	h.path = path

	sl, err := acquireSharedHTTPListener(h.cfgPort, h.cfg.TLS, h.logger)
	if err != nil {
		return err
	}

	handlerFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		if !authenticateHTTP(r, h.cfg.Auth) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		msg := message.New("", body)
		msg.Transport = "http"
		if cs := iencoding.ExtractCharset(r.Header.Get("Content-Type")); cs != "" {
			msg.SourceCharset = iencoding.NormalizeCharset(cs)
		}
		http_ := msg.EnsureHTTP()
		for k, v := range r.Header {
			if len(v) > 0 {
				http_.Headers[k] = v[0]
			}
		}
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				http_.QueryParams[k] = v[0]
			}
		}
		http_.Method = r.Method
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

	if err := sl.router.Register(path, handlerFunc); err != nil {
		releaseSharedHTTPListener(h.cfgPort, ctx)
		return fmt.Errorf("register path on port %d: %w", h.cfgPort, err)
	}

	h.started = true
	h.logger.Info("HTTP channel registered", "port", h.cfgPort, "path", path)
	return nil
}

func (h *HTTPSource) Stop(ctx context.Context) error {
	if !h.started {
		return nil
	}
	h.started = false

	sharedMu.Lock()
	if sl, ok := sharedListeners[h.cfgPort]; ok {
		sl.router.Deregister(h.path)
	}
	sharedMu.Unlock()

	releaseSharedHTTPListener(h.cfgPort, ctx)
	return nil
}

func (h *HTTPSource) Type() string {
	return "http"
}

func (h *HTTPSource) Addr() string {
	sharedMu.Lock()
	sl, ok := sharedListeners[h.cfgPort]
	sharedMu.Unlock()
	if ok {
		return sl.listener.Addr().String()
	}
	return ""
}
