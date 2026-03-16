package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type FHIRSubscriptionSource struct {
	cfg      *config.FHIRSubscriptionListener
	server   *http.Server
	listener net.Listener
	wsConn   *websocket.Conn
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewFHIRSubscriptionSource(cfg *config.FHIRSubscriptionListener, logger *slog.Logger) *FHIRSubscriptionSource {
	return &FHIRSubscriptionSource{cfg: cfg, logger: logger}
}

func (f *FHIRSubscriptionSource) Start(ctx context.Context, handler MessageHandler) error {
	switch f.cfg.ChannelType {
	case "rest-hook":
		return f.startRestHook(ctx, handler)
	case "websocket":
		return f.startWebSocket(ctx, handler)
	default:
		return fmt.Errorf("fhir_subscription: unsupported channel_type %q (use rest-hook or websocket)", f.cfg.ChannelType)
	}
}

func (f *FHIRSubscriptionSource) startRestHook(ctx context.Context, handler MessageHandler) error {
	path := f.cfg.Path
	if path == "" {
		path = "/fhir/subscription-notification"
	}
	path = "/" + strings.Trim(path, "/")

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		msg := message.New("", body)
		msg.Transport = "fhir_subscription"
		msg.ContentType = "fhir_r4"
		msg.Metadata["source"] = "fhir_subscription"
		msg.Metadata["channel_type"] = "rest-hook"
		msg.Metadata["notification"] = true
		if f.cfg.Version != "" {
			msg.Metadata["fhir_version"] = f.cfg.Version
		}
		if f.cfg.SubscriptionID != "" {
			msg.Metadata["subscription_id"] = f.cfg.SubscriptionID
		}
		var payload map[string]any
		if json.Unmarshal(body, &payload) == nil {
			if id, ok := payload["subscription"].(string); ok {
				msg.Metadata["subscription_id"] = id
			}
			if n, ok := payload["eventNumber"].(float64); ok {
				msg.Metadata["event_number"] = int(n)
			}
		}

		if err := handler(r.Context(), msg); err != nil {
			f.logger.Error("fhir_subscription rest-hook handler error", "error", err)
			http.Error(w, "Processing failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	addr := ":" + strconv.Itoa(f.cfg.Port)
	f.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("fhir_subscription rest-hook listen: %w", err)
	}
	f.listener = ln

	if f.cfg.TLS != nil && f.cfg.TLS.Enabled {
		ln, err = applyTLSToListener(ln, f.server, f.cfg.TLS)
		if err != nil {
			f.listener.Close()
			return fmt.Errorf("fhir_subscription TLS: %w", err)
		}
	}

	go func() {
		if err := f.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			f.logger.Error("fhir_subscription server error", "error", err)
		}
	}()

	f.logger.Info("fhir_subscription rest-hook started", "addr", addr, "path", path)
	return nil
}

func (f *FHIRSubscriptionSource) startWebSocket(ctx context.Context, handler MessageHandler) error {
	if f.cfg.WebSocketURL == "" {
		return fmt.Errorf("fhir_subscription websocket: websocket_url is required")
	}

	backoff := 2 * time.Second
	if f.cfg.ReconnectBackoff != "" {
		if d, err := time.ParseDuration(f.cfg.ReconnectBackoff); err == nil {
			backoff = d
		}
	}
	maxAttempts := f.cfg.MaxReconnectAttempts

	ctx, f.cancel = context.WithCancel(ctx)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		attempt := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if maxAttempts > 0 && attempt >= maxAttempts {
				f.logger.Error("fhir_subscription websocket max reconnect attempts reached", "attempts", attempt)
				return
			}
			if attempt > 0 {
				f.logger.Info("fhir_subscription websocket reconnecting", "attempt", attempt+1)
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				backoff = min(backoff*2, 5*time.Minute)
			}

			conn, err := f.dialWebSocket(ctx)
			if err != nil {
				f.logger.Error("fhir_subscription websocket dial failed", "error", err)
				attempt++
				continue
			}
			f.wsConn = conn
			attempt = 0
			backoff = 2 * time.Second
			if f.cfg.ReconnectBackoff != "" {
				if d, err := time.ParseDuration(f.cfg.ReconnectBackoff); err == nil {
					backoff = d
				}
			}

			f.logger.Info("fhir_subscription websocket connected", "url", f.cfg.WebSocketURL)
			err = f.readWebSocketLoop(ctx, conn, handler)
			conn.Close()
			f.wsConn = nil
			if err != nil && ctx.Err() == nil {
				f.logger.Warn("fhir_subscription websocket read loop ended", "error", err)
			}
			attempt++
		}
	}()
	return nil
}

func (f *FHIRSubscriptionSource) dialWebSocket(ctx context.Context) (*websocket.Conn, error) {
	u, err := url.Parse(f.cfg.WebSocketURL)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else if u.Scheme == "http" {
		u.Scheme = "ws"
	}

	header := http.Header{}
	if f.cfg.Auth != nil {
		switch f.cfg.Auth.Type {
		case "bearer":
			header.Set("Authorization", "Bearer "+f.cfg.Auth.Token)
		case "oauth2_client_credentials":
			token, err := fetchOAuth2Token(f.cfg.Auth.TokenURL, f.cfg.Auth.ClientID, f.cfg.Auth.ClientSecret, f.cfg.Auth.Scopes)
			if err != nil {
				return nil, fmt.Errorf("oauth2 token: %w", err)
			}
			header.Set("Authorization", "Bearer "+token)
		}
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}
	if f.cfg.TLS != nil && f.cfg.TLS.Enabled {
		tlsCfg, err := auth.BuildTLSConfig(f.cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		dialer.TLSClientConfig = tlsCfg
	}

	conn, resp, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("dial: %w (status %d: %s)", err, resp.StatusCode, string(body))
		}
		return nil, err
	}
	return conn, nil
}

func (f *FHIRSubscriptionSource) readWebSocketLoop(ctx context.Context, conn *websocket.Conn, handler MessageHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		msg := message.New("", data)
		msg.Transport = "fhir_subscription"
		msg.ContentType = "fhir_r4"
		msg.Metadata["source"] = "fhir_subscription"
		msg.Metadata["channel_type"] = "websocket"
		msg.Metadata["notification"] = true
		if f.cfg.Version != "" {
			msg.Metadata["fhir_version"] = f.cfg.Version
		}
		if f.cfg.SubscriptionID != "" {
			msg.Metadata["subscription_id"] = f.cfg.SubscriptionID
		}
		var payload map[string]any
		if json.Unmarshal(data, &payload) == nil {
			if id, ok := payload["subscription"].(string); ok {
				msg.Metadata["subscription_id"] = id
			}
			if n, ok := payload["eventNumber"].(float64); ok {
				msg.Metadata["event_number"] = int(n)
			}
		}

		if err := handler(ctx, msg); err != nil {
			f.logger.Error("fhir_subscription websocket handler error", "error", err)
		}
	}
}

func (f *FHIRSubscriptionSource) Stop(ctx context.Context) error {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()

	if f.wsConn != nil {
		f.wsConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		f.wsConn.Close()
		f.wsConn = nil
	}
	if f.server != nil {
		return f.server.Shutdown(ctx)
	}
	return nil
}

func (f *FHIRSubscriptionSource) Type() string {
	return "fhir_subscription"
}

// Addr returns the listener address for rest-hook (e.g. "127.0.0.1:9090"). Empty for websocket.
func (f *FHIRSubscriptionSource) Addr() string {
	if f.listener != nil {
		return f.listener.Addr().String()
	}
	return ""
}
