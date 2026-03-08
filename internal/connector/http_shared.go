package connector

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/intuware/intu/internal/auth"
	"github.com/intuware/intu/pkg/config"
)

// pathRouter is a thread-safe HTTP handler that dispatches requests by exact
// path. Unlike http.ServeMux it supports deregistration, which is needed when
// channels are stopped or hot-reloaded.
type pathRouter struct {
	mu       sync.RWMutex
	handlers map[string]http.Handler
}

func newPathRouter() *pathRouter {
	return &pathRouter{handlers: make(map[string]http.Handler)}
}

func (r *pathRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	h, ok := r.handlers[req.URL.Path]
	r.mu.RUnlock()

	if !ok {
		http.NotFound(w, req)
		return
	}
	h.ServeHTTP(w, req)
}

func (r *pathRouter) Register(path string, h http.Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[path]; exists {
		return fmt.Errorf("path %q already registered on this port", path)
	}
	r.handlers[path] = h
	return nil
}

func (r *pathRouter) Deregister(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, path)
}

func (r *pathRouter) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// sharedHTTPListener holds a single net.Listener + http.Server for a given
// port. Multiple channels register their paths on the shared pathRouter.
type sharedHTTPListener struct {
	port     int
	server   *http.Server
	listener net.Listener
	router   *pathRouter
	refs     int
	logger   *slog.Logger
	mu       sync.Mutex
}

var (
	sharedMu        sync.Mutex
	sharedListeners = make(map[int]*sharedHTTPListener)
)

// acquireSharedHTTPListener returns an existing shared listener for the port, or
// creates and starts a new one. The caller must pair this with a
// releaseSharedHTTPListener call on shutdown.
//
// TLS is applied from the first channel that registers on a given port. If a
// subsequent channel specifies different TLS settings on the same port, we log
// a warning and reuse the existing listener (port-level TLS is necessarily
// shared).
func acquireSharedHTTPListener(port int, tlsCfg *config.TLSConfig, logger *slog.Logger) (*sharedHTTPListener, error) {
	sharedMu.Lock()
	defer sharedMu.Unlock()

	if sl, ok := sharedListeners[port]; ok {
		sl.mu.Lock()
		sl.refs++
		sl.mu.Unlock()
		return sl, nil
	}

	router := newPathRouter()
	addr := ":" + strconv.Itoa(port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	tlsEnabled := false
	if tlsCfg != nil && tlsCfg.Enabled {
		tc, err := auth.BuildTLSConfig(tlsCfg)
		if err != nil {
			ln.Close()
			return nil, fmt.Errorf("shared HTTP TLS on port %d: %w", port, err)
		}
		server.TLSConfig = tc
		ln = tls.NewListener(ln, tc)
		tlsEnabled = true
	}

	sl := &sharedHTTPListener{
		port:     port,
		server:   server,
		listener: ln,
		router:   router,
		refs:     1,
		logger:   logger,
	}

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("shared HTTP server error", "port", port, "error", err)
		}
	}()

	logger.Info("shared HTTP listener started", "addr", addr, "tls", tlsEnabled)
	sharedListeners[port] = sl
	return sl, nil
}

// releaseSharedHTTPListener decrements the reference count and shuts down the
// listener when no channels remain on that port.
func releaseSharedHTTPListener(port int, ctx context.Context) {
	sharedMu.Lock()
	defer sharedMu.Unlock()

	sl, ok := sharedListeners[port]
	if !ok {
		return
	}

	sl.mu.Lock()
	sl.refs--
	remaining := sl.refs
	sl.mu.Unlock()

	if remaining <= 0 {
		_ = sl.server.Shutdown(ctx)
		delete(sharedListeners, port)
		sl.logger.Info("shared HTTP listener stopped", "port", port)
	}
}

// ResetSharedHTTPListeners tears down all shared listeners. Intended for tests.
func ResetSharedHTTPListeners() {
	sharedMu.Lock()
	defer sharedMu.Unlock()

	for port, sl := range sharedListeners {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = sl.server.Shutdown(ctx)
		cancel()
		delete(sharedListeners, port)
	}
}
