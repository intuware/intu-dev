package connector

import (
	"context"
	"encoding/json"
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

type FHIRSource struct {
	cfg      *config.FHIRListener
	server   *http.Server
	listener net.Listener
	logger   *slog.Logger
}

func NewFHIRSource(cfg *config.FHIRListener, logger *slog.Logger) *FHIRSource {
	return &FHIRSource{cfg: cfg, logger: logger}
}

func (f *FHIRSource) Start(ctx context.Context, handler MessageHandler) error {
	basePath := f.cfg.BasePath
	if basePath == "" {
		basePath = "/fhir"
	}
	basePath = strings.TrimSuffix(basePath, "/")

	version := f.cfg.Version
	if version == "" {
		version = "R4"
	}

	mux := http.NewServeMux()

	mux.HandleFunc(basePath+"/metadata", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(f.capabilityStatement(version))
	})

	mux.HandleFunc(basePath+"/", func(w http.ResponseWriter, r *http.Request) {
		if !authenticateHTTP(r, f.cfg.Auth) {
			w.Header().Set("Content-Type", "application/fhir+json")
			w.WriteHeader(http.StatusUnauthorized)
			writeOperationOutcome(w, "error", "security", "Unauthorized")
			return
		}

		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			f.handleFHIRRead(w, r, basePath)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.Header().Set("Content-Type", "application/fhir+json")
			w.WriteHeader(http.StatusBadRequest)
			writeOperationOutcome(w, "error", "invalid", "Failed to read body")
			return
		}
		defer r.Body.Close()

		resourcePath := strings.TrimPrefix(r.URL.Path, basePath+"/")

		msg := message.New("", body)
		msg.ContentType = "fhir_r4"
		msg.Metadata["source"] = "fhir"
		msg.Metadata["fhir_version"] = version
		msg.Metadata["http_method"] = r.Method
		msg.Metadata["resource_path"] = resourcePath

		parts := strings.SplitN(resourcePath, "/", 2)
		if len(parts) > 0 {
			msg.Metadata["resource_type"] = parts[0]
		}
		if len(parts) > 1 {
			msg.Metadata["resource_id"] = parts[1]
		}

		for k, v := range r.Header {
			if len(v) > 0 {
				msg.Headers[k] = v[0]
			}
		}

		if err := handler(r.Context(), msg); err != nil {
			f.logger.Error("FHIR handler error", "error", err)
			w.Header().Set("Content-Type", "application/fhir+json")
			w.WriteHeader(http.StatusInternalServerError)
			writeOperationOutcome(w, "error", "exception", err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/fhir+json")
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		writeOperationOutcome(w, "information", "informational", "Resource accepted")
	})

	mux.HandleFunc(basePath+"/subscription-notification", func(w http.ResponseWriter, r *http.Request) {
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
		msg.ContentType = "fhir_r4"
		msg.Metadata["source"] = "fhir"
		msg.Metadata["fhir_version"] = version
		msg.Metadata["subscription_type"] = f.cfg.SubscriptionType
		msg.Metadata["notification"] = true

		if err := handler(r.Context(), msg); err != nil {
			f.logger.Error("FHIR subscription notification handler error", "error", err)
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
		return fmt.Errorf("FHIR listen on %s: %w", addr, err)
	}
	f.listener = ln

	tlsEnabled := false
	if f.cfg.TLS != nil && f.cfg.TLS.Enabled {
		ln, err = applyTLSToListener(ln, f.server, f.cfg.TLS)
		if err != nil {
			f.listener.Close()
			return fmt.Errorf("FHIR TLS: %w", err)
		}
		tlsEnabled = true
	}

	go func() {
		if err := f.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			f.logger.Error("FHIR server error", "error", err)
		}
	}()

	f.logger.Info("FHIR source started",
		"addr", addr,
		"base_path", basePath,
		"version", version,
		"tls", tlsEnabled,
	)
	return nil
}

func (f *FHIRSource) handleFHIRRead(w http.ResponseWriter, r *http.Request, basePath string) {
	w.Header().Set("Content-Type", "application/fhir+json")
	writeOperationOutcome(w, "error", "not-supported",
		"This FHIR endpoint is a listener-only source. Read operations are not supported.")
}

func (f *FHIRSource) capabilityStatement(version string) map[string]any {
	return map[string]any{
		"resourceType": "CapabilityStatement",
		"status":       "active",
		"kind":         "instance",
		"fhirVersion":  version,
		"format":       []string{"json", "xml"},
		"rest": []map[string]any{
			{
				"mode": "server",
				"resource": []map[string]any{
					{
						"type":        "Patient",
						"interaction": []map[string]any{{"code": "create"}, {"code": "update"}},
					},
					{
						"type":        "Observation",
						"interaction": []map[string]any{{"code": "create"}, {"code": "update"}},
					},
					{
						"type":        "Bundle",
						"interaction": []map[string]any{{"code": "create"}},
					},
				},
			},
		},
	}
}

func writeOperationOutcome(w http.ResponseWriter, severity, code, diagnostics string) {
	oo := map[string]any{
		"resourceType": "OperationOutcome",
		"issue": []map[string]any{
			{
				"severity":    severity,
				"code":        code,
				"diagnostics": diagnostics,
			},
		},
	}
	json.NewEncoder(w).Encode(oo)
}

func (f *FHIRSource) Stop(ctx context.Context) error {
	if f.server != nil {
		return f.server.Shutdown(ctx)
	}
	return nil
}

func (f *FHIRSource) Type() string {
	return "fhir"
}
