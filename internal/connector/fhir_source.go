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

	iencoding "github.com/intuware/intu-dev/internal/encoding"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type FHIRSource struct {
	cfg              *config.FHIRListener
	server           *http.Server
	listener         net.Listener
	logger           *slog.Logger
	allowedResources map[string]bool
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

	f.allowedResources = make(map[string]bool)
	for _, r := range f.cfg.Resources {
		f.allowedResources[strings.ToLower(r)] = true
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
		msg.Transport = "fhir"
		msg.ContentType = "fhir_r4"
		if cs := iencoding.ExtractCharset(r.Header.Get("Content-Type")); cs != "" {
			msg.SourceCharset = iencoding.NormalizeCharset(cs)
		}
		msg.Metadata["source"] = "fhir"
		msg.Metadata["fhir_version"] = version
		msg.Metadata["http_method"] = r.Method
		msg.Metadata["resource_path"] = resourcePath

		parts := strings.SplitN(resourcePath, "/", 2)
		resourceType := ""
		if len(parts) > 0 {
			resourceType = parts[0]
			msg.Metadata["resource_type"] = resourceType
		}
		if len(parts) > 1 {
			msg.Metadata["resource_id"] = parts[1]
		}

		if len(f.allowedResources) > 0 && resourceType != "" && !f.allowedResources[strings.ToLower(resourceType)] {
			w.Header().Set("Content-Type", "application/fhir+json")
			w.WriteHeader(http.StatusNotFound)
			writeOperationOutcome(w, "error", "not-supported",
				"Resource type '"+resourceType+"' is not supported by this endpoint")
			return
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

		if err := handler(r.Context(), msg); err != nil {
			f.logger.Error("FHIR handler error", "error", err)
			w.Header().Set("Content-Type", "application/fhir+json")
			status, severity, code, diag := classifyPipelineError(err)
			w.WriteHeader(status)
			writeOperationOutcome(w, severity, code, diag)
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
		msg.Transport = "fhir"
		msg.ContentType = "fhir_r4"
		if cs := iencoding.ExtractCharset(r.Header.Get("Content-Type")); cs != "" {
			msg.SourceCharset = iencoding.NormalizeCharset(cs)
		}
		msg.Metadata["source"] = "fhir"
		msg.Metadata["fhir_version"] = version
		msg.Metadata["subscription_type"] = f.cfg.SubscriptionType
		msg.Metadata["notification"] = true
		http_ := msg.EnsureHTTP()
		http_.Method = r.Method

		if err := handler(r.Context(), msg); err != nil {
			f.logger.Error("FHIR subscription notification handler error", "error", err)
			http.Error(w, "Processing failed", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	lowerBase := strings.ToLower(basePath)
	pathNormalizer := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lower := strings.ToLower(r.URL.Path)
		if strings.HasPrefix(lower, lowerBase) {
			r.URL.Path = basePath + r.URL.Path[len(basePath):]
		}
		mux.ServeHTTP(w, r)
	})

	addr := ":" + strconv.Itoa(f.cfg.Port)
	f.server = &http.Server{
		Addr:         addr,
		Handler:      pathNormalizer,
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
	resources := f.cfg.Resources
	if len(resources) == 0 {
		resources = []string{"Patient", "Observation", "Bundle"}
	}

	var resList []map[string]any
	for _, rt := range resources {
		resList = append(resList, map[string]any{
			"type":        rt,
			"interaction": []map[string]any{{"code": "create"}, {"code": "update"}},
		})
	}

	return map[string]any{
		"resourceType": "CapabilityStatement",
		"status":       "active",
		"kind":         "instance",
		"fhirVersion":  version,
		"format":       []string{"json", "xml"},
		"rest": []map[string]any{
			{
				"mode":     "server",
				"resource": resList,
			},
		},
	}
}

// classifyPipelineError extracts a clean user-facing message from the pipeline
// error chain and maps it to the appropriate HTTP status and FHIR issue code.
// Internal details like file paths are stripped.
func classifyPipelineError(err error) (status int, severity, code, diagnostics string) {
	raw := err.Error()

	// Strip Go error chain prefixes added by the pipeline/runner:
	//   "pipeline execute: validator: call validate in dist/.../validator.js: <message>"
	//   "pipeline execute: transformer: call transform in dist/.../transformer.js: <message>"
	diag := raw
	if idx := strings.LastIndex(raw, ".js: "); idx != -1 {
		diag = raw[idx+5:]
	} else if idx := strings.LastIndex(raw, ".ts: "); idx != -1 {
		diag = raw[idx+5:]
	}

	isValidation := strings.Contains(raw, "validator:")
	if isValidation {
		return http.StatusUnprocessableEntity, "error", "processing", diag
	}
	return http.StatusInternalServerError, "error", "exception", diag
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

func (f *FHIRSource) Addr() string {
	if f.listener != nil {
		return f.listener.Addr().String()
	}
	return ""
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
