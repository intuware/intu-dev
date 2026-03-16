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

type IHESource struct {
	cfg      *config.IHEListener
	server   *http.Server
	listener net.Listener
	logger   *slog.Logger
}

func NewIHESource(cfg *config.IHEListener, logger *slog.Logger) *IHESource {
	return &IHESource{cfg: cfg, logger: logger}
}

func (i *IHESource) Start(ctx context.Context, handler MessageHandler) error {
	profile := strings.ToLower(i.cfg.Profile)
	mux := http.NewServeMux()

	switch profile {
	case "xds_repository":
		i.registerXDSRepository(mux, handler)
	case "xds_registry":
		i.registerXDSRegistry(mux, handler)
	case "pix":
		i.registerPIX(mux, handler)
	case "pdq":
		i.registerPDQ(mux, handler)
	default:
		i.registerGenericIHE(mux, handler, profile)
	}

	mux.HandleFunc("/ihe/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"profile": i.cfg.Profile,
			"status":  "running",
			"port":    i.cfg.Port,
		})
	})

	addr := ":" + strconv.Itoa(i.cfg.Port)
	i.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("IHE listen on %s: %w", addr, err)
	}
	i.listener = ln

	tlsEnabled := false
	if i.cfg.TLS != nil && i.cfg.TLS.Enabled {
		ln, err = applyTLSToListener(ln, i.server, i.cfg.TLS)
		if err != nil {
			i.listener.Close()
			return fmt.Errorf("IHE TLS: %w", err)
		}
		tlsEnabled = true
	}

	go func() {
		if err := i.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			i.logger.Error("IHE server error", "error", err)
		}
	}()

	i.logger.Info("IHE source started",
		"addr", addr,
		"profile", i.cfg.Profile,
		"tls", tlsEnabled,
	)
	return nil
}

func (i *IHESource) registerXDSRepository(mux *http.ServeMux, handler MessageHandler) {
	mux.HandleFunc("/xds/repository/provide", func(w http.ResponseWriter, r *http.Request) {
		i.handleIHERequest(w, r, handler, "xds_repository", "ProvideAndRegisterDocumentSet")
	})

	mux.HandleFunc("/xds/repository/retrieve", func(w http.ResponseWriter, r *http.Request) {
		i.handleIHERequest(w, r, handler, "xds_repository", "RetrieveDocumentSet")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			i.handleIHERequest(w, r, handler, "xds_repository", "GenericRequest")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"profile": "xds_repository", "status": "active"})
	})
}

func (i *IHESource) registerXDSRegistry(mux *http.ServeMux, handler MessageHandler) {
	mux.HandleFunc("/xds/registry/register", func(w http.ResponseWriter, r *http.Request) {
		i.handleIHERequest(w, r, handler, "xds_registry", "RegisterDocumentSet")
	})

	mux.HandleFunc("/xds/registry/query", func(w http.ResponseWriter, r *http.Request) {
		i.handleIHERequest(w, r, handler, "xds_registry", "RegistryStoredQuery")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			i.handleIHERequest(w, r, handler, "xds_registry", "GenericRequest")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"profile": "xds_registry", "status": "active"})
	})
}

func (i *IHESource) registerPIX(mux *http.ServeMux, handler MessageHandler) {
	mux.HandleFunc("/pix/query", func(w http.ResponseWriter, r *http.Request) {
		i.handleIHERequest(w, r, handler, "pix", "PatientIdentityCrossReference")
	})

	mux.HandleFunc("/pix/feed", func(w http.ResponseWriter, r *http.Request) {
		i.handleIHERequest(w, r, handler, "pix", "PatientIdentityFeed")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			i.handleIHERequest(w, r, handler, "pix", "GenericRequest")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"profile": "pix", "status": "active"})
	})
}

func (i *IHESource) registerPDQ(mux *http.ServeMux, handler MessageHandler) {
	mux.HandleFunc("/pdq/query", func(w http.ResponseWriter, r *http.Request) {
		i.handleIHERequest(w, r, handler, "pdq", "PatientDemographicsQuery")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			i.handleIHERequest(w, r, handler, "pdq", "GenericRequest")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"profile": "pdq", "status": "active"})
	})
}

func (i *IHESource) registerGenericIHE(mux *http.ServeMux, handler MessageHandler, profile string) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			i.handleIHERequest(w, r, handler, profile, "GenericRequest")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"profile": profile, "status": "active"})
	})
}

func (i *IHESource) handleIHERequest(w http.ResponseWriter, r *http.Request, handler MessageHandler, profile, transaction string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !authenticateHTTP(r, i.cfg.Auth) {
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
	msg.Transport = "ihe"
	msg.ContentType = "xml"
	if cs := iencoding.ExtractCharset(r.Header.Get("Content-Type")); cs != "" {
		msg.SourceCharset = iencoding.NormalizeCharset(cs)
	}
	msg.Metadata["source"] = "ihe"
	msg.Metadata["ihe_profile"] = profile
	msg.Metadata["ihe_transaction"] = transaction
	msg.Metadata["request_path"] = r.URL.Path

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
		i.logger.Error("IHE handler error", "profile", profile, "transaction", transaction, "error", err)
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `<?xml version="1.0"?><error>%s</error>`, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<?xml version="1.0"?><response><status>accepted</status><profile>%s</profile><transaction>%s</transaction></response>`,
		profile, transaction)
}

func (i *IHESource) Addr() string {
	if i.listener != nil {
		return i.listener.Addr().String()
	}
	return ""
}

func (i *IHESource) Stop(ctx context.Context) error {
	if i.server != nil {
		return i.server.Shutdown(ctx)
	}
	return nil
}

func (i *IHESource) Type() string {
	return "ihe/" + strings.ToLower(i.cfg.Profile)
}
