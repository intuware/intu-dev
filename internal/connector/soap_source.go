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

	iencoding "github.com/intuware/intu-dev/internal/encoding"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type SOAPSource struct {
	cfg      *config.SOAPListener
	server   *http.Server
	listener net.Listener
	logger   *slog.Logger
}

func NewSOAPSource(cfg *config.SOAPListener, logger *slog.Logger) *SOAPSource {
	return &SOAPSource{cfg: cfg, logger: logger}
}

func (s *SOAPSource) Start(ctx context.Context, handler MessageHandler) error {
	wsdlPath := s.cfg.WSDLPath
	if wsdlPath == "" {
		wsdlPath = "/wsdl"
	}

	serviceName := s.cfg.ServiceName
	if serviceName == "" {
		serviceName = "IntuService"
	}

	mux := http.NewServeMux()

	mux.HandleFunc(wsdlPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			fmt.Fprint(w, s.generateWSDL(serviceName, s.cfg.Port, wsdlPath))
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if !authenticateHTTP(r, s.cfg.Auth) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		contentType := r.Header.Get("Content-Type")
		if !strings.Contains(contentType, "xml") && !strings.Contains(contentType, "soap") {
			http.Error(w, "Content-Type must be XML/SOAP", http.StatusUnsupportedMediaType)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		soapAction := r.Header.Get("SOAPAction")
		if soapAction == "" {
			soapAction = r.Header.Get("soapaction")
		}

		msg := message.New("", body)
		msg.Transport = "soap"
		msg.ContentType = "xml"
		if cs := iencoding.ExtractCharset(r.Header.Get("Content-Type")); cs != "" {
			msg.SourceCharset = iencoding.NormalizeCharset(cs)
		}
		msg.Metadata["source"] = "soap"
		msg.Metadata["service_name"] = serviceName
		msg.Metadata["soap_action"] = strings.Trim(soapAction, "\"")
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
			s.logger.Error("SOAP handler error", "error", err)
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, soapFaultResponse("Server", err.Error()))
			return
		}

		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, soapSuccessResponse())
	})

	addr := ":" + strconv.Itoa(s.cfg.Port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("SOAP listen on %s: %w", addr, err)
	}
	s.listener = ln

	tlsEnabled := false
	if s.cfg.TLS != nil && s.cfg.TLS.Enabled {
		ln, err = applyTLSToListener(ln, s.server, s.cfg.TLS)
		if err != nil {
			s.listener.Close()
			return fmt.Errorf("SOAP TLS: %w", err)
		}
		tlsEnabled = true
	}

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.logger.Error("SOAP server error", "error", err)
		}
	}()

	s.logger.Info("SOAP source started",
		"addr", addr,
		"service", serviceName,
		"wsdl_path", wsdlPath,
		"tls", tlsEnabled,
	)
	return nil
}

func (s *SOAPSource) generateWSDL(serviceName string, port int, wsdlPath string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
             xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
             xmlns:tns="http://intu.local/%s"
             targetNamespace="http://intu.local/%s"
             name="%s">
  <types/>
  <message name="IntuRequest"><part name="body" type="xsd:anyType"/></message>
  <message name="IntuResponse"><part name="body" type="xsd:anyType"/></message>
  <portType name="%sPortType">
    <operation name="process">
      <input message="tns:IntuRequest"/>
      <output message="tns:IntuResponse"/>
    </operation>
  </portType>
  <binding name="%sBinding" type="tns:%sPortType">
    <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
    <operation name="process">
      <soap:operation soapAction="process"/>
    </operation>
  </binding>
  <service name="%s">
    <port name="%sPort" binding="tns:%sBinding">
      <soap:address location="http://localhost:%d/"/>
    </port>
  </service>
</definitions>`, serviceName, serviceName, serviceName,
		serviceName, serviceName, serviceName,
		serviceName, serviceName, serviceName, port)
}

func soapFaultResponse(faultCode, faultString string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <soap:Fault>
      <faultcode>soap:%s</faultcode>
      <faultstring>%s</faultstring>
    </soap:Fault>
  </soap:Body>
</soap:Envelope>`, faultCode, faultString)
}

func soapSuccessResponse() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ProcessResponse xmlns="http://intu.local/">
      <status>accepted</status>
    </ProcessResponse>
  </soap:Body>
</soap:Envelope>`
}

func (s *SOAPSource) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

func (s *SOAPSource) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *SOAPSource) Type() string {
	return "soap"
}
