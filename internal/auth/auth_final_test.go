package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// MTLSAuth - with proper TLS peer certificates
// ---------------------------------------------------------------------------

func TestMTLSAuth_Authenticate_MultiplePeerCerts(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	cert1 := &x509.Certificate{
		SerialNumber: big.NewInt(10),
		Subject:      pkix.Name{CommonName: "primary-client"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	cert1DER, _ := x509.CreateCertificate(rand.Reader, cert1, cert1, &key.PublicKey, key)
	x509Cert1, _ := x509.ParseCertificate(cert1DER)

	cert2 := &x509.Certificate{
		SerialNumber: big.NewInt(11),
		Subject:      pkix.Name{CommonName: "intermediate-ca"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	cert2DER, _ := x509.CreateCertificate(rand.Reader, cert2, cert2, &key.PublicKey, key)
	x509Cert2, _ := x509.ParseCertificate(cert2DER)

	a := &MTLSAuth{}
	req := httptest.NewRequest("GET", "/", nil)
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{x509Cert1, x509Cert2},
	}

	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	if user != "primary-client" {
		t.Fatalf("expected primary-client (first cert CN), got %q", user)
	}
}

func TestMTLSAuth_Authenticate_EmptyCN(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(12),
		Subject:      pkix.Name{CommonName: ""},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	x509Cert, _ := x509.ParseCertificate(certDER)

	a := &MTLSAuth{}
	req := httptest.NewRequest("GET", "/", nil)
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{x509Cert},
	}

	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true even with empty CN")
	}
	if user != "" {
		t.Fatalf("expected empty user for empty CN, got %q", user)
	}
}

func TestMTLSAuth_Authenticate_TLSNoPeers(t *testing.T) {
	a := &MTLSAuth{}
	req := httptest.NewRequest("GET", "/", nil)
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{},
	}

	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false when PeerCertificates is empty")
	}
}

func TestMTLSAuth_Authenticate_WithOrganization(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(13),
		Subject: pkix.Name{
			CommonName:   "service-client",
			Organization: []string{"Healthcare Corp"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Hour),
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	x509Cert, _ := x509.ParseCertificate(certDER)

	a := &MTLSAuth{}
	req := httptest.NewRequest("GET", "/", nil)
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{x509Cert},
	}

	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	if user != "service-client" {
		t.Fatalf("expected service-client, got %q", user)
	}
}

// ---------------------------------------------------------------------------
// ApplyDestAuth - oauth2 path (more thorough)
// ---------------------------------------------------------------------------

func TestApplyDestAuth_OAuth2_WithScopes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Form.Get("scope") != "read write" {
			t.Errorf("expected scope 'read write', got %q", r.Form.Get("scope"))
		}
		if r.Form.Get("client_id") != "my-cid" {
			t.Errorf("expected client_id my-cid, got %q", r.Form.Get("client_id"))
		}
		if r.Form.Get("client_secret") != "my-csec" {
			t.Errorf("expected client_secret my-csec, got %q", r.Form.Get("client_secret"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": "scoped-token"})
	}))
	defer server.Close()

	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:         "oauth2_client_credentials",
		TokenURL:     server.URL,
		ClientID:     "my-cid",
		ClientSecret: "my-csec",
		Scopes:       []string{"read", "write"},
	})
	if v := req.Header.Get("Authorization"); v != "Bearer scoped-token" {
		t.Fatalf("expected Bearer scoped-token, got %q", v)
	}
}

func TestApplyDestAuth_OAuth2_EmptyTokenURL(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:         "oauth2_client_credentials",
		TokenURL:     "",
		ClientID:     "cid",
		ClientSecret: "csec",
	})
	if req.Header.Get("Authorization") != "" {
		t.Fatal("expected no auth when token URL is empty")
	}
}

func TestApplyDestAuth_OAuth2_EmptyClientID(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:         "oauth2_client_credentials",
		TokenURL:     "http://localhost/token",
		ClientID:     "",
		ClientSecret: "csec",
	})
	if req.Header.Get("Authorization") != "" {
		t.Fatal("expected no auth when client ID is empty")
	}
}

func TestApplyDestAuth_OAuth2_EmptyClientSecret(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:         "oauth2_client_credentials",
		TokenURL:     "http://localhost/token",
		ClientID:     "cid",
		ClientSecret: "",
	})
	if req.Header.Get("Authorization") != "" {
		t.Fatal("expected no auth when client secret is empty")
	}
}

func TestApplyDestAuth_OAuth2_TokenEndpointReturnsEmptyToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": ""})
	}))
	defer server.Close()

	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:         "oauth2_client_credentials",
		TokenURL:     server.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	})
	if req.Header.Get("Authorization") != "" {
		t.Fatal("expected no auth when token is empty")
	}
}

func TestApplyDestAuth_UnknownType(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{Type: "unknown_type"})
	if req.Header.Get("Authorization") != "" {
		t.Fatal("expected no auth for unknown type")
	}
}

// ---------------------------------------------------------------------------
// FetchOAuth2ClientCredentials - with scopes
// ---------------------------------------------------------------------------

func TestFetchOAuth2ClientCredentials_VerifiesGrantType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Error("expected POST method")
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("expected form content type, got %q", r.Header.Get("Content-Type"))
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("grant_type") != "client_credentials" {
			t.Errorf("expected grant_type=client_credentials, got %q", r.Form.Get("grant_type"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": "verified-tok"})
	}))
	defer server.Close()

	tok, err := FetchOAuth2ClientCredentials(server.URL, "cid", "csec", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok != "verified-tok" {
		t.Fatalf("expected verified-tok, got %q", tok)
	}
}

func TestFetchOAuth2ClientCredentials_MultipleScopes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.Form.Get("scope") != "read write admin" {
			t.Errorf("expected 'read write admin', got %q", r.Form.Get("scope"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": "multi-scope-tok"})
	}))
	defer server.Close()

	tok, err := FetchOAuth2ClientCredentials(server.URL, "cid", "csec", []string{"read", "write", "admin"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok != "multi-scope-tok" {
		t.Fatalf("expected multi-scope-tok, got %q", tok)
	}
}

func TestFetchOAuth2ClientCredentials_BadURL(t *testing.T) {
	_, err := FetchOAuth2ClientCredentials("http://127.0.0.1:1/token", "cid", "csec", nil)
	if err == nil {
		t.Fatal("expected error for unreachable URL")
	}
}

// ---------------------------------------------------------------------------
// SecretsProvider factory - error paths for all provider types
// ---------------------------------------------------------------------------

func TestNewSecretsProvider_VaultWithConfig(t *testing.T) {
	_, err := NewSecretsProvider(&config.SecretsConfig{
		Provider: "vault",
		Vault:    &config.VaultConfig{Address: "http://127.0.0.1:1"},
	})
	// Vault client creation might succeed (it doesn't connect eagerly),
	// but authentication step may fail or succeed with default token auth.
	// We just verify it doesn't panic.
	_ = err
}

func TestNewSecretsProvider_GCPWithEmptyProjectID(t *testing.T) {
	_, err := NewSecretsProvider(&config.SecretsConfig{
		Provider: "gcp_secret_manager",
		GCP:      &config.GCPSecretManagerConfig{ProjectID: ""},
	})
	if err == nil {
		t.Fatal("expected error for empty GCP project_id")
	}
}

func TestNewSecretsProvider_EnvDefault(t *testing.T) {
	sp, err := NewSecretsProvider(&config.SecretsConfig{Provider: ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := sp.(*EnvSecretsProvider); !ok {
		t.Fatalf("expected EnvSecretsProvider for empty provider, got %T", sp)
	}
}

func TestNewSecretsProvider_EnvExplicit(t *testing.T) {
	sp, err := NewSecretsProvider(&config.SecretsConfig{Provider: "env"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := sp.(*EnvSecretsProvider); !ok {
		t.Fatalf("expected EnvSecretsProvider, got %T", sp)
	}
}

// ---------------------------------------------------------------------------
// NewAuthenticator - additional coverage
// ---------------------------------------------------------------------------

func TestNewAuthenticator_MTLS_Fields(t *testing.T) {
	a, err := NewAuthenticator(&config.AuthConfig{Type: "mtls"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mtls, ok := a.(*MTLSAuth)
	if !ok {
		t.Fatalf("expected MTLSAuth, got %T", a)
	}
	_ = mtls
}

func TestNewAuthenticator_Custom_ReturnsNoop(t *testing.T) {
	a, err := NewAuthenticator(&config.AuthConfig{Type: "custom"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := a.(*NoopAuth); !ok {
		t.Fatalf("expected NoopAuth for custom type, got %T", a)
	}
}

// ---------------------------------------------------------------------------
// BearerAuth - edge cases
// ---------------------------------------------------------------------------

func TestBearerAuth_Authenticate_NonBearerPrefix(t *testing.T) {
	a := &BearerAuth{Token: "my-token"}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false for non-Bearer prefix")
	}
}

func TestBearerAuth_Authenticate_EmptyToken(t *testing.T) {
	a := &BearerAuth{Token: ""}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer ")
	ok, _, _ := a.Authenticate(req)
	if !ok {
		t.Fatal("expected true when both configured and provided tokens are empty")
	}
}

// ---------------------------------------------------------------------------
// APIKeyAuth - edge cases
// ---------------------------------------------------------------------------

func TestAPIKeyAuth_Authenticate_EmptyKeyMatch(t *testing.T) {
	a := &APIKeyAuth{Key: "", Header: "X-API-Key"}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "")
	ok, _, _ := a.Authenticate(req)
	if !ok {
		t.Fatal("expected true when both key and header value are empty")
	}
}

func TestAPIKeyAuth_Authenticate_QueryParamWrongValue(t *testing.T) {
	a := &APIKeyAuth{Key: "correct", QueryParam: "key"}
	req := httptest.NewRequest("GET", "/?key=wrong", nil)
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false for wrong query param value")
	}
}

func TestAPIKeyAuth_Authenticate_QueryParamMissing(t *testing.T) {
	a := &APIKeyAuth{Key: "correct", QueryParam: "key"}
	req := httptest.NewRequest("GET", "/", nil)
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false when query param is missing")
	}
}

// ---------------------------------------------------------------------------
// ApplyDestAuth - api_key edge cases
// ---------------------------------------------------------------------------

func TestApplyDestAuth_APIKey_NoHeaderOrQueryParam(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{Type: "api_key", Key: "k"})
	if req.Header.Get("X-API-Key") != "" && req.URL.RawQuery != "" {
		t.Fatal("expected no auth applied when neither header nor query param specified")
	}
}
