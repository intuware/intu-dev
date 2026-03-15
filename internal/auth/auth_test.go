package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// auth.go - NewAuthenticator
// ---------------------------------------------------------------------------

func TestNewAuthenticator_Nil(t *testing.T) {
	a, err := NewAuthenticator(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := a.(*NoopAuth); !ok {
		t.Fatalf("expected NoopAuth, got %T", a)
	}
}

func TestNewAuthenticator_None(t *testing.T) {
	for _, typ := range []string{"", "none"} {
		a, err := NewAuthenticator(&config.AuthConfig{Type: typ})
		if err != nil {
			t.Fatalf("type %q: unexpected error: %v", typ, err)
		}
		if _, ok := a.(*NoopAuth); !ok {
			t.Fatalf("type %q: expected NoopAuth, got %T", typ, a)
		}
	}
}

func TestNewAuthenticator_Bearer(t *testing.T) {
	a, err := NewAuthenticator(&config.AuthConfig{Type: "bearer", Token: "secret-token"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ba, ok := a.(*BearerAuth)
	if !ok {
		t.Fatalf("expected BearerAuth, got %T", a)
	}
	if ba.Token != "secret-token" {
		t.Fatalf("expected token 'secret-token', got %q", ba.Token)
	}
}

func TestNewAuthenticator_Basic(t *testing.T) {
	a, err := NewAuthenticator(&config.AuthConfig{Type: "basic", Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ba, ok := a.(*BasicAuth)
	if !ok {
		t.Fatalf("expected BasicAuth, got %T", a)
	}
	if ba.Username != "u" || ba.Password != "p" {
		t.Fatalf("expected u/p, got %q/%q", ba.Username, ba.Password)
	}
}

func TestNewAuthenticator_APIKey(t *testing.T) {
	a, err := NewAuthenticator(&config.AuthConfig{Type: "api_key", Key: "k", Header: "X-API-Key"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	aka, ok := a.(*APIKeyAuth)
	if !ok {
		t.Fatalf("expected APIKeyAuth, got %T", a)
	}
	if aka.Key != "k" || aka.Header != "X-API-Key" {
		t.Fatalf("expected key k header X-API-Key, got %q/%q", aka.Key, aka.Header)
	}

	a2, err := NewAuthenticator(&config.AuthConfig{Type: "api_key", Key: "k2", QueryParam: "apikey"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	aka2 := a2.(*APIKeyAuth)
	if aka2.QueryParam != "apikey" {
		t.Fatalf("expected query param apikey, got %q", aka2.QueryParam)
	}
}

func TestNewAuthenticator_MTLS(t *testing.T) {
	a, err := NewAuthenticator(&config.AuthConfig{Type: "mtls"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := a.(*MTLSAuth); !ok {
		t.Fatalf("expected MTLSAuth, got %T", a)
	}
}

func TestNewAuthenticator_Custom(t *testing.T) {
	a, err := NewAuthenticator(&config.AuthConfig{Type: "custom"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := a.(*NoopAuth); !ok {
		t.Fatalf("expected NoopAuth for custom, got %T", a)
	}
}

func TestNewAuthenticator_Unsupported(t *testing.T) {
	_, err := NewAuthenticator(&config.AuthConfig{Type: "ldap"})
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
	if err.Error() != "unsupported auth type: ldap" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// auth.go - NoopAuth
// ---------------------------------------------------------------------------

func TestNoopAuth_Authenticate(t *testing.T) {
	a := &NoopAuth{}
	req := httptest.NewRequest("GET", "/", nil)
	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	if user != "" {
		t.Fatalf("expected empty user, got %q", user)
	}
}

// ---------------------------------------------------------------------------
// auth.go - BearerAuth
// ---------------------------------------------------------------------------

func TestBearerAuth_Authenticate_Valid(t *testing.T) {
	a := &BearerAuth{Token: "my-token"}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer my-token")
	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	if user != "bearer-user" {
		t.Fatalf("expected bearer-user, got %q", user)
	}
}

func TestBearerAuth_Authenticate_Invalid(t *testing.T) {
	a := &BearerAuth{Token: "my-token"}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false")
	}
}

func TestBearerAuth_Authenticate_MissingHeader(t *testing.T) {
	a := &BearerAuth{Token: "my-token"}
	req := httptest.NewRequest("GET", "/", nil)
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false")
	}
}

// ---------------------------------------------------------------------------
// auth.go - BasicAuth
// ---------------------------------------------------------------------------

func TestBasicAuth_Authenticate_Valid(t *testing.T) {
	a := &BasicAuth{Username: "alice", Password: "secret"}
	req := httptest.NewRequest("GET", "/", nil)
	req.SetBasicAuth("alice", "secret")
	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	if user != "alice" {
		t.Fatalf("expected alice, got %q", user)
	}
}

func TestBasicAuth_Authenticate_Invalid(t *testing.T) {
	a := &BasicAuth{Username: "alice", Password: "secret"}
	req := httptest.NewRequest("GET", "/", nil)
	req.SetBasicAuth("alice", "wrong")
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false")
	}
}

func TestBasicAuth_Authenticate_Missing(t *testing.T) {
	a := &BasicAuth{Username: "alice", Password: "secret"}
	req := httptest.NewRequest("GET", "/", nil)
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false")
	}
}

// ---------------------------------------------------------------------------
// auth.go - APIKeyAuth
// ---------------------------------------------------------------------------

func TestAPIKeyAuth_Authenticate_Header(t *testing.T) {
	a := &APIKeyAuth{Key: "key123", Header: "X-API-Key"}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "key123")
	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	if user != "api-key-user" {
		t.Fatalf("expected api-key-user, got %q", user)
	}
}

func TestAPIKeyAuth_Authenticate_QueryParam(t *testing.T) {
	a := &APIKeyAuth{Key: "key456", QueryParam: "apikey"}
	req := httptest.NewRequest("GET", "/?apikey=key456", nil)
	ok, user, err := a.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	if user != "api-key-user" {
		t.Fatalf("expected api-key-user, got %q", user)
	}
}

func TestAPIKeyAuth_Authenticate_HeaderWrong(t *testing.T) {
	a := &APIKeyAuth{Key: "key123", Header: "X-API-Key"}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "wrong")
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false")
	}
}

func TestAPIKeyAuth_Authenticate_NeitherHeaderNorQueryParam(t *testing.T) {
	a := &APIKeyAuth{Key: "key123"}
	req := httptest.NewRequest("GET", "/", nil)
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false")
	}
}

// ---------------------------------------------------------------------------
// auth.go - MTLSAuth
// ---------------------------------------------------------------------------

func TestMTLSAuth_Authenticate_NoTLS(t *testing.T) {
	a := &MTLSAuth{}
	req := httptest.NewRequest("GET", "/", nil)
	ok, _, _ := a.Authenticate(req)
	if ok {
		t.Fatal("expected false when no TLS")
	}
}

func TestMTLSAuth_Authenticate_WithCert(t *testing.T) {
	// Create a cert with CN for mtls check
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "client-cn"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
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
	if user != "client-cn" {
		t.Fatalf("expected client-cn, got %q", user)
	}
}

// ---------------------------------------------------------------------------
// auth.go - ApplyDestAuth
// ---------------------------------------------------------------------------

func TestApplyDestAuth_Nil(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, nil)
	if req.Header.Get("Authorization") != "" {
		t.Fatalf("expected no auth header")
	}
}

func TestApplyDestAuth_Bearer(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{Type: "bearer", Token: "token123"})
	if v := req.Header.Get("Authorization"); v != "Bearer token123" {
		t.Fatalf("expected Bearer token123, got %q", v)
	}
}

func TestApplyDestAuth_Basic(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{Type: "basic", Username: "u", Password: "p"})
	v := req.Header.Get("Authorization")
	if v == "" || len(v) < 6 || v[:6] != "Basic " {
		t.Fatalf("expected Basic <base64>, got %q", v)
	}
	decoded, err := base64.StdEncoding.DecodeString(v[6:])
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}
	if string(decoded) != "u:p" {
		t.Fatalf("expected u:p, got %q", string(decoded))
	}
}

func TestApplyDestAuth_APIKey_Header(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{Type: "api_key", Key: "key1", Header: "X-API-Key"})
	if v := req.Header.Get("X-API-Key"); v != "key1" {
		t.Fatalf("expected X-API-Key: key1, got %q", v)
	}
}

func TestApplyDestAuth_APIKey_QueryParam(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{Type: "api_key", Key: "key2", QueryParam: "apikey"})
	if req.URL.Query().Get("apikey") != "key2" {
		t.Fatalf("expected apikey=key2 in query, got %q", req.URL.RawQuery)
	}
}

func TestApplyDestAuth_OAuth2ClientCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = r.ParseForm()
		if r.Form.Get("grant_type") != "client_credentials" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": "oauth-token-xyz"})
	}))
	defer server.Close()

	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:           "oauth2_client_credentials",
		TokenURL:       server.URL,
		ClientID:       "cid",
		ClientSecret:   "csecret",
		Scopes:         []string{"s1", "s2"},
	})
	if v := req.Header.Get("Authorization"); v != "Bearer oauth-token-xyz" {
		t.Fatalf("expected Bearer oauth-token-xyz, got %q", v)
	}
}

func TestApplyDestAuth_OAuth2MissingCreds(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:         "oauth2_client_credentials",
		TokenURL:     "http://localhost:99999",
		ClientID:     "",
		ClientSecret: "",
	})
	if req.Header.Get("Authorization") != "" {
		t.Fatalf("expected no auth when creds missing")
	}
}

func TestApplyDestAuth_OAuth2TokenFetchFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{
		Type:           "oauth2_client_credentials",
		TokenURL:       server.URL,
		ClientID:       "cid",
		ClientSecret:   "csecret",
	})
	if req.Header.Get("Authorization") != "" {
		t.Fatalf("expected no auth when token fetch fails")
	}
}

// ---------------------------------------------------------------------------
// auth.go - FetchOAuth2ClientCredentials
// ---------------------------------------------------------------------------

func TestFetchOAuth2ClientCredentials_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": "tok123"})
	}))
	defer server.Close()

	tok, err := FetchOAuth2ClientCredentials(server.URL, "cid", "csec", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok != "tok123" {
		t.Fatalf("expected tok123, got %q", tok)
	}
}

func TestFetchOAuth2ClientCredentials_WithScopes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Form.Get("scope") != "a b" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": "tok"})
	}))
	defer server.Close()

	tok, err := FetchOAuth2ClientCredentials(server.URL, "cid", "csec", []string{"a", "b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok != "tok" {
		t.Fatalf("expected tok, got %q", tok)
	}
}

func TestFetchOAuth2ClientCredentials_NonOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := FetchOAuth2ClientCredentials(server.URL, "cid", "csec", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFetchOAuth2ClientCredentials_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	_, err := FetchOAuth2ClientCredentials(server.URL, "cid", "csec", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// rbac.go
// ---------------------------------------------------------------------------

func TestNewRBACManager_Empty(t *testing.T) {
	rm := NewRBACManager(nil)
	if rm == nil {
		t.Fatal("expected non-nil")
	}
	if rm.HasPermission("any", "anything") {
		t.Fatal("empty manager should not grant permission")
	}
}

func TestNewRBACManager_WithRoles(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{
		{Name: "admin", Permissions: []string{"*"}},
		{Name: "viewer", Permissions: []string{"channels.read", "channels.*"}},
		{Name: "ops", Permissions: []string{"channels.deploy", "channels.undeploy"}},
	})
	if rm == nil {
		t.Fatal("expected non-nil")
	}
}

func TestRBACManager_HasPermission_ExactMatch(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{
		{Name: "viewer", Permissions: []string{"channels.read"}},
	})
	if !rm.HasPermission("viewer", "channels.read") {
		t.Fatal("expected true for exact match")
	}
	if rm.HasPermission("viewer", "channels.write") {
		t.Fatal("expected false for different permission")
	}
}

func TestRBACManager_HasPermission_Wildcard(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{
		{Name: "admin", Permissions: []string{"*"}},
	})
	if !rm.HasPermission("admin", "anything") {
		t.Fatal("expected true for wildcard")
	}
	if !rm.HasPermission("admin", "channels.delete") {
		t.Fatal("expected true for wildcard")
	}
}

func TestRBACManager_HasPermission_PrefixWildcard(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{
		{Name: "viewer", Permissions: []string{"channels.*"}},
	})
	if !rm.HasPermission("viewer", "channels.read") {
		t.Fatal("expected true for channels.read")
	}
	if !rm.HasPermission("viewer", "channels.deploy") {
		t.Fatal("expected true for channels.deploy")
	}
	if rm.HasPermission("viewer", "messages.read") {
		t.Fatal("expected false for messages.read")
	}
}

func TestRBACManager_HasPermission_NoRole(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{
		{Name: "viewer", Permissions: []string{"channels.read"}},
	})
	if rm.HasPermission("nonexistent", "channels.read") {
		t.Fatal("expected false for unknown role")
	}
}

func TestRBACManager_GetRole_Found(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{
		{Name: "admin", Permissions: []string{"*"}},
	})
	role, err := rm.GetRole("admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if role.Name != "admin" {
		t.Fatalf("expected admin, got %q", role.Name)
	}
	if !role.Permissions["*"] {
		t.Fatal("expected * permission")
	}
}

func TestRBACManager_GetRole_NotFound(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{})
	_, err := rm.GetRole("missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != `role "missing" not found` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRBACManager_ListRoles(t *testing.T) {
	rm := NewRBACManager([]config.RoleConfig{
		{Name: "a", Permissions: nil},
		{Name: "b", Permissions: nil},
	})
	names := rm.ListRoles()
	if len(names) != 2 {
		t.Fatalf("expected 2 roles, got %d", len(names))
	}
	// Order is non-deterministic (map iteration)
	seen := make(map[string]bool)
	for _, n := range names {
		seen[n] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("expected a and b, got %v", names)
	}
}

// ---------------------------------------------------------------------------
// secrets.go
// ---------------------------------------------------------------------------

func TestNewSecretsProvider_Nil(t *testing.T) {
	sp, err := NewSecretsProvider(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := sp.(*EnvSecretsProvider); !ok {
		t.Fatalf("expected EnvSecretsProvider, got %T", sp)
	}
}

func TestNewSecretsProvider_Env(t *testing.T) {
	for _, p := range []string{"", "env"} {
		sp, err := NewSecretsProvider(&config.SecretsConfig{Provider: p})
		if err != nil {
			t.Fatalf("provider %q: unexpected error: %v", p, err)
		}
		if _, ok := sp.(*EnvSecretsProvider); !ok {
			t.Fatalf("provider %q: expected EnvSecretsProvider, got %T", p, sp)
		}
	}
}

func TestNewSecretsProvider_Unsupported(t *testing.T) {
	_, err := NewSecretsProvider(&config.SecretsConfig{Provider: "unknown"})
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "unsupported secrets provider: unknown" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnvSecretsProvider_Get_Set(t *testing.T) {
	key := "AUTH_TEST_SECRET_KEY"
	os.Setenv(key, "secret-value")
	defer os.Unsetenv(key)

	sp := &EnvSecretsProvider{}
	val, err := sp.Get(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "secret-value" {
		t.Fatalf("expected secret-value, got %q", val)
	}
}

func TestEnvSecretsProvider_Get_Unset(t *testing.T) {
	key := "AUTH_TEST_NONEXISTENT_VAR_XYZ"
	os.Unsetenv(key)

	sp := &EnvSecretsProvider{}
	_, err := sp.Get(key)
	if err == nil {
		t.Fatal("expected error for unset var")
	}
}

func TestEnvSecretsProvider_Get_Empty(t *testing.T) {
	key := "AUTH_TEST_EMPTY_KEY"
	os.Setenv(key, "")
	defer os.Unsetenv(key)

	sp := &EnvSecretsProvider{}
	_, err := sp.Get(key)
	if err == nil {
		t.Fatal("expected error for empty var")
	}
}

// ---------------------------------------------------------------------------
// tls.go - BuildTLSConfig
// ---------------------------------------------------------------------------

func generateTestCerts(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	os.WriteFile(certFile, certPEM, 0o644)
	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	os.WriteFile(keyFile, keyPEM, 0o600)
	return certFile, keyFile
}

func TestBuildTLSConfig_Nil(t *testing.T) {
	cfg, err := BuildTLSConfig(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil config")
	}
}

func TestBuildTLSConfig_MinVersion(t *testing.T) {
	for _, tc := range []struct {
		ver     string
		wantVer uint16
	}{
		{"1.0", tls.VersionTLS10},
		{"1.1", tls.VersionTLS11},
		{"1.3", tls.VersionTLS13},
		{"", tls.VersionTLS12},
		{"1.2", tls.VersionTLS12},
		{"x", tls.VersionTLS12},
	} {
		cfg, err := BuildTLSConfig(&config.TLSConfig{MinVersion: tc.ver})
		if err != nil {
			t.Fatalf("MinVersion %q: %v", tc.ver, err)
		}
		if cfg.MinVersion != tc.wantVer {
			t.Fatalf("MinVersion %q: expected %d, got %d", tc.ver, tc.wantVer, cfg.MinVersion)
		}
	}
}

func TestBuildTLSConfig_CertAndKey(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)
	cfg, err := BuildTLSConfig(&config.TLSConfig{CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Certificates) == 0 {
		t.Fatal("expected at least one certificate")
	}
}

func TestBuildTLSConfig_CAFile(t *testing.T) {
	certFile, _ := generateTestCerts(t)
	caFile := certFile
	cfg, err := BuildTLSConfig(&config.TLSConfig{CAFile: caFile})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("expected RootCAs")
	}
	if cfg.ClientCAs == nil {
		t.Fatal("expected ClientCAs")
	}
}

func TestBuildTLSConfig_CAFile_Invalid(t *testing.T) {
	_, err := BuildTLSConfig(&config.TLSConfig{CAFile: "/nonexistent/ca.pem"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBuildTLSConfig_CAFile_InvalidPEM(t *testing.T) {
	f := filepath.Join(t.TempDir(), "bad.pem")
	os.WriteFile(f, []byte("not valid pem"), 0o644)
	_, err := BuildTLSConfig(&config.TLSConfig{CAFile: f})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBuildTLSConfig_ClientAuth(t *testing.T) {
	for _, tc := range []struct {
		auth     string
		wantAuth tls.ClientAuthType
	}{
		{"require", tls.RequireAndVerifyClientCert},
		{"request", tls.RequestClientCert},
		{"", tls.NoClientCert},
		{"x", tls.NoClientCert},
	} {
		cfg, err := BuildTLSConfig(&config.TLSConfig{ClientAuth: tc.auth})
		if err != nil {
			t.Fatalf("ClientAuth %q: %v", tc.auth, err)
		}
		if cfg.ClientAuth != tc.wantAuth {
			t.Fatalf("ClientAuth %q: expected %v, got %v", tc.auth, tc.wantAuth, cfg.ClientAuth)
		}
	}
}

func TestBuildTLSConfig_ClientCertAndKey(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)
	cfg, err := BuildTLSConfig(&config.TLSConfig{
		ClientCertFile: certFile,
		ClientKeyFile:  keyFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Certificates) == 0 {
		t.Fatal("expected at least one certificate")
	}
}

func TestBuildTLSConfig_InsecureSkipVerify(t *testing.T) {
	cfg, err := BuildTLSConfig(&config.TLSConfig{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify true")
	}
}

func TestBuildTLSConfig_BadCertFile(t *testing.T) {
	_, err := BuildTLSConfig(&config.TLSConfig{CertFile: "/nonexistent", KeyFile: "/nonexistent"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// tls.go - BuildTLSConfigFromMap
// ---------------------------------------------------------------------------

func TestBuildTLSConfigFromMap_Nil(t *testing.T) {
	cfg, err := BuildTLSConfigFromMap(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil")
	}
}

func TestBuildTLSConfigFromMap_Disabled(t *testing.T) {
	cfg, err := BuildTLSConfigFromMap(&config.TLSMapConfig{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil when disabled")
	}
}

func TestBuildTLSConfigFromMap_Enabled(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)
	cfg, err := BuildTLSConfigFromMap(&config.TLSMapConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if len(cfg.Certificates) == 0 {
		t.Fatal("expected certificates")
	}
}

// ---------------------------------------------------------------------------
// tls.go - DialTLS
// ---------------------------------------------------------------------------

func TestDialTLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	addr := server.Listener.Addr().String()
	conn, err := DialTLS(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()
}

// ---------------------------------------------------------------------------
// audit.go - AuditLogger
// ---------------------------------------------------------------------------

func testAuditLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewAuditLogger(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	if al == nil {
		t.Fatal("expected non-nil")
	}
}

func TestAuditLogger_Log_Disabled(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: false}, testAuditLogger())
	al.Log("login", "user1", nil)
	entries := al.GetEntries(10)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries when disabled, got %d", len(entries))
	}
}

func TestAuditLogger_Log_NilConfig(t *testing.T) {
	al := NewAuditLogger(nil, testAuditLogger())
	al.Log("login", "user1", nil)
	entries := al.GetEntries(10)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries when nil config, got %d", len(entries))
	}
}

func TestAuditLogger_Log_Enabled(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	al.Log("login", "user1", map[string]any{"ip": "1.2.3.4"})
	entries := al.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Event != "login" || entries[0].User != "user1" {
		t.Fatalf("unexpected entry: %+v", entries[0])
	}
	if entries[0].Details["ip"] != "1.2.3.4" {
		t.Fatalf("expected ip in details")
	}
}

func TestAuditLogger_Log_TrackedEvents(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true, Events: []string{"login", "logout"}}, testAuditLogger())
	al.Log("login", "u", nil)
	al.Log("logout", "u", nil)
	al.Log("other", "u", nil)
	entries := al.GetEntries(10)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (login, logout), got %d", len(entries))
	}
}

func TestAuditLogger_Log_EmptyEventsTracksAll(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true, Events: []string{}}, testAuditLogger())
	al.Log("any-event", "u", nil)
	entries := al.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry when events empty (track all), got %d", len(entries))
	}
}

func TestAuditLogger_SetStore(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	store := NewMemoryAuditStore()
	al.SetStore(store)
	al.Log("e", "u", nil)
	entries := al.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 from store, got %d", len(entries))
	}
}

func TestAuditLogger_GetEntries_Limit(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	for i := 0; i < 5; i++ {
		al.Log("e", "u", nil)
	}
	entries := al.GetEntries(2)
	if len(entries) != 2 {
		t.Fatalf("expected 2 with limit 2, got %d", len(entries))
	}
}

func TestAuditLogger_GetEntries_StoreQueryError(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	al.Log("e", "u", nil)
	al.SetStore(&failingQueryStore{})
	entries := al.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("expected fallback to in-memory (1 entry), got %d", len(entries))
	}
}

type failingQueryStore struct{}

func (f *failingQueryStore) Save(entry *AuditEntry) error { return nil }
func (f *failingQueryStore) Query(opts AuditQueryOpts) ([]AuditEntry, error) {
	return nil, os.ErrNotExist
}
func (f *failingQueryStore) Close() error { return nil }

func TestAuditLogger_Close(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	store := NewMemoryAuditStore()
	al.SetStore(store)
	err := al.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuditLogger_Close_NoStore(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	err := al.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuditLogger_Log_StoreSaveFails(t *testing.T) {
	al := NewAuditLogger(&config.AuditConfig{Enabled: true}, testAuditLogger())
	al.SetStore(&failingSaveStore{})
	al.Log("login", "u", nil)
	entries := al.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 in-memory entry despite store save failure, got %d", len(entries))
	}
}

type failingSaveStore struct{}

func (f *failingSaveStore) Save(entry *AuditEntry) error { return os.ErrPermission }
func (f *failingSaveStore) Query(opts AuditQueryOpts) ([]AuditEntry, error) {
	return nil, os.ErrClosed
}
func (f *failingSaveStore) Close() error { return nil }

// ---------------------------------------------------------------------------
// audit.go - MemoryAuditStore
// ---------------------------------------------------------------------------

func TestMemoryAuditStore_Save(t *testing.T) {
	store := NewMemoryAuditStore()
	entry := &AuditEntry{Timestamp: time.Now(), Event: "e", User: "u"}
	err := store.Save(entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entries, _ := store.Query(AuditQueryOpts{})
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestMemoryAuditStore_Query_EventFilter(t *testing.T) {
	store := NewMemoryAuditStore()
	store.Save(&AuditEntry{Timestamp: time.Now(), Event: "login", User: "u1"})
	store.Save(&AuditEntry{Timestamp: time.Now(), Event: "logout", User: "u1"})
	entries, _ := store.Query(AuditQueryOpts{Event: "login"})
	if len(entries) != 1 {
		t.Fatalf("expected 1 login, got %d", len(entries))
	}
	if entries[0].Event != "login" {
		t.Fatalf("expected login, got %s", entries[0].Event)
	}
}

func TestMemoryAuditStore_Query_UserFilter(t *testing.T) {
	store := NewMemoryAuditStore()
	store.Save(&AuditEntry{Timestamp: time.Now(), Event: "e", User: "alice"})
	store.Save(&AuditEntry{Timestamp: time.Now(), Event: "e", User: "bob"})
	entries, _ := store.Query(AuditQueryOpts{User: "alice"})
	if len(entries) != 1 {
		t.Fatalf("expected 1 alice, got %d", len(entries))
	}
}

func TestMemoryAuditStore_Query_ReverseOrder(t *testing.T) {
	store := NewMemoryAuditStore()
	store.Save(&AuditEntry{Timestamp: time.Now().Add(-2 * time.Hour), Event: "e", User: "u"})
	store.Save(&AuditEntry{Timestamp: time.Now().Add(-1 * time.Hour), Event: "e", User: "u"})
	store.Save(&AuditEntry{Timestamp: time.Now(), Event: "e", User: "u"})
	entries, _ := store.Query(AuditQueryOpts{})
	if len(entries) != 3 {
		t.Fatalf("expected 3, got %d", len(entries))
	}
	if entries[0].Timestamp.Before(entries[1].Timestamp) {
		t.Fatal("expected reverse chronological order")
	}
}

func TestMemoryAuditStore_Query_Limit(t *testing.T) {
	store := NewMemoryAuditStore()
	for i := 0; i < 5; i++ {
		store.Save(&AuditEntry{Timestamp: time.Now(), Event: "e", User: "u"})
	}
	entries, _ := store.Query(AuditQueryOpts{Limit: 2})
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
}

func TestMemoryAuditStore_Query_Offset(t *testing.T) {
	store := NewMemoryAuditStore()
	store.Save(&AuditEntry{Timestamp: time.Now().Add(-2 * time.Hour), Event: "e", User: "u"})
	store.Save(&AuditEntry{Timestamp: time.Now().Add(-1 * time.Hour), Event: "e", User: "u"})
	store.Save(&AuditEntry{Timestamp: time.Now(), Event: "e", User: "u"})
	entries, _ := store.Query(AuditQueryOpts{Offset: 1, Limit: 1})
	if len(entries) != 1 {
		t.Fatalf("expected 1, got %d", len(entries))
	}
}

func TestMemoryAuditStore_Query_SinceBefore(t *testing.T) {
	store := NewMemoryAuditStore()
	mid := time.Now().Add(-1 * time.Hour)
	store.Save(&AuditEntry{Timestamp: mid.Add(-2 * time.Hour), Event: "e", User: "u"})
	store.Save(&AuditEntry{Timestamp: mid, Event: "e", User: "u"})
	store.Save(&AuditEntry{Timestamp: mid.Add(2 * time.Hour), Event: "e", User: "u"})
	entries, _ := store.Query(AuditQueryOpts{Since: mid.Add(-30 * time.Minute), Before: mid.Add(30 * time.Minute)})
	if len(entries) != 1 {
		t.Fatalf("expected 1 in range, got %d", len(entries))
	}
}

func TestMemoryAuditStore_Close(t *testing.T) {
	store := NewMemoryAuditStore()
	err := store.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
