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
		Type:         "oauth2_client_credentials",
		TokenURL:     server.URL,
		ClientID:     "cid",
		ClientSecret: "csecret",
		Scopes:       []string{"s1", "s2"},
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
		Type:         "oauth2_client_credentials",
		TokenURL:     server.URL,
		ClientID:     "cid",
		ClientSecret: "csecret",
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
func TestNewSessionStore(t *testing.T) {
	ss := NewSessionStore()
	if ss == nil {
		t.Fatal("expected non-nil SessionStore")
	}
	if ss.sessions == nil {
		t.Fatal("expected sessions map to be initialized")
	}
}

func TestSessionStore_SetAndGet(t *testing.T) {
	ss := NewSessionStore()
	sess := &Session{
		User:      "alice",
		Email:     "alice@example.com",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}
	ss.Set("sid-1", sess)

	got, ok := ss.Get("sid-1")
	if !ok {
		t.Fatal("expected session to be found")
	}
	if got.User != "alice" {
		t.Fatalf("expected User alice, got %q", got.User)
	}
	if got.Email != "alice@example.com" {
		t.Fatalf("expected Email alice@example.com, got %q", got.Email)
	}
}

func TestSessionStore_GetMissing(t *testing.T) {
	ss := NewSessionStore()
	_, ok := ss.Get("nonexistent")
	if ok {
		t.Fatal("expected session not found")
	}
}

func TestSessionStore_GetExpired(t *testing.T) {
	ss := NewSessionStore()
	sess := &Session{
		User:      "bob",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	}
	ss.Set("sid-expired", sess)

	got, ok := ss.Get("sid-expired")
	if ok {
		t.Fatal("expected expired session to not be returned")
	}
	if got != nil {
		t.Fatal("expected nil session for expired entry")
	}
}

func TestSessionStore_Delete(t *testing.T) {
	ss := NewSessionStore()
	ss.Set("sid-del", &Session{
		User:      "charlie",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	ss.Delete("sid-del")
	_, ok := ss.Get("sid-del")
	if ok {
		t.Fatal("expected session to be deleted")
	}
}

func TestSessionStore_DeleteNonexistent(t *testing.T) {
	ss := NewSessionStore()
	ss.Delete("nonexistent")
}

func TestSessionStore_Cleanup(t *testing.T) {
	ss := NewSessionStore()
	ss.Set("active", &Session{
		User:      "active-user",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})
	ss.Set("expired-1", &Session{
		User:      "expired-user-1",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	})
	ss.Set("expired-2", &Session{
		User:      "expired-user-2",
		ExpiresAt: time.Now().Add(-2 * time.Hour),
	})

	ss.Cleanup()

	if _, ok := ss.Get("active"); !ok {
		t.Fatal("active session should survive cleanup")
	}

	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if _, exists := ss.sessions["expired-1"]; exists {
		t.Fatal("expired-1 should have been cleaned up")
	}
	if _, exists := ss.sessions["expired-2"]; exists {
		t.Fatal("expired-2 should have been cleaned up")
	}
}

func TestSessionStore_CleanupEmpty(t *testing.T) {
	ss := NewSessionStore()
	ss.Cleanup()
}

func TestNewOIDCProvider_NilConfig(t *testing.T) {
	_, err := NewOIDCProvider(nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "OIDC issuer is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewOIDCProvider_EmptyIssuer(t *testing.T) {
	_, err := NewOIDCProvider(&config.OIDCConfig{Issuer: ""}, nil, nil)
	if err == nil {
		t.Fatal("expected error for empty issuer")
	}
	if err.Error() != "OIDC issuer is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewOIDCProvider_InvalidIssuer(t *testing.T) {
	_, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:   "http://127.0.0.1:0/not-a-real-issuer",
		ClientID: "cid",
	}, nil, nil)
	if err == nil {
		t.Fatal("expected error for unreachable issuer")
	}
}

func TestGenerateState_NonEmpty(t *testing.T) {
	state := generateState()
	if state == "" {
		t.Fatal("expected non-empty state string")
	}
}

func TestGenerateState_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		s := generateState()
		if seen[s] {
			t.Fatalf("duplicate state generated: %q", s)
		}
		seen[s] = true
	}
}

func TestGenerateState_Length(t *testing.T) {
	state := generateState()

	if len(state) < 40 {
		t.Fatalf("expected state length >= 40, got %d", len(state))
	}
}

func TestExtractCN_Standard(t *testing.T) {
	dn := "CN=Admins,OU=Groups,DC=example,DC=com"
	cn := extractCN(dn)
	if cn != "Admins" {
		t.Fatalf("expected Admins, got %q", cn)
	}
}

func TestExtractCN_Lowercase(t *testing.T) {
	dn := "cn=developers,ou=Groups,dc=example,dc=com"
	cn := extractCN(dn)
	if cn != "developers" {
		t.Fatalf("expected developers, got %q", cn)
	}
}

func TestExtractCN_MixedCase(t *testing.T) {
	dn := "Cn=Operators,OU=IT,DC=corp,DC=net"
	cn := extractCN(dn)
	if cn != "Operators" {
		t.Fatalf("expected Operators, got %q", cn)
	}
}

func TestExtractCN_NoCN(t *testing.T) {
	dn := "OU=Groups,DC=example,DC=com"
	cn := extractCN(dn)
	if cn != "" {
		t.Fatalf("expected empty string for DN without CN, got %q", cn)
	}
}

func TestExtractCN_EmptyDN(t *testing.T) {
	cn := extractCN("")
	if cn != "" {
		t.Fatalf("expected empty string for empty DN, got %q", cn)
	}
}

func TestExtractCN_WithSpaces(t *testing.T) {
	dn := " CN=Admin Group , OU=Groups , DC=example , DC=com "
	cn := extractCN(dn)
	if cn != "Admin Group" {
		t.Fatalf("expected 'Admin Group', got %q", cn)
	}
}

func TestExtractCN_OnlyCN(t *testing.T) {
	dn := "CN=SingleValue"
	cn := extractCN(dn)
	if cn != "SingleValue" {
		t.Fatalf("expected SingleValue, got %q", cn)
	}
}

func TestNewVaultSecretsProvider_NilConfig(t *testing.T) {
	_, err := NewVaultSecretsProvider(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "vault config is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewAWSSecretsProvider_NilConfig(t *testing.T) {
	_, err := NewAWSSecretsProvider(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "AWS secrets manager config is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewGCPSecretsProvider_NilConfig(t *testing.T) {
	_, err := NewGCPSecretsProvider(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "GCP secret manager config is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewGCPSecretsProvider_EmptyProjectID(t *testing.T) {
	_, err := NewGCPSecretsProvider(&config.GCPSecretManagerConfig{ProjectID: ""})
	if err == nil {
		t.Fatal("expected error for empty project_id")
	}
	if err.Error() != "GCP project_id is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewSecretsProvider_VaultNilSubConfig(t *testing.T) {
	_, err := NewSecretsProvider(&config.SecretsConfig{Provider: "vault", Vault: nil})
	if err == nil {
		t.Fatal("expected error when vault sub-config is nil")
	}
}

func TestNewSecretsProvider_AWSNilSubConfig(t *testing.T) {
	_, err := NewSecretsProvider(&config.SecretsConfig{Provider: "aws_secrets_manager", AWS: nil})
	if err == nil {
		t.Fatal("expected error when AWS sub-config is nil")
	}
}

func TestNewSecretsProvider_GCPNilSubConfig(t *testing.T) {
	_, err := NewSecretsProvider(&config.SecretsConfig{Provider: "gcp_secret_manager", GCP: nil})
	if err == nil {
		t.Fatal("expected error when GCP sub-config is nil")
	}
}
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

func TestNewSecretsProvider_VaultWithConfig(t *testing.T) {
	_, err := NewSecretsProvider(&config.SecretsConfig{
		Provider: "vault",
		Vault:    &config.VaultConfig{Address: "http://127.0.0.1:1"},
	})

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

func TestApplyDestAuth_APIKey_NoHeaderOrQueryParam(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	ApplyDestAuth(req, &config.HTTPAuthConfig{Type: "api_key", Key: "k"})
	if req.Header.Get("X-API-Key") != "" && req.URL.RawQuery != "" {
		t.Fatal("expected no auth applied when neither header nor query param specified")
	}
}
func boostTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func mockOIDCServerFull() *httptest.Server {
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"issuer":                 ts.URL,
				"authorization_endpoint": ts.URL + "/auth",
				"token_endpoint":         ts.URL + "/token",
				"userinfo_endpoint":      ts.URL + "/userinfo",
				"jwks_uri":               ts.URL + "/jwks",
			})
		case "/auth":

			state := r.URL.Query().Get("state")
			redirect := r.URL.Query().Get("redirect_uri")
			http.Redirect(w, r, redirect+"?code=mock-auth-code&state="+state, http.StatusFound)
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-access-token",
				"token_type":   "Bearer",
				"id_token":     "mock-id-token",
				"expires_in":   3600,
			})
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"sub":    "user-sub-123",
				"email":  "test@example.com",
				"name":   "Test User",
				"groups": []string{"admin", "developers"},
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"keys":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return ts
}

func TestOIDCProvider_FullFlowSetup(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	rbac := NewRBACManager([]config.RoleConfig{
		{Name: "admin", Permissions: []string{"*"}},
		{Name: "viewer", Permissions: []string{"channels.read"}},
	})

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "test-client",
		ClientSecret: "test-secret",
	}, rbac, boostTestLogger())
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	if provider.oauth2.ClientID != "test-client" {
		t.Fatalf("expected ClientID test-client, got %q", provider.oauth2.ClientID)
	}
	if provider.oauth2.ClientSecret != "test-secret" {
		t.Fatalf("expected ClientSecret test-secret, got %q", provider.oauth2.ClientSecret)
	}
	if provider.oauth2.Endpoint.TokenURL != ts.URL+"/token" {
		t.Fatalf("expected token endpoint %s/token, got %q", ts.URL, provider.oauth2.Endpoint.TokenURL)
	}
}

func TestOIDCProvider_HandleLogin_RedirectURL(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid-login",
		ClientSecret: "csec-login",
	}, nil, boostTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/login", nil)
	w := httptest.NewRecorder()
	provider.HandleLogin(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307, got %d", resp.StatusCode)
	}

	loc := resp.Header.Get("Location")
	if loc == "" {
		t.Fatal("expected Location header")
	}

	checks := []string{"client_id=cid-login", "response_type=code", "scope=", "state="}
	for _, check := range checks {
		if !stringContains(loc, check) {
			t.Errorf("redirect URL missing %q: %s", check, loc)
		}
	}

	var stateCookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "intu_oidc_state" {
			stateCookie = c
		}
	}
	if stateCookie == nil {
		t.Fatal("expected intu_oidc_state cookie")
	}
	if stateCookie.MaxAge != 300 {
		t.Fatalf("expected MaxAge 300, got %d", stateCookie.MaxAge)
	}
}

func TestOIDCProvider_HandleCallback_TokenExchangeWithMockServer(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid-cb",
		ClientSecret: "csec-cb",
	}, nil, boostTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/callback?code=test-code&state=test-state", nil)
	req.AddCookie(&http.Cookie{Name: "intu_oidc_state", Value: "test-state"})
	w := httptest.NewRecorder()
	provider.HandleCallback(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for invalid id_token, got %d", w.Code)
	}
}

func TestOIDCProvider_SessionManagement_EndToEnd(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid-sess",
		ClientSecret: "csec-sess",
	}, nil, boostTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	sessionID := "manual-test-session"
	provider.sessions.Set(sessionID, &Session{
		User:      "testuser",
		Email:     "test@example.com",
		Roles:     []string{"admin"},
		ExpiresAt: time.Now().Add(1 * time.Hour),
		Claims: map[string]any{
			"sub":   "sub-123",
			"email": "test@example.com",
		},
	})

	req := httptest.NewRequest("GET", "/api/data", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: sessionID})
	ok, user, err := provider.Authenticate(req)
	if err != nil {
		t.Fatalf("auth error: %v", err)
	}
	if !ok {
		t.Fatal("expected authentication to succeed")
	}
	if user != "test@example.com" {
		t.Fatalf("expected test@example.com, got %q", user)
	}

	infoReq := httptest.NewRequest("GET", "/auth/userinfo", nil)
	infoReq.AddCookie(&http.Cookie{Name: "intu_session", Value: sessionID})
	info, err := provider.GetUserInfo(infoReq)
	if err != nil {
		t.Fatalf("get user info: %v", err)
	}
	if info["email"] != "test@example.com" {
		t.Fatalf("expected email test@example.com, got %v", info["email"])
	}
	roles, ok := info["roles"].([]string)
	if !ok || len(roles) != 1 || roles[0] != "admin" {
		t.Fatalf("expected [admin], got %v", info["roles"])
	}

	logoutReq := httptest.NewRequest("GET", "/auth/logout", nil)
	logoutReq.AddCookie(&http.Cookie{Name: "intu_session", Value: sessionID})
	logoutW := httptest.NewRecorder()
	provider.HandleLogout(logoutW, logoutReq)

	if logoutW.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 on logout, got %d", logoutW.Code)
	}

	if _, ok := provider.sessions.Get(sessionID); ok {
		t.Fatal("session should be deleted after logout")
	}

	req2 := httptest.NewRequest("GET", "/api/data", nil)
	req2.AddCookie(&http.Cookie{Name: "intu_session", Value: sessionID})
	ok2, _, _ := provider.Authenticate(req2)
	if ok2 {
		t.Fatal("expected auth to fail after logout")
	}
}

func TestVaultSecretsProvider_Get_MockServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/secret/data/intu":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"data": map[string]any{
						"db_password": "super-secret-123",
						"api_key":     "key-abc-def",
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
	})
	if err != nil {
		t.Fatalf("create vault provider: %v", err)
	}

	val, err := provider.Get("db_password")
	if err != nil {
		t.Fatalf("get db_password: %v", err)
	}
	if val != "super-secret-123" {
		t.Fatalf("expected 'super-secret-123', got %q", val)
	}

	val2, err := provider.Get("api_key")
	if err != nil {
		t.Fatalf("get api_key: %v", err)
	}
	if val2 != "key-abc-def" {
		t.Fatalf("expected 'key-abc-def', got %q", val2)
	}
}

func TestVaultSecretsProvider_Get_CacheHit(t *testing.T) {
	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"data": map[string]any{
					"cached_key": "cached_value",
				},
			},
		})
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	val1, err := provider.Get("cached_key")
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if val1 != "cached_value" {
		t.Fatalf("expected 'cached_value', got %q", val1)
	}

	firstCallCount := callCount

	val2, err := provider.Get("cached_key")
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if val2 != "cached_value" {
		t.Fatalf("expected 'cached_value', got %q", val2)
	}

	if callCount != firstCallCount {
		t.Fatalf("expected cache hit (no additional server calls), but server was called %d times total", callCount)
	}
}

func TestVaultSecretsProvider_Get_KeyNotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"data": map[string]any{
					"existing": "value",
				},
			},
		})
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = provider.Get("nonexistent_key")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
}

func TestVaultSecretsProvider_Get_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = provider.Get("any_key")
	if err == nil {
		t.Fatal("expected error for server error response")
	}
}

func TestVaultSecretsProvider_Get_EmptyData(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = provider.Get("missing")
	if err == nil {
		t.Fatal("expected error for 404")
	}
}

func TestVaultSecretsProvider_Get_CustomPath(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/custom/path" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"data": map[string]any{
					"my_secret": "custom-path-secret",
				},
			},
		})
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
		Path:    "custom/path",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	val, err := provider.Get("my_secret")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != "custom-path-secret" {
		t.Fatalf("expected 'custom-path-secret', got %q", val)
	}
}

func TestVaultSecretsProvider_Authenticate_TokenType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"key1": "val1"},
		})
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
		Auth:    &config.VaultAuthConfig{Type: "token"},
	})
	if err != nil {
		t.Fatalf("create with token auth: %v", err)
	}
	_ = provider
}

func TestVaultSecretsProvider_Authenticate_UnsupportedType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	_, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
		Auth:    &config.VaultAuthConfig{Type: "unsupported"},
	})
	if err == nil {
		t.Fatal("expected error for unsupported auth type")
	}
}

func TestLDAPProvider_GetUserRole_NoRBAC_NoGroups(t *testing.T) {

	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, nil, boostTestLogger())

	_, err := provider.GetUserRole("testuser")
	if err == nil {
		t.Fatal("expected error when LDAP unreachable")
	}
}

func TestLDAPProvider_GetUserRole_WithRBAC_ConnectionFails(t *testing.T) {
	rbac := NewRBACManager([]config.RoleConfig{
		{Name: "admin", Permissions: []string{"*"}},
		{Name: "developers", Permissions: []string{"channels.*"}},
	})
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, rbac, boostTestLogger())

	_, err := provider.GetUserRole("user")
	if err == nil {
		t.Fatal("expected error from unreachable LDAP")
	}
}

func TestLDAPProvider_Constructor_AllFields(t *testing.T) {
	rbac := NewRBACManager([]config.RoleConfig{
		{Name: "viewer", Permissions: []string{"channels.read"}},
	})
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:          "ldap://ldap.example.com:389",
		BaseDN:       "dc=example,dc=com",
		BindDN:       "cn=admin,dc=example,dc=com",
		BindPassword: "secret",
	}, rbac, boostTestLogger())

	if provider.cfg.BindDN != "cn=admin,dc=example,dc=com" {
		t.Fatalf("expected BindDN, got %q", provider.cfg.BindDN)
	}
	if provider.rbac == nil {
		t.Fatal("expected RBAC to be set")
	}
}

func TestOIDCMiddleware_AuthErrorPath(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, boostTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/userinfo", nil)
	provider.sessions.Set("mw-test-sid", &Session{
		User:      "mw-user",
		Email:     "mw@test.com",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "mw-test-sid"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var info map[string]any
	json.NewDecoder(w.Body).Decode(&info)
	if info["email"] != "mw@test.com" {
		t.Fatalf("expected email mw@test.com, got %v", info["email"])
	}
}

func TestVaultSecretsProvider_Get_NonStringValue(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"data": map[string]any{
					"num_val": 12345,
				},
			},
		})
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = provider.Get("num_val")
	if err == nil {
		t.Fatal("expected error for non-string value")
	}
}

func TestSessionStore_HighConcurrency(t *testing.T) {
	ss := NewSessionStore()
	const goroutines = 20
	const ops = 100

	done := make(chan struct{}, goroutines*3)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			for j := 0; j < ops; j++ {
				key := generateState()
				ss.Set(key, &Session{
					User:      "user",
					ExpiresAt: time.Now().Add(1 * time.Minute),
				})
				ss.Get(key)
			}
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < ops; j++ {
				ss.Cleanup()
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < ops; j++ {
				ss.Delete(generateState())
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < goroutines*3; i++ {
		<-done
	}
}

func TestVaultSecretsProvider_Get_KVv1Style(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"kv1_key": "kv1_value",
			},
		})
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	val, err := provider.Get("kv1_key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != "kv1_value" {
		t.Fatalf("expected 'kv1_value', got %q", val)
	}
}
func pushLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPush_OIDCMiddleware_APIUnauth(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated API request, got %d", w.Code)
	}

	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["login_url"] != "/auth/login" {
		t.Fatalf("expected login_url=/auth/login, got %q", body["login_url"])
	}
}

func TestPush_OIDCMiddleware_AuthenticatedRequest(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("valid-session", &Session{
		User:      "testuser",
		Email:     "test@test.com",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	var capturedUser string
	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUser = r.Header.Get("X-Auth-User")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/data", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "valid-session"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if capturedUser != "test@test.com" {
		t.Fatalf("expected X-Auth-User=test@test.com, got %q", capturedUser)
	}
}

func TestPush_OIDCMiddleware_DisableLoginPage(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, true)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 redirect to OIDC login, got %d", w.Code)
	}
}

func TestPush_OIDCMiddleware_LoginRoute(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/login", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 for login route, got %d", w.Code)
	}
}

func TestPush_OIDCMiddleware_LogoutRoute(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("logout-session", &Session{
		User:      "user",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/logout", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "logout-session"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 for logout route, got %d", w.Code)
	}
}

func TestPush_OIDCMiddleware_UserinfoUnauth(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/userinfo", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestPush_OIDCMiddleware_CallbackMissingState(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/callback?code=test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing state cookie, got %d", w.Code)
	}
}

func TestPush_OIDCMiddleware_CallbackStateMismatch(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/callback?code=test&state=wrong", nil)
	req.AddCookie(&http.Cookie{Name: "intu_oidc_state", Value: "correct"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for state mismatch, got %d", w.Code)
	}
}

func TestPush_OIDCMiddleware_CallbackMissingCode(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	mw := NewOIDCAuthMiddleware(provider, false)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/callback?state=mystate", nil)
	req.AddCookie(&http.Cookie{Name: "intu_oidc_state", Value: "mystate"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing code, got %d", w.Code)
	}
}

func TestPush_LDAPMiddleware_NonAPIPath(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=test,dc=com",
	}, nil, pushLogger())

	mw := NewLDAPAuthMiddleware(provider)
	var called bool
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if !called {
		t.Fatal("expected handler to be called for non-API path")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPush_LDAPMiddleware_APIPathUnauth(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=test,dc=com",
	}, nil, pushLogger())

	mw := NewLDAPAuthMiddleware(provider)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
	if w.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("expected WWW-Authenticate header")
	}
}

func TestPush_LDAPMiddleware_APIPathBadCredentials(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=test,dc=com",
	}, nil, pushLogger())

	mw := NewLDAPAuthMiddleware(provider)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	req.SetBasicAuth("user", "pass")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 (LDAP unreachable), got %d", w.Code)
	}
}

func TestPush_VaultSecretsProvider_AuthNilAuth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"key1": "val1"}})
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
		Auth:    nil,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = provider
}

func TestPush_VaultSecretsProvider_AuthEmptyType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	provider, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
		Auth:    &config.VaultAuthConfig{Type: ""},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = provider
}

func TestPush_VaultSecretsProvider_AuthAppRoleMissingCreds(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	_, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
		Auth:    &config.VaultAuthConfig{Type: "approle"},
	})
	if err == nil {
		t.Fatal("expected error for approle with missing credentials")
	}
}

func TestPush_VaultSecretsProvider_AuthKubernetesMissingToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	_, err := NewVaultSecretsProvider(&config.VaultConfig{
		Address: ts.URL,
		Auth:    &config.VaultAuthConfig{Type: "kubernetes", RoleID: "test-role"},
	})
	if err == nil {
		t.Fatal("expected error for kubernetes with missing token file")
	}
}

func TestPush_OIDCProvider_AuthenticateViaCookie_UserFallback(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("user-only-session", &Session{
		User:      "username-only",
		Email:     "",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/api/data", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "user-only-session"})
	ok, user, err := provider.Authenticate(req)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	if !ok {
		t.Fatal("expected auth to succeed")
	}
	if user != "username-only" {
		t.Fatalf("expected username-only, got %q", user)
	}
}

func TestPush_OIDCProvider_AuthenticateNoCookie(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/data", nil)
	ok, _, _ := provider.Authenticate(req)
	if ok {
		t.Fatal("expected false when no cookie/bearer")
	}
}

func TestPush_OIDCProvider_GetUserInfo_NoCookie(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/", nil)
	_, err = provider.GetUserInfo(req)
	if err == nil {
		t.Fatal("expected error for no cookie")
	}
}

func TestPush_OIDCProvider_GetUserInfo_ExpiredSession(t *testing.T) {
	ts := mockOIDCServerFull()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, pushLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("expired", &Session{
		User:      "expuser",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "expired"})
	_, err = provider.GetUserInfo(req)
	if err == nil {
		t.Fatal("expected error for expired session")
	}
}

func TestPush_JoinStrings(t *testing.T) {
	result := joinStrings(nil, ", ")
	if result != "" {
		t.Fatalf("expected empty, got %q", result)
	}

	result = joinStrings([]string{"a"}, ", ")
	if result != "a" {
		t.Fatalf("expected 'a', got %q", result)
	}

	result = joinStrings([]string{"a", "b", "c"}, " AND ")
	if result != "a AND b AND c" {
		t.Fatalf("expected 'a AND b AND c', got %q", result)
	}
}
