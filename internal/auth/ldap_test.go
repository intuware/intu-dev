package auth

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/intuware/intu-dev/pkg/config"
)

func ldapTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// NewLDAPProvider - constructor
// ---------------------------------------------------------------------------

func TestNewLDAPProvider_Basic(t *testing.T) {
	cfg := &config.LDAPConfig{
		URL:    "ldap://localhost:389",
		BaseDN: "dc=example,dc=com",
	}
	provider := NewLDAPProvider(cfg, nil, ldapTestLogger())
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
	if provider.cfg != cfg {
		t.Fatal("expected config to be stored")
	}
	if provider.rbac != nil {
		t.Fatal("expected nil RBAC when not provided")
	}
}

func TestNewLDAPProvider_WithRBAC(t *testing.T) {
	rbac := NewRBACManager([]config.RoleConfig{
		{Name: "admin", Permissions: []string{"*"}},
	})
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://localhost:389",
		BaseDN: "dc=example,dc=com",
	}, rbac, ldapTestLogger())
	if provider.rbac == nil {
		t.Fatal("expected RBAC manager to be set")
	}
}

func TestNewLDAPProvider_NilConfig(t *testing.T) {
	provider := NewLDAPProvider(nil, nil, ldapTestLogger())
	if provider == nil {
		t.Fatal("expected non-nil provider even with nil config")
	}
}

// ---------------------------------------------------------------------------
// extractCN - additional edge cases
// ---------------------------------------------------------------------------

func TestExtractCN_MultipleCN(t *testing.T) {
	dn := "CN=First,CN=Second,DC=example,DC=com"
	cn := extractCN(dn)
	if cn != "First" {
		t.Fatalf("expected First (first CN), got %q", cn)
	}
}

func TestExtractCN_CNWithEqualsSign(t *testing.T) {
	dn := "CN=Name=Value,DC=example,DC=com"
	cn := extractCN(dn)
	if cn != "Name=Value" {
		t.Fatalf("expected 'Name=Value', got %q", cn)
	}
}

func TestExtractCN_CNAtEnd(t *testing.T) {
	dn := "OU=Groups,DC=example,CN=LastCN"
	cn := extractCN(dn)
	if cn != "LastCN" {
		t.Fatalf("expected LastCN, got %q", cn)
	}
}

func TestExtractCN_EmptyValue(t *testing.T) {
	dn := "CN=,DC=example,DC=com"
	cn := extractCN(dn)
	if cn != "" {
		t.Fatalf("expected empty string for empty CN value, got %q", cn)
	}
}

func TestExtractCN_WhitespaceOnly(t *testing.T) {
	dn := "   "
	cn := extractCN(dn)
	if cn != "" {
		t.Fatalf("expected empty for whitespace-only DN, got %q", cn)
	}
}

func TestExtractCN_ComplexDN(t *testing.T) {
	dn := "CN=Healthcare Admins,OU=Security Groups,OU=Groups,DC=hospital,DC=local"
	cn := extractCN(dn)
	if cn != "Healthcare Admins" {
		t.Fatalf("expected 'Healthcare Admins', got %q", cn)
	}
}

// ---------------------------------------------------------------------------
// Authenticate - no BasicAuth header
// ---------------------------------------------------------------------------

func TestLDAPProvider_Authenticate_NoBasicAuth(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://localhost:389",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	req := httptest.NewRequest("GET", "/api/test", nil)
	ok, user, err := provider.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false when no BasicAuth provided")
	}
	if user != "" {
		t.Fatalf("expected empty user, got %q", user)
	}
}

// ---------------------------------------------------------------------------
// Authenticate - invalid LDAP URL causes connection failure
// ---------------------------------------------------------------------------

func TestLDAPProvider_Authenticate_ConnectionFailure(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.SetBasicAuth("testuser", "testpass")

	ok, _, err := provider.Authenticate(req)
	if err != nil {
		t.Fatalf("Authenticate should not return error, but got: %v", err)
	}
	if ok {
		t.Fatal("expected false when LDAP connection fails")
	}
}

func TestLDAPProvider_Authenticate_EmptyURL(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.SetBasicAuth("testuser", "testpass")

	ok, _, err := provider.Authenticate(req)
	if err != nil {
		t.Fatalf("Authenticate should not return error, but got: %v", err)
	}
	if ok {
		t.Fatal("expected false when URL is empty")
	}
}

// ---------------------------------------------------------------------------
// NewLDAPAuthMiddleware
// ---------------------------------------------------------------------------

func TestNewLDAPAuthMiddleware_NonAPIPassthrough(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	handler := NewLDAPAuthMiddleware(provider)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("passed"))
	}))

	req := httptest.NewRequest("GET", "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for non-API path, got %d", w.Code)
	}
	if w.Body.String() != "passed" {
		t.Fatalf("expected 'passed' body, got %q", w.Body.String())
	}
}

func TestNewLDAPAuthMiddleware_APIWithoutAuth(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	handler := NewLDAPAuthMiddleware(provider)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for /api/ without auth, got %d", w.Code)
	}

	wwwAuth := w.Header().Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Fatal("expected WWW-Authenticate header")
	}
}

func TestNewLDAPAuthMiddleware_APIWithBadCredentials(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	handler := NewLDAPAuthMiddleware(provider)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	req.SetBasicAuth("user", "pass")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for /api/ with bad LDAP creds, got %d", w.Code)
	}
}

func TestNewLDAPAuthMiddleware_Creation(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://localhost:389",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	mw := NewLDAPAuthMiddleware(provider)
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	handler := mw(inner)
	if handler == nil {
		t.Fatal("expected non-nil handler from middleware")
	}
}

// ---------------------------------------------------------------------------
// GetUserGroups / GetUserRole - connection failure paths
// ---------------------------------------------------------------------------

func TestLDAPProvider_GetUserGroups_ConnectionFailure(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	_, err := provider.GetUserGroups("testuser")
	if err == nil {
		t.Fatal("expected error when LDAP connection fails")
	}
}

func TestLDAPProvider_GetUserRole_ConnectionFailure(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "ldap://127.0.0.1:1",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	_, err := provider.GetUserRole("testuser")
	if err == nil {
		t.Fatal("expected error when LDAP connection fails in GetUserRole")
	}
}

func TestLDAPProvider_GetUserGroups_EmptyURL(t *testing.T) {
	provider := NewLDAPProvider(&config.LDAPConfig{
		URL:    "",
		BaseDN: "dc=example,dc=com",
	}, nil, ldapTestLogger())

	_, err := provider.GetUserGroups("testuser")
	if err == nil {
		t.Fatal("expected error when URL is empty")
	}
}
