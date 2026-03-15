package auth

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

func pushLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ===================================================================
// OIDC middleware — additional paths
// ===================================================================

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

// ===================================================================
// LDAP middleware
// ===================================================================

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

// ===================================================================
// Vault — authenticate with auth types
// ===================================================================

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

// ===================================================================
// OIDCProvider — Authenticate with User fallback (Name, Sub)
// ===================================================================

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

// ===================================================================
// OIDCProvider — GetUserInfo errors
// ===================================================================

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

// ===================================================================
// joinStrings helper
// ===================================================================

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

