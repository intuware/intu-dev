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

func oidcTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mockOIDCServer returns an httptest.Server whose URL acts as a minimal
// OIDC issuer (discovery, token, userinfo, jwks endpoints).
func mockOIDCServer() *httptest.Server {
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
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"keys":[]}`))
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-access-token",
				"token_type":   "Bearer",
				"id_token":     "mock-id-token",
			})
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"sub":   "user123",
				"email": "user@example.com",
				"name":  "Test User",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return ts
}

// ---------------------------------------------------------------------------
// NewOIDCProvider with mock issuer
// ---------------------------------------------------------------------------

func TestNewOIDCProvider_MockIssuer_Success(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
	if provider.sessions == nil {
		t.Fatal("expected sessions to be initialized")
	}
	if provider.oauth2 == nil {
		t.Fatal("expected oauth2 config to be initialized")
	}
	if provider.oauth2.ClientID != "test-client-id" {
		t.Fatalf("expected client ID test-client-id, got %q", provider.oauth2.ClientID)
	}
}

func TestNewOIDCProvider_MockIssuer_WithRBAC(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	rbac := NewRBACManager([]config.RoleConfig{
		{Name: "admin", Permissions: []string{"*"}},
	})

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, rbac, oidcTestLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if provider.rbac == nil {
		t.Fatal("expected RBAC manager to be set")
	}
}

// ---------------------------------------------------------------------------
// SessionStore - TTL-based expiry cleanup
// ---------------------------------------------------------------------------

func TestSessionStore_CleanupWithSpecificTTL(t *testing.T) {
	ss := NewSessionStore()

	ss.Set("short-lived", &Session{
		User:      "short",
		ExpiresAt: time.Now().Add(10 * time.Millisecond),
	})
	ss.Set("long-lived", &Session{
		User:      "long",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	time.Sleep(20 * time.Millisecond)

	ss.Cleanup()

	if _, ok := ss.Get("short-lived"); ok {
		t.Fatal("short-lived session should have been cleaned up")
	}
	if _, ok := ss.Get("long-lived"); !ok {
		t.Fatal("long-lived session should still exist")
	}
}

func TestSessionStore_ConcurrentAccess(t *testing.T) {
	ss := NewSessionStore()
	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func(idx int) {
			key := "session-" + string(rune('a'+idx))
			ss.Set(key, &Session{
				User:      "user",
				ExpiresAt: time.Now().Add(1 * time.Hour),
			})
			ss.Get(key)
			ss.Delete(key)
			done <- true
		}(i)
	}
	for i := 0; i < 5; i++ {
		go func() {
			ss.Cleanup()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// ---------------------------------------------------------------------------
// HandleLogin - redirect to authorization endpoint
// ---------------------------------------------------------------------------

func TestOIDCProvider_HandleLogin_Redirect(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "test-cid",
		ClientSecret: "test-csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/login", nil)
	w := httptest.NewRecorder()

	provider.HandleLogin(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 redirect, got %d", resp.StatusCode)
	}

	loc := resp.Header.Get("Location")
	if loc == "" {
		t.Fatal("expected Location header")
	}

	if !containsSubstring(loc, ts.URL+"/auth") {
		t.Fatalf("expected redirect to authorization endpoint, got %q", loc)
	}

	if !containsSubstring(loc, "client_id=test-cid") {
		t.Fatalf("expected client_id in redirect URL, got %q", loc)
	}

	if !containsSubstring(loc, "state=") {
		t.Fatalf("expected state parameter in redirect URL, got %q", loc)
	}

	cookies := resp.Cookies()
	var stateCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "intu_oidc_state" {
			stateCookie = c
		}
	}
	if stateCookie == nil {
		t.Fatal("expected intu_oidc_state cookie to be set")
	}
	if !stateCookie.HttpOnly {
		t.Fatal("state cookie should be HttpOnly")
	}
}

// ---------------------------------------------------------------------------
// HandleLogout - clear session
// ---------------------------------------------------------------------------

func TestOIDCProvider_HandleLogout_ClearsSession(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("test-session-id", &Session{
		User:      "testuser",
		Email:     "test@example.com",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/auth/logout", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "test-session-id"})
	w := httptest.NewRecorder()

	provider.HandleLogout(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 redirect, got %d", resp.StatusCode)
	}

	if _, ok := provider.sessions.Get("test-session-id"); ok {
		t.Fatal("session should have been deleted after logout")
	}

	cookies := resp.Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "intu_session" {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("expected intu_session cookie to be cleared")
	}
	if sessionCookie.MaxAge != -1 {
		t.Fatalf("expected MaxAge -1 to clear cookie, got %d", sessionCookie.MaxAge)
	}
}

func TestOIDCProvider_HandleLogout_NoSessionCookie(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/logout", nil)
	w := httptest.NewRecorder()

	provider.HandleLogout(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 redirect even without cookie, got %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// HandleCallback - error cases
// ---------------------------------------------------------------------------

func TestOIDCProvider_HandleCallback_MissingStateCookie(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/callback?code=abc&state=xyz", nil)
	w := httptest.NewRecorder()

	provider.HandleCallback(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing state cookie, got %d", w.Code)
	}
}

func TestOIDCProvider_HandleCallback_StateMismatch(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/callback?code=abc&state=wrong-state", nil)
	req.AddCookie(&http.Cookie{Name: "intu_oidc_state", Value: "correct-state"})
	w := httptest.NewRecorder()

	provider.HandleCallback(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for state mismatch, got %d", w.Code)
	}
}

func TestOIDCProvider_HandleCallback_MissingCode(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/callback?state=the-state", nil)
	req.AddCookie(&http.Cookie{Name: "intu_oidc_state", Value: "the-state"})
	w := httptest.NewRecorder()

	provider.HandleCallback(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing code, got %d", w.Code)
	}
}

func TestOIDCProvider_HandleCallback_TokenExchangeFails(t *testing.T) {
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			// Point to a failing token endpoint
			json.NewEncoder(w).Encode(map[string]any{
				"issuer":                 "",
				"authorization_endpoint": "http://127.0.0.1:1/auth",
				"token_endpoint":         "http://127.0.0.1:1/token",
				"userinfo_endpoint":      "http://127.0.0.1:1/userinfo",
				"jwks_uri":               "http://127.0.0.1:1/jwks",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer failServer.Close()

	// Manually fix the issuer in the discovery doc
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"issuer":                 ts.URL,
				"authorization_endpoint": ts.URL + "/auth",
				"token_endpoint":         "http://127.0.0.1:1/token",
				"userinfo_endpoint":      ts.URL + "/userinfo",
				"jwks_uri":              ts.URL + "/jwks",
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"keys":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/callback?code=test-code&state=the-state", nil)
	req.AddCookie(&http.Cookie{Name: "intu_oidc_state", Value: "the-state"})
	w := httptest.NewRecorder()

	provider.HandleCallback(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for failed token exchange, got %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// GetUserInfo
// ---------------------------------------------------------------------------

func TestOIDCProvider_GetUserInfo_NoSession(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/auth/userinfo", nil)
	_, userErr := provider.GetUserInfo(req)
	if userErr == nil {
		t.Fatal("expected error when no session cookie")
	}
}

func TestOIDCProvider_GetUserInfo_ExpiredSession(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("expired-sid", &Session{
		User:      "user",
		Email:     "user@example.com",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/auth/userinfo", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "expired-sid"})

	_, userErr := provider.GetUserInfo(req)
	if userErr == nil {
		t.Fatal("expected error for expired session")
	}
}

func TestOIDCProvider_GetUserInfo_ValidSession(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("valid-sid", &Session{
		User:      "testuser",
		Email:     "test@example.com",
		Roles:     []string{"admin"},
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/auth/userinfo", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "valid-sid"})

	info, userErr := provider.GetUserInfo(req)
	if userErr != nil {
		t.Fatalf("unexpected error: %v", userErr)
	}
	if info["user"] != "testuser" {
		t.Fatalf("expected user testuser, got %v", info["user"])
	}
	if info["email"] != "test@example.com" {
		t.Fatalf("expected email test@example.com, got %v", info["email"])
	}
	roles, ok := info["roles"].([]string)
	if !ok {
		t.Fatalf("expected roles to be []string, got %T", info["roles"])
	}
	if len(roles) != 1 || roles[0] != "admin" {
		t.Fatalf("expected [admin], got %v", roles)
	}
}

// ---------------------------------------------------------------------------
// Authenticate - session cookie path
// ---------------------------------------------------------------------------

func TestOIDCProvider_Authenticate_ValidSession(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("auth-sid", &Session{
		User:      "sessionuser",
		Email:     "session@example.com",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "auth-sid"})

	ok, user, authErr := provider.Authenticate(req)
	if authErr != nil {
		t.Fatalf("unexpected error: %v", authErr)
	}
	if !ok {
		t.Fatal("expected authentication to succeed")
	}
	if user != "session@example.com" {
		t.Fatalf("expected session@example.com, got %q", user)
	}
}

func TestOIDCProvider_Authenticate_SessionUserFallback(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("auth-sid-2", &Session{
		User:      "fallbackuser",
		Email:     "",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "auth-sid-2"})

	ok, user, authErr := provider.Authenticate(req)
	if authErr != nil {
		t.Fatalf("unexpected error: %v", authErr)
	}
	if !ok {
		t.Fatal("expected authentication to succeed")
	}
	if user != "fallbackuser" {
		t.Fatalf("expected fallbackuser, got %q", user)
	}
}

func TestOIDCProvider_Authenticate_NoCookie(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/test", nil)

	ok, _, _ := provider.Authenticate(req)
	if ok {
		t.Fatal("expected false when no cookie or bearer token")
	}
}

func TestOIDCProvider_Authenticate_InvalidBearerToken(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.Header.Set("Authorization", "Bearer invalid-jwt-token")

	ok, _, authErr := provider.Authenticate(req)
	if authErr != nil {
		t.Fatalf("unexpected error: %v", authErr)
	}
	if ok {
		t.Fatal("expected false for invalid bearer token")
	}
}

func TestOIDCProvider_Authenticate_ExpiredSession(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("expired-auth-sid", &Session{
		User:      "expired",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	})

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "expired-auth-sid"})

	ok, _, _ := provider.Authenticate(req)
	if ok {
		t.Fatal("expected false for expired session")
	}
}

// ---------------------------------------------------------------------------
// generateState - additional tests
// ---------------------------------------------------------------------------

func TestGenerateState_Base64URLEncoded(t *testing.T) {
	state := generateState()
	if containsSubstring(state, "+") || containsSubstring(state, "/") {
		t.Fatalf("state should be base64url encoded (no + or /), got %q", state)
	}
}

// ---------------------------------------------------------------------------
// NewOIDCAuthMiddleware
// ---------------------------------------------------------------------------

func TestNewOIDCAuthMiddleware_RoutesLogin(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	handler := NewOIDCAuthMiddleware(provider, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/login", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected redirect for /auth/login, got %d", w.Code)
	}
}

func TestNewOIDCAuthMiddleware_RoutesLogout(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	handler := NewOIDCAuthMiddleware(provider, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/logout", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected redirect for /auth/logout, got %d", w.Code)
	}
}

func TestNewOIDCAuthMiddleware_RoutesCallback(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	handler := NewOIDCAuthMiddleware(provider, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/callback", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for /auth/callback without state, got %d", w.Code)
	}
}

func TestNewOIDCAuthMiddleware_RoutesUserinfo(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	handler := NewOIDCAuthMiddleware(provider, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/auth/userinfo", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for /auth/userinfo without session, got %d", w.Code)
	}
}

func TestNewOIDCAuthMiddleware_UnauthenticatedAPI(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	handler := NewOIDCAuthMiddleware(provider, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated /api/ request, got %d", w.Code)
	}

	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["login_url"] != "/auth/login" {
		t.Fatalf("expected login_url /auth/login, got %q", body["login_url"])
	}
}

func TestNewOIDCAuthMiddleware_UnauthenticatedNonAPI_DisableLoginPage(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	handler := NewOIDCAuthMiddleware(provider, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected redirect to login when disableLoginPage=true, got %d", w.Code)
	}
}

func TestNewOIDCAuthMiddleware_UnauthenticatedNonAPI_PassThrough(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	handler := NewOIDCAuthMiddleware(provider, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("passed through"))
	}))

	req := httptest.NewRequest("GET", "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 pass-through when disableLoginPage=false, got %d", w.Code)
	}
}

func TestNewOIDCAuthMiddleware_AuthenticatedSetsHeader(t *testing.T) {
	ts := mockOIDCServer()
	defer ts.Close()

	provider, err := NewOIDCProvider(&config.OIDCConfig{
		Issuer:       ts.URL,
		ClientID:     "cid",
		ClientSecret: "csec",
	}, nil, oidcTestLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	provider.sessions.Set("mw-sid", &Session{
		User:      "mwuser",
		Email:     "mw@example.com",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	})

	var capturedUser string
	handler := NewOIDCAuthMiddleware(provider, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUser = r.Header.Get("X-Auth-User")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	req.AddCookie(&http.Cookie{Name: "intu_session", Value: "mw-sid"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for authenticated request, got %d", w.Code)
	}
	if capturedUser != "mw@example.com" {
		t.Fatalf("expected X-Auth-User mw@example.com, got %q", capturedUser)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && stringContains(s, sub))
}

func stringContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
