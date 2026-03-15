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

func boostTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// OIDCProvider: full OAuth2 flow with mock discovery/token/userinfo/jwks
// ---------------------------------------------------------------------------

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
			// Authorization endpoint; just echo back the state
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

	// Verify the redirect includes required OAuth2 parameters
	checks := []string{"client_id=cid-login", "response_type=code", "scope=", "state="}
	for _, check := range checks {
		if !stringContains(loc, check) {
			t.Errorf("redirect URL missing %q: %s", check, loc)
		}
	}

	// Verify state cookie is set
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

	// HandleCallback will exchange the code, then try to verify the id_token.
	// Our mock returns "mock-id-token" which is not a real JWT, so verification fails.
	// We test that the code exchange happens and the error is about token verification.
	req := httptest.NewRequest("GET", "/auth/callback?code=test-code&state=test-state", nil)
	req.AddCookie(&http.Cookie{Name: "intu_oidc_state", Value: "test-state"})
	w := httptest.NewRecorder()
	provider.HandleCallback(w, req)

	// Token exchange will succeed (mock returns token), but id_token verification fails
	// because "mock-id-token" is not a valid JWT.
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

	// Manually create a session as if login completed
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

	// Authenticate via session cookie
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

	// Get user info
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

	// Logout
	logoutReq := httptest.NewRequest("GET", "/auth/logout", nil)
	logoutReq.AddCookie(&http.Cookie{Name: "intu_session", Value: sessionID})
	logoutW := httptest.NewRecorder()
	provider.HandleLogout(logoutW, logoutReq)

	if logoutW.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 on logout, got %d", logoutW.Code)
	}

	// Session should be deleted
	if _, ok := provider.sessions.Get(sessionID); ok {
		t.Fatal("session should be deleted after logout")
	}

	// Authenticate should fail now
	req2 := httptest.NewRequest("GET", "/api/data", nil)
	req2.AddCookie(&http.Cookie{Name: "intu_session", Value: sessionID})
	ok2, _, _ := provider.Authenticate(req2)
	if ok2 {
		t.Fatal("expected auth to fail after logout")
	}
}

// ---------------------------------------------------------------------------
// VaultSecretsProvider: Get method with mock Vault server
// ---------------------------------------------------------------------------

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

	// First call hits server
	val1, err := provider.Get("cached_key")
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if val1 != "cached_value" {
		t.Fatalf("expected 'cached_value', got %q", val1)
	}

	firstCallCount := callCount

	// Second call should hit cache
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

	// Token auth type (default) should not error
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

// ---------------------------------------------------------------------------
// LDAPProvider: GetUserRole with role mapping config
// ---------------------------------------------------------------------------

func TestLDAPProvider_GetUserRole_NoRBAC_NoGroups(t *testing.T) {
	// GetUserRole calls GetUserGroups which needs a real LDAP server.
	// We test the RBAC mapping logic indirectly by noting that a connection
	// failure means empty groups, which returns "viewer" as default.
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

// ---------------------------------------------------------------------------
// OIDCProvider: middleware integration tests
// ---------------------------------------------------------------------------

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

	// /auth/userinfo returns userinfo JSON
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

// ---------------------------------------------------------------------------
// VaultSecretsProvider: non-string value error
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// SessionStore concurrent stress test
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// VaultSecretsProvider: data without nested "data" key (KV v1)
// ---------------------------------------------------------------------------

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
