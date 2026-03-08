package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/intuware/intu/pkg/config"
	"golang.org/x/oauth2"
)

type OIDCProvider struct {
	cfg      *config.OIDCConfig
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	oauth2   *oauth2.Config
	rbac     *RBACManager
	logger   *slog.Logger
	sessions *SessionStore
}

type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

type Session struct {
	User      string
	Email     string
	Roles     []string
	ExpiresAt time.Time
	Claims    map[string]any
}

func NewSessionStore() *SessionStore {
	return &SessionStore{
		sessions: make(map[string]*Session),
	}
}

func (ss *SessionStore) Set(id string, session *Session) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.sessions[id] = session
}

func (ss *SessionStore) Get(id string) (*Session, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	s, ok := ss.sessions[id]
	if ok && time.Now().After(s.ExpiresAt) {
		return nil, false
	}
	return s, ok
}

func (ss *SessionStore) Delete(id string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.sessions, id)
}

func (ss *SessionStore) Cleanup() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	now := time.Now()
	for id, s := range ss.sessions {
		if now.After(s.ExpiresAt) {
			delete(ss.sessions, id)
		}
	}
}

func NewOIDCProvider(cfg *config.OIDCConfig, rbac *RBACManager, logger *slog.Logger) (*OIDCProvider, error) {
	if cfg == nil || cfg.Issuer == "" {
		return nil, fmt.Errorf("OIDC issuer is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider: %w", err)
	}

	redirectURI := "http://localhost:3000/auth/callback"

	oauth2Config := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		Endpoint:     provider.Endpoint(),
		RedirectURL:  redirectURI,
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}

	verifier := provider.Verifier(&oidc.Config{
		ClientID: cfg.ClientID,
	})

	op := &OIDCProvider{
		cfg:      cfg,
		provider: provider,
		verifier: verifier,
		oauth2:   oauth2Config,
		rbac:     rbac,
		logger:   logger,
		sessions: NewSessionStore(),
	}

	go op.sessionCleanupLoop()

	return op, nil
}

func (op *OIDCProvider) sessionCleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		op.sessions.Cleanup()
	}
}

func (op *OIDCProvider) Authenticate(r *http.Request) (bool, string, error) {
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		idToken, err := op.verifier.Verify(r.Context(), token)
		if err != nil {
			return false, "", nil
		}

		var claims struct {
			Sub   string `json:"sub"`
			Email string `json:"email"`
			Name  string `json:"name"`
		}
		if err := idToken.Claims(&claims); err != nil {
			return false, "", fmt.Errorf("parse claims: %w", err)
		}

		user := claims.Email
		if user == "" {
			user = claims.Name
		}
		if user == "" {
			user = claims.Sub
		}

		return true, user, nil
	}

	cookie, err := r.Cookie("intu_session")
	if err != nil {
		return false, "", nil
	}

	session, ok := op.sessions.Get(cookie.Value)
	if !ok {
		return false, "", nil
	}

	user := session.Email
	if user == "" {
		user = session.User
	}
	return true, user, nil
}

func (op *OIDCProvider) HandleLogin(w http.ResponseWriter, r *http.Request) {
	state := generateState()
	http.SetCookie(w, &http.Cookie{
		Name:     "intu_oidc_state",
		Value:    state,
		MaxAge:   300,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})

	authURL := op.oauth2.AuthCodeURL(state)
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

func (op *OIDCProvider) HandleCallback(w http.ResponseWriter, r *http.Request) {
	stateCookie, err := r.Cookie("intu_oidc_state")
	if err != nil {
		http.Error(w, "missing state cookie", http.StatusBadRequest)
		return
	}

	if r.URL.Query().Get("state") != stateCookie.Value {
		http.Error(w, "state mismatch", http.StatusBadRequest)
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "missing authorization code", http.StatusBadRequest)
		return
	}

	oauth2Token, err := op.oauth2.Exchange(r.Context(), code)
	if err != nil {
		http.Error(w, "token exchange failed", http.StatusInternalServerError)
		op.logger.Error("OIDC token exchange failed", "error", err)
		return
	}

	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		http.Error(w, "no id_token in token response", http.StatusInternalServerError)
		return
	}

	idToken, err := op.verifier.Verify(r.Context(), rawIDToken)
	if err != nil {
		http.Error(w, "id_token verification failed", http.StatusInternalServerError)
		op.logger.Error("OIDC id_token verification failed", "error", err)
		return
	}

	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		http.Error(w, "failed to parse claims", http.StatusInternalServerError)
		return
	}

	email, _ := claims["email"].(string)
	name, _ := claims["name"].(string)
	sub, _ := claims["sub"].(string)

	user := email
	if user == "" {
		user = name
	}
	if user == "" {
		user = sub
	}

	var roles []string
	if groupsClaim, ok := claims["groups"]; ok {
		if groups, ok := groupsClaim.([]any); ok {
			for _, g := range groups {
				if gs, ok := g.(string); ok {
					roles = append(roles, gs)
				}
			}
		}
	}
	if rolesClaim, ok := claims["roles"]; ok {
		if rs, ok := rolesClaim.([]any); ok {
			for _, r := range rs {
				if s, ok := r.(string); ok {
					roles = append(roles, s)
				}
			}
		}
	}

	sessionID := generateState()
	op.sessions.Set(sessionID, &Session{
		User:      user,
		Email:     email,
		Roles:     roles,
		ExpiresAt: time.Now().Add(8 * time.Hour),
		Claims:    claims,
	})

	http.SetCookie(w, &http.Cookie{
		Name:     "intu_session",
		Value:    sessionID,
		MaxAge:   28800,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})

	http.SetCookie(w, &http.Cookie{
		Name:   "intu_oidc_state",
		MaxAge: -1,
		Path:   "/",
	})

	op.logger.Info("OIDC login successful", "user", user, "email", email)
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (op *OIDCProvider) HandleLogout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("intu_session")
	if err == nil {
		op.sessions.Delete(cookie.Value)
	}

	http.SetCookie(w, &http.Cookie{
		Name:   "intu_session",
		MaxAge: -1,
		Path:   "/",
	})

	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (op *OIDCProvider) GetUserInfo(r *http.Request) (map[string]any, error) {
	cookie, err := r.Cookie("intu_session")
	if err != nil {
		return nil, fmt.Errorf("no session")
	}

	session, ok := op.sessions.Get(cookie.Value)
	if !ok {
		return nil, fmt.Errorf("session expired")
	}

	info := map[string]any{
		"user":  session.User,
		"email": session.Email,
		"roles": session.Roles,
	}
	return info, nil
}

func NewOIDCAuthMiddleware(provider *OIDCProvider, disableLoginPage bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/auth/login" {
				provider.HandleLogin(w, r)
				return
			}
			if r.URL.Path == "/auth/callback" {
				provider.HandleCallback(w, r)
				return
			}
			if r.URL.Path == "/auth/logout" {
				provider.HandleLogout(w, r)
				return
			}
			if r.URL.Path == "/auth/userinfo" {
				info, err := provider.GetUserInfo(r)
				if err != nil {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]string{"error": "not authenticated"})
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(info)
				return
			}

			ok, user, err := provider.Authenticate(r)
			if err != nil {
				http.Error(w, "authentication error", http.StatusInternalServerError)
				return
			}

			if !ok {
				if strings.HasPrefix(r.URL.Path, "/api/") {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]string{
						"error":     "authentication required",
						"login_url": "/auth/login",
					})
					return
				}
				if disableLoginPage {
					provider.HandleLogin(w, r)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			r.Header.Set("X-Auth-User", user)
			next.ServeHTTP(w, r)
		})
	}
}

func generateState() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}
