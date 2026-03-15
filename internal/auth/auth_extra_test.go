package auth

import (
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// SessionStore
// ---------------------------------------------------------------------------

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
	ss.Delete("nonexistent") // should not panic
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
	ss.Cleanup() // should not panic
}

// ---------------------------------------------------------------------------
// OIDCProvider - constructor error cases
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// generateState
// ---------------------------------------------------------------------------

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
	// 32 bytes -> base64url ~44 chars
	if len(state) < 40 {
		t.Fatalf("expected state length >= 40, got %d", len(state))
	}
}

// ---------------------------------------------------------------------------
// extractCN
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// VaultSecretsProvider - constructor error cases
// ---------------------------------------------------------------------------

func TestNewVaultSecretsProvider_NilConfig(t *testing.T) {
	_, err := NewVaultSecretsProvider(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "vault config is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// AWS Secrets Provider - constructor error cases
// ---------------------------------------------------------------------------

func TestNewAWSSecretsProvider_NilConfig(t *testing.T) {
	_, err := NewAWSSecretsProvider(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "AWS secrets manager config is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GCP Secrets Provider - constructor error cases
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// NewSecretsProvider - vault/aws/gcp nil sub-config pass-through
// ---------------------------------------------------------------------------

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
