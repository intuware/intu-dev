package auth

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	vault "github.com/hashicorp/vault/api"
	"github.com/intuware/intu/pkg/config"
)

type VaultSecretsProviderReal struct {
	client *vault.Client
	cfg    *config.VaultConfig
	cache  map[string]cachedSecret
	mu     sync.RWMutex
	ttl    time.Duration
}

type cachedSecret struct {
	value     string
	expiresAt time.Time
}

func NewVaultSecretsProvider(cfg *config.VaultConfig) (*VaultSecretsProviderReal, error) {
	if cfg == nil {
		return nil, fmt.Errorf("vault config is nil")
	}

	vaultCfg := vault.DefaultConfig()
	if cfg.Address != "" {
		vaultCfg.Address = cfg.Address
	}

	client, err := vault.NewClient(vaultCfg)
	if err != nil {
		return nil, fmt.Errorf("create vault client: %w", err)
	}

	provider := &VaultSecretsProviderReal{
		client: client,
		cfg:    cfg,
		cache:  make(map[string]cachedSecret),
		ttl:    5 * time.Minute,
	}

	if err := provider.authenticate(); err != nil {
		return nil, fmt.Errorf("vault authentication: %w", err)
	}

	return provider, nil
}

func (v *VaultSecretsProviderReal) authenticate() error {
	if v.cfg.Auth == nil {
		return nil
	}

	switch v.cfg.Auth.Type {
	case "token", "":
		return nil
	case "approle":
		return v.authenticateAppRole()
	case "kubernetes":
		return v.authenticateKubernetes()
	default:
		return fmt.Errorf("unsupported vault auth type: %s", v.cfg.Auth.Type)
	}
}

func (v *VaultSecretsProviderReal) authenticateAppRole() error {
	if v.cfg.Auth.RoleID == "" || v.cfg.Auth.SecretID == "" {
		return fmt.Errorf("approle auth requires role_id and secret_id")
	}

	data := map[string]interface{}{
		"role_id":   v.cfg.Auth.RoleID,
		"secret_id": v.cfg.Auth.SecretID,
	}

	resp, err := v.client.Logical().Write("auth/approle/login", data)
	if err != nil {
		return fmt.Errorf("approle login: %w", err)
	}
	if resp == nil || resp.Auth == nil {
		return fmt.Errorf("approle login returned no auth data")
	}

	v.client.SetToken(resp.Auth.ClientToken)

	if resp.Auth.Renewable {
		go v.renewToken(resp.Auth.LeaseDuration)
	}

	return nil
}

func (v *VaultSecretsProviderReal) authenticateKubernetes() error {
	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return fmt.Errorf("read k8s service account token: %w", err)
	}
	jwt := strings.TrimSpace(string(tokenBytes))

	role := v.cfg.Auth.RoleID
	if role == "" {
		return fmt.Errorf("kubernetes auth requires role_id (used as role name)")
	}

	data := map[string]interface{}{
		"jwt":  jwt,
		"role": role,
	}

	resp, err := v.client.Logical().Write("auth/kubernetes/login", data)
	if err != nil {
		return fmt.Errorf("kubernetes login: %w", err)
	}
	if resp == nil || resp.Auth == nil {
		return fmt.Errorf("kubernetes login returned no auth data")
	}

	v.client.SetToken(resp.Auth.ClientToken)

	if resp.Auth.Renewable {
		go v.renewToken(resp.Auth.LeaseDuration)
	}

	return nil
}

func (v *VaultSecretsProviderReal) renewToken(leaseDuration int) {
	renewBefore := time.Duration(leaseDuration) * time.Second / 3
	if renewBefore < 10*time.Second {
		renewBefore = 10 * time.Second
	}

	ticker := time.NewTicker(renewBefore)
	defer ticker.Stop()

	for range ticker.C {
		secret, err := v.client.Auth().Token().RenewSelf(leaseDuration)
		if err != nil {
			return
		}
		if secret == nil || secret.Auth == nil {
			return
		}
		leaseDuration = secret.Auth.LeaseDuration
		renewBefore = time.Duration(leaseDuration) * time.Second / 3
		if renewBefore < 10*time.Second {
			renewBefore = 10 * time.Second
		}
		ticker.Reset(renewBefore)
	}
}

func (v *VaultSecretsProviderReal) Get(key string) (string, error) {
	v.mu.RLock()
	if cached, ok := v.cache[key]; ok && time.Now().Before(cached.expiresAt) {
		v.mu.RUnlock()
		return cached.value, nil
	}
	v.mu.RUnlock()

	path := v.cfg.Path
	if path == "" {
		path = "secret/data/intu"
	}

	secret, err := v.client.Logical().Read(path)
	if err != nil {
		return "", fmt.Errorf("vault read %s: %w", path, err)
	}
	if secret == nil || secret.Data == nil {
		return "", fmt.Errorf("vault path %s returned no data", path)
	}

	data := secret.Data
	if inner, ok := data["data"].(map[string]interface{}); ok {
		data = inner
	}

	val, ok := data[key]
	if !ok {
		return "", fmt.Errorf("key %q not found at vault path %s", key, path)
	}

	strVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("vault key %q is not a string", key)
	}

	v.mu.Lock()
	v.cache[key] = cachedSecret{
		value:     strVal,
		expiresAt: time.Now().Add(v.ttl),
	}
	v.mu.Unlock()

	return strVal, nil
}
