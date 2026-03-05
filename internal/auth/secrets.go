package auth

import (
	"fmt"
	"os"

	"github.com/intuware/intu/pkg/config"
)

type SecretsProvider interface {
	Get(key string) (string, error)
}

func NewSecretsProvider(cfg *config.SecretsConfig) (SecretsProvider, error) {
	if cfg == nil {
		return &EnvSecretsProvider{}, nil
	}

	switch cfg.Provider {
	case "", "env":
		return &EnvSecretsProvider{}, nil
	case "vault":
		return &VaultSecretsProvider{cfg: cfg.Vault}, nil
	case "aws_secrets_manager":
		return &StubSecretsProvider{provider: "aws_secrets_manager"}, nil
	case "gcp_secret_manager":
		return &StubSecretsProvider{provider: "gcp_secret_manager"}, nil
	default:
		return nil, fmt.Errorf("unsupported secrets provider: %s", cfg.Provider)
	}
}

type EnvSecretsProvider struct{}

func (e *EnvSecretsProvider) Get(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", fmt.Errorf("env var %s not set", key)
	}
	return val, nil
}

type VaultSecretsProvider struct {
	cfg *config.VaultConfig
}

func (v *VaultSecretsProvider) Get(key string) (string, error) {
	return "", fmt.Errorf("vault secrets provider not yet implemented (path: %s, key: %s)", v.cfg.Path, key)
}

type StubSecretsProvider struct {
	provider string
}

func (s *StubSecretsProvider) Get(key string) (string, error) {
	return "", fmt.Errorf("%s secrets provider not yet implemented", s.provider)
}
