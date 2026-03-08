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
		return NewVaultSecretsProvider(cfg.Vault)
	case "aws_secrets_manager":
		return NewAWSSecretsProvider(cfg.AWS)
	case "gcp_secret_manager":
		return NewGCPSecretsProvider(cfg.GCP)
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
