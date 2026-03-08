package auth

import (
	"context"
	"fmt"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/intuware/intu/pkg/config"
)

type AWSSecretsProvider struct {
	client   *secretsmanager.Client
	cache    map[string]cachedSecret
	mu       sync.RWMutex
	cacheTTL time.Duration
}

func NewAWSSecretsProvider(cfg *config.AWSSecretsManagerConfig) (*AWSSecretsProvider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("AWS secrets manager config is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	cacheTTL := 5 * time.Minute
	if cfg.CacheTTL != "" {
		if d, err := time.ParseDuration(cfg.CacheTTL); err == nil {
			cacheTTL = d
		}
	}

	return &AWSSecretsProvider{
		client:   secretsmanager.NewFromConfig(awsCfg),
		cache:    make(map[string]cachedSecret),
		cacheTTL: cacheTTL,
	}, nil
}

func (a *AWSSecretsProvider) Get(key string) (string, error) {
	a.mu.RLock()
	if cached, ok := a.cache[key]; ok && time.Now().Before(cached.expiresAt) {
		a.mu.RUnlock()
		return cached.value, nil
	}
	a.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := a.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &key,
	})
	if err != nil {
		return "", fmt.Errorf("aws secrets manager get %q: %w", key, err)
	}

	if result.SecretString == nil {
		return "", fmt.Errorf("aws secret %q has no string value (binary secrets not supported)", key)
	}

	val := *result.SecretString

	a.mu.Lock()
	a.cache[key] = cachedSecret{
		value:     val,
		expiresAt: time.Now().Add(a.cacheTTL),
	}
	a.mu.Unlock()

	return val, nil
}
