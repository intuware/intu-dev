package auth

import (
	"context"
	"fmt"
	"sync"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/intuware/intu/pkg/config"
	"google.golang.org/api/option"
)

type GCPSecretsProvider struct {
	client    *secretmanager.Client
	projectID string
	cache     map[string]cachedSecret
	mu        sync.RWMutex
	cacheTTL  time.Duration
}

func NewGCPSecretsProvider(cfg *config.GCPSecretManagerConfig) (*GCPSecretsProvider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("GCP secret manager config is nil")
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("GCP project_id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	client, err := secretmanager.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create GCP secret manager client: %w", err)
	}

	cacheTTL := 5 * time.Minute
	if cfg.CacheTTL != "" {
		if d, err := time.ParseDuration(cfg.CacheTTL); err == nil {
			cacheTTL = d
		}
	}

	return &GCPSecretsProvider{
		client:    client,
		projectID: cfg.ProjectID,
		cache:     make(map[string]cachedSecret),
		cacheTTL:  cacheTTL,
	}, nil
}

func (g *GCPSecretsProvider) Get(key string) (string, error) {
	g.mu.RLock()
	if cached, ok := g.cache[key]; ok && time.Now().Before(cached.expiresAt) {
		g.mu.RUnlock()
		return cached.value, nil
	}
	g.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	name := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", g.projectID, key)

	result, err := g.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	})
	if err != nil {
		return "", fmt.Errorf("gcp secret manager access %q: %w", key, err)
	}

	if result.Payload == nil || result.Payload.Data == nil {
		return "", fmt.Errorf("gcp secret %q has no payload data", key)
	}

	val := string(result.Payload.Data)

	g.mu.Lock()
	g.cache[key] = cachedSecret{
		value:     val,
		expiresAt: time.Now().Add(g.cacheTTL),
	}
	g.mu.Unlock()

	return val, nil
}
