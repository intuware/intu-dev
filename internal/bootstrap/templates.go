package bootstrap

import "fmt"

var projectDirectories = []string{
	"channels/sample-channel",
}

func projectFiles(projectName string) map[string]string {
	return map[string]string{
		"intu.yaml":                              intuYAML,
		"intu.dev.yaml":                          intuDevYAML,
		"intu.prod.yaml":                         intuProdYAML,
		".env":                                   dotEnv,
		"channels/sample-channel/channel.yaml":   fmt.Sprintf(channelYAMLTpl, "sample-channel"),
		"channels/sample-channel/transformer.ts": transformerTSTpl,
		"channels/sample-channel/validator.ts":   validatorTSTpl,
		"package.json":                           packageJSON,
		"tsconfig.json":                          tsConfigJSON,
		"README.md":                              projectREADME,
		"Dockerfile":                             dockerfile,
		"docker-compose.yml":                     fmt.Sprintf(dockerComposeTpl, projectName, projectName),
		".dockerignore":                          dockerignore,
	}
}

const intuYAML = `runtime:
  name: intu
  profile: dev
  log_level: info
  mode: standalone
  js_runtime: node
  worker_pool: 4
  storage:
    driver: memory
    postgres_dsn: ${INTU_POSTGRES_DSN}

channels_dir: channels

message_storage:
  driver: memory
  mode: full

destinations:
  file-output:
    type: file
    file:
      directory: ./output
      filename_pattern: "{{channelId}}_{{messageId}}_{{timestamp}}.json"

dashboard:
  enabled: true
  port: 3000
  auth:
    provider: basic
    username: admin
    password: admin

audit:
  enabled: true
  destination: memory
`

const intuDevYAML = `# Dev profile overrides -- merged on top of intu.yaml
runtime:
  profile: dev
  log_level: debug
  mode: standalone
`

const intuProdYAML = `# ============================================================================
# intu Production Profile
# Uncomment sections below to enable enterprise features.
# Environment variables (${VAR}) are resolved at startup from .env or OS env.
# ============================================================================

runtime:
  profile: prod
  log_level: info
  mode: standalone           # standalone | cluster
  js_runtime: node
  worker_pool: 8
  storage:
    driver: postgres
    postgres_dsn: ${INTU_POSTGRES_DSN}

# --- Message Storage ---------------------------------------------------------
# Controls how messages are persisted globally. Channels can override per-channel.
# Drivers: memory | postgres | s3
message_storage:
  driver: postgres
  dsn: ${INTU_POSTGRES_DSN}
  mode: status               # none | status (metadata only) | full (payloads + metadata)

# To use S3 instead of postgres for message content:
# message_storage:
#   driver: s3
#   mode: full
#   s3:
#     bucket: ${INTU_S3_BUCKET}
#     region: ${INTU_AWS_REGION}
#     prefix: intu/messages

# --- Dashboard ---------------------------------------------------------------
# Auth providers: basic | ldap | oidc | none
# Only one provider block should be active at a time.

# Option 1: Basic auth (default) -- simple username/password login form
dashboard:
  enabled: true
  port: 3000
  auth:
    provider: basic
    username: ${INTU_DASHBOARD_USER}
    password: ${INTU_DASHBOARD_PASS}

# Option 2: LDAP auth -- authenticates against your corporate directory
# dashboard:
#   enabled: true
#   port: 3000
#   auth:
#     provider: ldap

# Option 3: OIDC/SSO auth -- authenticates via OpenID Connect (Google, Okta, Azure AD, etc.)
# dashboard:
#   enabled: true
#   port: 3000
#   auth:
#     provider: oidc

# Option 4: No auth -- open access (only for trusted internal networks)
# dashboard:
#   enabled: true
#   port: 3000
#   auth:
#     provider: none

# --- Audit -------------------------------------------------------------------
audit:
  enabled: true
  destination: postgres      # memory | postgres
  events:                    # Restrict to specific events (omit for all)
    - message.reprocess
    - channel.deploy
    - channel.undeploy
    - channel.restart

# --- Cluster Mode (Horizontal Scaling) --------------------------------------
# Enables running multiple intu instances coordinated via Redis.
# When enabling, also change runtime.mode above to "cluster".
# cluster:
#   enabled: true
#   instance_id: ${HOSTNAME}
#   heartbeat_interval: 10s
#   coordination:
#     type: redis
#     redis:
#       address: ${INTU_REDIS_ADDRESS}
#       password: ${INTU_REDIS_PASSWORD}
#       db: 0
#       pool_size: 10
#       min_idle_conns: 3
#       key_prefix: intu
#       tls:
#         enabled: false
#   channel_assignment:
#     strategy: balanced       # balanced | tag_affinity
#     tag_affinity:
#       instance-a: [hl7, fhir]
#       instance-b: [x12, dicom]
#   deduplication:
#     enabled: true
#     window: 5m
#     store: redis             # memory | redis
#     key_extractor: message_id

# --- Secrets Provider --------------------------------------------------------
# Centralizes credential management. Only one provider should be active.
# Default: env (reads from OS environment variables).

# Option 1: Environment variables (default -- no config needed)
# secrets:
#   provider: env

# Option 2: HashiCorp Vault
# secrets:
#   provider: vault
#   vault:
#     address: ${VAULT_ADDR}
#     token: ${VAULT_TOKEN}
#     mount: secret
#     path: intu/prod

# Option 3: AWS Secrets Manager
# secrets:
#   provider: aws
#   aws:
#     region: ${INTU_AWS_REGION}
#     secret_name: intu/prod

# Option 4: Google Cloud Secret Manager
# secrets:
#   provider: gcp
#   gcp:
#     project: ${GCP_PROJECT_ID}
#     secret_name: intu-prod

# --- Observability -----------------------------------------------------------

# OpenTelemetry (traces + metrics exported via OTLP)
# observability:
#   opentelemetry:
#     enabled: true
#     endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT}
#     protocol: grpc           # grpc | http
#     traces: true
#     metrics: true
#     service_name: intu
#     resource_attributes:
#       environment: production
#       version: "1.0.0"

# Prometheus (pull-based metrics scrape endpoint)
# observability:
#   prometheus:
#     enabled: true
#     port: 9090
#     path: /metrics

# --- Log Transports ----------------------------------------------------------
# Ships structured logs to external platforms alongside stdout.
# Multiple transports can be active simultaneously.

# AWS CloudWatch Logs
# logging:
#   transports:
#     - type: cloudwatch
#       cloudwatch:
#         region: ${INTU_AWS_REGION}
#         log_group: /intu/prod
#         log_stream: ${HOSTNAME}

# Datadog
# logging:
#   transports:
#     - type: datadog
#       datadog:
#         api_key: ${DD_API_KEY}
#         site: datadoghq.com
#         service: intu
#         source: go
#         tags: ["env:prod", "team:integration"]

# Sumo Logic
# logging:
#   transports:
#     - type: sumologic
#       sumologic:
#         endpoint: ${SUMO_HTTP_ENDPOINT}
#         source_category: intu/prod
#         source_name: intu-engine

# Elasticsearch
# logging:
#   transports:
#     - type: elasticsearch
#       elasticsearch:
#         urls: ["${ES_URL}"]
#         index: intu-logs
#         username: ${ES_USER}
#         password: ${ES_PASS}

# File (with rotation)
# logging:
#   transports:
#     - type: file
#       file:
#         path: /var/log/intu/intu.log
#         max_size_mb: 100
#         max_files: 10
#         compress: true

# --- Access Control ----------------------------------------------------------
# Required when dashboard.auth.provider is ldap or oidc.

# LDAP configuration
# access_control:
#   enabled: true
#   provider: ldap
#   ldap:
#     url: ${LDAP_URL}
#     base_dn: ${LDAP_BASE_DN}
#     bind_dn: ${LDAP_BIND_DN}
#     bind_password: ${LDAP_BIND_PASSWORD}

# OIDC configuration (Google, Okta, Azure AD, Keycloak, etc.)
# access_control:
#   enabled: true
#   provider: oidc
#   oidc:
#     issuer: ${OIDC_ISSUER}
#     client_id: ${OIDC_CLIENT_ID}
#     client_secret: ${OIDC_CLIENT_SECRET}

# --- RBAC Roles --------------------------------------------------------------
# Maps authenticated users/groups to permission sets.
# roles:
#   - name: admin
#     permissions: ["*"]
#   - name: developer
#     permissions:
#       - channels.read
#       - channels.deploy
#       - channels.undeploy
#       - messages.read
#       - messages.reprocess
#   - name: viewer
#     permissions:
#       - channels.read
#       - messages.read
#       - metrics.read

# --- Health Check Endpoints --------------------------------------------------
# health:
#   port: 8081
#   path: /health
#   readiness_path: /ready
#   liveness_path: /live

# --- Alerts ------------------------------------------------------------------
# alerts:
#   - name: high-error-rate
#     trigger:
#       type: error_rate
#       channel: "*"
#       threshold: 50
#       window: 5m
#     destinations: ["slack-webhook"]
#   - name: slow-processing
#     trigger:
#       type: latency
#       channel: "*"
#       threshold_ms: 5000
#       percentile: p95
#       window: 5m
#     destinations: ["pagerduty-webhook"]

# --- Dead Letter Queue -------------------------------------------------------
# dead_letter:
#   enabled: true
#   destination: dlq-output
#   max_retries: 3
#   include_metadata: true

# --- Data Pruning ------------------------------------------------------------
# pruning:
#   schedule: "0 2 * * *"     # Daily at 2 AM
#   default_retention_days: 30
#   archive_before_prune: true
#   archive_destination: s3-archive
`

const dotEnv = `# intu Environment Variables
# Active profile (dev | prod)
INTU_PROFILE=dev

# --- Core ---
INTU_POSTGRES_DSN=postgres://postgres:postgres@localhost:5432/intu?sslmode=disable

# --- Dashboard ---
INTU_DASHBOARD_USER=admin
INTU_DASHBOARD_PASS=admin

# --- Cluster (uncomment when enabling cluster mode) ---
# INTU_REDIS_ADDRESS=localhost:6379
# INTU_REDIS_PASSWORD=

# --- AWS (uncomment for S3 storage, CloudWatch logs, AWS Secrets Manager) ---
# INTU_AWS_REGION=us-east-1
# INTU_S3_BUCKET=my-intu-bucket

# --- Secrets Providers (uncomment the provider you use) ---
# VAULT_ADDR=http://127.0.0.1:8200
# VAULT_TOKEN=
# GCP_PROJECT_ID=

# --- Observability (uncomment for OpenTelemetry) ---
# OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# --- Log Transports (uncomment as needed) ---
# DD_API_KEY=
# SUMO_HTTP_ENDPOINT=
# ES_URL=http://localhost:9200
# ES_USER=
# ES_PASS=

# --- Access Control (uncomment for LDAP or OIDC) ---
# LDAP_URL=ldap://localhost:389
# LDAP_BASE_DN=dc=example,dc=com
# LDAP_BIND_DN=cn=admin,dc=example,dc=com
# LDAP_BIND_PASSWORD=
# OIDC_ISSUER=https://accounts.google.com
# OIDC_CLIENT_ID=
# OIDC_CLIENT_SECRET=
`

const channelYAMLTpl = `id: %s
enabled: true

listener:
  type: http
  http:
    port: 8081

validator:
  runtime: node
  entrypoint: validator.ts

transformer:
  runtime: node
  entrypoint: transformer.ts

destinations:
  - file-output
`

const transformerTSTpl = `export function transform(msg: unknown, ctx: { channelId: string; correlationId: string }): unknown {
  return {
    ...(msg as object),
    processedAt: new Date().toISOString(),
    source: ctx.channelId,
  };
}
`

const validatorTSTpl = `export function validate(msg: unknown): void {
}
`

// channelFiles returns the file map for a channel (used by BootstrapChannel).
func channelFiles(channelName string) map[string]string {
	return map[string]string{
		"channels/" + channelName + "/channel.yaml":   fmt.Sprintf(channelYAMLTpl, channelName),
		"channels/" + channelName + "/transformer.ts": transformerTSTpl,
		"channels/" + channelName + "/validator.ts":  validatorTSTpl,
	}
}

const packageJSON = `{
  "name": "intu-channel-runtime",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "check": "tsc --noEmit -p tsconfig.json"
  },
  "devDependencies": {
    "typescript": "^5.6.0"
  }
}
`

const tsConfigJSON = `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "skipLibCheck": true,
    "rootDir": ".",
    "outDir": "dist"
  },
  "include": ["channels/**/*.ts"]
}
`

const projectREADME = `# intu Project

Bootstrapped project for the [intu](https://intu.dev) interoperability framework.

## Quick Start

1. Review and update configuration files:
   - intu.yaml
   - intu.dev.yaml
   - intu.prod.yaml
   - .env
2. Install transformer runtime dependencies:
   - npm install
3. Build TypeScript transformers:
   - intu build --dir .
4. Validate configuration:
   - intu validate --dir .
5. Run the engine (includes dashboard at http://localhost:3000):
   - intu serve --dir .
   - Default dashboard credentials: admin / admin

## Structure

- channels/sample-channel/channel.yaml: sample channel definition.
- channels/sample-channel/transformer.ts: pure transformer (JSON in, JSON out).
- channels/sample-channel/validator.ts: sample validator.

## Add a Channel

  intu c my-channel --dir .
  intu channel add my-channel --dir .

## JSON Schemas (IDE & AI Assistance)

intu provides JSON schemas for configuration files to enable autocompletion,
validation, and AI-assisted configuration generation.

- Channel schema: https://intu.dev/schema/channel.schema.json
- Profile schema: https://intu.dev/schema/profile.schema.json

### VS Code Setup

Add to your .vscode/settings.json:

  {
    "yaml.schemas": {
      "https://intu.dev/schema/channel.schema.json": "channels/*/channel.yaml",
      "https://intu.dev/schema/profile.schema.json": ["intu.yaml", "intu.*.yaml"]
    }
  }

### AI Assistants

When using AI coding assistants, reference the schemas above to help
generate valid intu configuration. The schemas define all valid
properties, types, and descriptions for channel.yaml and intu.yaml files.

## Documentation

Full documentation: https://intu.dev/documentation/
`

const dockerfile = `FROM node:22-alpine
RUN npm install -g intu-dev
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
EXPOSE 8081 3000
CMD ["intu", "serve", "--dir", ".", "--profile", "prod"]
`

const dockerComposeTpl = `services:
  %s:
    build: .
    container_name: %s
    ports:
      - "8081:8081"
      - "3000:3000"
    env_file:
      - .env
    volumes:
      - ./output:/app/output
`

const dockerignore = `node_modules
dist
output
.git
*.log
`
