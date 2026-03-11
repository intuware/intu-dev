package bootstrap

import (
	"fmt"
	"path/filepath"
)

var projectDirectories = []string{
	"src/channels/http-to-file",
	"src/channels/fhir-to-adt",
	"src/types",
	".vscode",
}

func projectFiles(projectName string) map[string]string {
	return map[string]string{
		"intu.yaml":                            intuYAML,
		"intu.dev.yaml":                        intuDevYAML,
		"intu.prod.yaml":                       intuProdYAML,
		".env":                                 dotEnv,
		"src/channels/http-to-file/channel.yaml":   httpToFileChannelYAML,
		"src/channels/http-to-file/transformer.ts": transformerTSTpl,
		"src/channels/http-to-file/validator.ts":   validatorTSTpl,
		"src/channels/fhir-to-adt/channel.yaml":    fhirToAdtChannelYAML,
		"src/channels/fhir-to-adt/transformer.ts":  fhirToAdtTransformerTS,
		"src/channels/fhir-to-adt/validator.ts":    fhirToAdtValidatorTS,
		"package.json":                         packageJSON,
		"tsconfig.json":                        tsConfigJSON,
		"src/types/intu.d.ts":                  intuDTS,
		"README.md":                            projectREADME,
		"Dockerfile":                           dockerfile,
		"docker-compose.yml":                   fmt.Sprintf(dockerComposeTpl, projectName, projectName, projectName, projectName),
		".dockerignore":                        dockerignore,
		".gitignore":                           gitignore,
		".vscode/settings.json":                vscodeSettings,
		".vscode/extensions.json":              vscodeExtensions,
	}
}

const intuYAML = `runtime:
  name: intu
  profile: dev
  log_level: info
  mode: standalone
  worker_pool: 4
  storage:
    driver: memory
    postgres_dsn: ${INTU_POSTGRES_DSN}

channels_dir: src/channels

message_storage:
  driver: memory
  mode: full
  memory:
    max_records: 100000       # evicts oldest when exceeded
    max_bytes: 536870912      # 512 MB; evicts oldest when exceeded

destinations:
  file-output:
    type: file
    file:
      directory: ./output
      filename_pattern: "{{channelId}}_{{messageId}}_{{timestamp}}.json"

  hl7-file-output:
    type: file
    file:
      directory: ./output
      filename_pattern: "{{channelId}}_{{messageId}}_{{timestamp}}.hl7"

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
  worker_pool: 8
  storage:
    driver: postgres
    postgres_dsn: ${INTU_POSTGRES_DSN}

# --- Message Storage ---------------------------------------------------------
# Controls how messages are persisted globally. Channels can override per-channel.
# Drivers: memory | postgres | s3
# Modes: none (disabled) | status (metadata only, no payloads) | full (full payloads)
message_storage:
  driver: postgres
  mode: status               # none | status (metadata only) | full (payloads + metadata)
  postgres:
    dsn: ${INTU_POSTGRES_DSN}
    table_prefix: intu_
    max_open_conns: 25
    max_idle_conns: 5

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
# Used by docker-compose: postgres://postgres:postgres@postgres:5432/intu?sslmode=disable
INTU_POSTGRES_DSN=postgres://postgres:postgres@localhost:5432/intu?sslmode=disable

# --- Dashboard ---
INTU_DASHBOARD_USER=admin
INTU_DASHBOARD_PASS=admin

# --- Cluster (enable cluster mode for horizontal scaling) ---
# docker-compose sets INTU_REDIS_ADDRESS automatically; override here if needed
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

const httpToFileChannelYAML = `id: http-to-file
enabled: true
description: "Receives HTTP messages, validates, transforms, and writes to file"
profiles: [dev]

listener:
  type: http
  http:
    port: 8081
    path: /ingest

validator:
  entrypoint: validator.ts

transformer:
  entrypoint: transformer.ts

destinations:
  - file-output
`

const fhirToAdtChannelYAML = `id: fhir-to-adt
enabled: true
description: "Converts FHIR Patient resources to HL7v2 ADT messages"
profiles: [dev]

listener:
  type: fhir
  fhir:
    port: 8082
    base_path: /fhir/r4
    version: R4
    resources:
      - Patient

validator:
  entrypoint: validator.ts

transformer:
  entrypoint: transformer.ts

destinations:
  - hl7-file-output
`

const transformerTSTpl = `export function transform(msg: IntuMessage, ctx: IntuContext): IntuMessage {
  return {
    body: {
      ...(msg.body as object),
      processedAt: new Date().toISOString(),
      source: ctx.channelId,
    },
  };
}
`

const validatorTSTpl = `export function validate(msg: IntuMessage): void {
  if (msg.body === null || msg.body === undefined) {
    throw new Error("Message body is empty");
  }
}
`

const fhirToAdtValidatorTS = `import type { Patient } from "fhir/r4";

export function validate(msg: IntuMessage): void {
  if (msg.body === null || msg.body === undefined || typeof msg.body !== "object") {
    throw new Error("Invalid input: expected a JSON object");
  }
  const resource = msg.body as { resourceType?: string };
  if (resource.resourceType !== "Patient") {
    throw new Error("Expected Patient resource, got: " + resource.resourceType);
  }
}
`

var fhirToAdtTransformerTS = `import type { Patient } from "fhir/r4";
import { Message } from "node-hl7-client";

function genderCode(g?: string): string {
  if (!g) return "U";
  switch (g.toLowerCase()) {
    case "male":   return "M";
    case "female": return "F";
    case "other":  return "O";
    default:       return "U";
  }
}

export function transform(msg: IntuMessage, ctx: IntuContext): IntuMessage {
  const p = msg.body as Patient;

  const hl7 = new Message({
    messageHeader: {
      msh_9_1: "ADT",
      msh_9_2: "A08",
      msh_11_1: "P",
    },
  });

  hl7.set("MSH.3", "INTU");
  hl7.set("MSH.4", "INTU_FAC");
  hl7.set("MSH.5", "DEST");
  hl7.set("MSH.6", "DEST_FAC");

  hl7.addSegment("EVN");
  hl7.set("EVN.1", "A08");
  hl7.set("EVN.2", hl7.get("MSH.7").toString());

  hl7.addSegment("PID");
  hl7.set("PID.3.1", p.id || p.identifier?.[0]?.value || "UNKNOWN");
  hl7.set("PID.5.1", p.name?.[0]?.family || "");
  hl7.set("PID.5.2", p.name?.[0]?.given?.join(" ") || "");
  hl7.set("PID.7", (p.birthDate || "").replace(/-/g, ""));
  hl7.set("PID.8", genderCode(p.gender));

  const addr = p.address?.[0];
  hl7.set("PID.11.1", addr?.line?.join(" ") || "");
  hl7.set("PID.11.3", addr?.city || "");
  hl7.set("PID.11.4", addr?.state || "");
  hl7.set("PID.11.5", addr?.postalCode || "");
  hl7.set("PID.13", p.telecom?.find((t) => t.system === "phone")?.value || "");

  hl7.addSegment("PV1");
  hl7.set("PV1.2", "O");

  return { body: hl7.toString() };
}
`

// channelFiles returns the file map for a channel (used by BootstrapChannel).
// channelName may contain slashes for subdirectory nesting (e.g. "vendor/fhir-to-adt").
func channelFiles(channelsDir, channelName string) map[string]string {
	channelID := filepath.Base(channelName)
	return map[string]string{
		channelsDir + "/" + channelName + "/channel.yaml": fmt.Sprintf(addChannelYAMLTpl, channelID),
	}
}

const addChannelYAMLTpl = `id: %s
enabled: true
description: ""

listener:
  type: http
  http:
    port: 8081

# validator:
#   entrypoint: validator.ts

# transformer:
#   entrypoint: transformer.ts

destinations:
  - file-output
`

const packageJSON = `{
  "name": "intu-channel-runtime",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "intu serve --profile dev",
    "serve": "intu serve",
    "start": "intu serve --profile prod",
    "build": "tsc -p tsconfig.json",
    "check": "tsc --noEmit -p tsconfig.json"
  },
  "dependencies": {
    "node-hl7-client": "^3.2.0"
  },
  "devDependencies": {
    "@types/fhir": "^0.0.41",
    "typescript": "^5.6.0"
  }
}
`

const tsConfigJSON = `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "skipLibCheck": true,
    "rootDir": ".",
    "outDir": "dist"
  },
  "include": ["src/channels/**/*.ts", "src/types/**/*.d.ts"]
}
`

const intuDTS = `type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
type IntuMap = Record<string, JsonValue>;

interface IntuHTTP {
  headers: Record<string, string>;
  queryParams: Record<string, string>;
  pathParams: Record<string, string>;
  method?: string;
  statusCode?: number;
}

interface IntuFile {
  filename: string;
  directory: string;
}

interface IntuFTP {
  filename: string;
  directory: string;
}

interface IntuKafka {
  headers: Record<string, string>;
  topic: string;
  key: string;
  partition?: number;
  offset?: number;
}

interface IntuTCP {
  remoteAddr: string;
}

interface IntuSMTP {
  from: string;
  to: string[];
  subject: string;
  cc?: string[];
  bcc?: string[];
}

interface IntuDICOM {
  callingAE: string;
  calledAE: string;
}

interface IntuDatabase {
  query: string;
  params: Record<string, JsonValue>;
}

interface IntuMessage {
  body: unknown;
  transport?: string;
  contentType?: string;

  http?: IntuHTTP;
  file?: IntuFile;
  ftp?: IntuFTP;
  kafka?: IntuKafka;
  tcp?: IntuTCP;
  smtp?: IntuSMTP;
  dicom?: IntuDICOM;
  database?: IntuDatabase;
}

interface IntuContext {
  channelId: string;
  correlationId: string;
  messageId: string;
  timestamp: string;
  inboundDataType?: string;
  outboundDataType?: string;
  destinationName?: string;
  sourceMessage?: IntuMessage;
  globalMap: IntuMap;
  channelMap: IntuMap;
  responseMap: IntuMap;
  connectorMap?: IntuMap;
}
`

const projectREADME = `# intu Project

Bootstrapped with [intu](https://intu.dev) — a Git-native, AI-friendly healthcare
interoperability framework. Build, version, and deploy integration pipelines
using YAML configuration and TypeScript transformers.

## Quick Start

    npm run dev

Dashboard: http://localhost:3000 (admin / admin)

## npm Scripts

| Script | Description |
|--------|-------------|
| npm run dev | Start in development mode (hot-reload, debug logging) |
| npm run serve | Start with default profile |
| npm start | Start in production mode |
| npm run build | Compile TypeScript (for CI/CD — intu serve auto-compiles) |

## Included Channels

| Channel | Listener | Description |
|---------|----------|-------------|
| http-to-file | HTTP :8081 POST /ingest | Receives JSON, writes to disk |
| fhir-to-adt | FHIR R4 :8082 /fhir/r4/Patient | Validates FHIR Patient, converts to HL7 ADT, writes .hl7 |

Test the channels:

    # JSON pass-through
    curl -X POST http://localhost:8081/ingest -H "Content-Type: application/json" -d '{"hello":"world"}'

    # FHIR Patient to HL7 ADT (uses FHIR R4 source — also serves /fhir/r4/metadata)
    curl -X POST http://localhost:8082/fhir/r4/Patient -H "Content-Type: application/json" \
      -d '{"resourceType":"Patient","id":"123","name":[{"family":"Smith","given":["John"]}],"gender":"male","birthDate":"1990-01-15"}'

## CLI Reference

| Command | Description |
|---------|-------------|
| intu init <name> | Bootstrap a new project (runs npm install) |
| intu serve | Start the runtime engine and dashboard |
| intu validate | Check YAML and TypeScript for errors |
| intu c <name> | Add a new channel |
| intu channel list | List all channels |
| intu channel clone <src> <dest> | Clone a channel |
| intu channel export <id> | Export a channel as a portable archive |
| intu channel import <file> | Import a channel archive |
| intu deploy <id> | Deploy (enable) a channel |
| intu undeploy <id> | Undeploy (disable) a channel |
| intu stats [id] | Show channel statistics |
| intu message list | Browse and search processed messages |
| intu reprocess message <id> | Reprocess a message |
| intu prune | Prune old message data |
| intu import mirth <file> | Import a Mirth Connect channel XML |
| intu --version | Show version |

## Supported Sources

- HTTP / REST
- TCP / MLLP (HL7v2)
- Kafka
- File / Directory watcher
- Database (polling)
- SFTP
- FHIR R4 server
- DICOM
- Email (IMAP / POP3)
- SMTP
- Amazon S3
- Google Cloud Storage

## Supported Destinations

- HTTP / REST
- TCP / MLLP
- Kafka
- File
- Database (insert/upsert)
- SFTP
- FHIR R4 server
- DICOM
- SMTP / Email
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- Slack / Webhooks

## Project Structure

    intu.yaml              Root config (runtime, destinations, dashboard)
    intu.dev.yaml          Dev profile overrides
    intu.prod.yaml         Production profile (postgres, cluster, RBAC)
    .env                   Environment variables referenced by YAML
    package.json           Node.js manifest for TypeScript compilation
    tsconfig.json          TypeScript compiler config
    Dockerfile             Production container image
    docker-compose.yml     One-command local deployment
    src/
      channels/
        http-to-file/      JSON pass-through channel
        fhir-to-adt/       FHIR Patient to HL7 ADT channel
      types/
        intu.d.ts          IntuMessage & IntuContext type declarations

## Configuration Schemas

intu provides JSON schemas for IDE autocompletion and AI-assisted configuration:

- Channel: https://intu.dev/schema/channel.schema.json
- Profile: https://intu.dev/schema/profile.schema.json

VS Code setup (.vscode/settings.json):

    {
      "yaml.schemas": {
        "https://intu.dev/schema/channel.schema.json": "src/channels/**/channel.yaml",
        "https://intu.dev/schema/profile.schema.json": ["intu.yaml", "intu.*.yaml"]
      }
    }

## Docker

    docker-compose up --build

## Documentation

https://intu.dev/documentation/index.html
`

const dockerfile = `# --- Build stage ---
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install
COPY src/ src/
RUN npm run build

# --- Runtime stage ---
FROM node:22-alpine
RUN apk add --no-cache tini && npm install -g intu-dev
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json ./
COPY src/ src/
COPY intu.yaml intu.*.yaml ./
COPY .env* ./
RUN mkdir -p /app/output
EXPOSE 8081 8082 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q --spider http://localhost:3000/ || exit 1
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["intu", "serve", "--dir", ".", "--profile", "prod"]
`

const dockerComposeTpl = `services:
  postgres:
    image: postgres:16-alpine
    container_name: %s-postgres
    restart: unless-stopped
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: intu
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    container_name: %s-redis
    restart: unless-stopped
    ports:
      - "6379:6379"
    volumes:
      - redisdata:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  %s:
    build: .
    container_name: %s
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    ports:
      - "8081:8081"
      - "8082:8082"
      - "3000:3000"
    env_file:
      - .env
    environment:
      INTU_POSTGRES_DSN: postgres://postgres:postgres@postgres:5432/intu?sslmode=disable
      INTU_REDIS_ADDRESS: redis:6379
    volumes:
      - ./output:/app/output

volumes:
  pgdata:
  redisdata:
`

const dockerignore = `node_modules
dist
output
.git
*.log
`

const gitignore = `node_modules/
dist/
output/
*.log
.env
.env.*
!.env.example
`

const vscodeSettings = `{
  "yaml.schemas": {
    "https://intu.dev/schema/channel.schema.json": "src/channels/**/channel.yaml",
    "https://intu.dev/schema/profile.schema.json": ["intu.yaml", "intu.*.yaml"]
  },
  "json.schemas": [
    {
      "fileMatch": ["src/channels/**/channel.yaml"],
      "url": "https://intu.dev/schema/channel.schema.json"
    }
  ],
  "files.associations": {
    "*.yaml": "yaml"
  }
}
`

const vscodeExtensions = `{
  "recommendations": [
    "redhat.vscode-yaml"
  ]
}
`
