package bootstrap

import "fmt"

var projectDirectories = []string{
	"channels/http-to-file",
	"channels/fhir-to-adt",
	"types",
}

func projectFiles(projectName string) map[string]string {
	return map[string]string{
		"intu.yaml":                              intuYAML,
		"intu.dev.yaml":                          intuDevYAML,
		"intu.prod.yaml":                         intuProdYAML,
		".env":                                   dotEnv,
		"channels/http-to-file/channel.yaml":     httpToFileChannelYAML,
		"channels/http-to-file/transformer.ts":   transformerTSTpl,
		"channels/http-to-file/validator.ts":     validatorTSTpl,
		"channels/fhir-to-adt/channel.yaml":      fhirToAdtChannelYAML,
		"channels/fhir-to-adt/transformer.ts":    fhirToAdtTransformerTS,
		"channels/fhir-to-adt/validator.ts":      fhirToAdtValidatorTS,
		"package.json":                           packageJSON,
		"tsconfig.json":                          tsConfigJSON,
		"types/hl7-standard.d.ts":                hl7StandardDTS,
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

const httpToFileChannelYAML = `id: http-to-file
enabled: true

listener:
  type: http
  http:
    port: 8081
    path: /ingest

validator:
  runtime: node
  entrypoint: validator.ts

transformer:
  runtime: node
  entrypoint: transformer.ts

destinations:
  - file-output
`

const fhirToAdtChannelYAML = `id: fhir-to-adt
enabled: true

listener:
  type: fhir
  fhir:
    port: 8082
    base_path: /fhir/r4
    version: R4
    resources:
      - Patient

validator:
  runtime: node
  entrypoint: validator.ts

transformer:
  runtime: node
  entrypoint: transformer.ts

destinations:
  - hl7-file-output
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

const fhirToAdtValidatorTS = `import type { Patient } from "fhir/r4";

export function validate(msg: unknown): void {
  if (msg === null || msg === undefined || typeof msg !== "object") {
    throw new Error("Invalid input: expected a JSON object");
  }
  const resource = msg as { resourceType?: string };
  if (resource.resourceType !== "Patient") {
    throw new Error("Expected Patient resource, got: " + resource.resourceType);
  }
}
`

var fhirToAdtTransformerTS = `import type { Patient } from "fhir/r4";
import HL7 from "hl7-standard";

function hl7Timestamp(): string {
  const d = new Date();
  const pad = (n: number, len = 2) => String(n).padStart(len, "0");
  return (
    d.getFullYear().toString() +
    pad(d.getMonth() + 1) +
    pad(d.getDate()) +
    pad(d.getHours()) +
    pad(d.getMinutes()) +
    pad(d.getSeconds())
  );
}

function genderCode(g?: string): string {
  if (!g) return "U";
  switch (g.toLowerCase()) {
    case "male":   return "M";
    case "female": return "F";
    case "other":  return "O";
    default:       return "U";
  }
}

export function transform(msg: unknown): string {
  const p = msg as Patient;
  const ts = hl7Timestamp();
  const hl7 = new HL7();

  hl7.createSegment("MSH");
  hl7.set("MSH.2", "^~\\&");
  hl7.set("MSH.3", "INTU");
  hl7.set("MSH.4", "INTU_FAC");
  hl7.set("MSH.5", "DEST");
  hl7.set("MSH.6", "DEST_FAC");
  hl7.set("MSH.7", ts);
  hl7.set("MSH.9", { "MSH.9.1": "ADT", "MSH.9.2": "A08" });
  hl7.set("MSH.10", "MSG" + Date.now());
  hl7.set("MSH.11", "P");
  hl7.set("MSH.12", "2.5.1");

  hl7.createSegment("EVN");
  hl7.set("EVN.1", "A08");
  hl7.set("EVN.2", ts);

  hl7.createSegment("PID");
  hl7.set("PID.3.1", p.id || p.identifier?.[0]?.value || "UNKNOWN");
  hl7.set("PID.5", {
    "PID.5.1": p.name?.[0]?.family || "",
    "PID.5.2": p.name?.[0]?.given?.join(" ") || "",
  });
  hl7.set("PID.7", (p.birthDate || "").replace(/-/g, ""));
  hl7.set("PID.8", genderCode(p.gender));
  const addr = p.address?.[0];
  hl7.set("PID.11", {
    "PID.11.1": addr?.line?.join(" ") || "",
    "PID.11.3": addr?.city || "",
    "PID.11.4": addr?.state || "",
    "PID.11.5": addr?.postalCode || "",
  });
  hl7.set("PID.13", p.telecom?.find((t) => t.system === "phone")?.value || "");

  hl7.createSegment("PV1");
  hl7.set("PV1.2", "O");

  return hl7.build();
}
`

// channelFiles returns the file map for a channel (used by BootstrapChannel).
func channelFiles(channelName string) map[string]string {
	return map[string]string{
		"channels/" + channelName + "/channel.yaml":   fmt.Sprintf(addChannelYAMLTpl, channelName),
		"channels/" + channelName + "/transformer.ts": transformerTSTpl,
		"channels/" + channelName + "/validator.ts":   validatorTSTpl,
	}
}

const addChannelYAMLTpl = `id: %s
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
    "hl7-standard": "^1.0.4"
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
  "include": ["channels/**/*.ts", "types/**/*.d.ts"]
}
`

const hl7StandardDTS = `declare module "hl7-standard" {
  class HL7 {
    constructor(data?: string, opts?: { subComponents?: string; repeatingFields?: string; lineEndings?: string });
    raw: string;
    transformed: Record<string, any>;
    encoded: string;

    transform(cb?: (err: Error | null, data: any) => void, batch?: boolean): void;
    build(): string;

    set(field: string, value: string | any[] | Record<string, any>, index?: number, sectionIndex?: number, subIndex?: number): any;
    get(field: string, index?: number, sectionIndex?: number, subIndex?: number): any;

    getSegment(segment: string): any;
    getSegments(segment?: string): any[];

    createSegment(segment: string): any;
    createSegmentAfter(segment: string, afterSegment: any): any;
    createSegmentBefore(segment: string, beforeSegment: any): any;

    deleteSegment(segment: any): void;
    deleteSegments(segments: any[]): void;

    getSegmentsAfter(start: any, name: string, consecutive?: boolean, stop?: string | string[]): any[];

    moveSegmentAfter(segment: any, afterSegment: any): void;
    moveSegmentBefore(segment: any, beforeSegment: any): void;
  }
  export default HL7;
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
    channels/
      http-to-file/        JSON pass-through channel
      fhir-to-adt/         FHIR Patient to HL7 ADT channel

## Configuration Schemas

intu provides JSON schemas for IDE autocompletion and AI-assisted configuration:

- Channel: https://intu.dev/schema/channel.schema.json
- Profile: https://intu.dev/schema/profile.schema.json

VS Code setup (.vscode/settings.json):

    {
      "yaml.schemas": {
        "https://intu.dev/schema/channel.schema.json": "channels/*/channel.yaml",
        "https://intu.dev/schema/profile.schema.json": ["intu.yaml", "intu.*.yaml"]
      }
    }

## Docker

    docker-compose up --build

## Documentation

https://intu.dev/documentation/index.html
`

const dockerfile = `FROM node:22-alpine
RUN npm install -g intu-dev
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
EXPOSE 8081 8082 3000
CMD ["intu", "serve", "--dir", ".", "--profile", "prod"]
`

const dockerComposeTpl = `services:
  %s:
    build: .
    container_name: %s
    ports:
      - "8081:8081"
      - "8082:8082"
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
