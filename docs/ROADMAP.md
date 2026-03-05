# intu Feature Roadmap

> Goal: Make intu a **Mirth Connect–class** healthcare integration engine that is Git-native, config-driven (YAML), and code-extensible (TypeScript).

---

## Table of Contents

- [Current State](#current-state)
- [Design Principles](#design-principles)
- [Gap Analysis: intu vs Mirth Connect](#gap-analysis-intu-vs-mirth-connect)
- [Phase 0 — Runtime Engine Foundation](#phase-0--runtime-engine-foundation)
- [Phase 1 — Connectors & Protocols](#phase-1--connectors--protocols)
- [Phase 2 — Authentication & Security](#phase-2--authentication--security)
- [Phase 3 — Data Types & Payload Handling](#phase-3--data-types--payload-handling)
- [Phase 4 — Channel Pipeline (Full Mirth Parity)](#phase-4--channel-pipeline-full-mirth-parity)
- [Phase 5 — Logging, Observability & Message Storage](#phase-5--logging-observability--message-storage)
- [Phase 6 — Error Handling, Retry & Dead-Letter](#phase-6--error-handling-retry--dead-letter)
- [Phase 7 — Code Reuse & Shared Libraries](#phase-7--code-reuse--shared-libraries)
- [Phase 8 — Channel Management & Operations](#phase-8--channel-management--operations)
- [Phase 9 — Alerting, Monitoring & Dashboard](#phase-9--alerting-monitoring--dashboard)
- [Phase 10 — Access Control & Multi-Tenancy](#phase-10--access-control--multi-tenancy)
- [Phase 11 — Advanced Connectors & Healthcare Standards](#phase-11--advanced-connectors--healthcare-standards)
- [Phase 12 — Clustering, HA & Horizontal Scaling](#phase-12--clustering-ha--horizontal-scaling)
- [Appendix A — Full YAML Config Reference (Target)](#appendix-a--full-yaml-config-reference-target)
- [Appendix B — Full Channel YAML Reference (Target)](#appendix-b--full-channel-yaml-reference-target)
- [Appendix C — TypeScript API Surface (Target)](#appendix-c--typescript-api-surface-target)
- [Appendix D — Mirth Feature Parity Checklist](#appendix-d--mirth-feature-parity-checklist)

---

## Current State

| Area | Status |
|------|--------|
| Project scaffolding (`intu init`) | ✅ Implemented |
| Channel scaffolding (`intu c`, `intu channel add`) | ✅ Implemented |
| Channel list / describe | ✅ Implemented |
| YAML config with profile layering | ✅ Implemented |
| Env-var expansion (`${VAR}`) | ✅ Implemented |
| TypeScript transformer / validator per channel | ✅ Implemented |
| `intu build` (tsc compilation) | ✅ Implemented |
| `intu validate` | ✅ Implemented |
| Structured JSON logging (4 levels) | ✅ Implemented |
| npm cross-platform binary distribution | ✅ Implemented |
| Runtime engine (`intu serve`) | ❌ Stub only |
| Actual message processing | ❌ Not implemented |
| Listener execution | ❌ Not implemented |
| Destination dispatch | ❌ Not implemented |
| Auth beyond static bearer token | ❌ Not implemented |
| Any data-type parsing (HL7, FHIR, CSV…) | ❌ Not implemented |
| SFTP / TCP / MLLP / File / DB connectors | ❌ Not implemented |
| Error handling / retry / DLQ | ❌ Not implemented |
| Message storage / audit | ❌ Not implemented |
| Dashboard / alerting | ❌ Not implemented |

---

## Design Principles

Every feature follows the same separation of concerns:

| Concern | Where | Format |
|---------|-------|--------|
| **What** to connect, auth, retry, log | `intu.yaml` / `channel.yaml` | Declarative YAML |
| **How** to transform, filter, route | `*.ts` files in `channels/` | Imperative TypeScript |
| **Shared logic** | `lib/*.ts` at project root | Reusable TS modules |
| **Runtime behavior** | Go binary (`intu serve`) | Executes YAML + compiled JS |

Rules:
1. YAML declares infrastructure: connectors, auth, retry, TLS, log levels, data types.
2. TypeScript declares business logic: transforms, filters, routers, enrichers.
3. Go runtime reads YAML, boots connectors, invokes compiled JS via an embedded engine.
4. No business logic in YAML. No infra config in TypeScript.

---

## Gap Analysis: intu vs Mirth Connect

| Mirth Connect Feature | intu Equivalent | Gap |
|------------------------|-----------------|-----|
| **Source Connectors** | | |
| HTTP Listener | `listener.type: http` (config only) | Need runtime implementation |
| TCP Listener (MLLP) | — | New connector |
| File Reader (local/FTP/SFTP/S3/SMB) | — | New connector |
| Database Reader (JDBC) | — | New connector |
| DICOM Listener | — | New connector |
| JMS Listener | — | New connector |
| JavaScript Reader (poll) | — | New connector |
| Channel Reader (inter-channel) | — | New connector |
| Email Reader (IMAP/POP3) | — | New connector |
| Web Service Listener (SOAP) | — | New connector |
| Kafka Consumer | — | New connector |
| **Destination Connectors** | | |
| HTTP Sender | `destinations.*.type: http` (config only) | Need runtime implementation |
| Kafka Producer | `destinations.*.type: kafka` (config only) | Need runtime implementation |
| TCP Sender (MLLP) | — | New connector |
| File Writer (local/FTP/SFTP/S3/SMB) | — | New connector |
| Database Writer (SQL) | — | New connector |
| DICOM Sender | — | New connector |
| JMS Sender | — | New connector |
| SMTP Sender (Email) | — | New connector |
| Channel Writer (inter-channel) | — | New connector |
| Web Service Sender (SOAP) | — | New connector |
| **Data Types** | | |
| HL7 v2.x | — | Parser + serializer |
| HL7 v3.x (CDA/CCDA) | — | XML-based parser |
| FHIR R4 (JSON + XML) | — | Parser + serializer |
| X12 EDI | — | Parser + serializer |
| DICOM | — | Parser + serializer |
| Raw / JSON / XML / CSV / Delimited | JSON only | Extend type system |
| Binary (Base64) | — | New type |
| **Pipeline Stages** | | |
| Preprocessor | — | New stage |
| Source Filter | — | New stage |
| Source Transformer | `transformer.ts` | Exists, needs runtime |
| Destination Filter | — | New stage |
| Destination Transformer | — | New stage |
| Response Transformer | — | New stage |
| Postprocessor | — | New stage |
| Deploy / Undeploy scripts | — | New stage |
| **Authentication** | | |
| Static Bearer Token | `auth.type` + `auth.token` on HTTP dest | Exists in config |
| Basic Auth | — | New auth type |
| OAuth 2.0 (Client Credentials) | — | New auth type |
| OAuth 2.0 (Authorization Code) | — | New auth type |
| API Key (header / query) | — | New auth type |
| mTLS / Client Certificates | — | New auth type |
| SAML | — | New auth type |
| Custom (TS hook) | — | New auth type |
| **Logging & Observability** | | |
| Per-channel log level | Global only | Extend |
| Source / transformed / response payload logging | — | New |
| Silent mode (< 10ms transport) | — | New |
| Message content storage | — | New |
| Structured correlation IDs | Template only | Need runtime |
| OpenTelemetry traces/metrics | — | New |
| **Error Handling** | | |
| Destination queuing | — | New |
| Retry policies (backoff) | — | New |
| Dead-letter queue | — | New |
| Error alerting | — | New |
| ACK/NACK generation | — | New |
| **Operations** | | |
| Channel deploy / undeploy | — | New |
| Channel enable / disable at runtime | `enabled` field | Need runtime enforcement |
| Channel tags / groups | — | New |
| Channel statistics | — | New |
| Message reprocessing | — | New |
| Message search | — | New |
| Data pruning | — | New |
| **Security** | | |
| TLS on all connectors | — | New |
| Keystore / truststore management | — | New |
| Credential encryption (secrets) | Env-var only | Extend |
| RBAC | — | New |
| Audit log | — | New |
| **Code Reuse** | | |
| Code template libraries | — | New (`lib/` directory) |
| Global scripts | — | New |
| Shared TS modules | — | New |

---

## Phase 0 — Runtime Engine Foundation

> **Goal**: `intu serve` starts, loads config, boots channels, processes a message end-to-end through the HTTP listener → validator → transformer → HTTP/Kafka destination pipeline.

### 0.1 Embedded JS Runtime

Embed a lightweight JavaScript runtime in Go to execute compiled TypeScript.

**Options**: [goja](https://github.com/nicot/goja) (pure Go, no CGo) or Deno/Node subprocess.

**Go interfaces** (`internal/runtime/`):

```go
type JSRunner interface {
    Call(fn string, entrypoint string, args ...any) (any, error)
    Close() error
}
```

### 0.2 Connector Abstraction

```go
// internal/connector/connector.go
type SourceConnector interface {
    Start(ctx context.Context, handler MessageHandler) error
    Stop(ctx context.Context) error
    Type() string
}

type DestinationConnector interface {
    Send(ctx context.Context, msg *Message) (*Response, error)
    Stop(ctx context.Context) error
    Type() string
}

type MessageHandler func(ctx context.Context, msg *Message) error
```

### 0.3 Message Envelope

```go
// internal/message/message.go
type Message struct {
    ID            string
    CorrelationID string
    ChannelID     string
    Raw           []byte
    ContentType   ContentType
    Headers       map[string]string
    Metadata      map[string]any
    Timestamp     time.Time
}

type Response struct {
    StatusCode int
    Body       []byte
    Headers    map[string]string
    Error      error
}
```

### 0.4 Channel Runtime

```go
// internal/runtime/channel.go
type ChannelRuntime struct {
    ID           string
    Config       ChannelConfig
    Source       connector.SourceConnector
    Destinations []connector.DestinationConnector
    Pipeline     Pipeline
}
```

### 0.5 Engine Implementation

Implements the existing `Engine` interface in `internal/runtime/engine.go`:

```go
type DefaultEngine struct {
    cfg      *config.Config
    channels map[string]*ChannelRuntime
    logger   *slog.Logger
}

func (e *DefaultEngine) Start(ctx context.Context) error { /* boot all enabled channels */ }
func (e *DefaultEngine) Stop(ctx context.Context) error  { /* graceful shutdown */ }
```

### 0.6 `intu serve` Wired Up

```go
// cmd/serve.go — reads config, builds engine, starts with graceful shutdown on SIGINT/SIGTERM
```

### Config unchanged — existing `intu.yaml` + `channel.yaml` already declares listeners and destinations.

---

## Phase 1 — Connectors & Protocols

### 1.1 HTTP Listener (runtime)

Bring the existing `listener.type: http` config to life.

**channel.yaml** (already exists):
```yaml
listener:
  type: http
  http:
    port: 8080
    path: /                    # NEW: custom path
    methods: [POST, PUT]       # NEW: allowed methods
    tls:                       # NEW: optional TLS
      cert_file: ${TLS_CERT}
      key_file: ${TLS_KEY}
    auth:                      # NEW: listener-side auth
      type: bearer
      token: ${LISTENER_TOKEN}
```

### 1.2 TCP/MLLP Listener

**channel.yaml**:
```yaml
listener:
  type: tcp
  tcp:
    port: 2575
    mode: mllp               # raw | mllp
    max_connections: 100
    timeout_ms: 30000
    tls:
      cert_file: ${TLS_CERT}
      key_file: ${TLS_KEY}
```

### 1.3 SFTP Listener & Processor

**channel.yaml**:
```yaml
listener:
  type: sftp
  sftp:
    host: sftp.partner.com
    port: 22
    poll_interval: 30s
    directory: /inbound
    file_pattern: "*.hl7"
    move_to: /inbound/processed   # move after read (empty = delete)
    error_dir: /inbound/errors
    auth:
      type: key                    # password | key
      username: ${SFTP_USER}
      private_key_file: ${SFTP_KEY}
      passphrase: ${SFTP_PASS}
    sort_by: modified              # name | modified | size
```

### 1.4 File Reader (Local / FTP / S3 / SMB)

**channel.yaml**:
```yaml
listener:
  type: file
  file:
    scheme: local               # local | ftp | s3 | smb
    directory: /data/inbound
    file_pattern: "*.csv"
    poll_interval: 10s
    move_to: /data/processed
    error_dir: /data/errors
    sort_by: modified
    # FTP/SFTP sub-config
    ftp:
      host: ftp.example.com
      port: 21
      auth:
        type: password
        username: ${FTP_USER}
        password: ${FTP_PASS}
    # S3 sub-config
    s3:
      bucket: my-hl7-bucket
      region: us-east-1
      prefix: inbound/
      auth:
        type: credentials
        access_key_id: ${AWS_KEY}
        secret_access_key: ${AWS_SECRET}
    # SMB sub-config
    smb:
      host: //fileserver/share
      auth:
        type: password
        username: ${SMB_USER}
        password: ${SMB_PASS}
        domain: ${SMB_DOMAIN}
```

### 1.5 Database Reader

**channel.yaml**:
```yaml
listener:
  type: database
  database:
    driver: postgres            # postgres | mysql | mssql | oracle
    dsn: ${DB_DSN}
    poll_interval: 15s
    query: |
      SELECT id, payload, created_at
      FROM outbox
      WHERE processed = false
      ORDER BY created_at
      LIMIT 100
    post_process_statement: |
      UPDATE outbox SET processed = true WHERE id = :id
    tls:
      ca_file: ${DB_CA_CERT}
```

### 1.6 Kafka Consumer (Listener)

**channel.yaml**:
```yaml
listener:
  type: kafka
  kafka:
    brokers:
      - ${KAFKA_BROKER}
    topic: inbound-hl7
    group_id: intu-channel-group
    offset: latest              # earliest | latest
    auth:
      type: sasl_plain          # none | sasl_plain | sasl_scram | mtls
      username: ${KAFKA_USER}
      password: ${KAFKA_PASS}
    tls:
      enabled: true
      ca_file: ${KAFKA_CA}
```

### 1.7 Channel Reader (Inter-Channel)

**channel.yaml**:
```yaml
listener:
  type: channel
  channel:
    source_channel_id: adt-intake
```

### 1.8 Email Reader

**channel.yaml**:
```yaml
listener:
  type: email
  email:
    protocol: imap              # imap | pop3
    host: mail.example.com
    port: 993
    poll_interval: 60s
    tls:
      enabled: true
    auth:
      type: password
      username: ${EMAIL_USER}
      password: ${EMAIL_PASS}
    folder: INBOX
    filter: "subject:HL7"
    read_attachments: true
    delete_after_read: false
```

### 1.9 DICOM Listener

**channel.yaml**:
```yaml
listener:
  type: dicom
  dicom:
    port: 11112
    ae_title: INTU_SCP
    tls:
      cert_file: ${TLS_CERT}
      key_file: ${TLS_KEY}
```

### 1.10 Web Service Listener (SOAP)

**channel.yaml**:
```yaml
listener:
  type: soap
  soap:
    port: 8443
    wsdl_path: /wsdl
    service_name: PatientService
    tls:
      cert_file: ${TLS_CERT}
      key_file: ${TLS_KEY}
```

### 1.11 Destination Connectors

All destination types configured in **`intu.yaml`** (root-level named destinations) or inline in **`channel.yaml`**.

#### HTTP Sender (extend existing)
```yaml
destinations:
  ehr-api:
    type: http
    http:
      url: https://ehr.example.com/api/patients
      method: POST              # GET | POST | PUT | PATCH | DELETE
      headers:
        Content-Type: application/fhir+json
        X-Correlation-Id: "{{correlationId}}"
      timeout_ms: 30000
      auth:                     # see Phase 2 for full auth options
        type: oauth2_client_credentials
        token_url: https://auth.ehr.com/token
        client_id: ${EHR_CLIENT_ID}
        client_secret: ${EHR_CLIENT_SECRET}
        scopes: ["patient/*.read"]
      tls:
        ca_file: ${EHR_CA_CERT}
        client_cert_file: ${EHR_CLIENT_CERT}
        client_key_file: ${EHR_CLIENT_KEY}
      retry:                    # see Phase 6
        max_attempts: 3
        backoff: exponential
        initial_delay_ms: 1000
```

#### TCP/MLLP Sender
```yaml
destinations:
  lab-mllp:
    type: tcp
    tcp:
      host: lab.example.com
      port: 2575
      mode: mllp
      timeout_ms: 10000
      tls:
        enabled: true
```

#### File Writer
```yaml
destinations:
  archive-sftp:
    type: file
    file:
      scheme: sftp              # local | ftp | sftp | s3 | smb
      directory: /outbound
      filename_pattern: "{{channelId}}_{{timestamp}}.hl7"
      sftp:
        host: sftp.partner.com
        port: 22
        auth:
          type: key
          username: ${SFTP_USER}
          private_key_file: ${SFTP_KEY}
```

#### Database Writer
```yaml
destinations:
  audit-db:
    type: database
    database:
      driver: postgres
      dsn: ${AUDIT_DB_DSN}
      statement: |
        INSERT INTO audit_log (channel_id, correlation_id, payload, created_at)
        VALUES (:channelId, :correlationId, :payload, NOW())
```

#### SMTP Sender
```yaml
destinations:
  notify-email:
    type: smtp
    smtp:
      host: smtp.example.com
      port: 587
      from: intu@example.com
      to: [ops@example.com]
      subject: "Channel {{channelId}} Alert"
      auth:
        type: password
        username: ${SMTP_USER}
        password: ${SMTP_PASS}
      tls:
        enabled: true
```

#### Channel Writer (Inter-Channel)
```yaml
destinations:
  downstream-channel:
    type: channel
    channel:
      target_channel_id: fhir-outbound
```

#### DICOM Sender
```yaml
destinations:
  pacs-dicom:
    type: dicom
    dicom:
      host: pacs.example.com
      port: 11112
      ae_title: INTU_SCU
      called_ae_title: PACS_SCP
```

#### JMS Sender
```yaml
destinations:
  mq-output:
    type: jms
    jms:
      provider: activemq        # activemq | rabbitmq | solace
      url: tcp://mq.example.com:61616
      queue: outbound.hl7
      auth:
        type: password
        username: ${JMS_USER}
        password: ${JMS_PASS}
```

---

## Phase 2 — Authentication & Security

### 2.1 Auth Types

All auth blocks share the same schema. They appear on both **listeners** (source-side) and **destinations** (destination-side).

```yaml
auth:
  type: <auth_type>
  # type-specific fields below
```

| `type` | Fields | Direction |
|--------|--------|-----------|
| `none` | — | Both |
| `basic` | `username`, `password` | Both |
| `bearer` | `token` | Both |
| `api_key` | `key`, `header` (or `query_param`) | Both |
| `oauth2_client_credentials` | `token_url`, `client_id`, `client_secret`, `scopes` | Destination |
| `oauth2_authorization_code` | `token_url`, `auth_url`, `client_id`, `client_secret`, `redirect_uri` | Destination |
| `mtls` | `client_cert_file`, `client_key_file`, `ca_file` | Both |
| `sasl_plain` | `username`, `password` | Kafka |
| `sasl_scram` | `username`, `password`, `mechanism` (SHA-256/SHA-512) | Kafka |
| `key` | `username`, `private_key_file`, `passphrase` | SFTP |
| `custom` | `handler: auth-hook.ts` | Both |

#### Listener-Side Auth Examples

```yaml
# HTTP listener with Basic Auth
listener:
  type: http
  http:
    port: 8080
    auth:
      type: basic
      username: ${LISTENER_USER}
      password: ${LISTENER_PASS}

# HTTP listener with API Key
listener:
  type: http
  http:
    port: 8080
    auth:
      type: api_key
      key: ${API_KEY}
      header: X-API-Key

# HTTP listener with mTLS
listener:
  type: http
  http:
    port: 8443
    auth:
      type: mtls
      ca_file: ${CLIENT_CA}
    tls:
      cert_file: ${SERVER_CERT}
      key_file: ${SERVER_KEY}
```

#### Destination-Side Auth Examples

```yaml
# OAuth 2.0 Client Credentials
destinations:
  epic-fhir:
    type: http
    http:
      url: https://fhir.epic.com/api/FHIR/R4
      auth:
        type: oauth2_client_credentials
        token_url: https://fhir.epic.com/oauth2/token
        client_id: ${EPIC_CLIENT_ID}
        client_secret: ${EPIC_CLIENT_SECRET}
        scopes: ["patient/*.read", "patient/*.write"]
```

#### Custom Auth (TypeScript Hook)

**channel.yaml**:
```yaml
listener:
  type: http
  http:
    port: 8080
    auth:
      type: custom
      handler: auth-hook.ts
```

**channels/my-channel/auth-hook.ts**:
```typescript
import type { AuthContext, AuthResult } from "@intu/sdk";

export async function authenticate(ctx: AuthContext): Promise<AuthResult> {
  const token = ctx.headers["authorization"]?.replace("Bearer ", "");
  const valid = await verifyJWT(token, ctx.secrets["JWT_PUBLIC_KEY"]);
  return { authenticated: valid, principal: valid ? decodeJWT(token).sub : null };
}
```

### 2.2 TLS Configuration

Universal TLS block available on every connector:

```yaml
tls:
  enabled: true
  cert_file: ${TLS_CERT}
  key_file: ${TLS_KEY}
  ca_file: ${CA_CERT}
  min_version: "1.2"           # 1.0 | 1.1 | 1.2 | 1.3
  client_auth: require         # none | request | require
  insecure_skip_verify: false
```

### 2.3 Secrets Management

**intu.yaml**:
```yaml
secrets:
  provider: env                 # env | vault | aws_secrets_manager | gcp_secret_manager
  vault:
    address: https://vault.example.com
    path: secret/data/intu
    auth:
      type: approle
      role_id: ${VAULT_ROLE_ID}
      secret_id: ${VAULT_SECRET_ID}
```

### 2.4 Credential Encryption at Rest

```yaml
runtime:
  encryption:
    key_file: .intu/keystore.enc
    algorithm: aes-256-gcm
```

---

## Phase 3 — Data Types & Payload Handling

### 3.1 Content Type System

Each channel declares inbound and outbound data types.

**channel.yaml**:
```yaml
data_types:
  inbound: hl7v2               # raw | json | xml | csv | hl7v2 | hl7v3 | fhir_r4 | dicom | x12 | delimited | binary
  outbound: fhir_r4

  inbound_properties:
    # HL7v2-specific
    segment_delimiter: "\r"
    strip_namespaces: false

  outbound_properties:
    # FHIR-specific
    fhir_version: R4
    pretty_print: true
```

### 3.2 Supported Data Types

| Type | Parse | Serialize | Tree Access | Batch |
|------|-------|-----------|-------------|-------|
| `raw` | Pass-through | Pass-through | String only | Line-based |
| `json` | JSON parse | JSON stringify | Dot-path (`msg.patient.id`) | Array unwrap |
| `xml` | XML DOM | XML serialize | XPath | Root element split |
| `csv` / `delimited` | Column parse | Column serialize | Index / header name | Row-based |
| `hl7v2` | Segment/field/component parse | HL7 serialize | `msg.PID.3.1` (HL7 path) | Batch `BHS/BTS` |
| `hl7v3` | CDA XML parse | CDA serialize | XPath | — |
| `fhir_r4` | FHIR JSON/XML parse | FHIR serialize | FHIRPath | Bundle unwrap |
| `x12` | Segment parse | Segment serialize | Segment path | ISA/IEA envelope |
| `dicom` | Tag parse | Tag serialize | Tag access | — |
| `binary` | Base64 decode | Base64 encode | — | — |

### 3.3 TypeScript Typed Messages

The transformer receives a parsed, typed message based on the declared `data_types.inbound`:

```typescript
// When inbound is hl7v2, msg is typed:
import type { HL7v2Message } from "@intu/sdk";

export function transform(msg: HL7v2Message, ctx: TransformContext): FHIRBundle {
  return {
    resourceType: "Bundle",
    type: "transaction",
    entry: [{
      resource: {
        resourceType: "Patient",
        identifier: [{ value: msg.PID[3][1] }],
        name: [{ family: msg.PID[5][1], given: [msg.PID[5][2]] }],
      }
    }]
  };
}
```

### 3.4 Batch Processing

**channel.yaml**:
```yaml
batch:
  enabled: true
  type: split                   # split | aggregate
  split_on: newline             # newline | hl7_batch | fhir_bundle | xml_root | custom
  custom_splitter: splitter.ts  # only if split_on: custom
  max_batch_size: 1000
  batch_timeout_ms: 5000
```

### 3.5 Attachment Handling

For large payloads (images, PDFs, DICOM files):

**channel.yaml**:
```yaml
attachments:
  enabled: true
  store: filesystem             # filesystem | s3 | database
  max_size_mb: 100
  filesystem:
    directory: /data/attachments
  s3:
    bucket: intu-attachments
    region: us-east-1
  inline_threshold_kb: 256      # payloads under this stay inline
```

---

## Phase 4 — Channel Pipeline (Full Mirth Parity)

### 4.1 Pipeline Stages

Mirth Connect processes messages through a defined sequence. intu adopts the same model, configured via YAML and implemented via TypeScript.

```
┌──────────────────────────────────────────────────────────────┐
│                        CHANNEL                               │
│                                                              │
│  Source Connector                                             │
│       │                                                      │
│       ▼                                                      │
│  [Preprocessor]  ──  preprocessor.ts                         │
│       │                                                      │
│       ▼                                                      │
│  [Source Filter]  ──  source-filter.ts                        │
│       │                                                      │
│       ▼                                                      │
│  [Source Transformer]  ──  transformer.ts                     │
│       │                                                      │
│       ├──────────────────┬──────────────────┐                │
│       ▼                  ▼                  ▼                │
│  Destination 1      Destination 2     Destination N          │
│  [Dest Filter]      [Dest Filter]     [Dest Filter]          │
│  [Dest Transformer]  [Dest Tfm]       [Dest Tfm]             │
│  [Send]             [Send]            [Send]                 │
│  [Response Tfm]     [Response Tfm]    [Response Tfm]         │
│       │                  │                  │                │
│       └──────────────────┴──────────────────┘                │
│                          │                                   │
│                          ▼                                   │
│                    [Postprocessor]  ──  postprocessor.ts      │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 4.2 Channel YAML — Full Pipeline

```yaml
id: adt-to-fhir
enabled: true
tags: [adt, production, hospital-a]

data_types:
  inbound: hl7v2
  outbound: fhir_r4

listener:
  type: tcp
  tcp:
    port: 2575
    mode: mllp

pipeline:
  preprocessor: preprocessor.ts         # optional
  source_filter: source-filter.ts       # optional — return false to drop
  transformer: transformer.ts           # required

  postprocessor: postprocessor.ts       # optional — runs after all destinations

destinations:
  - name: epic-fhir
    ref: epic-fhir                      # references root-level named destination
    filter: dest-filter-epic.ts         # optional per-destination filter
    transformer: dest-transform-epic.ts # optional per-destination transform
    response_transformer: response-epic.ts

  - name: audit-db
    ref: audit-db
    transformer: dest-transform-audit.ts
```

### 4.3 Pipeline TypeScript Signatures

Each stage has a clear TypeScript signature. All live in the channel directory.

```typescript
// preprocessor.ts — raw bytes, before parsing
export function preprocess(raw: Buffer, ctx: PipelineContext): Buffer {
  return raw; // modify raw bytes if needed
}

// source-filter.ts — return true to continue, false to drop
export function filter(msg: unknown, ctx: PipelineContext): boolean {
  const hl7 = msg as HL7v2Message;
  return hl7.MSH[9][1] === "ADT"; // only process ADT messages
}

// transformer.ts — main source-level transform
export function transform(msg: unknown, ctx: TransformContext): unknown {
  // transform HL7v2 to FHIR
  return { resourceType: "Patient", /* ... */ };
}

// dest-filter-epic.ts — per-destination filter
export function filter(msg: unknown, ctx: DestinationContext): boolean {
  return ctx.destinationName === "epic-fhir";
}

// dest-transform-epic.ts — per-destination transform
export function transform(msg: unknown, ctx: DestinationContext): unknown {
  return msg; // customize payload for this destination
}

// response-epic.ts — process the destination's response
export function transformResponse(response: DestinationResponse, ctx: DestinationContext): unknown {
  if (response.statusCode >= 400) {
    ctx.logger.error("FHIR server rejected", { status: response.statusCode });
  }
  return response.body;
}

// postprocessor.ts — runs after all destinations complete
export function postprocess(msg: unknown, results: DestinationResult[], ctx: PipelineContext): void {
  const allSucceeded = results.every(r => r.success);
  if (!allSucceeded) ctx.logger.warn("Some destinations failed");
}
```

### 4.4 Destination Set Routing (Mirth Parity)

In the source transformer, dynamically control which destinations receive the message:

```typescript
export function transform(msg: unknown, ctx: TransformContext): unknown {
  const hl7 = msg as HL7v2Message;

  if (hl7.MSH[9][2] === "A08") {
    ctx.routeTo("epic-fhir");          // only send updates to Epic
  } else {
    ctx.routeTo("epic-fhir", "audit-db"); // send to both
  }

  return transformToFHIR(hl7);
}
```

### 4.5 Deploy / Undeploy Scripts

**channel.yaml**:
```yaml
lifecycle:
  on_deploy: deploy.ts
  on_undeploy: undeploy.ts
```

```typescript
// deploy.ts
export async function onDeploy(ctx: LifecycleContext): Promise<void> {
  await ctx.cache.set("lookup_table", await fetchLookupTable());
}

// undeploy.ts
export async function onUndeploy(ctx: LifecycleContext): Promise<void> {
  ctx.logger.info("Channel undeployed, cleaning up...");
}
```

### 4.6 ACK/NACK Generation

For HL7v2 MLLP listeners, automatic acknowledgment:

**channel.yaml**:
```yaml
listener:
  type: tcp
  tcp:
    port: 2575
    mode: mllp
    ack:
      auto: true                 # auto-generate ACK/NACK
      success_code: AA
      error_code: AE
      reject_code: AR
```

Override via TypeScript:

```typescript
export function generateAck(msg: HL7v2Message, ctx: PipelineContext): string {
  return buildACK(msg, "AA", "Message accepted");
}
```

---

## Phase 5 — Logging, Observability & Message Storage

### 5.1 Per-Channel Log Levels

**channel.yaml**:
```yaml
logging:
  level: debug                  # debug | info | warn | error | silent
```

**intu.yaml** (global default):
```yaml
runtime:
  log_level: info               # global fallback
```

### 5.2 Payload Logging Granularity

Control exactly which payloads are stored/logged per channel:

**channel.yaml**:
```yaml
logging:
  level: info
  payloads:
    source: true                # log raw inbound payload
    transformed: true           # log post-transformer payload
    sent: true                  # log what was sent to destination
    response: true              # log destination response
    filtered: false             # log dropped messages (filter=false)
  truncate_at: 10000            # max chars per payload log entry (0 = unlimited)
```

### 5.3 Silent / High-Performance Mode

For channels requiring sub-10ms transport with minimal overhead:

**channel.yaml**:
```yaml
logging:
  level: silent                 # no logs at all
  payloads:
    source: false
    transformed: false
    sent: false
    response: false
  message_storage: none         # don't persist messages

performance:
  zero_copy: true               # skip serialization between stages where possible
  sync_destinations: false      # fire-and-forget to destinations (no response wait)
```

### 5.4 Message Content Storage

Persist message content for audit, replay, and debugging:

**intu.yaml**:
```yaml
message_storage:
  driver: postgres              # memory | postgres | s3
  postgres:
    dsn: ${INTU_STORE_DSN}
    table_prefix: intu_
  s3:
    bucket: intu-messages
    region: us-east-1
  retention:
    days: 30
    prune_interval: 24h
    prune_errored: false        # keep errored messages longer
    errored_retention_days: 90
```

Per-channel storage override:

**channel.yaml**:
```yaml
message_storage:
  enabled: true                 # false to skip storage for this channel
  content_types:                # which stages to store
    - raw
    - transformed
    - sent
    - response
    - error
  retention_days: 7             # override global
```

### 5.5 Correlation & Tracing

Every message gets a correlation ID. Propagated across inter-channel routing.

**channel.yaml**:
```yaml
tracing:
  correlation_id_header: X-Correlation-Id   # extract from inbound header
  propagate: true                            # pass to destinations
```

### 5.6 OpenTelemetry Integration

**intu.yaml**:
```yaml
observability:
  opentelemetry:
    enabled: true
    endpoint: ${OTEL_ENDPOINT}
    protocol: grpc              # grpc | http
    traces: true
    metrics: true
    service_name: intu
    resource_attributes:
      deployment.environment: production
  prometheus:
    enabled: true
    port: 9090
    path: /metrics
```

Exported metrics:
- `intu_messages_received_total{channel, source_type}`
- `intu_messages_processed_total{channel, status}`
- `intu_messages_errored_total{channel, destination, error_type}`
- `intu_message_processing_duration_ms{channel, stage}`
- `intu_destination_latency_ms{channel, destination}`
- `intu_channel_queue_depth{channel, destination}`
- `intu_active_connections{channel, connector_type}`

---

## Phase 6 — Error Handling, Retry & Dead-Letter

### 6.1 Retry Policies

Per-destination retry configuration:

**intu.yaml** (destination-level):
```yaml
destinations:
  epic-fhir:
    type: http
    http:
      url: https://fhir.epic.com/api/FHIR/R4
    retry:
      max_attempts: 5
      backoff: exponential       # fixed | linear | exponential
      initial_delay_ms: 500
      max_delay_ms: 30000
      jitter: true
      retry_on:                  # which errors trigger retry
        - timeout
        - connection_refused
        - status_5xx
      no_retry_on:
        - status_4xx             # don't retry client errors
```

### 6.2 Destination Queuing

**channel.yaml**:
```yaml
destinations:
  - name: epic-fhir
    ref: epic-fhir
    queue:
      enabled: true
      max_size: 10000
      overflow: drop_oldest      # drop_oldest | reject | block
      persist: true              # survive restart
      threads: 4                 # concurrent senders
```

### 6.3 Dead-Letter Queue (DLQ)

Messages that exhaust retries go to the DLQ:

**intu.yaml**:
```yaml
dead_letter:
  enabled: true
  destination: dlq-store        # reference a named destination
  include_error: true           # attach error details
  include_original: true        # include original payload
```

Or per-channel:

**channel.yaml**:
```yaml
error_handling:
  on_error: queue               # stop | queue | discard | dlq
  dlq:
    destination: dlq-store
  alert:
    destination: notify-email   # send an alert on error
```

### 6.4 ACK/NACK Response Handling

```yaml
listener:
  type: tcp
  tcp:
    port: 2575
    mode: mllp
    response:
      on_success: ack           # ack | custom
      on_error: nack
      on_filter_drop: ack       # still ACK filtered messages
      wait_for_destinations: true # wait for all destinations before ACK
```

---

## Phase 7 — Code Reuse & Shared Libraries

### 7.1 Shared TypeScript Libraries

Project-level shared code in `lib/`:

```
project-root/
├── lib/
│   ├── hl7-helpers.ts
│   ├── fhir-builder.ts
│   ├── lookups.ts
│   └── index.ts
├── channels/
│   ├── adt-intake/
│   └── lab-results/
```

Channel transformers import from `lib/`:

```typescript
import { buildPatientResource } from "../../lib/fhir-builder";
import { lookupFacility } from "../../lib/lookups";

export function transform(msg: HL7v2Message, ctx: TransformContext): FHIRBundle {
  const facility = lookupFacility(msg.MSH[4][1]);
  return buildPatientResource(msg.PID, facility);
}
```

### 7.2 Global Hooks

**intu.yaml**:
```yaml
global:
  hooks:
    on_startup: lib/startup.ts
    on_shutdown: lib/shutdown.ts
    on_deploy_all: lib/deploy-all.ts
```

```typescript
// lib/startup.ts
export async function onStartup(ctx: GlobalContext): Promise<void> {
  ctx.globalMap.set("facility_lookup", await loadFacilityTable());
}
```

### 7.3 npm Package Ecosystem

Publish reusable transforms as npm packages:

```json
{
  "dependencies": {
    "@intu/sdk": "^1.0.0",
    "@intu/hl7v2": "^1.0.0",
    "@intu/fhir-r4": "^1.0.0",
    "my-org-transforms": "^2.0.0"
  }
}
```

---

## Phase 8 — Channel Management & Operations

### 8.1 Channel Tags & Groups

**channel.yaml**:
```yaml
id: adt-to-fhir
tags: [adt, production, hospital-a]
group: clinical-feeds
priority: high                  # low | normal | high | critical
```

CLI commands:
```bash
intu channel list --tag production
intu channel list --group clinical-feeds
```

### 8.2 Channel Deploy / Undeploy / Enable / Disable

```bash
intu deploy adt-to-fhir           # deploy single channel
intu deploy --all                 # deploy all
intu deploy --tag production      # deploy by tag
intu undeploy adt-to-fhir
intu enable adt-to-fhir
intu disable adt-to-fhir
intu restart adt-to-fhir
```

### 8.3 Channel Dependencies

**channel.yaml**:
```yaml
depends_on:
  - auth-refresh-channel         # start this channel first
  - lookup-loader-channel
startup_order: 10                # numeric ordering within group
```

### 8.4 Channel Statistics

```bash
intu stats                        # all channels
intu stats adt-to-fhir            # single channel
intu stats --json                 # machine-readable
```

Output:
```
Channel: adt-to-fhir
  Status:     RUNNING
  Received:   15,234
  Filtered:   1,203
  Processed:  14,031
  Errored:    12
  Queued:     0
  Avg Latency: 4.2ms
  Uptime:     3d 12h 44m
```

### 8.5 Message Reprocessing

```bash
intu reprocess adt-to-fhir --message-id abc123
intu reprocess adt-to-fhir --status errored --since 2025-01-01
intu reprocess adt-to-fhir --filter 'msg.PID[3][1] == "12345"'
```

### 8.6 Message Search

```bash
intu messages adt-to-fhir --status errored --limit 50
intu messages adt-to-fhir --search "patientId:12345"
intu messages adt-to-fhir --id abc123 --content raw
intu messages adt-to-fhir --id abc123 --content transformed
```

### 8.7 Data Pruning

**intu.yaml**:
```yaml
pruning:
  enabled: true
  schedule: "0 2 * * *"         # cron — run at 2 AM daily
  default_retention_days: 30
  archive_before_prune: true
  archive_destination: archive-s3
```

Per-channel override in **channel.yaml**:
```yaml
pruning:
  retention_days: 7
  prune_errored: false
```

CLI:
```bash
intu prune --channel adt-to-fhir --before 2025-01-01 --dry-run
intu prune --all --confirm
```

---

## Phase 9 — Alerting, Monitoring & Dashboard

### 9.1 Alert Configuration

**intu.yaml**:
```yaml
alerts:
  - name: channel-errors
    trigger:
      type: error_count         # error_count | error_rate | queue_depth | latency | channel_down
      channel: "*"              # all channels, or specific ID
      threshold: 10
      window: 5m
    destinations:
      - notify-email
      - slack-webhook

  - name: high-latency
    trigger:
      type: latency
      channel: adt-to-fhir
      threshold_ms: 5000
      percentile: p99
      window: 1m
    destinations:
      - slack-webhook

  - name: queue-buildup
    trigger:
      type: queue_depth
      channel: "*"
      threshold: 1000
    destinations:
      - notify-email
```

Alert destinations use the same named destinations from root config.

### 9.2 Web Dashboard

```bash
intu dashboard --port 3000
```

Lightweight web UI (bundled or separate `@intu/dashboard` package):

- Real-time channel status (running/stopped/errored)
- Message throughput graphs
- Per-channel message counts (received/filtered/processed/errored/queued)
- Latency histograms
- Error log viewer
- Message content inspector (raw → transformed → sent → response)
- Message reprocessing from UI
- Channel deploy/undeploy/restart from UI

### 9.3 Webhook / Slack / PagerDuty Integrations

```yaml
destinations:
  slack-webhook:
    type: http
    http:
      url: ${SLACK_WEBHOOK_URL}
      method: POST
      headers:
        Content-Type: application/json

  pagerduty:
    type: http
    http:
      url: https://events.pagerduty.com/v2/enqueue
      method: POST
      auth:
        type: bearer
        token: ${PD_ROUTING_KEY}
```

---

## Phase 10 — Access Control & Multi-Tenancy

### 10.1 Role-Based Access Control (RBAC)

**intu.yaml**:
```yaml
access_control:
  enabled: true
  provider: local               # local | ldap | oidc
  ldap:
    url: ldaps://ldap.example.com
    base_dn: dc=example,dc=com
    bind_dn: cn=admin,dc=example,dc=com
    bind_password: ${LDAP_PASS}
  oidc:
    issuer: https://auth.example.com
    client_id: ${OIDC_CLIENT_ID}
    client_secret: ${OIDC_SECRET}

roles:
  - name: admin
    permissions: ["*"]
  - name: developer
    permissions:
      - channel.read
      - channel.deploy
      - channel.undeploy
      - message.read
      - message.reprocess
  - name: viewer
    permissions:
      - channel.read
      - message.read
      - stats.read
  - name: operator
    permissions:
      - channel.read
      - channel.deploy
      - channel.undeploy
      - channel.enable
      - channel.disable
      - stats.read
      - alert.read
```

### 10.2 Audit Log

Every administrative and data access action is logged:

**intu.yaml**:
```yaml
audit:
  enabled: true
  destination: audit-db          # named destination
  events:
    - channel.deploy
    - channel.undeploy
    - channel.config_change
    - message.view
    - message.reprocess
    - user.login
    - user.permission_change
```

### 10.3 Multi-Tenancy

**intu.yaml**:
```yaml
tenancy:
  mode: single                  # single | multi
  isolation: schema             # schema | database | namespace
  tenant_header: X-Tenant-Id
```

---

## Phase 11 — Advanced Connectors & Healthcare Standards

### 11.1 HL7 FHIR R4 Native Support

First-class FHIR operations as a destination type:

```yaml
destinations:
  fhir-server:
    type: fhir
    fhir:
      base_url: https://fhir.example.com/R4
      version: R4
      auth:
        type: oauth2_client_credentials
        token_url: https://auth.example.com/token
        client_id: ${FHIR_CLIENT_ID}
        client_secret: ${FHIR_SECRET}
      operations:
        - create
        - update
        - search
      retry:
        max_attempts: 3
```

FHIR listener (for FHIR Subscriptions / Webhooks):

```yaml
listener:
  type: fhir
  fhir:
    port: 8443
    base_path: /fhir
    version: R4
    subscription_type: rest-hook  # rest-hook | websocket
    tls:
      cert_file: ${TLS_CERT}
      key_file: ${TLS_KEY}
```

### 11.2 HL7v2 Helpers (npm package: `@intu/hl7v2`)

```typescript
import { parseHL7, buildACK, HL7v2Message } from "@intu/hl7v2";

export function transform(msg: HL7v2Message, ctx: TransformContext) {
  const patientId = msg.PID[3][1];          // patient identifier
  const eventType = msg.MSH[9][1];          // message type
  const sendingApp = msg.MSH[3][1];         // sending application
  // ...
}
```

### 11.3 X12 EDI Support

```yaml
data_types:
  inbound: x12
  inbound_properties:
    transaction_set: "837"      # 837 | 835 | 270 | 271 | 276 | 277 | 834
    version: "005010X222A1"
```

### 11.4 CCDA / CDA Support

```yaml
data_types:
  inbound: ccda
  inbound_properties:
    template: CCD               # CCD | Discharge_Summary | Progress_Note
    validate_schematron: true
```

### 11.5 Direct Messaging (Direct Protocol for HIE)

```yaml
destinations:
  hie-direct:
    type: direct
    direct:
      to: provider@direct.example.com
      from: intu@direct.myorg.com
      smtp:
        host: smtp.direct.myorg.com
        port: 25
      certificate: ${DIRECT_CERT}
```

### 11.6 IHE Profile Support

Cross-enterprise Document Sharing (XDS), Patient Identity Cross-Reference (PIX), Patient Demographics Query (PDQ):

```yaml
listener:
  type: ihe
  ihe:
    profile: xds_repository     # xds_repository | xds_registry | pix | pdq
    port: 8443
    tls:
      cert_file: ${TLS_CERT}
      key_file: ${TLS_KEY}
```

---

## Phase 12 — Clustering, HA & Horizontal Scaling

### 12.1 Multi-Instance Coordination

**intu.yaml**:
```yaml
cluster:
  enabled: true
  mode: active-active           # active-active | active-passive
  coordination:
    type: redis                 # redis | postgres | etcd
    redis:
      address: ${REDIS_URL}
      password: ${REDIS_PASS}
  instance_id: ${HOSTNAME}
  heartbeat_interval: 5s
```

### 12.2 Channel Partitioning

Distribute channels across instances:

```yaml
cluster:
  channel_assignment:
    strategy: auto              # auto | manual | tag-based
    tag_affinity:
      hospital-a: [instance-1, instance-2]
      hospital-b: [instance-3, instance-4]
```

### 12.3 Message Deduplication

```yaml
cluster:
  deduplication:
    enabled: true
    window: 5m
    store: redis
    key_extractor: dedup-key.ts
```

```typescript
// dedup-key.ts
export function extractKey(msg: unknown, ctx: PipelineContext): string {
  const hl7 = msg as HL7v2Message;
  return `${hl7.MSH[10]}`;  // message control ID
}
```

### 12.4 Health Checks

```yaml
runtime:
  health:
    port: 8081
    path: /health
    readiness_path: /ready
    liveness_path: /live
```

```bash
curl http://localhost:8081/health
# { "status": "healthy", "channels": { "running": 12, "stopped": 0, "errored": 1 } }
```

---

## Appendix A — Full YAML Config Reference (Target)

Complete `intu.yaml` schema after all phases:

```yaml
runtime:
  name: string
  profile: string               # dev | staging | prod | custom
  log_level: string             # debug | info | warn | error | silent
  storage:
    driver: string              # memory | postgres
    postgres_dsn: string
  encryption:
    key_file: string
    algorithm: string           # aes-256-gcm
  health:
    port: number
    path: string
    readiness_path: string
    liveness_path: string

channels_dir: string            # default: "channels"

secrets:
  provider: string              # env | vault | aws_secrets_manager | gcp_secret_manager
  vault:
    address: string
    path: string
    auth:
      type: string
      role_id: string
      secret_id: string

destinations:
  <name>:
    type: string                # http | kafka | tcp | file | database | smtp | channel | dicom | jms | fhir | direct
    # type-specific config...
    retry:
      max_attempts: number
      backoff: string           # fixed | linear | exponential
      initial_delay_ms: number
      max_delay_ms: number
      jitter: boolean
      retry_on: [string]
      no_retry_on: [string]

kafka:
  brokers: [string]
  client_id: string

dead_letter:
  enabled: boolean
  destination: string
  include_error: boolean
  include_original: boolean

message_storage:
  driver: string                # memory | postgres | s3
  retention:
    days: number
    prune_interval: string
    prune_errored: boolean
    errored_retention_days: number

pruning:
  enabled: boolean
  schedule: string              # cron expression
  default_retention_days: number
  archive_before_prune: boolean
  archive_destination: string

observability:
  opentelemetry:
    enabled: boolean
    endpoint: string
    protocol: string            # grpc | http
    traces: boolean
    metrics: boolean
    service_name: string
  prometheus:
    enabled: boolean
    port: number
    path: string

alerts:
  - name: string
    trigger:
      type: string              # error_count | error_rate | queue_depth | latency | channel_down
      channel: string
      threshold: number
      window: string
      threshold_ms: number
      percentile: string
    destinations: [string]

access_control:
  enabled: boolean
  provider: string              # local | ldap | oidc

roles:
  - name: string
    permissions: [string]

audit:
  enabled: boolean
  destination: string
  events: [string]

cluster:
  enabled: boolean
  mode: string                  # active-active | active-passive
  coordination:
    type: string                # redis | postgres | etcd
  instance_id: string
  heartbeat_interval: string
  deduplication:
    enabled: boolean
    window: string
    store: string
    key_extractor: string

global:
  hooks:
    on_startup: string
    on_shutdown: string
    on_deploy_all: string
```

---

## Appendix B — Full Channel YAML Reference (Target)

Complete `channel.yaml` schema after all phases:

```yaml
id: string
enabled: boolean
tags: [string]
group: string
priority: string                # low | normal | high | critical
depends_on: [string]
startup_order: number

data_types:
  inbound: string               # raw | json | xml | csv | hl7v2 | hl7v3 | fhir_r4 | dicom | x12 | ccda | delimited | binary
  outbound: string
  inbound_properties: {}        # type-specific parsing config
  outbound_properties: {}       # type-specific serialization config

listener:
  type: string                  # http | tcp | sftp | file | database | kafka | channel | email | dicom | soap | fhir | ihe
  # type-specific config with auth and tls blocks...

pipeline:
  preprocessor: string          # filename.ts (optional)
  source_filter: string         # filename.ts (optional)
  transformer: string           # filename.ts (required)
  postprocessor: string         # filename.ts (optional)

validator:
  runtime: string               # node
  entrypoint: string

transformer:                    # legacy (pre-pipeline) — still supported
  runtime: string
  entrypoint: string

destinations:
  - name: string
    ref: string                 # reference to root-level destination
    # or inline destination config...
    filter: string              # per-destination filter TS file
    transformer: string         # per-destination transformer TS file
    response_transformer: string
    queue:
      enabled: boolean
      max_size: number
      overflow: string          # drop_oldest | reject | block
      persist: boolean
      threads: number

error_handling:
  on_error: string              # stop | queue | discard | dlq
  dlq:
    destination: string
  alert:
    destination: string

lifecycle:
  on_deploy: string             # filename.ts
  on_undeploy: string           # filename.ts

logging:
  level: string                 # debug | info | warn | error | silent
  payloads:
    source: boolean
    transformed: boolean
    sent: boolean
    response: boolean
    filtered: boolean
  truncate_at: number

message_storage:
  enabled: boolean
  content_types: [string]       # raw | transformed | sent | response | error
  retention_days: number

batch:
  enabled: boolean
  type: string                  # split | aggregate
  split_on: string              # newline | hl7_batch | fhir_bundle | xml_root | custom
  custom_splitter: string
  max_batch_size: number
  batch_timeout_ms: number

attachments:
  enabled: boolean
  store: string                 # filesystem | s3 | database
  max_size_mb: number
  inline_threshold_kb: number

tracing:
  correlation_id_header: string
  propagate: boolean

performance:
  zero_copy: boolean
  sync_destinations: boolean

pruning:
  retention_days: number
  prune_errored: boolean
```

---

## Appendix C — TypeScript API Surface (Target)

The `@intu/sdk` package exposes these types and utilities:

```typescript
// Context objects passed to every pipeline stage
interface PipelineContext {
  channelId: string;
  correlationId: string;
  messageId: string;
  timestamp: Date;
  logger: Logger;
  globalMap: Map<string, unknown>;
  channelMap: Map<string, unknown>;
  secrets: Record<string, string>;
  routeTo(...destinations: string[]): void;
}

interface TransformContext extends PipelineContext {
  sourceType: string;
  inboundDataType: string;
  outboundDataType: string;
}

interface DestinationContext extends PipelineContext {
  destinationName: string;
  destinationType: string;
}

interface AuthContext {
  headers: Record<string, string>;
  query: Record<string, string>;
  remoteAddress: string;
  secrets: Record<string, string>;
  method: string;
  path: string;
}

interface AuthResult {
  authenticated: boolean;
  principal?: string;
  roles?: string[];
  metadata?: Record<string, unknown>;
}

interface DestinationResponse {
  statusCode: number;
  body: unknown;
  headers: Record<string, string>;
  latencyMs: number;
}

interface DestinationResult {
  destinationName: string;
  success: boolean;
  response?: DestinationResponse;
  error?: string;
}

interface LifecycleContext {
  channelId: string;
  logger: Logger;
  cache: CacheAPI;
  globalMap: Map<string, unknown>;
}

interface GlobalContext {
  logger: Logger;
  globalMap: Map<string, unknown>;
  config: Record<string, unknown>;
}

interface Logger {
  debug(msg: string, meta?: Record<string, unknown>): void;
  info(msg: string, meta?: Record<string, unknown>): void;
  warn(msg: string, meta?: Record<string, unknown>): void;
  error(msg: string, meta?: Record<string, unknown>): void;
}

interface CacheAPI {
  get<T>(key: string): Promise<T | undefined>;
  set<T>(key: string, value: T, ttl?: number): Promise<void>;
  delete(key: string): Promise<void>;
}

// Healthcare-specific types (from @intu/hl7v2, @intu/fhir-r4, etc.)
interface HL7v2Message {
  MSH: HL7Segment;
  PID: HL7Segment;
  PV1: HL7Segment;
  [segment: string]: HL7Segment | HL7Segment[];
}

type HL7Segment = Record<number, HL7Field>;
type HL7Field = Record<number, string> & { toString(): string };

interface FHIRBundle {
  resourceType: "Bundle";
  type: string;
  entry: FHIRBundleEntry[];
}

// ... and many more FHIR resource types
```

---

## Appendix D — Mirth Feature Parity Checklist

| # | Mirth Connect Feature | Phase | Priority |
|---|----------------------|-------|----------|
| 1 | HTTP Listener | 0, 1.1 | P0 |
| 2 | HTTP Sender | 0, 1.11 | P0 |
| 3 | Kafka Producer | 0, 1.11 | P0 |
| 4 | Source Transformer | 0 | P0 |
| 5 | Validator | 0 | P0 |
| 6 | Runtime engine (`intu serve`) | 0 | P0 |
| 7 | TCP/MLLP Listener | 1.2 | P0 |
| 8 | TCP/MLLP Sender | 1.11 | P0 |
| 9 | SFTP Listener | 1.3 | P0 |
| 10 | File Reader (local/FTP/S3/SMB) | 1.4 | P1 |
| 11 | File Writer (local/FTP/SFTP/S3/SMB) | 1.11 | P1 |
| 12 | Database Reader | 1.5 | P1 |
| 13 | Database Writer | 1.11 | P1 |
| 14 | Kafka Consumer | 1.6 | P1 |
| 15 | Channel Reader (inter-channel) | 1.7 | P1 |
| 16 | Channel Writer (inter-channel) | 1.11 | P1 |
| 17 | Email Reader | 1.8 | P2 |
| 18 | SMTP Sender | 1.11 | P2 |
| 19 | DICOM Listener | 1.9 | P2 |
| 20 | DICOM Sender | 1.11 | P2 |
| 21 | SOAP Listener | 1.10 | P2 |
| 22 | JMS Listener | 1.11 | P3 |
| 23 | JMS Sender | 1.11 | P3 |
| 24 | Basic Auth (source + dest) | 2.1 | P0 |
| 25 | Bearer Token Auth (source + dest) | 2.1 | P0 |
| 26 | API Key Auth (source + dest) | 2.1 | P0 |
| 27 | OAuth 2.0 Client Credentials | 2.1 | P0 |
| 28 | OAuth 2.0 Authorization Code | 2.1 | P1 |
| 29 | mTLS / Client Certificates | 2.1 | P1 |
| 30 | SAML | 2.1 | P3 |
| 31 | Custom Auth (TS hook) | 2.1 | P1 |
| 32 | TLS on all connectors | 2.2 | P0 |
| 33 | Secrets management (Vault, AWS, GCP) | 2.3 | P1 |
| 34 | Credential encryption at rest | 2.4 | P2 |
| 35 | Raw data type | 3.1 | P0 |
| 36 | JSON data type | 3.1 | P0 |
| 37 | XML data type | 3.1 | P0 |
| 38 | CSV / Delimited data type | 3.1 | P1 |
| 39 | HL7v2 data type (parse + serialize) | 3.1 | P0 |
| 40 | HL7v3 / CDA data type | 3.1 | P2 |
| 41 | FHIR R4 data type | 3.1 | P0 |
| 42 | X12 EDI data type | 3.1 | P2 |
| 43 | DICOM data type | 3.1 | P3 |
| 44 | Binary (Base64) data type | 3.1 | P1 |
| 45 | Batch processing (split/aggregate) | 3.4 | P1 |
| 46 | Attachment handling | 3.5 | P2 |
| 47 | Preprocessor stage | 4.1 | P1 |
| 48 | Source filter | 4.1 | P0 |
| 49 | Destination filter | 4.1 | P1 |
| 50 | Destination transformer | 4.1 | P1 |
| 51 | Response transformer | 4.1 | P1 |
| 52 | Postprocessor stage | 4.1 | P2 |
| 53 | Destination set routing | 4.4 | P1 |
| 54 | Deploy/undeploy scripts | 4.5 | P2 |
| 55 | ACK/NACK generation | 4.6 | P0 |
| 56 | Per-channel log levels | 5.1 | P0 |
| 57 | Payload logging (source/transformed/response) | 5.2 | P0 |
| 58 | Silent mode (< 10ms) | 5.3 | P1 |
| 59 | Message content storage | 5.4 | P0 |
| 60 | Correlation ID propagation | 5.5 | P0 |
| 61 | OpenTelemetry integration | 5.6 | P1 |
| 62 | Prometheus metrics | 5.6 | P1 |
| 63 | Retry policies (exponential backoff) | 6.1 | P0 |
| 64 | Destination queuing | 6.2 | P0 |
| 65 | Dead-letter queue | 6.3 | P0 |
| 66 | ACK/NACK response handling | 6.4 | P0 |
| 67 | Shared TS libraries (`lib/`) | 7.1 | P1 |
| 68 | Global hooks (startup/shutdown) | 7.2 | P2 |
| 69 | npm package ecosystem (@intu/*) | 7.3 | P1 |
| 70 | Channel tags & groups | 8.1 | P1 |
| 71 | Channel deploy/undeploy CLI | 8.2 | P0 |
| 72 | Channel dependencies & ordering | 8.3 | P2 |
| 73 | Channel statistics | 8.4 | P0 |
| 74 | Message reprocessing | 8.5 | P1 |
| 75 | Message search | 8.6 | P1 |
| 76 | Data pruning | 8.7 | P1 |
| 77 | Alert configuration | 9.1 | P1 |
| 78 | Web dashboard | 9.2 | P2 |
| 79 | Webhook/Slack/PagerDuty alerts | 9.3 | P1 |
| 80 | RBAC | 10.1 | P2 |
| 81 | Audit log | 10.2 | P1 |
| 82 | Multi-tenancy | 10.3 | P3 |
| 83 | FHIR-native destination | 11.1 | P1 |
| 84 | HL7v2 SDK package | 11.2 | P0 |
| 85 | X12 EDI support | 11.3 | P2 |
| 86 | CCDA support | 11.4 | P2 |
| 87 | Direct messaging (HIE) | 11.5 | P3 |
| 88 | IHE profiles (XDS, PIX, PDQ) | 11.6 | P3 |
| 89 | Clustering / HA | 12.1 | P2 |
| 90 | Channel partitioning | 12.2 | P2 |
| 91 | Message deduplication | 12.3 | P2 |
| 92 | Health checks | 12.4 | P0 |

### Priority Legend

| Priority | Meaning | Target |
|----------|---------|--------|
| **P0** | Must-have for v1.0 — core runtime, basic connectors, HL7/FHIR | Phases 0–2 |
| **P1** | Should-have for v1.x — operational maturity, observability | Phases 3–6 |
| **P2** | Nice-to-have for v2.0 — dashboard, RBAC, clustering | Phases 7–10 |
| **P3** | Future — niche protocols, multi-tenancy, IHE | Phases 11–12 |
