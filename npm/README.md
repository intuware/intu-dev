# intu

intu is a Git-native, AI-friendly healthcare interoperability framework that lets teams build, version, and deploy integration pipelines using YAML configuration and TypeScript transformers.

## Install

```bash
npm i -g intu-dev
```

## Commands

### Project & Build

| Command | Description |
|---------|-------------|
| `intu init <project-name> [--dir] [--force]` | Bootstrap a new project and install dependencies |
| `intu validate [--dir] [--profile]` | Validate project configuration and channel layout |
| `intu build [--dir]` | Compile TypeScript transformers (optional — `intu serve` auto-compiles) |
| `intu serve [--dir] [--profile]` | Start the runtime engine and process messages for all enabled channels |

### Channel Management

| Command | Description |
|---------|-------------|
| `intu c <channel-name> [--dir] [--force]` | Shorthand to scaffold a new channel in an existing project |
| `intu channel add <channel-name> [--dir] [--force]` | Add a new channel via the `channel` subcommand |
| `intu channel list [--dir] [--profile] [--tag] [--group]` | List channels with optional tag/group filters |
| `intu channel describe <id> [--dir] [--profile]` | Show the raw `channel.yaml` for a channel |
| `intu channel clone <source> <new> [--dir]` | Clone a channel to create a new one with a different ID |
| `intu channel export <id> [--dir] [-o file]` | Export a channel as a portable `.tar.gz` archive |
| `intu channel import <archive> [--dir] [--force]` | Import a channel from a `.tar.gz` archive |

### Deployment & Operations

| Command | Description |
|---------|-------------|
| `intu deploy [channel-id] [--dir] [--profile] [--all] [--tag]` | Mark channels as `enabled` (deploy) |
| `intu undeploy <channel-id> [--dir] [--profile]` | Mark a channel as `disabled` (undeploy) |
| `intu enable <channel-id> [--dir] [--profile]` | Enable a channel (alias for `deploy` on a single channel) |
| `intu disable <channel-id> [--dir] [--profile]` | Disable a channel (alias for `undeploy`) |
| `intu stats [channel-id] [--dir] [--profile] [--json]` | Show channel statistics with live metrics |
| `intu prune [--dir] [--channel\|--all] [--before] [--dry-run] [--confirm]` | Prune stored message data |

### Message Browser

| Command | Description |
|---------|-------------|
| `intu message list [--channel] [--status] [--since] [--before] [--limit] [--json]` | List messages from the store with filters |
| `intu message get <message-id> [--json]` | Get a specific message by ID |
| `intu message count [--channel] [--status]` | Count messages in the store |

### Advanced

| Command | Description |
|---------|-------------|
| `intu dashboard [--dir] [--profile] [--port]` | Launch the dashboard standalone (included in `intu serve` by default) |

## Quick Start

```bash
intu init my-project
cd my-project
npm run dev
```

`intu init` scaffolds the project and runs `npm install` automatically. `npm run dev` starts the engine (which auto-compiles TypeScript).

Add a channel:

```bash
intu c my-channel --dir .
```

## Project Structure (after `intu init`)

```
my-project/
├── intu.yaml           # Root config + named destinations
├── intu.dev.yaml       # Dev profile overrides
├── intu.prod.yaml      # Prod profile overrides
├── .env
├── channels/
│   └── sample-channel/
│       ├── channel.yaml
│       ├── transformer.ts
│       └── validator.ts
├── lib/
│   └── index.ts        # Shared utilities
├── package.json
├── tsconfig.json
└── README.md
```

## Channel Structure (after `intu c my-channel`)

```
channels/my-channel/
├── channel.yaml        # Listener, validator, transformer, destinations
├── transformer.ts     # Pure function: JSON in → JSON out
└── validator.ts       # Validates input, throws on invalid
```

## Destinations

Define named destinations in `intu.yaml`, reference in channels:

```yaml
destinations:
  kafka-output:
    type: kafka
    kafka:
      brokers: [${INTU_KAFKA_BROKER}]
      topic: output-topic
    retry:
      max_attempts: 3
      backoff: exponential
```

Channels support multi-destination:

```yaml
destinations:
  - kafka-output
  - name: audit-http
    type: http
    http:
      url: https://audit.example.com/events
```

## Sources & Destinations

This package ships the prebuilt `intu` CLI binary. All connectors listed below are **fully implemented** in the current runtime.

### Supported Sources (12 types)

- **HTTP** (`listener.type: http`): REST listener with path, methods, TLS, and auth (bearer, basic, api_key, mTLS).
- **TCP/MLLP** (`listener.type: tcp`): Raw TCP or MLLP mode for HL7 transport, with TLS and ACK/NACK.
- **File** (`listener.type: file`): Local filesystem poller with glob patterns, move/error dirs, and ordering.
- **Kafka** (`listener.type: kafka`): Kafka consumer with TLS and SASL auth.
- **Database** (`listener.type: database`): SQL polling reader for Postgres, MySQL, MSSQL, SQLite.
- **SFTP** (`listener.type: sftp`): SFTP poller with password/key auth.
- **Channel** (`listener.type: channel`): In-memory channel-to-channel bridge for fan-in/fan-out.
- **Email** (`listener.type: email`): IMAP/POP3 reader with TLS.
- **DICOM** (`listener.type: dicom`): DICOM SCP with AE title validation and TLS.
- **SOAP** (`listener.type: soap`): SOAP/WSDL listener with TLS and auth.
- **FHIR** (`listener.type: fhir`): FHIR R4 server with capability statement and subscriptions.
- **IHE** (`listener.type: ihe`): IHE profiles (XDS Repository/Registry, PIX, PDQ).

### Supported Destinations (13 types)

- **HTTP** (`type: http`): HTTP sender with headers, auth (bearer, basic, api_key, OAuth2), and TLS.
- **Kafka** (`type: kafka`): Kafka producer with TLS and SASL auth.
- **TCP/MLLP** (`type: tcp`): TCP sender with MLLP support and TLS.
- **File** (`type: file`): Filesystem writer with templated filenames.
- **Database** (`type: database`): SQL writer with parameterized statements.
- **SFTP** (`type: sftp`): SFTP file writer with auth.
- **SMTP** (`type: smtp`): Email sender with TLS and STARTTLS.
- **Channel** (`type: channel`): In-memory channel-to-channel routing.
- **DICOM** (`type: dicom`): DICOM SCU sender with TLS.
- **JMS** (`type: jms`): JMS via HTTP REST (ActiveMQ, etc.).
- **FHIR** (`type: fhir`): FHIR R4 client for create/update/transaction bundles.
- **Direct** (`type: direct`): Direct messaging protocol for HIE.
- **Log** (`type: log`): Structured logging destination.

## Runtime Features

- **Pipeline Stages**: Preprocessor, validator, source filter, transformer, per-destination filter/transformer, response transformer, postprocessor.
- **Retry & DLQ**: Configurable retry with backoff (fixed, linear, exponential) and dead-letter queue.
- **Destination Queuing**: Per-destination queues with overflow policies and concurrent workers.
- **Metrics**: Message counts (received, processed, filtered, errored), latency tracking.
- **Message Storage**: Persist messages at each pipeline stage for audit and replay.
- **Alerting**: Periodic alert evaluation with configurable triggers (error count, queue depth).
- **Batch Processing**: Split inbound messages using HL7 batch, FHIR bundle, newline, or XML splitters.
- **Map Variables**: globalMap, channelMap, responseMap, and connectorMap for sharing data across pipeline stages.
- **Code Template Libraries**: Share TypeScript functions across channels.
- **Channel Dependencies**: `depends_on` and `startup_order` for controlling channel boot sequence.
- **Channel Clone/Export/Import**: Clone channels, export as `.tar.gz`, import from archives.

## Data Types

HL7v2, FHIR R4, X12, CCDA, JSON, XML, CSV, binary, and raw pass-through.
