# intu

intu is a Git-native, AI-friendly healthcare interoperability framework that lets teams build, version, and deploy integration pipelines using YAML configuration and TypeScript transformers.

## Install

```bash
npm i -g intu-dev
```

## Commands

| Command | Description |
|---------|-------------|
| `intu init <project-name> [--dir] [--force]` | Bootstrap a new project into the target directory |
| `intu c <channel-name> [--dir] [--force]` | Shorthand to scaffold a new channel in an existing project |
| `intu channel add <channel-name> [--dir] [--force]` | Add a new channel via the `channel` subcommand |
| `intu channel list [--dir] [--profile] [--tag] [--group]` | List channels with optional tag/group filters |
| `intu channel describe <id> [--dir] [--profile]` | Show the raw `channel.yaml` for a channel |
| `intu serve [--dir] [--profile]` | Start the runtime engine and process messages for all enabled channels |
| `intu validate [--dir] [--profile]` | Validate project configuration and channel layout |
| `intu build [--dir]` | Run `npm run build` to compile TypeScript transformers |
| `intu stats [channel-id] [--dir] [--profile] [--json]` | Show structural stats for one or all channels |
| `intu deploy [channel-id] [--dir] [--profile] [--all] [--tag]` | Mark channels as `enabled` (deploy) |
| `intu undeploy <channel-id> [--dir] [--profile]` | Mark a channel as `disabled` (undeploy) |
| `intu enable <channel-id> [--dir] [--profile]` | Enable a channel (alias for `deploy` on a single channel) |
| `intu disable <channel-id> [--dir] [--profile]` | Disable a channel (alias for `undeploy`) |
| `intu prune [--dir] [--channel|--all] [--before] [--dry-run] [--confirm]` | Prune stored message data (when message storage is enabled) |
| `intu dashboard [--dir] [--profile] [--port]` | Start a local read‑only dashboard for channels and metrics |

## Quick Start

```bash
intu init my-project --dir .
cd my-project
npm install
intu build --dir .
```

Add a channel:

```bash
intu c my-channel --dir my-project
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

This package ships the prebuilt `intu` CLI binary. The following connectors are **implemented in the current runtime**.

### Supported sources (listeners)

- **HTTP (`listener.type: http`)**: JSON HTTP listener with configurable path, methods, and auth (`bearer`, `basic`, `api_key`, or none).
- **TCP (`listener.type: tcp`)**: Raw TCP or MLLP (`mode: raw|mllp`) listener suitable for HL7-style message transport.
- **File (`listener.type: file`, `scheme: local`)**: Local filesystem poller for directories with glob patterns, optional move/error folders, and ordering.
- **Channel (`listener.type: channel`)**: In‑memory channel‑to‑channel bridge for fan‑in/fan‑out and internal routing.

### Supported destinations

- **HTTP (`destinations.*.type: http`)**: HTTP sender with headers and auth (`bearer`, `basic`, `api_key`) and response capture.
- **File (`destinations.*.type: file`)**: Local filesystem writer with templated filenames (channel ID, correlation ID, timestamps, etc).
- **Channel (`destinations.*.type: channel`)**: Sends the message to another channel via the in‑memory bus.
- **Log (`destinations.*.type: log`)**: Structured logging destination, also used as a safe fallback for some not‑yet‑implemented destination types.

Additional connector types appear in the YAML schema and roadmap (for example `kafka`, `sftp`, `database`), but those are currently wired to logging stubs until their full implementations land.
