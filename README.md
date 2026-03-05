# intu

`intu` is a Git-native, AI-friendly healthcare interoperability framework that lets teams build, version, and deploy integration pipelines using YAML configuration and TypeScript transformers.

## Features

- Go-based CLI/runtime foundation.
- `intu init <project-name>` to bootstrap a project.
- `intu c <channel-name>` / `intu channel add <channel-name>` to add channels.
- Root-level named destinations; channels reference by name; multi-destination support.
- YAML configuration with profile layering (`intu.yaml`, `intu.dev.yaml`, `intu.prod.yaml`).
- Pure TypeScript transformers (JSON in, JSON out).

## Architecture

High-level architecture of an `intu` deployment:

![intu architecture](docs/architecture.svg)

## Install

**Via npm (recommended):**

```bash
npm i -g intu-dev
```

**From source:**

```bash
go mod tidy
go build -o intu .
# or: go run . init ...
```

## Quick Start

```bash
intu init my-project --dir .
cd my-project
npm install
intu build --dir .
```

Add a new channel:

```bash
go run . c my-channel --dir .
# or
go run . channel add my-channel --dir .
```

## CLI commands

All commands accept the global flag `--log-level (debug|info|warn|error)` (default: `info`).

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

## Bootstrapped Structure

```text
.
├── intu.yaml           # Root config + named destinations
├── intu.dev.yaml
├── intu.prod.yaml
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

## Destinations

Define named destinations in `intu.yaml`:

```yaml
destinations:
  kafka-output:
    type: kafka
    kafka:
      brokers: [${INTU_KAFKA_BROKER}]
      topic: output-topic
```

Channels reference them (multi-destination):

```yaml
destinations:
  - kafka-output
  - name: audit-http
    type: http
    http:
      url: https://audit.example.com/events
```

## Sources & destinations

This reflects **what is implemented in the current runtime**, not just what is planned in the roadmap.

### Supported sources (listeners)

- **HTTP (`listener.type: http`)**: JSON HTTP listener with configurable path, methods, and auth (`bearer`, `basic`, `api_key`, or none).
- **TCP (`listener.type: tcp`)**: Raw TCP or MLLP (`mode: raw|mllp`) listener suitable for HL7 over TCP.
- **File (`listener.type: file`, `scheme: local`)**: Local filesystem poller for directories with glob patterns, optional move/error folders, and ordering.
- **Channel (`listener.type: channel`)**: In‑memory channel‑to‑channel bridge for fan‑in/fan‑out and internal routing.

Other listener types (for example `sftp`, `database`, `kafka`) are accepted in config today but currently wired to stub implementations; their full behavior is tracked in the [roadmap](ROADMAP.md).

### Supported destinations

- **HTTP (`destinations.*.type: http`)**: HTTP sender with headers and auth (`bearer`, `basic`, `api_key`) and response capture.
- **File (`destinations.*.type: file`)**: Local filesystem writer with templated filenames (channel ID, correlation ID, timestamps, etc).
- **Channel (`destinations.*.type: channel`)**: Sends the message to another channel via the in‑memory bus.
- **Log (`destinations.*.type: log`)**: Structured logging destination, also used as a safe fallback for some not‑yet‑implemented destination types.

Destination types like `kafka`, `tcp`, `database`, `smtp`, `dicom`, `jms`, `fhir`, and `direct` are currently routed to the logging destination; they are present so config can be written ahead of full connector support.

## Contributing

Contributions (bug reports, docs, and code) are very welcome.

- **Issues & PRs**: Open them on the GitHub repository.
- **Maintainer contact**: `ramnish@intuware.com`

If you are proposing a larger feature, please skim the [ROADMAP](ROADMAP.md) first so we can keep the design aligned.

## License

`intu` is licensed under the **Mozilla Public License 2.0 (MPL‑2.0)**.  
See the [`LICENSE`](LICENSE) file for the full text and details about copyleft scope.
