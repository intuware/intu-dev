# intu

`intu` is a Git-native, AI-friendly healthcare interoperability framework that lets teams build, version, and deploy integration pipelines using YAML configuration and TypeScript transformers.

## Features

- Go-based CLI/runtime foundation.
- `intu init <project-name>` to bootstrap a project.
- `intu c <channel-name>` / `intu channel add <channel-name>` to add channels.
- Root-level named destinations; channels reference by name; multi-destination support.
- YAML configuration with profile layering (`intu.yaml`, `intu.dev.yaml`, `intu.prod.yaml`).
- Pure TypeScript transformers (JSON in, JSON out).

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

## Commands

| Command | Description |
|---------|-------------|
| `intu init <project-name>` | Bootstrap a new project |
| `intu c <channel-name>` | Add a new channel |
| `intu channel add <channel-name>` | Same as `intu c` |
| `intu channel list` | List channels |
| `intu channel describe <id>` | Show channel config |
| `intu validate` | Validate project and channels |
| `intu build` | Compile TypeScript channels |

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
