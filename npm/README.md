# intu

intu is a Git-native, AI-friendly healthcare interoperability framework that lets teams build, version, and deploy integration pipelines using YAML configuration and TypeScript transformers.

## Install

```bash
npm i -g intu-dev
```

## Commands

| Command | Description |
|---------|-------------|
| `intu init <project-name> [--dir]` | Bootstrap a new project |
| `intu c <channel-name> [--dir]` | Add a new channel |
| `intu channel add <channel-name> [--dir]` | Same as `intu c` |
| `intu channel list [--dir]` | List channels |
| `intu channel describe <id> [--dir]` | Show channel config |
| `intu validate [--dir]` | Validate project and channels |
| `intu build [--dir]` | Compile TypeScript channels |

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
