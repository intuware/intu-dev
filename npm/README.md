# intu

**Integration as Code for Healthcare**

[![npm version](https://img.shields.io/npm/v/intu-dev?color=cb3837&label=npm)](https://www.npmjs.com/package/intu-dev)
[![npm downloads](https://img.shields.io/npm/dm/intu-dev?color=38bdf8)](https://www.npmjs.com/package/intu-dev)
[![CI](https://github.com/intuware/intu-dev/actions/workflows/ci.yml/badge.svg)](https://github.com/intuware/intu-dev/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MPL--2.0-blue)](https://github.com/intuware/intu-dev/blob/main/LICENSE)

[Website](https://intu.dev) · [Docs](https://intu.dev/getting-started/) · [GitHub](https://github.com/intuware/intu-dev) · [Issues](https://github.com/intuware/intu-dev/issues)

---

This package installs the `intu` CLI — a Git-native healthcare interoperability engine. Define integration channels as YAML + TypeScript, store everything in Git, and run a production-grade pipeline with one command.

## Install

```bash
npm i -g intu-dev
```

This downloads the prebuilt Go binary for your platform (macOS, Linux, Windows — x64 and arm64). No Go toolchain required.

## Quick Start

```bash
intu init my-project
cd my-project
npm run dev
```

That's it. `intu init` scaffolds the project and runs `npm install`. `npm run dev` starts the engine with hot-reload — edit YAML or TypeScript and channels restart automatically.

**Dashboard**: http://localhost:3000 (admin / admin)

### Try the included channels

```bash
# JSON pass-through
curl -X POST http://localhost:8081/ingest \
  -H "Content-Type: application/json" -d '{"hello":"world"}'

# FHIR Patient → HL7 ADT
curl -X POST http://localhost:8082/fhir/r4/Patient \
  -H "Content-Type: application/json" \
  -d '{"resourceType":"Patient","id":"123","name":[{"family":"Smith","given":["John"]}]}'
```

### Add a channel

```bash
intu c my-channel
```

## What You Get

```
my-project/
├── intu.yaml              # Root config + named destinations
├── intu.dev.yaml          # Dev profile overrides
├── intu.prod.yaml         # Production profile
├── .env                   # Environment variables
├── src/
│   ├── channels/
│   │   ├── http-to-file/      # JSON pass-through channel
│   │   │   ├── channel.yaml
│   │   │   ├── transformer.ts
│   │   │   └── validator.ts
│   │   └── fhir-to-adt/       # FHIR Patient → HL7 ADT channel
│   └── types/
│       └── intu.d.ts
├── package.json
├── tsconfig.json
├── Dockerfile
└── docker-compose.yml
```

Each channel is a folder with `channel.yaml` (config) and TypeScript files (transform logic). Pure functions, fully testable, version-controlled.

## npm Scripts

| Script | Description |
|--------|-------------|
| `npm run dev` | Start with hot-reload and debug logging |
| `npm run serve` | Start with default profile |
| `npm start` | Start in production mode |
| `npm run build` | Compile TypeScript (CI/CD — `serve` auto-compiles) |

## CLI Reference

| Command | Description |
|---------|-------------|
| `intu init <name>` | Scaffold a new project |
| `intu serve` | Start the runtime engine |
| `intu c <name>` | Add a new channel |
| `intu validate` | Validate config and channels |
| `intu build` | Compile TypeScript |
| `intu deploy [id]` | Enable channel(s) |
| `intu undeploy <id>` | Disable a channel |
| `intu stats [id]` | Show channel statistics |
| `intu dashboard` | Launch the web dashboard |
| `intu channel list\|describe\|clone\|export\|import` | Channel management |
| `intu message list\|get\|count` | Browse stored messages |
| `intu import mirth <file>` | Import a Mirth Connect channel |

All commands accept `--dir` (project root) and `--log-level (debug|info|warn|error)`.

See the [full CLI docs](https://intu.dev/reference/cli/) for all flags and options.

## Connectors

**14 sources**: HTTP, TCP/MLLP, FHIR R4, FHIR Poll, FHIR Subscription (R4b), Kafka, Database, File, SFTP, Email, DICOM, SOAP, IHE (XDS/PIX/PDQ), Channel

**13 destinations**: HTTP, Kafka, TCP/MLLP, File, Database, SFTP, SMTP, Channel, DICOM, JMS, FHIR R4, Direct, Log

**Data types**: HL7v2 · FHIR R4 · X12 · CCDA · JSON · XML · CSV · binary

## Pipeline

Every message flows through up to 8 stages — each optional, each a TypeScript function:

**Preprocessor** → **Validator** → **Source Filter** → **Transformer** → **Dest Filter** → **Dest Transformer** → **Response Transformer** → **Postprocessor**

## Included Packages

Scaffolded projects ship with these healthcare data packages:

| Package | Purpose |
|---------|---------|
| `@types/fhir` | FHIR R4 TypeScript types |
| `node-hl7-client` | HL7v2 message builder/parser with native TypeScript types |

## Requirements

- **Node.js** >= 18
- Supported platforms: macOS (arm64, x64), Linux (x64, arm64), Windows (x64, arm64)

## License

[MPL-2.0](https://github.com/intuware/intu-dev/blob/main/LICENSE)
