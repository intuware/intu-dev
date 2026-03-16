# AGENTS.md

## Cursor Cloud specific instructions

**Product**: `intu` is a Go-based healthcare interoperability engine. It scaffolds projects, manages channels, validates config, compiles TypeScript transformers, and runs a runtime engine with a web dashboard. 

**Prerequisites**: Go 1.25+ and Node.js >= 18 (with npm). The `intu-dev` npm package must be installed globally for the Node.js runtime (`npm install -g intu-dev`). No external services are needed for development or testing — the default config uses in-memory storage and standalone mode.

**Production** (optional): PostgreSQL (message/audit storage), Redis (cluster coordination), Kafka (source/destination), S3 (message storage), Vault/AWS/GCP (secrets).

## CLI commands

| Command | Description |
|---------|-------------|
| `intu init <name>` | Scaffold a new project and run `npm install` |
| `intu serve` | Start the runtime engine (auto-compiles TS, dashboard, hot-reload) |
| `intu build` | Compile TypeScript (optional — `intu serve` auto-compiles) |
| `intu validate` | Validate project config and channel definitions |
| `intu c <name>` | Add a new channel (shorthand for `intu channel add`) |
| `intu channel list\|describe\|clone\|export\|import` | Channel management |
| `intu deploy <id>` | Deploy (enable) a channel |
| `intu undeploy <id>` | Undeploy (disable) a channel |
| `intu stats [id]` | Show channel statistics |
| `intu message list\|get\|count` | Browse and search messages |
| `intu reprocess message\|batch` | Reprocess messages |
| `intu prune` | Prune old message data |
| `intu import mirth <file>` | Import a Mirth Connect channel XML |
| `intu dashboard` | Launch the dashboard standalone (included in `intu serve`) |

## Common dev commands

| Task | Command |
|------|---------|
| Build binary | `go build -o intu .` |
| Run all tests | `go test ./... -v` |
| Lint | `go vet ./...` |
| Run from source | `go run . <command> [flags]` |

## End-to-end demo workflow

```bash
go run . init demo-project --dir /tmp
cd /tmp/demo-project
go run <path-to-intu-repo> c my-channel --dir .
go run <path-to-intu-repo> validate --dir .
go run <path-to-intu-repo> serve --dir .
```

`intu init` runs `npm install` automatically. `intu serve` auto-compiles TypeScript before starting. You can also use `npm run dev` instead of `intu serve`.

## Project structure

| Path | Purpose |
|------|---------|
| `cmd/` | Cobra CLI commands and `channel/` subpackage |
| `internal/runtime/` | Engine, channel lifecycle, pipeline, Node.js runner, Goja fallback, hot-reload |
| `internal/dashboard/` | Web dashboard server (Go) and embedded SPA |
| `internal/storage/` | Message stores: memory, postgres, s3, composite (mode: none/status/full) |
| `internal/connector/` | Sources (HTTP, TCP, Kafka, file, DB, FHIR, email, DICOM) and destinations |
| `internal/bootstrap/` | Project/channel scaffolding templates |
| `internal/auth/` | OIDC, LDAP, basic auth, RBAC, audit |
| `internal/cluster/` | Redis coordinator, deduplication, health checks |
| `internal/retry/` | Retry queues, DLQ |
| `internal/observability/` | Prometheus, OpenTelemetry metrics |
| `internal/datatype/` | HL7v2, FHIR, X12, CCDA, JSON, XML, CSV, binary parsers |
| `pkg/config/` | YAML config loading, channel config, validation |
| `pkg/logging/` | Structured logging, log transports |
| `npm/` | `intu-dev` Node.js runtime package |
| `docs/` | HTML documentation, JSON schemas, roadmap |
| `sample/` | Gitignored local sample project |

## Tests

Tests span multiple packages — not just scaffolding:

| Package | File | Focus |
|---------|------|-------|
| `internal/bootstrap` | `scaffolder_test.go` | Project/channel scaffolding |
| `internal/storage` | `storage_test.go` | Memory store, composite store, query filters |
| `internal/dashboard` | `server_test.go` | API endpoints, dedup, payload, storage info |
| `internal/runtime` | `noderunner_test.go`, `e2e_test.go` | Node runner, end-to-end pipeline |
| `internal/connector` | `connector_test.go`, `destination_test.go` | Source/destination connectors |
| `internal/cluster` | `cluster_test.go` | Cluster coordination |
| `internal/retry` | `queue_test.go` | Retry queue logic |
| `pkg/config` | `channel_test.go` | Config parsing, listener endpoint validation |
| `pkg/logging` | `transport_test.go` | Log transports |

All tests are pure Go with no external service dependencies (memory stores, temp dirs, httptest).

## Notes

- The `--dir` flag on most commands sets the working directory for the project (defaults to `.`).
- `intu init` runs `npm install` automatically after scaffolding.
- `intu serve` auto-compiles TypeScript before starting, so a separate `intu build` step is not required during development.
- `intu build` is available for CI/CD pipelines or explicit compilation (`npm run build` / `tsc`).
- `intu serve` starts the engine with hot-reload (fsnotify) — editing channel YAML or TypeScript triggers automatic reload.
- The dashboard runs on port 3000 by default with basic auth (admin/admin in dev). It is included in `intu serve`; `intu dashboard` launches it standalone.
- Scaffolded projects include npm scripts: `npm run dev`, `npm run serve`, `npm start`, `npm run build`.
- Two JS runtimes exist: Node.js (primary, spawns worker processes) and Goja (Go-native fallback). Default is Node.
- Scaffolded projects include a Dockerfile (multi-stage Node build) and docker-compose.yml.
- HTTP sources sharing the same port are multiplexed via a shared listener with path-based routing. Duplicate port+path combinations are caught at build/validate time.
