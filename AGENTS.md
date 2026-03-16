# AGENTS.md

**Product**: Go-based healthcare interoperability engine — project scaffolding, channel management, config validation, TypeScript compilation, runtime engine, web dashboard.

**Prerequisites**: Go 1.25+ and Node.js ≥18. Install Node runtime: `npm install -g intu-dev`. Dev/test uses in-memory storage; no external services required. Production may use PostgreSQL, Redis, Kafka, S3, Vault (optional).

## CLI

| Command | Description |
|---------|-------------|
| `intu init <name>` | Scaffold project, runs `npm install` |
| `intu serve` | Start engine (auto-compiles TS, dashboard, hot-reload) |
| `intu build` | Compile TypeScript (optional; `serve` auto-compiles) |
| `intu validate` | Validate project and channel config |
| `intu c <name>` | Add channel (`intu channel add`) |
| `intu channel list`, `describe`, `clone`, `export`, `import` | Channel ops |
| `intu deploy` / `undeploy <id>` | Enable / disable channel |
| `intu stats [id]` | Channel statistics |
| `intu message list`, `get`, `count` | Browse messages |
| `intu reprocess message` / `batch` | Reprocess messages |
| `intu prune` | Prune old message data |
| `intu import mirth <file>` | Import Mirth Connect channel XML |
| `intu dashboard` | Dashboard only (also in `intu serve`) |

Use `--dir` for project directory (default `.`). Run from source: `go run . <command> [flags]`.

## Dev

- Build: `go build -o intu .`
- Test: `go test ./... -v` (pure Go, no external deps)
- Lint: `go vet ./...`

## Project layout

| Path | Purpose |
|------|---------|
| `cmd/` | Cobra CLI, channel subcommands |
| `internal/runtime/` | Engine, pipeline, Node runner, Goja fallback, hot-reload |
| `internal/dashboard/` | Dashboard server + embedded SPA |
| `internal/storage/` | Memory, postgres, s3, composite stores |
| `internal/connector/` | Sources & destinations (HTTP, TCP, Kafka, file, DB, FHIR, etc.) |
| `internal/bootstrap/` | Project/channel scaffolding |
| `internal/auth/` | OIDC, LDAP, basic auth, RBAC, audit |
| `internal/cluster/` | Redis coordinator, dedup, health |
| `internal/retry/` | Retry queues, DLQ |
| `internal/observability/` | Prometheus, OpenTelemetry |
| `internal/datatype/` | HL7v2, FHIR, X12, CCDA, JSON, XML, CSV, binary |
| `pkg/config/` | YAML config, channel config, validation |
| `pkg/logging/` | Structured logging, transports |
| `npm/` | `intu-dev` Node runtime |

## Quick demo

```bash
go run . init demo-project --dir /tmp
cd /tmp/demo-project
go run <path-to-intu> c my-channel --dir .
go run <path-to-intu> validate --dir .
go run <path-to-intu> serve --dir .
```

## Notes

- Dashboard: port 3000, basic auth `admin`/`admin` in dev.
- JS runtime: Node (default); Goja fallback.
- HTTP sources on same port use shared listener with path-based routing; duplicate port+path caught at validate.
