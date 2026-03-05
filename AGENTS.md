# AGENTS.md

## Cursor Cloud specific instructions

**Product**: `intu` is a Go CLI tool for healthcare interoperability. It scaffolds projects, manages channels, validates config, and compiles TypeScript transformers. There is no long-running server (`intu serve` is not yet implemented).

**Prerequisites**: Go 1.22+ and Node.js >= 18 (with npm). No external services (Kafka, PostgreSQL) are needed for development/testing.

**Common commands** (see `README.md` for full reference):

| Task | Command |
|------|---------|
| Build binary | `go build -o intu .` |
| Run tests | `go test ./... -v` |
| Lint | `go vet ./...` |
| Run from source | `go run . <command> [flags]` |

**End-to-end demo workflow** (useful to verify the CLI works after changes):
```bash
go run . init demo-project --dir /tmp
cd /tmp/demo-project && npm install
go run /workspace/. c my-channel --dir .
go run /workspace/. validate --dir .
go run /workspace/. build --dir .
```

**Notes**:
- The `intu build` command runs `npm run build` (which invokes `tsc`) inside the scaffolded project, so Node.js must be available.
- Tests live only in `internal/bootstrap/scaffolder_test.go`; they are pure Go tests with no external dependencies.
- The `--dir` flag on most commands sets the working directory for the project (defaults to the current directory).
