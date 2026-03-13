# Intu End-to-End Testing Strategy

## Overview

This document defines a comprehensive, fully automated testing strategy for `intu` — covering every source/destination connector, all pipeline stages, and external dependency management. The goal is to give early adopters high confidence that the system works correctly in realistic scenarios.

## Test Pyramid

```
                    ┌─────────────────┐
                    │   E2E / Smoke   │  Full engine + real infra (testcontainers)
                    │   (minutes)     │  CI: on PR merge / nightly
                    ├─────────────────┤
                 ┌──┴─────────────────┴──┐
                 │   Integration Tests   │  Connector + real services
                 │   (seconds–minutes)   │  CI: on every PR
                 ├───────────────────────┤
              ┌──┴───────────────────────┴──┐
              │      Contract Tests         │  Interface compliance
              │      (milliseconds)         │  CI: on every push
              ├─────────────────────────────┤
           ┌──┴─────────────────────────────┴──┐
           │          Unit Tests               │  Pure logic, no I/O
           │          (milliseconds)           │  CI: on every push
           └───────────────────────────────────┘
```

### Layer Definitions

| Layer | Build Tag | Scope | External Services |
|-------|-----------|-------|-------------------|
| **Unit** | *(none)* | Pure functions, parsers, config validation, in-memory stores | None |
| **Contract** | *(none)* | Interface compliance — every connector implements `SourceConnector`/`DestinationConnector` correctly | None (stubs/mocks) |
| **Integration** | `integration` | Single connector against a real service via testcontainers | Kafka, PostgreSQL, SFTP, SMTP (Mailhog) |
| **E2E** | `e2e` | Full engine pipeline: source → transformer → destination with real services | Multiple containers orchestrated together |

## Connector Coverage Matrix

### Sources

| Source | Unit Tests | Contract Tests | Integration Tests | External Dependency |
|--------|-----------|----------------|-------------------|---------------------|
| HTTP | ✅ existing | ✅ new | ✅ existing e2e | `httptest` (stdlib) |
| TCP/MLLP | ✅ existing | ✅ new | ✅ existing e2e | `net.Listen` (stdlib) |
| File | ✅ existing | ✅ new | ✅ existing e2e | `os.TempDir` (stdlib) |
| Channel | ✅ existing | ✅ new | ✅ existing e2e | In-process bus |
| SFTP | ⬜ | ✅ new | ✅ **new** | `atmoz/sftp` container |
| Kafka | ⬜ | ✅ new | ✅ **new** | `confluentinc/cp-kafka` container |
| Database | ⬜ | ✅ new | ✅ **new** | `postgres:16-alpine` container |
| Email | ⬜ | ✅ new | ✅ **new** | `mailhog/mailhog` container |
| DICOM | ✅ existing | ✅ new | ✅ existing | Custom TCP protocol |
| SOAP | ✅ existing | ✅ new | ✅ existing | `httptest` (stdlib) |
| FHIR | ✅ existing | ✅ new | ✅ existing | `httptest` (stdlib) |
| IHE | ✅ existing | ✅ new | ✅ existing | `httptest` (stdlib) |

### Destinations

| Destination | Unit Tests | Contract Tests | Integration Tests | External Dependency |
|-------------|-----------|----------------|-------------------|---------------------|
| HTTP | ✅ existing | ✅ new | ✅ existing e2e | `httptest` (stdlib) |
| TCP | ✅ existing | ✅ new | ✅ existing e2e | `net.Listen` (stdlib) |
| File | ✅ existing | ✅ new | ✅ existing e2e | `os.TempDir` (stdlib) |
| Channel | ✅ existing | ✅ new | ✅ existing e2e | In-process bus |
| Kafka | ⬜ | ✅ new | ✅ **new** | `confluentinc/cp-kafka` container |
| Database | ⬜ | ✅ new | ✅ **new** | `postgres:16-alpine` container |
| SFTP | ⬜ | ✅ new | ✅ **new** | `atmoz/sftp` container |
| SMTP | ⬜ | ✅ new | ✅ **new** | `mailhog/mailhog` container |
| FHIR | ✅ existing | ✅ new | ✅ existing | `httptest` (stdlib) |
| DICOM | ✅ existing | ✅ new | ✅ existing | Custom TCP protocol |
| JMS | ✅ existing | ✅ new | ⬜ (no OSS JMS broker) | — |
| Direct (email) | ⬜ | ✅ new | ⬜ | — |
| Log | ✅ existing | ✅ new | — (no side effects) | — |

## External Dependencies via Testcontainers

All integration/e2e tests that need external services use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up real Docker containers. No manual infrastructure setup is needed.

### Container Specifications

| Service | Image | Purpose | Port(s) |
|---------|-------|---------|---------|
| **Kafka** | `confluentinc/cp-kafka:7.6.0` (KRaft) | Kafka source/dest integration | 9092 (PLAINTEXT) |
| **PostgreSQL** | `postgres:16-alpine` | Database source/dest integration | 5432 |
| **SFTP** | `atmoz/sftp:alpine` | SFTP source/dest integration | 22 |
| **MailHog** | `mailhog/mailhog:latest` | SMTP dest + Email source integration | 1025 (SMTP), 8025 (API) |

### Testcontainer Lifecycle

```go
// Shared across all integration tests in a package via TestMain
func TestMain(m *testing.M) {
    ctx := context.Background()
    
    // Start containers once, reuse across tests
    kafkaC, _ := StartKafkaContainer(ctx)
    pgC, _ := StartPostgresContainer(ctx)
    sftpC, _ := StartSFTPContainer(ctx)
    
    code := m.Run()
    
    // Cleanup
    kafkaC.Terminate(ctx)
    pgC.Terminate(ctx)
    sftpC.Terminate(ctx)
    
    os.Exit(code)
}
```

## Test Organization

```
internal/
├── connector/
│   ├── connector_test.go          # Unit tests (existing)
│   ├── destination_test.go        # Unit tests (existing)
│   └── contract_test.go           # Contract tests (new)
├── runtime/
│   ├── noderunner_test.go         # Unit tests (existing)
│   └── e2e_test.go                # In-process E2E tests (existing)
└── integration/                   # NEW: integration test package
    ├── testutil/
    │   ├── containers.go          # Testcontainer helpers (Kafka, PG, SFTP, etc.)
    │   ├── helpers.go             # Shared test utilities
    │   └── fixtures.go            # HL7, FHIR, JSON test fixtures
    ├── kafka_test.go              # Kafka source + dest integration
    ├── sftp_test.go               # SFTP source + dest integration
    ├── database_test.go           # PostgreSQL source + dest integration
    ├── smtp_test.go               # SMTP dest + Email source integration
    ├── pipeline_test.go           # Full pipeline E2E with real services
    └── engine_test.go             # Engine-level E2E with testcontainers
```

## Recommended Libraries

| Library | Purpose | Why |
|---------|---------|-----|
| `testing` (stdlib) | Test framework | Already used throughout; no extra dependency needed |
| `net/http/httptest` | HTTP mock servers | Already used; perfect for HTTP/FHIR/SOAP connectors |
| `testcontainers-go` | Docker container lifecycle | Industry standard; CI-friendly; no manual infra |
| `github.com/stretchr/testify` | Assertions + require | Cleaner assertions; `require` for fatal checks |

## Example Test Flows

### Flow 1: Kafka Source → HL7 Transform → FHIR HTTP Dest

```
┌─────────────────┐    ┌───────────────┐    ┌──────────────────┐    ┌──────────────┐
│ Test publishes   │───>│ Kafka Source  │───>│ HL7→FHIR         │───>│ FHIR httptest│
│ HL7 to Kafka     │    │ (container)   │    │ Transformer (JS) │    │ Server       │
└─────────────────┘    └───────────────┘    └──────────────────┘    └──────┬───────┘
                                                                           │
                                                                   Assert FHIR Bundle
                                                                   contains Patient
```

**Steps:**
1. Start Kafka container via testcontainers
2. Create topic `hl7-inbound`
3. Start httptest server capturing requests
4. Configure channel: Kafka source → HL7 parser → JS transformer → HTTP dest
5. Build and start ChannelRuntime
6. Produce HL7 ADT^A01 message to Kafka topic
7. Wait for capture server to receive FHIR Bundle
8. Assert: Bundle contains Patient resource with correct demographics

### Flow 2: SFTP Source → Validate → Transform → Database Dest

```
┌─────────────────┐    ┌───────────────┐    ┌──────────────────┐    ┌──────────────┐
│ Test writes file │───>│ SFTP Source   │───>│ Validate + Trans │───>│ PostgreSQL   │
│ to SFTP server   │    │ (container)   │    │ (Node.js)        │    │ (container)  │
└─────────────────┘    └───────────────┘    └──────────────────┘    └──────┬───────┘
                                                                           │
                                                                   Assert row in DB
                                                                   with correct data
```

### Flow 3: HTTP Source → Multi-Dest (Kafka + File + SMTP)

```
                                            ┌──────────────┐
                                       ┌───>│ Kafka Dest   │──> Assert message on topic
┌─────────────────┐    ┌────────┐     │    │ (container)  │
│ HTTP POST       │───>│ Engine │─────┤    └──────────────┘
│ JSON payload    │    │        │     │    ┌──────────────┐
└─────────────────┘    └────────┘     ├───>│ File Dest    │──> Assert file on disk
                                      │    └──────────────┘
                                      │    ┌──────────────┐
                                      └───>│ SMTP Dest    │──> Assert email via MailHog API
                                           │ (container)  │
                                           └──────────────┘
```

## CI/CD Pipeline Design

### GitHub Actions Workflow: `ci.yml` (updated)

```yaml
# Triggers: push to main, all PRs
jobs:
  unit-and-contract:
    # Fast: runs on every push
    - go vet ./...
    - go test ./... -v -race -count=1  # unit + contract tests

  integration:
    # Requires Docker: runs on PRs to main
    needs: unit-and-contract
    services: []  # testcontainers manage their own containers
    - go test ./internal/integration/... -v -race -tags=integration -timeout=10m

  build:
    # Cross-platform binary build
    needs: unit-and-contract
    strategy:
      matrix:
        goos: [linux, darwin, windows]
        goarch: [amd64, arm64]
```

### GitHub Actions Workflow: `integration.yml` (new)

```yaml
# Dedicated integration test workflow
# Triggers: PR to main, nightly schedule
# Uses: ubuntu-latest with Docker pre-installed

jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - checkout
      - setup-go
      - setup-node (for Node.js runner)
      - npm install -g intu-dev
      - go test ./internal/integration/... -v -race -tags=integration -timeout=10m
```

### Pipeline Trigger Summary

| Event | Unit + Contract | Integration | Build |
|-------|----------------|-------------|-------|
| Push to any branch | ✅ | ❌ | ❌ |
| PR to `main` | ✅ | ✅ | ✅ |
| Merge to `main` | ✅ | ✅ | ✅ |
| Nightly (cron) | ✅ | ✅ | ❌ |
| Release tag `v*` | — | — | ✅ (release workflow) |

## Build Tags

```go
//go:build integration

package integration
```

- **No tag**: unit tests + contract tests — run everywhere, always fast
- **`integration`**: tests that require Docker (testcontainers) — skipped in quick local runs
- **`e2e`**: full engine integration tests — run in CI or explicitly

Run locally:
```bash
# Fast: unit + contract only
go test ./... -v

# Full: includes integration tests (needs Docker)
go test ./... -v -tags=integration -timeout=10m

# Specific package
go test ./internal/integration/... -v -tags=integration
```

## Test Data Fixtures

### HL7v2 ADT^A01

```
MSH|^~\&|LABSYS|HOSPITAL|FHIRSYS|CLOUD|20230615120000||ADT^A01|MSG001|P|2.5
PID|1||MRN12345||Smith^Jane^M||19850301|F|||123 Main St^^Springfield^IL^62704
PV1|1|I|ICU^101^A|E|||1234^Jones^Robert^^^Dr||||||||||||||V001
```

### FHIR R4 Patient

```json
{
  "resourceType": "Patient",
  "identifier": [{"system": "urn:oid:2.16.840.1.113883.2.1", "value": "MRN12345"}],
  "name": [{"family": "Smith", "given": ["Jane", "M"]}],
  "gender": "female",
  "birthDate": "1985-03-01"
}
```

### JSON Registration

```json
{
  "patientId": "P001",
  "firstName": "Alice",
  "lastName": "Brown",
  "dob": "1990-05-15",
  "mrn": "MRN-001"
}
```

## Maintenance

- **Adding a new connector**: Add entries to the coverage matrix, create contract test, create integration test if external service is needed
- **Adding a new pipeline stage**: Add unit test for the stage logic, extend e2e tests to exercise the stage
- **Updating container versions**: Change image tags in `testutil/containers.go`
- **Local development**: Run `go test ./...` for fast feedback; run with `-tags=integration` when testing connector changes
