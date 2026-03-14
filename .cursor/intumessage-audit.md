# IntuMessage Structure Audit

Internal gap analysis and implementation plan. Audited against the product vision:
IntuMessage is the universal envelope wrapping DATA through all pipeline stages.
It must support elegant Go/TypeScript conversion, faithful import/export/replay,
encoding preservation, transport awareness, and future plugin extensibility.

---

## Gap Analysis

### GAP-1: Two IntuMessage JSON Shapes (High)

**What:** `buildIntuMessage()` (pipeline path) and `ToIntuJSON()` (storage path) produce
structurally different JSON for the same message. Pipeline body is parsed data; storage
body is raw string or base64. Neither includes `sourceCharset` or the generic `metadata` map.

**Impact:** "IntuMessage" means different things depending on context. Transformers cannot
access `sourceCharset` or custom metadata set by source connectors. Storage envelope is
incomplete for faithful reconstruction.

**Fix:** Unify the shapes. Add `sourceCharset` and `metadata` to both serialization paths.
The body type difference (parsed vs raw) is intentional — document it as "pipeline form"
vs "storage form".

**Files:** `internal/runtime/pipeline.go`, `internal/message/message.go`

### GAP-2: SourceCharset Missing from Pipeline IntuMessage (High)

**What:** `msg.SourceCharset` is set by source connectors (e.g. HTTP extracts charset from
Content-Type header) but is NOT included in the `map[string]any` passed to TypeScript
transformers. The TypeScript `IntuMessage` interface has no `sourceCharset` field.

**Impact:** Transformers processing multi-encoding healthcare data (common with HL7v2 from
legacy systems sending ISO-8859-1 or Windows-1252) cannot determine the original encoding.
Breaks the vision of IntuMessage as the sole data carrier.

**Fix:** Add `sourceCharset` to `buildIntuMessage()`, `ToIntuJSON()`, `FromIntuJSON()`,
and the TypeScript `IntuMessage` interface in `templates.go`.

**Files:** `internal/runtime/pipeline.go`, `internal/message/message.go`, `internal/bootstrap/templates.go`

### GAP-3: Metadata Map Missing from Pipeline IntuMessage (High)

**What:** Sources set custom metadata (e.g. `metadata["filename"]` from file source,
`metadata["reprocessed"]` from reprocessing). The `Metadata` map is NOT included in
`buildIntuMessage()` output.

**Impact:** TypeScript transformers cannot access connector-set metadata. Custom metadata
set during preprocessing is invisible to downstream stages. Severely limits metadata
usefulness and blocks plugin extensibility vision.

**Fix:** Add `metadata` to both pipeline and storage IntuMessage serialization. Add to
TypeScript interface as `metadata?: Record<string, unknown>`. Add to `parseIntuResult()`
for round-trip.

**Files:** `internal/runtime/pipeline.go`, `internal/message/message.go`, `internal/bootstrap/templates.go`

### GAP-4: ToIntuJSON() Does Not Round-Trip Identity Fields (High)

**What:** `ToIntuJSON()` does not include `id`, `correlationId`, `timestamp`, `channelId`,
or `sourceCharset`. `FromIntuJSON()` calls `message.New()` which generates fresh UUID and
timestamp. A round-trip produces a message with different identity.

**Impact:** Export and re-import of messages is lossy. Product vision requires "import,
export, replay IntuMessage should produce 100% predictable outcome". Only the CLI reprocess
path compensates by restoring `CorrelationID` from the `MessageRecord` wrapper.

**Fix:** Include `id`, `correlationId`, `timestamp`, `channelId`, `sourceCharset`, and
`metadata` in `ToIntuJSON()`. Update `FromIntuJSON()` to restore these fields instead of
generating new ones. Makes `.intuJSON` a fully self-contained portable envelope.

**Files:** `internal/message/message.go`

### GAP-5: Dashboard Reprocess Path Inconsistency (Medium)

**What:** CLI reprocess (`cmd/reprocess.go`) uses `FromIntuJSON()` to rebuild messages,
preserving transport metadata. Dashboard reprocess (`cmd/serve.go` callback) constructs
`message.New(channelID, rawContent)` directly, losing transport metadata.

**Impact:** Messages replayed via dashboard behave differently than CLI replay. Breaks
predictability guarantee.

**Fix:** Unify both paths to use `FromIntuJSON()`. Extract shared logic into a
`message.Rebuild()` function.

**Files:** `cmd/serve.go`, `cmd/reprocess.go`

### GAP-6: No Formal .intuJSON Specification (Medium)

**What:** No JSON schema, file extension convention, or version field for the IntuMessage
JSON envelope. Format exists only as Go code in `ToIntuJSON()` / `FromIntuJSON()`.

**Impact:** No forward/backward compatibility guarantees. External tools cannot validate
`.intuJSON` files. Plugin authors have no formal specification.

**Fix:** Add `version` field (e.g. `"version": "1"`) to envelope. Create JSON schema at
`docs/schema/intumessage.schema.json`. Define `.intuJSON` as canonical file extension.

**Files:** `internal/message/message.go`, `docs/schema/`

### GAP-7: No Plugin Interface for Custom Pipeline Stages (Medium, Future)

**What:** Pipeline stages are hard-coded in `pipeline.go`. No middleware chain or
registration mechanism for custom stages between listener and destination.

**Impact:** The vision of `Plugin(stage, process(IntuMessage))` cannot be realized. Custom
processing (audit logging, enrichment, compliance checks) must be embedded in transformers
rather than composed as independent stages.

**Fix:** Design a `PipelineStage` interface:

```go
type PipelineStage interface {
    Name() string
    Phase() Phase  // BeforeValidation, AfterTransform, BeforeDestination, etc.
    Process(ctx context.Context, msg *message.Message) (*message.Message, error)
}
```

Register via channel YAML `plugins:` block. Execute at declared phase. Requires explicit
`stage` field in `IntuContext`.

**Files:** `internal/runtime/pipeline.go`, `pkg/config/channel.go`

### GAP-8: Context Missing Explicit Stage Field (Medium)

**What:** `buildPipelineCtx()` does not include a `stage` field. Current stage is implicit
based on which pipeline function is called.

**Impact:** Reusable TypeScript modules can't introspect their execution context. Plugin
support requires stage awareness.

**Fix:** Add `stage?: string` to `IntuContext`. Set in each pipeline step: `preprocessor`,
`validator`, `source_filter`, `transformer`, `destination_filter`,
`destination_transformer`, `response_transformer`, `postprocessor`.

**Files:** `internal/runtime/pipeline.go`, `internal/bootstrap/templates.go`

### GAP-9: Redis Queue Uses Separate Serialization (Low)

**What:** `redisQueueItem` in `internal/retry/redis_queue.go` mirrors `message.Message`
field-by-field with JSON struct tags. Separate serialization path kept in manual sync.

**Impact:** Adding fields to `Message` (e.g. `sourceCharset`) requires updating the Redis
struct separately or data is lost during retry.

**Fix:** Replace `redisQueueItem` with `ToIntuJSON()` / `FromIntuJSON()` once GAP-4 is
resolved and the envelope includes identity fields.

**Files:** `internal/retry/redis_queue.go`

### GAP-10: Transformers Docs Incomplete (Low, Fixed)

**What:** Transformers documentation table listed HTTP, File, FTP, Kafka, TCP transport
metadata but omitted SMTP, DICOM, and Database.

**Status:** Fixed in this PR — added to `docs/documentation/transformers.html`.

---

## Implementation Plan

Each phase is independently shippable and backward-compatible.

### Phase 1: Envelope Completeness (High Priority) ✅ IMPLEMENTED

| #   | Task | Gaps | Files | Status |
|-----|------|------|-------|--------|
| 1.1 | Add `sourceCharset` and `metadata` to `buildIntuMessage()` | GAP-2, GAP-3 | `internal/runtime/pipeline.go` | ✅ |
| 1.2 | Add `sourceCharset` and `metadata` to `ToIntuJSON()` / `FromIntuJSON()` | GAP-1, GAP-2, GAP-3 | `internal/message/message.go` | ✅ |
| 1.3 | Add `id`, `correlationId`, `timestamp`, `channelId` to `ToIntuJSON()`; restore in `FromIntuJSON()` | GAP-4 | `internal/message/message.go` | ✅ |
| 1.4 | Update TypeScript `IntuMessage` interface: add `sourceCharset?` and `metadata?` | GAP-2, GAP-3 | `internal/bootstrap/templates.go` | ✅ |
| 1.5 | Add `metadata` to `parseIntuResult()` round-trip | GAP-3 | `internal/runtime/pipeline.go` | ✅ |
| 1.6 | Update tests for round-trip fidelity | GAP-1, GAP-4 | `internal/message/`, `internal/runtime/` | ✅ |

### Phase 2: Consistency & Specification (Medium Priority) ✅ IMPLEMENTED

| #   | Task | Gaps | Files | Status |
|-----|------|------|-------|--------|
| 2.1 | Unify dashboard reprocess to use `FromIntuJSON()` via `message.Rebuild()` | GAP-5 | `cmd/serve.go`, `internal/dashboard/server.go` | ✅ |
| 2.2 | Add `version` field to `.intuJSON` envelope | GAP-6 | `internal/message/message.go` | ✅ |
| 2.3 | Create `docs/schema/intumessage.schema.json` | GAP-6 | `docs/schema/` | ✅ |
| 2.4 | Add `stage` to `IntuContext`, set in each pipeline step | GAP-8 | `internal/runtime/pipeline.go`, `internal/bootstrap/templates.go` | ✅ |
| 2.5 | Replace `redisQueueItem` with `ToIntuJSON` / `FromIntuJSON` | GAP-9 | `internal/retry/redis_queue.go` | ✅ |

### Phase 3: Plugin Architecture ✅ IMPLEMENTED

| #   | Task | Gaps | Files | Status |
|-----|------|------|-------|--------|
| 3.1 | Design `PipelineStage` Go interface: `Name()`, `Phase()`, `Process()` | GAP-7 | `internal/runtime/plugin.go` | ✅ |
| 3.2 | Add plugin registration in channel YAML (`plugins:` block) | GAP-7 | `pkg/config/channel.go` | ✅ |
| 3.3 | Implement plugin execution loop in `Pipeline.Execute()` | GAP-7 | `internal/runtime/pipeline.go` | ✅ |
| 3.4 | Support TypeScript-based plugins via Node.js runner | GAP-7 | `internal/runtime/plugin.go` | ✅ |
| 3.5 | Publish plugin SDK types in `intu-dev` npm package | GAP-7 | `npm/types/intu.d.ts` | ✅ |
