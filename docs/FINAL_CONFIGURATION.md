# Final Configuration Decisions - Distill HotLog Aggregator

**Date**: 2026-08-01  
**Status**: All Decisions CONFIRMED ✅  
**Version**: 0.2.0 (Final Before Programming)

---

## Q&A Summary

### Environment & Infrastructure

| # | Question | Answer |
|---|----------|--------|
| 1 | Rust toolchain version | **stable 1.97.1** ✅ |
| 2 | Protobuf support strategy | TBD - will choose free+fast option (`prost` + `tonic` preferred if both criteria met) |
| 3 | Docker setup | **Not scaffolded yet**. Will scaffold docker-compose.yml for RabbitMQ, PostgreSQL, and the aggregator service. |
| 4 | External datastore (testing) | **Local PostgreSQL in Docker container** during testing; PostgreSQL is the v1 persistence sink ✅ |

### Configuration

| # | Question | Answer |
|---|----------|--------|
| 5 | Config storage format | **JSON files** ✅ |
| 6 | Default values | Start with documented defaults (50MB buffer, 3600s TTL), make all configurable via `ChannelDefinition` struct fields. |

### Dependencies & Libraries

| # | Question | Answer |
|---|----------|--------|
| 7 | HTTP framework preference | **Developer experience priority** → likely `Axum` or `Actix-web` (will benchmark DX) |
| 8 | Error handling | Typed domain/API errors with `thiserror`; `anyhow` is restricted to binary startup and orchestration boundaries |
| 9 | Async runtime | **tokio** (default) ✅ |

### Testing & Validation

| # | Question | Answer |
|---|----------|--------|
| 10 | Test coverage priority | **Integration tests first**, unit tests secondary ✅ |
| 11 | Mocking strategy | **mockall** for free mocking capability ✅ |

### Implementation Details

| # | Question | Answer |
|---|----------|--------|
| 12 | Concurrency model | **One Tokio runtime with one bounded channel actor/task per channel**; shared worker pools remain a future optimization ✅ |
| 13 | Schema registry cache | LRU cache with configurable limit (default 100MB as documented), using runtime descriptors |
| 14 | Metrics format | **Both**: Prometheus endpoint + OpenTelemetry tracing/exporter ✅ |

### Project Identity

| # | Question | Answer |
|---|----------|--------|
| 15 | Project name | **Distill** (internal project name) ✅ |
| 16 | Timeline constraints | **None** - implement as fast as possible ✅ |

| 17 | Persistence model | **User-provided sink connection string per channel**; PostgreSQL is the v1 adapter, with a future pluggable sink system for MSSQL, MySQL, SQLite, Seq, and others ✅ |
| 18 | Runtime schemas | **`prost` plus `prost-reflect` descriptors**; no runtime `protoc` process in v1 ✅ |
| 19 | Memory model | **Safe bounded queues, 50 MiB default per channel, 512 MiB default process budget, proactive spill/backpressure** ✅ |

---

## Additional Decisions & Findings

### Key Architecture Decisions

#### Channel Actor Model
- **Decision**: Run one bounded channel actor/task per channel on a shared Tokio runtime.
- **Rationale**: Each channel retains isolated ownership of its state without creating an OS thread, runtime, or database pool per channel.
- **Trade-off**: The registry and mailbox must enforce bounded channel count, queue depth, and memory usage.
- **Fallback plan**: Move channel work to a shared worker pool only after profiling demonstrates a scaling limit.

#### Error Handling Strategy
- **Domain/API errors**: Use `thiserror` enums with stable error codes and HTTP mappings.
- **Application boundary**: Use `anyhow` only in `main` and startup/orchestration code.
- **Pattern**: Do not expose `anyhow` from domain modules or HTTP handlers.

#### Metrics & Observability Stack
- **Primary**: Prometheus metrics endpoint (industry standard, Grafana-ready)
- **Secondary**: OpenTelemetry for distributed tracing and structured logs
- **Exporter**: `opentelemetry-prometheus` bridge if needed
- **Logging**: Use structured `tracing` events with named fields and redaction for secrets, payloads, and connection strings.

#### Runtime and Quality Defaults

- Use `mimalloc` as the global allocator for the application binary.
- Enable the compiler and Clippy lint sets required by the `ms-rust` skill.
- Avoid `unsafe` in v1. Any future unsafe code requires a documented safety argument, benchmarks where performance-motivated, and Miri coverage.

#### Config Storage
- **Format**: JSON files in `/config` directory
- **Reload strategy**: Watch for file changes or REST API reload endpoint
- **Validation**: Load config once at startup, validate against schema

### Documentation Requirements

Following M-CANONICAL-DOCS format:
1. Summary sentence (< 15 words)
2. Extended documentation
3. Examples section
4. Errors section
5. Panics section (if applicable)
6. Safety section (for unsafe blocks)
7. Abort section (for critical failures)

### Rust Guideline Compliance

Before any `.rs` file modifications:
1. Load applicable guideline from `.agents/skills/ms-rust-skill-main/`:
   - `01_ai_guidelines.md` - AI/LLM code generation
   - `02_application_guidelines.md` - CLI/desktop apps, performance
   - `03_documentation.md` - API docs
   - `04_ffi_guidelines.md` - FFI boundaries
   - `05_library_guidelines.md` - Library design
   - `06_performance_guidelines.md` - Hot paths
   - `07_safety_guidelines.md` - Unsafe code
   - `08_universal_guidelines.md` - General best practices
   - etc.

2. Apply relevant rules from loaded guidelines

3. Add compliance header: `// Rust guideline compliant 2026-02-21` (after any license headers)

### Next Steps

1. ✅ **Document configuration decisions** (this document)
2. ⏳ **Initialize Cargo project** with dependencies
3. ⏳ **Create project structure** per modular monolith principles
4. ⏳ **Implement core data structures** (LogEvent, ChannelDefinition, etc.)
5. ⏳ **Build ring buffer system** for aggregation windows
6. ⏳ **Implement hot path engine** (routing + persistence)
7. ⏳ **Add channel management** layer (dynamic creation)
8. ⏳ **Implement HTTP interface** (ingest, metrics, channels endpoints)
9. ⏳ **Add error handling & recovery** logic
10. ⏳ **Write integration tests** (priority)

---

## Implementation Checklist

### Phase 1: Foundation

- [ ] Initialize Cargo.toml with dependencies:
  - `tokio` runtime
  - `serde`/`serde_json` serialization
  - `axum` or `actix-web` HTTP framework
  - `prost` and `prost-reflect` for Protobuf descriptors and runtime validation
  - `thiserror` error types
  - `anyhow` internal errors
  - `mockall` mocking
  - `opentelemetry` + `opentelemetry-prometheus` metrics
  - PostgreSQL client/connection pool for the v1 sink
  - `mimalloc` application allocator
  - `tracing` and OpenTelemetry instrumentation

- [ ] Create project structure:
  ```
  src/
  ├── main.rs
  ├── hot_path/          # Message routing engine
  │   ├── mod.rs
  │   ├── router.rs
  │   └── persistence.rs
  ├── channel_manager/   # Dynamic channel management
  │   ├── mod.rs
  │   └── registry.rs
  ├── aggregation/       # Rolling aggregate computation
  │   ├── mod.rs
  │   ├── buffer.rs
  │   └── accumulator.rs
  ├── schema_registry/   # Protobuf schema generation
  │   ├── mod.rs
  │   └── cache.rs
  ├── errors.rs          # Error types
  ├── config/            # Configuration parsing
  │   ├── mod.rs
  │   └── channel.rs
  └── metrics/           # Metrics and tracing hooks
      ├── mod.rs
      └── prometheus.rs
  ```

- [ ] Define core protobuf messages:
  - `LogEvent`
  - `AggregatedLog`
  - `SeverityLevel` enum
  - `ChannelDefinition` struct
  - `MessageSchemaDef` struct

### Phase 2: Core Components

- [ ] Implement a safe, owner-local `VecDeque` buffer with byte accounting
- [ ] Implement aggregation accumulators (average, max, min, sum, count)
- [ ] Build severity threshold routing logic
- [ ] Create channel registry with LRU eviction

### Phase 3: Infrastructure

- [ ] HTTP ingest endpoint (`/ingest` POST)
- [ ] Channel management endpoints (PUT/GET `/channels/{id}`)
- [ ] Metrics endpoint (`/metrics` GET)
- [ ] Error response handlers

### Phase 4: Testing

- [ ] Integration tests for full data paths
- [ ] Test RabbitMQ buffering, routing, retry, and DLQ behavior
- [ ] Mock or isolate the PostgreSQL persistence sink in integration tests
- [ ] Load testing for hot path performance

---

## Sign-off

**Decisions documented**: ✅  
**Next action**: Initialize Cargo project structure  
**Confidence**: High - all critical questions answered
