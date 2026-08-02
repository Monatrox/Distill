# Project Plan - RabbitMQ HotLog Aggregator

## Executive Summary

Build a Rust-based hot-path logging aggregator that provides per-channel autonomy, dynamic channel creation without redeployment, and intelligent aggregation for memory-efficient log storage.

---

## Phase 1: Foundation (Weeks 1-2)

### Goals
- Establish project structure and dependencies
- Implement core data structures
- Build prototype ring buffer system

### Deliverables

#### Week 1: Project Setup & Core Types
```
⏳ Initialize Rust project with Cargo.toml
   - Dependencies: tokio, serde, prost, prost-reflect, tracing, mimalloc, PostgreSQL client, and bincode only if benchmarks justify it
   - Add clippy, fmt checks to CI config
   
⏳ Define core message structures
   - LogEvent protobuf structs
   - SeverityLevel enum
   - AggregatedLog response types
   
⏳ Implement Timestamp utilities
   - Event timestamp type backed by UTC wall-clock time
   - `Instant`-based elapsed-duration helpers
   - Stable event and aggregate identifiers for idempotent delivery
   
⏳ Create basic configuration parser
   - ChannelDefinition JSON deserialization
   - Validation of required fields
   - Default value application for optional fields
```

#### Week 2: Ring Buffer Implementation
```
⏳ Implement channel-specific ring buffers
   - Safe owner-local `VecDeque` buffer
   - Explicit byte accounting before allocation
   - Bounded channel mailbox and process-wide memory budget
   
⏳ Build aggregation accumulator types
   - AverageAccumulator with overflow protection
   - MaxAccumulator (f64::MAX initialization)
   - MinAccumulator (f64::NEG_INFINITY initialization)
   - SumAccumulator with saturating arithmetic
   - CountAccumulator (owned `u64`)
   
⏳ Implement window trigger logic
   - Time-based flushing (> 3600s since last flush)
   - Size-based flushing (> 50MB buffered)
   - Event-count based flushing (per N events)
```

### Success Criteria
- [ ] Ring buffer achieves <1µs push latency at 10k ops/sec
- [ ] Memory usage bounded to configured limits per channel
- [ ] No data loss during rapid burst ingestion

---

## Phase 2: Hot Path Engine (Weeks 3-5)

### Goals
- Implement severity-based routing logic
- Build dynamic channel management layer
- Integrate Protobuf schema generation

### Deliverables

#### Week 3: Routing Engine
```
⏳ Implement threshold comparison logic
   - SeverityLevel enum ordering/serialization
   - Per-channel threshold lookup (hash map)
   
⏳ Build direct-persist path
   - Serialize to Protobuf or JSON based on config
   - Create bounded RabbitMQ delivery path with retry logic
   - Handle connection failures with backoff
   
⏳ Implement no-aggregator immediate-persist path
   - Immediate persistence when no aggregator is configured; no event dropping
   - Optional metrics logging
```

#### Week 4: Channel Management
```
⏳ Build channel registry
   - Concurrent channel registry with bounded admission
   - In-memory cache with eviction policy (LRU, max entries)
   
⏳ Implement dynamic channel creation
   - Parse /channels/{id} PUT requests
   - Validate channel_definition JSON
   - Generate unique queue/exchange names
     (channel_prefix_rabbit_{timestamp}_{hash})
   
⏳ Build schema registry integration
   - Cache runtime Protobuf descriptors per message type with `prost-reflect`
   - LRU eviction beyond 100MB limit
   
⏳ Implement channel hot-reload mechanism
   - File-watch for config reloads (inotify on Linux, fsnotify on Windows)
   - Drain and durably hand off affected channels before applying new configuration
```

#### Week 5: Aggregation Engine Core
```
⏳ Build time-window aggregation handler
   - Monitor buffer timestamps
   - Trigger flush when window expires
   
⏳ Implement event-count aggregation
   - Track entry count per window
   - Flush when N events collected
   
⏳ Build continuous aggregation mode
   - Stream aggregate without window boundaries
   - Persist individual aggregates on threshold breach
   
⏳ Implement aggregation result serialization
   - Compute final aggregate values
   - Generate AggregatedLog protobuf message
```

### Success Criteria
- [ ] Hot path processes 10k+ events/sec per core
- [ ] Dynamic channel creation takes <500ms p99
- [ ] Schema generation and validation completes in <100ms

---

## Phase 3: Infrastructure (Weeks 6-7)

### Goals
- Build HTTP ingest interface
- Implement metrics/monitoring endpoints
- Add error handling and recovery

### Deliverables

#### Week 6: HTTP Interface
```
⏳ Implement /ingest POST endpoint
   - Parse JSON array of log events
   - Parse protobuf-encoded payload (optional)
   - Route each event to appropriate channel
   
⏳ Build /channels/{id}/config GET endpoint
   - Return current channel configuration
   - Handle missing channels with 404
   
⏳ Implement /channels/{id} PUT endpoint
   - Create new channel or update existing
   - Validate against JSON schema
   - Trigger dynamic queue/exchange creation
   
⏳ Add /metrics GET endpoint
   - Buffer utilization per channel (percentage)
   - Events processed/sec (rate)
   - Errors count and rate
   - RabbitMQ connection health
```

#### Week 7: Error Handling & Recovery
```
⏳ Implement error types and propagation
   - ParseError, ChannelNotFound, SchemaValidationError
   - BufferFull, PersistError, AggregationOverflow
   
⏳ Build retry logic for sink failures
   - Exponential backoff (1s → 60s max)
   - Dead-letter queue fallback after max retries
   
⏳ Implement RabbitMQ and PostgreSQL connection management
   - Bounded RabbitMQ and PostgreSQL resources shared by channel actors
   - Automatic reconnection on failure
   
⏳ Add metrics/alerting hooks
   - Export Prometheus-compatible metrics
   - Alert thresholds for buffer utilization >80%
```

### Success Criteria
- [ ] Ingest endpoint handles mixed JSON/protobuf payloads
- [ ] System recovers from RabbitMQ or PostgreSQL disconnection without duplicate sink records or data loss
- [ ] Metrics accurately reflect system health in real-time

---

## Phase 4: Testing & Validation (Weeks 8-9)

### Goals
- Build comprehensive test suite
- Validate edge cases and error conditions
- Performance benchmarking

### Deliverables

#### Week 8: Unit Tests
```
⏳ Core data structure tests
   - Ring buffer push/pop correctness
   - Accumulator overflow behavior
   - Window trigger logic
   
⏳ Routing engine tests
   - Severity threshold comparisons
   - Direct-persist vs aggregation paths
   
⏳ Channel management tests
   - Dynamic creation and deletion
   - Schema generation validation
```

#### Week 9: Integration & Load Tests
```
⏳ End-to-end ingestion test
   - Generate 1M synthetic events
   - Verify routing correctness
   - Validate aggregation results
   
⏳ Performance benchmarks
   - Throughput at various core counts (4, 8, 16 cores)
   - Latency percentiles (p50, p95, p99)
   - Memory usage over time
   
⏳ Failure scenario tests
   - RabbitMQ connection drops
   - Buffer exhaustion handling
   - Schema validation failures
```

### Success Criteria
- [ ] All unit tests pass (100% coverage on hot path logic)
- [ ] System sustains >50k events/sec with 8 cores
- [ ] p99 latency <1ms under load
- [ ] Graceful degradation during failures

---

## Phase 5: Documentation & Release (Weeks 10-11)

### Goals
- Document architecture and usage
- Prepare deployment artifacts
- Release v0.1.0

### Deliverables

#### Week 10: Documentation
```
⏳ Architecture documentation (this document)
⏳ API reference (endpoints, request/response formats)
⏳ Configuration guide (channel config examples)
⏳ Deployment guide (startup/shutdown procedures)
⏳ Troubleshooting guide (common issues and resolutions)
```

#### Week 11: Release Preparation
```
⏳ Generate release binaries (Linux/Windows/macOS)
⏳ Create Dockerfile for containerized deployment
⏳ Prepare CHANGELOG.md
⏳ Tag v0.1.0 release on Git repository
⏳ Publish README.md with quickstart guide
```

### Success Criteria
- [ ] All documentation complete and reviewed
- [ ] Release artifacts available for distribution
- [ ] First user deployment successful

---

## Risk Mitigation

| Risk | Probability | Impact | Mitigation Strategy |
|------|------------|--------|---------------------|
| RabbitMQ connection instability | High | Medium | Connection pooling + exponential backoff |
| Buffer exhaustion under burst load | Medium | High | Metrics monitoring + operator alerts |
| Schema generation latency spikes | Low | Low | Cache warmup on startup + async schema loading |
| Memory leak in long-running process | Medium | High | Fuzz testing + memory profiling (perf/valgrind) |

---

## Milestone Timeline

```
Week 1-2: Foundation      [            ] Not started
Week 3-5: Hot Path Engine [            ] Planned
Week 6-7: Infrastructure  [            ] Planned
Week 8-9: Testing         [            ] Planned
Week 10-11: Release       [            ] Planned

Total Duration: ~11 weeks (2.75 months)
```

---

## Dependencies & Prerequisites

### External Services
- **RabbitMQ 3.11+** - Message broker for buffering, routing, retries, and DLQ handling
- **PostgreSQL 15+** - v1 persistence sink for channel logs and aggregates
- **JSON Schema Validator** - Config validation (serde_json_schema crate)
- **Protobuf runtime descriptors** - `prost` and `prost-reflect`; no runtime `protoc` process in v1

### Infrastructure Requirements
- **CPU**: Minimum 4 cores recommended, 8+ for production
- **Memory**: 512 MiB initial process budget; 50 MiB default per-channel aggregation buffer
- **Disk**: 10GB for application binaries + config storage
- **Network**: Stable connection to RabbitMQ cluster

---

## Appendix: Glossary

| Term | Definition |
|------|------------|
| **Channel** | Logical partition for a log category (e.g., "app.production.api") |
| **Severity Threshold** | Minimum severity level that triggers direct persistence |
| **Aggregator** | Function that computes summaries from buffered logs (average, max, min, sum) |
| **Granularity** | Aggregation window definition (per N events, per T time, or overall stream) |
| **Ring Buffer** | Fixed-size circular buffer for temporary log storage before aggregation |
| **Hot Path** | Core message processing pipeline requiring maximum throughput and low latency |
| **Dynamic Provisioning** | Creating queues/exchanges at runtime without code changes or restarts |
