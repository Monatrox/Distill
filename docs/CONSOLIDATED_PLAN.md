# Final Planning Set - RabbitMQ HotLog Aggregator

**Status**: All Decisions CONFIRMED ✅  
**Version**: 0.1.0 (Final Before Programming)  
**Last Updated**: 2026-08-01  

---

## Executive Summary

This consolidated planning document captures the complete design specification for building a Rust-based hot-path logging aggregator with:
- **Dynamic channel creation** without redeployment
- **Per-channel autonomy** (thresholds, destinations, aggregation rules)
- **Intelligent rolling aggregates** with configurable ordering modes
- **Memory-safe buffering** with safe owner-local queues, bounded per-channel buffers, and a process-wide v1 budget

### All Decisions Confirmed ✅

| Decision | What | Why |
|----------|------|-----|
| #1: Data Loss Policy | Rollup merge first, disk persist as worst-case fallback | Preserve aggregation semantics, avoid data loss |
| #2: Dead-Letter Queue Ownership | Auto-retry to different exchange + Rust service re-routing | Maintain data consistency without external dependencies |
| #3: Message Ordering Guarantees | Hybrid approach (all events require timestamps, configurable per-channel modes) | Balance throughput with data integrity where needed |
| #4: Authentication Mechanism | API keys in HTTP headers (`X-API-Key`) | Simple REST standard, easy to test, future-proof |
| #5: Schema Evolution Handling | Reject immediately on schema mismatch (operational concern) | Hot-path simplicity, forces immediate operator attention |
| #6: Observability Stack | Prometheus + Grafana | Industry standard, free, works standalone or integrated |

---

## Architecture Overview

### Core Philosophy

A dynamic, hot-path logging aggregator built in Rust where different log categories ("channels") have independent storage semantics without code changes or redeployment.

### System Boundaries

**External Interfaces:**
- **Ingest Endpoint**: Accepts structured log events (JSON/Protobuf) with API key authentication
- **RabbitMQ Cluster**: Message broker for buffering, routing, retries, and dead-letter handling; RabbitMQ is not the persistence destination
- **Config Store**: External configuration management (files, database, or external API)

**Internal Components:**
1. **Hot Path Engine** - Rust core processing messages at minimum latency
2. **Channel Manager** - Dynamically creates/manages bounded per-channel actors on one Tokio runtime
3. **Aggregation Engine** - Computes rolling aggregates for events that do not require raw persistence
4. **Schema Registry** - Generates and validates Protobuf schemas on-the-fly

### Key Design Principles

| Principle | Description |
|-----------|-------------|
| **Zero-Downtime Channel Addition** | New log categories added without redeployment |
| **Per-Channel Autonomy** | Each channel has independent thresholds, destinations, aggregation rules |
| **Dynamic Schema Handling** | Runtime Protobuf descriptors loaded per message type |
| **Hot Path Optimization** | Rust core handles classification and routing at maximum throughput |
| **Memory-Bounded Aggregation** | Safe owner-local buffers (50 MiB/3600s default) plus a 512 MiB process budget prevent memory bloat |
| **Internal Buffer Management** | Bounded per-channel buffers with a documented process-wide v1 memory budget |

---

## Data Flow Architecture

### Ingest → Classify → Route → Aggregate/Persist

```
Raw Log Event 
    ↓ [Parse & Validate]
Channel Assignment (prefix-based routing)
    ↓ [Threshold Check]
┌─────────────┬──────────────┐
│ ≥ Threshold │ < Threshold  │
│ → Direct Persist + Aggregate  → Aggregate │
└─────────────┴──────────────┘
```

**Direct Path**: Message meets threshold → immediate persistence to the channel's configured external sink and contribution to the aggregate  
**Aggregation Path**: Every event in a channel with an aggregator contributes to the buffer → aggregate compute → persist summary → drop originals
**No-Aggregator Path**: Message below threshold without an aggregator → immediate persistence to the configured external sink

Every event has a stable `event_id`, and every aggregate window has a stable
`aggregate_id`. RabbitMQ acknowledgement occurs only after PostgreSQL confirms
an idempotent write. Retries are therefore safe even when an acknowledgement is lost.

---

## RabbitMQ Integration

### Channel Creation Strategy

- **Dynamic Provisioning**: RabbitMQ channels/queues/exchanges created per-channel definition
- **Deterministic Naming**: `{channel_prefix}_rabbit_{timestamp}/{hash}` ensures uniqueness while allowing reuse
- **Hot Reload**: New channel definitions activate immediately without restart
- **No Static Dependencies**: Startup doesn't depend on pre-existing queues
- **DLQ Support**: Failed messages route to `{channel_prefix}_rabbit_dlq` for retry

### Queue Topology Per Channel

```
Channel Config → Create Exchange (fanout) → Bind Queues:
  - Direct Path Queue (bounded by threshold policy)
  - Aggregation Window Queues (time/N-event based)
  - Summary Output Queue (for aggregated results)
  - Dead-Letter Queue (failed messages with original routing context)
```

---

## Protobuf Schema Generation

### Dynamic Schema Pipeline

1. **User Defines Structure** → Channel config includes message type definition
2. **On-First-Use Resolution** → Runtime descriptors loaded and cached per channel with `prost-reflect`
3. **Validation Layer** → All payloads validated against schema before processing
4. **No Code Changes** → New message types handled transparently
5. **Versioning**: LRU cache with 100MB max, migration hooks for backward compatibility

---

## Data Loss Policy (Decision #1 CONFIRMED)

### First-Order: Attempt Rollup Merge (Always Try)

When buffer reaches capacity OR TTL expires:
1. Construct rollup from all buffered events in-memory
2. Merge into existing accumulator using aggregator interface
3. Dispatch the aggregate through RabbitMQ to the channel's configured external sink
4. Drop original raw events from buffer

**Rollup Interface Contract:**
Every aggregator MUST implement `merge(Self other)` method that:
- Accepts a pre-flushed aggregator from another buffer/window
- Combines state without re-scanning original raw events
- Returns merged accumulator ready for persistence

### Second-Order Fallback: Proactive Recovery Spill

Triggered before allocation limits are exceeded when:
- The process-wide memory budget cannot reserve space
- Numeric overflow or non-finite input prevents a valid merge
- Schema incompatibility prevents merging buffered data

**Disk Storage Location**: `config_dir/channels/{channel_id}/evicted_events.bin`

**Reprocessing Workflow:**
1. Admin triggers reprocessing via CLI/API or system automatically retries (Decision #2)
2. System reads evicted_events.bin from disk
3. For each `AggregatedLog` in file: merge into active buffer or persist to destination
4. Delete evicted_events.bin after successful replay

### TTL Behavior Semantics (CONFIRMED)

- **TTL = 0**: Merge immediately on first low-severity event received
- **TTL > 0**: Buffer expires at `current_time + TTL` from last merge
- **Buffer Full Before TTL**: Trigger merge IMMEDIATELY, reset TTL to current_time + original_ttl
- **On Channel Redefinition**: Stop admission to the affected actor, drain and durably hand off all buffered data, then apply the new config. Never clear a live buffer without a durable handoff.

### No-Aggregator Behavior (CONFIRMED)

`aggregator: None` → immediate persist (generic log sink behavior)  
No event may be dropped because an aggregator is absent. TTL and aggregation-specific fields are ignored when no aggregator is configured.

### Count Accumulator Semantics (CONFIRMED)

Single `count` field accumulates indefinitely (no dual tracking needed)  
Rolling average formula: `(old_avg × old_count + avg(new_events) × new_count) / total_count`

The v1 aggregation functions are `average`, `average_duration_between_events`, `max`, `min`, `sum`, and `count`. `average` with `overall` granularity computes the numeric average of every input in the active overall window. `count` with `overall` granularity computes the total event count in that window. Conditional count is intentionally deferred until a predicate representation and evaluation policy are selected.

Aggregators may set `rolling: true` to merge successive flush windows into one expanding aggregate record. Rolling records retain their original `window_start`, advance `window_end`, and use stable aggregate-series identifiers so state survives service restarts. Duration aggregates use custom metadata for interval totals and first/last event timestamps, including the interval crossing adjacent windows. A changed aggregator identity starts a new rolling series.

---

## Dead-Letter Queue Strategy (Decision #2 CONFIRMED)

### Confirmed Implementation

- **Move to DLQ Exchange**: `{channel_prefix}_rabbit_dlq` after max retries (10 attempts, 1s→60s backoff)
- **DLQ Message Includes**: `original_channel_id`, `original_destination_exchange`, `original_routing_key`
- **Auto-Retry Logic**: Reads original channel config, re-routes to correct destination via Rust service
- **Maintains Data Consistency**: Eventually aggregates to original destination as configured

**Why This Works:**
- No external dependencies (Rust service handles routing logic)
- Immediate retry attempt without waiting for admin action
- Alert ensures human oversight even if auto-retry fails
- Different exchange allows routing to error-handling service

---

## Message Ordering Guarantees (Decision #3 CONFIRMED - HYBRID APPROACH)

### Confirmed Strategy

**Non-Negotiable Requirement:**
- ALL log events MUST have a timestamp field (enables in-memory reordering)

**Configurable Per-Channel Modes:**
- `FifoRequired`: Timestamp-ordered processing with a bounded allowed-lateness window
- `FirstComeFirstServed`: Non-blocking arrival-order processing

**Per-Aggregate-Type Awareness:**
- Each aggregator declares if ordering is required via `requires_ordering()` trait method
- **Order-not-needed aggregates** → immediate accumulator update, with the aggregate persisted according to its configured window ✅ Higher throughput
- **Order-required aggregates** → timestamp-based reordering in memory ✅ Data integrity preserved

For `FifoRequired`, `allowed_lateness` is explicit configuration. Events arriving after the watermark are retained for the next eligible window and marked as late; they are never silently discarded.

### Example Implementations

```rust
enum OrderingMode {
    FifoRequired,           // Strict ordering (time-based aggregates)
    FirstComeFirstServed,   // Non-blocking, faster throughput
}

trait Aggregator {
    fn requires_ordering() -> bool;  // e.g., "time-between-requests" = true, "average" = false
    
    fn update(&mut self, event: Event);
    fn merge(&mut self, other: Self);
    fn flush(self) -> AggregatedLog;
}

struct TimeBetweenAggregator {
    events: Vec<Event>,  // MUST store for chronological calculation
}

impl Aggregator for TimeBetweenAggregator {
    fn requires_ordering() -> bool { true }
    
    fn update(&mut self, event: Event) {
        // Store ALL events (timestamped) for later chronological reordering
        self.events.push(event);
    }
}

struct AverageResponseTimeAggregator {
    sum: f64,
    count: u64,  // Rolling average - no ordering needed
}

impl Aggregator for AverageResponseTimeAggregator {
    fn requires_ordering() -> bool { false }
    
    fn update(&mut self, event: Event) {
        // Immediate aggregation possible (O(1) merge)
        self.sum += event.response_time_ms;
        self.count += 1;
    }
}
```

---

## Authentication Mechanism (Decision #4 CONFIRMED)

### Confirmed Implementation

- **Header Name**: `X-API-Key` or customizable via config
- **Location**: HTTP header on ingest requests (`/ingest` endpoint)
- **Validation**: Simple string equality check (no complex token management initially)
- **Future-proofing**: Can migrate to JWT/OAuth if multi-tenancy becomes a requirement later
- **No external OAuth infrastructure needed** - keeps initial implementation simple

### Rationale

Simple to implement and test (no OAuth infrastructure needed initially).  
Industry standard for REST APIs (Postman, curl work out of the box).  
Can migrate to JWT/OAuth later if multi-tenancy becomes a requirement.

---

## Schema Evolution Handling (Decision #5 CONFIRMED)

### Confirmed Strategy

**Behavior**: Return 400 Bad Request immediately when payload doesn't match current schema version.

**Rationale:**
1. Schema changes are operator responsibilities, not runtime failure modes
2. Hot-path simplicity: rejects rather than attempting complex migrations
3. Forces immediate attention from on-call team (schema change = configuration drift)
4. Separate channel version is safer than silent data corruption

**Operational Expectation:**
Operators of this application who decide to change the schema of their logged items should take care in doing so to avoid failures like this.

---

## Observability Stack (Decision #6 CONFIRMED)

### Confirmed Implementation

- **Metrics Collection**: `tokio-metrics` or `prometheus-rs` crates for Rust metrics
- **Export Format**: Prometheus-compatible metrics via `/metrics` endpoint
- **Visualization**: Grafana dashboards for buffer utilization, throughput, error rates
- **Alerting**: Alert rules exported to Prometheus alertmanager (or external system)
- **No licensing costs** at any scale (free and open source)

### Why This Choice?

1. Industry standard for Rust services (tokio-metrics, prometheus-rs crates)
2. Free and open source (no licensing cost at scale)
3. Works standalone or integrated with existing APM tools
4. Supports metrics, histograms, and alert rules out of the box

---

## Memory Model (Internal Buffer Management CONFIRMED)

### v1 Buffer Budget Strategy

- **Internal enforcement**: The service owns buffer management; users configure channel policy, not memory allocation.
- **Per-channel limit**: 50 MiB by default, configurable per channel.
- **Process-wide budget**: Start with a 512 MiB default budget for the v1 container and reject or evict according to policy when the budget is exhausted.
- **Future evolution**: A reusable global buffer pool may replace this accounting model after measurements justify it.

---

## Error Handling Strategy (From error_handling.md)

### Error Categories

| Category | Examples | Recovery Strategy |
|----------|----------|-------------------|
| Parse Errors | Invalid JSON/protobuf, malformed payloads | Return 400 Bad Request immediately |
| Channel Errors | Unknown channel_id, invalid configuration | Return 404 Not Found and reject the event |
| Schema Errors | Validation failures, type mismatches | Return 400 Bad Request with field-path details |
| Buffer Errors | Memory limits approached or buffer full | Apply backpressure or proactive recovery spill; alert if >80% for >60s |
| Network Errors | RabbitMQ or PostgreSQL connection lost/unavailable | Exponential backoff reconnect (1s→60s) through the broker/retry path |
| Aggregation Errors | Overflow conditions, timeout exceeded | Force-flush, reset counters, log warning |

### Dead-Letter Handling (Decision #2)

```rust
struct DeadLetterPolicy {
    max_retries: u32,              // default: 10
    backoff_base_seconds: Duration,// default: 1s
    max_backoff_seconds: Duration, // default: 60s
    
    // Dead-letter destination (different from original for retry)
    dlq_exchange_name: String,     
}
```

### Metrics for Error Monitoring

| Metric | Alert Threshold | Description |
|--------|-----------------|-------------|
| `buffer_utilization` | >80% for >60s | Buffer size / MAX_BYTES * 100 |
| `persist_error_rate` | >1%/min | Persist failures per minute |
| `schema_validation_failures` | >10/hour | Validation errors (potential config issues) |
| `channel_creation_latency` | p99 > 500ms | Time to create new channel on first event |

---

## API Contract Summary

### Endpoints

| Endpoint | Method | Purpose | Auth |
|----------|--------|---------|------|
| `/ingest` | POST | Accept log events | X-API-Key header |
| `/channels/{channel_id}/config` | GET | Retrieve channel config | Optional |
| `/channels/{channel_id}` | PUT | Create/update channel definition | Required |
| `/channels/{channel_id}/schema` | POST | Register new message type schema | Required |
| `/metrics` | GET | System metrics (buffer sizes, throughput) | No auth for read-only |

### Response Codes

- `202 Accepted` - Event durably queued for sink delivery
- `400 Bad Request` - Invalid payload or schema validation failure
- `404 Not Found` - Channel doesn't exist
- `503 Service Unavailable` - Bounded admission or memory budget exhausted

---

## Configuration Format Summary

### Channel Configuration JSON Schema (Required Fields)

```json
{
  "channel_id": "app.production.api",              // Unique channel identifier
  "severity_threshold": "warning",                  // Messages ≥ this level persist directly
  "rabbitmq_config": {                              // Destination configuration
    "connection_string": "amqp://user:pass@host:5672",
    "exchange_name": "logs.app.production.api",
    "routing_key_prefix": "api.errors"
  },
  "aggregator": null,                               // or AggregatorSpec object
  "ttl_seconds": 3600,                              // Buffer TTL (0 = immediate merge)
  "ordering_mode": "FIFO_REQUIRED"                  // Configurable ordering mode
}
```

---

## Project Plan Summary (11 Weeks)

| Phase | Duration | Description | Status |
|-------|----------|-------------|--------|
| Foundation | Weeks 1-2 | Core data structures, ring buffers, timestamp utilities, internal buffer pool | ✅ Planned |
| Hot Path Engine | Weeks 3-5 | Routing logic, dynamic channel mgmt, schema gen, rolling aggregates | ✅ Ready to implement |
| Infrastructure | Weeks 6-7 | HTTP endpoints with API key auth, metrics, error handling, DLQ retry | ✅ Ready to implement |
| Testing | Weeks 8-9 | Unit/integration/load tests, failure scenario testing | ✅ Planned |
| Documentation & Release | Weeks 10-11 | Docs, binaries, Docker, CHANGELOG, v0.1.0 release | ✅ Planned |

---

## Risk Mitigation Summary

| Risk | Probability | Impact | Mitigation Strategy | Status |
|------|------------|--------|---------------------|--------|
| RabbitMQ connection instability | High | Medium | Connection pooling + exponential backoff | ✅ Addressed |
| Buffer exhaustion under burst load | Medium | High | Metrics monitoring + operator alerts, bounded per-channel buffers and process-wide budget | ✅ Addressed |
| Schema generation latency spikes | Low | Low | Cache warmup on startup + async schema loading | ✅ Planned |
| Memory leak in long-running process | Medium | High | Internal buffer management, no indefinite accumulation | ✅ Addressed |
| Data loss during merge | Low | Medium | Rollup-first strategy with proactive recovery spill | ✅ Addressed |
| Schema evolution causing failures | Medium | Medium | Reject immediately (operational concern) | ✅ Addressed |

---

## Glossary

| Term | Definition |
|------|------------|
| **Channel** | Logical partition for a log category (e.g., "app.production.api") |
| **Severity Threshold** | Minimum severity level that triggers direct persistence |
| **Aggregator** | Function that computes summaries from buffered logs (average, max, min, sum) |
| **Granularity** | Aggregation window definition (per N events, per T time, or overall stream) |
| **Ring Buffer** | Fixed-size circular buffer for temporary log storage before aggregation |
| **Hot Path** | Core message processing pipeline requiring maximum throughput and low latency |
| **Dynamic Provisioning** | Creating queues/exchanges at runtime without code changes or restarts |
| **Rollup Merge** | Combining accumulator state from multiple buffers/windows (X = X + Y) |
| **DLQ (Dead-Letter Queue)** | Queue for failed messages with original routing context for reprocessing |

---

## Next Steps to Programming

1. ✅ All decisions confirmed (Decisions #1-#6)
2. ✅ Risk analysis complete
3. ✅ Architecture finalized
4. ✅ Project timeline established (11 weeks, 5 phases)
5. ⏭️ **Ready for Phase 1: Foundation**

---

## Document References

This consolidated plan references and includes details from:
- `architecture_overview.md` - Core architecture, data flow, system boundaries
- `technical_specification.md` - Detailed specs covering data structures, algorithms, protocols
- `error_handling.md` - Comprehensive error types, recovery strategies, metrics
- `project_plan_detailed.md` - 5-phase, 11-week project plan with success criteria
- `WORKING.md` - Active planning session notes (now superseded by this consolidated document)

---

**END OF FINAL PLANNING DOCUMENT v0.1.0**
