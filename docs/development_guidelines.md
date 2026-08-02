# Development Guidelines - RabbitMQ HotLog Aggregator

## Overview

This document establishes the development guidelines, architecture principles, and project structure for the Rust-based HotLog Aggregator service. These guidelines ensure:

- **Modular monolith** architecture with clear boundaries
- **Vertical-slice architecture** for cohesive feature implementation
- **Minimal blast radius** when updating any component
- **Observability first-class citizen** (metrics, logging, tracing)
- **Testability built into design** from the start

### Required Rust Quality Gates

- Run `cargo fmt -- --check`, `cargo check`, and `cargo clippy --all-targets --all-features -- -D warnings` in CI.
- Enable the compiler and Clippy lint groups required by the `ms-rust` skill.
- Run dependency auditing and unused-dependency checks as the project grows.
- Keep v1 free of `unsafe`; any later unsafe abstraction requires a written safety argument, adversarial tests, benchmarks where performance-motivated, and Miri coverage.

---

## Implementation Details: Achieving Development Guidelines

This section provides concrete implementation patterns and architectural decisions to realize the principles outlined above.

---

## Modular Monolith Architecture

### Boundary Definition Strategy

**Component Boundaries:**

| Component | Responsibility | Public API Surface |
|-----------|---------------|-------------------|
| `hot_path_engine` | Ingest, classify, route messages at minimum latency | `process_log_events()`, `handle_ingest_request()` |
| `channel_manager` | Dynamic channel creation/management, registry state | `create_channel()`, `get_channel_def()`, `remove_channel()` |
| `aggregation_engine` | Rolling aggregate computation, window management | `flush_window()`, `merge_accumulators()` |
| `schema_registry` | Protobuf schema generation/validation/caching | `register_schema()`, `validate_payload()` |

### Vertical-Slice Implementation Pattern

Each feature should be implemented as a cohesive vertical slice containing:

- **Ingress layer**: HTTP handler + request validation
- **Business logic**: Core processing in hot path engine
- **Storage layer**: RabbitMQ producer, aggregation buffer state
- **Metrics hooks**: Prometheus metrics for each stage

Example vertical slice for channel creation:

```
┌─────────────────────────────────────────────────────────┐
│ Channel Creation Vertical Slice                         │
├─────────────────────────────────────────────────────────┤
│ HTTP Layer: /channels/{id} PUT                          │
│   └── Request validation (JSON schema)                  │
│                                                       │
│ Business Layer: ChannelManager::create()                │
│   └── Parse ChannelDefinition                           │
│   └── Generate unique queue names                        │
│                                                       │
│ Storage Layer: RabbitMQChannelTopology::build()         │
│   └── Create fanout exchange                             │
│   └── Bind direct-path, aggregation, DLQ queues          │
│                                                       │
│ Metrics Layer: prometheus_registry.register_gauge()     │
│   └── Active channels gauge                              │
└─────────────────────────────────────────────────────────┘
```

### Minimizing Blast Radius

**Single Responsibility Per Module:**

- `hot_path_engine` handles ONLY message routing and dispatches persistence through the sink boundary
- `channel_manager` handles ONLY registry state (no message processing)
- `aggregation_engine` handles ONLY aggregate computation (no routing decisions)

This ensures any component can be updated independently without cascading changes.

---

## Data Loss Policy Implementation (Decision #1)

### Rollup Merge Interface Contract

Every aggregator must implement the `merge()` method:

```rust
pub trait Aggregator: Send + Sync {
    fn merge(&mut self, other: Self);
    
    // Required for aggregation result persistence
    fn compute_aggregate(&self) -> f64;
    fn get_count(&self) -> u64;
}
```

**Implementation Example - AverageAccumulator:**
```rust
struct AverageAccumulator {
    sum: f64,
    count: u64,
}

impl AverageAccumulator {
    fn merge(&mut self, other: Self) {
        // Keep the sum and count separately; compute the average only when flushing.
        self.sum += other.sum;
        self.count = self.count.saturating_add(other.count);
    }

    fn average(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }
}
```

### Eviction Workflow at Buffer Full

When `ChannelBuffer.len() >= buffer.capacity()`:

1. **Construct Rollup**: Iterate buffer, compute aggregate via merge chain
2. **Merge to Accumulator**: Combine with existing active accumulator
3. **Persist**: Dispatch the aggregated result through RabbitMQ to the configured external sink
4. **Evict**: Drop oldest entries (FIFO) from buffer
5. **Alert**: Increment `buffer_eviction_count` metric

### TTL Behavior Implementation

```rust
pub struct ChannelBuffer {
    capacity_bytes: usize,           // default 50MB
    window_size_seconds: Duration,   // default 3600s
    entries: VecDeque<LogEvent>,    // owned by one channel actor
    resident_bytes: usize,          // checked before each allocation
    last_merge_time: Instant,       // tracks TTL expiry
}

impl ChannelBuffer {
    fn check_and_flush(&self) {
        let now = Instant::now();
        
        // Check time-based expiry
        if self.last_merge_time.elapsed() > self.window_size_seconds {
            self.trigger_flush(now);
            return;
        }
        
        // Check buffer full before TTL expiry
        if self.entries.len() >= self.capacity_bytes / 1024 {
            self.trigger_flush(now);
        }
    }
    
    fn trigger_flush(&self, now: Instant) {
        let aggregator = self.compute_aggregator();
        self.persist_aggregate(aggregator);
        self.evict_entries();
        self.last_merge_time.store(now);
    }
}
```

---

## Dead-Letter Queue Strategy (Decision #2)

### Retry Logic Implementation

```rust
struct RetryPolicy {
    max_retries: u32,           // default 10
    backoff_base_seconds: Duration,  // default 1s
    max_backoff_seconds: Duration,  // default 60s
}

impl RetryPolicy {
    fn backoff_duration(&self, attempt: u32) -> Duration {
        let base = self.backoff_base_seconds * (2u32.pow(attempt));
        if base > self.max_backoff_seconds {
            self.max_backoff_seconds
        } else {
            base
        }
    }
}

// Dead-letter message structure
struct DeadLetterMessage {
    original_channel_id: String,
    original_destination_exchange: String,
    original_routing_key: String,
    original_payload: Vec<u8>,
    error_info: PersistError,      // for debugging
}
```

**DLQ Message Content:**
- Original channel config snapshot
- Original routing context (exchange + queue)
- Raw payload for replay if needed
- Error count and last error message

### Auto-Retry Service

A background service reads from the DLQ exchange:
1. Parse `DeadLetterMessage` from RabbitMQ
2. Load original channel configuration from config store
3. Re-route message to correct destination
4. Delete from DLQ if successful, increment retry count if not
5. Alert operator after max retries exhausted

---

## Message Ordering Guarantees (Decision #3)

### Hybrid Ordering Implementation

**Timestamp Requirement:** All `LogEvent` MUST include a `timestamp` field, regardless of ordering mode. The timestamp is needed for persistence, diagnostics, failure analysis, and optional reordering.

```protobuf
message LogEvent {
    int64 timestamp = 1;           // Unix timestamp in milliseconds
    string channel_id = 2;
    SeverityLevel severity_level = 3;
    bytes payload = 4;
    map<string, string> metadata = 5;
}
```

**Per-Channel Ordering Mode:**
```rust
pub enum OrderingMode {
    FifoRequired,          // Strict ordering via time-based windowing
    FirstComeFirstServed,  // Non-blocking, faster throughput
    None,                   // Aggregation does not require ordering
}

impl ChannelDefinition {
    pub fn ordering_mode(&self) -> OrderingMode {
        self.ordering.unwrap_or(OrderingMode::FirstComeFirstServed)
    }
    
    pub fn requires_timestamp(&self) -> bool { true }
}
```

**Aggregator-Ordered Awareness:**
```rust
pub trait AggregationTrait {
    fn requires_ordering() -> bool;  // Each aggregator declares ordering needs
    fn merge(&mut self, other: Self);
}

impl AggregationTrait for AverageAccumulator {
    fn requires_ordering() -> bool { false } // Average is order-independent
}

impl AggregationTrait for CountAccumulator {
    fn requires_ordering() -> bool { false } // Simple counters don't need ordering
}
```

**Order-not-needed aggregators** (e.g., simple count, max): Update their accumulator immediately, then persist according to the configured window.  
**Order-required aggregators** (e.g., time-windowed averages): Buffer in timestamp-sorted queue before flush.

---

## Memory-Bounded Aggregation

### Bounded Buffer Management

```rust
pub struct BufferBudget {
    resident_bytes: usize,
    max_resident_bytes: usize,  // 512 MiB default process budget
}

impl BufferBudget {
    fn reserve(&mut self, bytes: usize) -> Result<(), BufferFullError> {
        let next = self.resident_bytes.checked_add(bytes)
            .ok_or(BufferFullError)?;
        if next > self.max_resident_bytes { return Err(BufferFullError); }
        self.resident_bytes = next;
        Ok(())
    }
}
```

### Prevention of Creeping Memory Usage

- Pool size bounded at `total_memory_budget * BUFFER_OVERHEAD_FACTOR`
- LRU eviction policy for inactive buffers (>24h no activity)
- Schema cache uses explicit LRU with 100MB hard limit
- Per-channel ring buffers evict oldest entries when full (FIFO)

---

## Schema Registry Implementation

### On-First-Use Descriptor Pipeline

```rust
pub struct SchemaRegistry {
    schemas: HashMap<String, CompiledSchema>,  // channel_id -> schema
    cache_size_bytes: usize,                    // 100MB max
    eviction_policy: LruCache<SemVer, Timestamp>,
}

impl SchemaRegistry {
    pub fn get_or_compile(&mut self, channel_id: &str) -> Result<CompiledSchema, SchemaError> {
        if let Some(schema) = self.schemas.get(channel_id).cloned() {
            return Ok(schema);
        }

        self.compile_and_cache(channel_id)
    }
    
    fn compile_and_cache(&mut self, channel_id: &str) -> Result<CompiledSchema, SchemaError> {
        let schema_def = fetch_schema_from_config(channel_id);
        let compiled = prost_reflect::DescriptorPool::decode(schema_def)?;
        
        if self.exceeds_size_limit() {
            self.evict_oldest();  // LRU beyond size limit
        }
        
        self.schemas.insert(channel_id.to_string(), compiled);
        Ok(compiled)
    }
}
```

### Schema Evolution Strategy

- **Versioned schemas**: `MessageSchemaDef` includes semantic version
- **Migration hooks**: New fields added to end of protobuf definitions
- **Immediate rejection on mismatch**: Forces operator attention for breaking changes

---

## Observability Stack Integration

### Prometheus Metrics Registry

```rust
pub struct MetricsRegistry {
    // Buffer utilization per channel (gauge)
    buffer_utilization: RegisterGaugeVec,
    
    // Events processed/sec (counter rate)
    events_processed: RegisterCounterVec,
    
    // Errors count and rate (counter)
    errors_total: RegisterCounter,
    
    // RabbitMQ connection health (gauge)
    rabbitmq_connection_health: RegisterGauge,
}
```

**Key Metrics:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `hotlog_buffer_utilization_bytes` | Gauge | `channel_id` | Current buffer usage |
| `hotlog_events_processed_total` | Counter | - | Total events processed |
| `hotlog_errors_total` | Counter | `error_type` | Cumulative errors |
| `hotlog_ingest_latency_seconds` | Histogram | `handler` | Ingest handler latency |

### Structured Logging

Log format includes:

- `timestamp`: Event time supplied by the producer, required for every event
- `elapsed_ns`: Monotonic duration measurement for local tracing
- `channel_id`: Channel identifier
- `event_id`: Unique ID per message (for correlation)
- `severity_level`: Original severity from log event

Use named `tracing` fields rather than interpolated strings. Never log API keys,
connection strings, raw payloads, or unredacted metadata.

### Sink Delivery Contract

- Every event has a stable `event_id`.
- Every aggregate window has a stable `aggregate_id`.
- RabbitMQ acknowledgement occurs only after the PostgreSQL sink confirms an idempotent write.
- A lost acknowledgement may cause a retry, so sink writes must be safe to repeat.
- Bounded delivery queues apply backpressure instead of allocating without limit.

---

## Testing Strategy

### Component Test Organization

```
src/
├── tests/
│   ├── integration/          # End-to-end scenarios
│   │   ├── channel_creation.rs
│   │   ├── aggregation_window.rs
│   │   └── dlq_retry.rs
│   └── unit/                 # Individual component tests
│       ├── ring_buffer.rs
│       ├── accumulator.rs
│       └── schema_registry.rs
├── benches/                  # Performance benchmarks
│   ├── ingest_throughput.rs
│   └── aggregation_latency.rs
```

### Performance Benchmark Targets

| Component | Target | Measurement |
|-----------|--------|-------------|
| Ring buffer push | <1µs latency | 10k ops/sec, 95th percentile |
| Channel creation p99 | <500ms | End-to-end from HTTP request to queue created |
| Schema compilation | <100ms | On-first-use cache miss |
| Ingest throughput | 10k+ events/sec/core | Sustained load test |

---

## Error Handling Implementation

### Error Types and Propagation

```rust
#[derive(Debug, thiserror::Error)]
pub enum HotLogError {
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Channel not found: {0}")]
    ChannelNotFound(String),
    
    #[error("Schema validation error: field={0}, expected_type={1}, actual_value={2}")]
    SchemaValidation(String, String, serde_json::Value),
    
    #[error("Buffer full, eviction triggered for channel {0}")]
    BufferFull(String),
    
    #[error("Persist error (retrying): {0}")]
    Persist(PersistError),
    
    #[error("Aggregation overflow: {0}")]
    AggregationOverflow(AggregationOverflowError),
}
```

### Graceful Degradation Paths

| Error Type | Hot Path Behavior | Metrics Trigger | Alert Condition |
|------------|-------------------|-----------------|-----------------|
| ParseError | Return 400 + detailed error | `errors_total{type="parse"}` | N/A |
| ChannelNotFound | Return 404 for HTTP; count and reject the event internally | `errors_total{type="channel_not_found"}` | Rate > 1%/min |
| BufferFull | Evict oldest entries | `buffer_eviction_count`, `buffer_utilization` | >80% for >60s |
| PersistError | Retry with backoff, move to DLQ if exhausted | `persist_retry_count`, `dlq_messages_total` | Max retries exceeded |

---

## Configuration Hot Reload

### File-Watch Implementation

**Linux:** Use `inotify` via `notify` crate  
**Windows:** Use `windows-service` + directory enumeration

```rust
struct ConfigWatcher {
    paths: Vec<PathBuf>,
    debounce_ms: Duration,  // Wait Xms after file write before reload
}

impl ConfigWatcher {
    pub fn watch(&self) -> Result<(), ConfigError> {
        for path in &self.paths {
            let watcher = notify::new(path)?;
            watcher.watch(
                |event| match event.kind {
                    notify::EventKind::Modify(_) => self.handle_config_update(event.path),
                    _ => {}
                },
                RecursiveMode::NonRecursive,
            );
        }

        Ok(())
    }
    
    fn handle_config_update(&self, path: &Path) {
        match path.read_to_string().and_then(|contents| Config::from_json(&contents)) {
            Ok(new_config) => self.apply_new_config(&new_config),
            Err(error) => tracing::warn!(error = %error, "configuration reload rejected"),
        }
    }
    
    fn apply_new_config(&self, config: &Config) {
        // Graceful restart of affected channels
        for channel_id in config.channels.keys() {
            if let Some(channel_def) = config.get_channel(channel_id) {
                self.channel_manager.create_channel(channel_def)?;
            }
        }
    }
}
```

### Hot Reload Behavior

- New channel definitions activate immediately without restart
- Existing channels not in new config: retain state until next message processed
- Schema registry caches warmed from new definitions on first use
- Affected channel actors drain and durably hand off buffered events before configuration changes apply

---

## Security Considerations

### API Key Authentication

**Location**: HTTP header `X-API-Key`  
**Validation**: Constant-time comparison to prevent timing attacks

```rust
fn validate_api_key(auth_header: &str) -> Result<(), ApiKeyError> {
    let provided_key = auth_header.trim();
    
    // Constant-time comparison
    if constant_time_eq(provided_key, env!("API_KEY")) {
        Ok(())
    } else {
        Err(ApiKeyError::InvalidKey)
    }
}
```

### Schema Evolution Security

- **Immediate rejection on breaking changes**: Prevents silent data corruption
- **Version pinning in config**: Operators explicitly opt-in to new schema versions
- **Audit logging**: All schema register operations logged with operator identity
