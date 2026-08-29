# Technical Specification - RabbitMQ HotLog Aggregator

## 1. Architecture Decisions

### 1.1 Language Choices

| Component | Language | Rationale |
|-----------|----------|-----------|
| Hot Path Engine | Rust | Memory safety, zero-cost abstractions, high throughput |
| Schema Registry | Rust | Consistent with hot path, shared data structures |
| Config Parser | Rust | In-process config loading, no external dependencies |

### 1.2 Concurrency Model

**Concurrency decision**: Use one Tokio runtime with one bounded channel actor/task per channel. Each actor owns its channel configuration, buffer, and accumulator; a shared RabbitMQ consumer routes confirmed deliveries to the PostgreSQL sink registered for each channel.

```
HTTP handlers → bounded channel mailbox → channel actor/task
                                      ├─ safe owner-local buffer
                                      ├─ aggregation state
                                      └─ bounded sink delivery
```

The registry, mailbox capacity, process-wide memory budget, and maximum active channel count are bounded. A shared worker pool may replace the actor scheduler later only after profiling demonstrates a scaling limit.

---

### 1.3 Memory Model

#### Hot Path Memory Budget
| Component | Size Limit | Behavior at Limit |
|-----------|------------|-------------------|
| Per-Channel Buffer | 50 MiB default | Apply backpressure or proactively spill before the limit |
| Process Resident Budget | 512 MiB default | Reject new buffered work or spill before allocation |
| Active Channel Registry | Configurable bounded count | Stop admission or evict inactive channel state only after durable handoff |
| Schema Cache | 100 MiB | LRU eviction beyond limit |
| Aggregation Accumulators | N/A - computed and discarded | Immediate memory reclamation after persist |

#### Ring Buffer Implementation

```rust
// Per-channel ring buffer for aggregation windows
struct ChannelBuffer {
    capacity_bytes: usize,     // 50 MiB default, enforced before allocation
    window_size: Duration,     // 3600 seconds default
    entries: VecDeque<LogEvent>, // owner-local queue; every event includes a timestamp
    resident_bytes: usize,
}
```

#### Aggregation Accumulators (per-window)

```rust
enum Aggregator {
    Average(AccumulatorSum),
    Max(MaxAccumulator),
    Min(MinAccumulator),
    Sum(AccumulatorSum),
    Count(u64),
}

struct AccumulatorSum {
    sum: f64,
    count: u64,
}
```

---

## 2. Data Structures

### 2.1 Log Event Structure

```rust
#[derive(Serialize, Deserialize, Protobuf)]
pub struct LogEvent {
    pub event_id: EventId,          // stable producer or service-generated id for idempotent delivery
    pub timestamp: Timestamp,
    pub channel_id: String,      // prefix-based routing key
    pub severity_level: SeverityLevel, // trace < debug < info < warning < error < critical
    pub payload: Vec<u8>,        // protobuf-encoded message
    pub metadata: HashMap<String, String>, // optional fields
}

#[derive(Protobuf)]
pub enum SeverityLevel {
    Unknown = 0,
    Trace = 1,
    Debug = 2,
    Info = 3,
    Warning = 4,
    Error = 5,
    Critical = 6,
}
```

### 2.2 Channel Definition

```rust
#[derive(Serialize, Deserialize)]
pub struct ChannelDefinition {
    pub channel_id: String,              // unique prefix (e.g., "app.production.api")
    pub severity_threshold: SeverityLevel, // messages ≥ this level persist directly
    
    // Routing configuration
    pub rabbitmq_config: RabbitMQDest,   // broker connection and routing information
    pub sink_config: PostgresSinkConfig, // v1 persistence destination; abstracted behind a future Sink trait
    
    // Aggregation rules for messages below threshold
    pub aggregator: Option<AggregatorSpec>,
    
    // Schema definition for runtime descriptor loading and validation
    pub message_schema: MessageSchemaDef,
    
    // Optional metadata
    pub retention_hours: Option<u32>,     // unsupported in v1; rejected when set
    pub compression_enabled: bool,        // unsupported in v1; rejected when true
}

#[derive(Serialize, Deserialize)]
pub struct AggregatorSpec {
    pub function: AggregationFunction,    // average, average_duration_between_events, max, min, sum, count
    pub granularity: GranularityType,     // per_N_events, per_T_time, overall
    pub params: AggregatorParams,         // N count, T duration, etc.
    pub rolling: bool,                    // PostgreSQL only: merge windows into one expanding aggregate record
}

#[derive(Protobuf)]
pub struct MessageSchemaDef {
    pub message_type: String,             // fully-qualified descriptor message name
    pub version: SemVer,                  // schema version for migrations
    pub descriptor_set_base64: String,    // serialized FileDescriptorSet for prost-reflect
}
```

### 2.3 Aggregation Result Structure

```rust
#[derive(Protobuf)]
pub struct AggregatedLog {
    pub aggregate_id: AggregateId,  // stable window identity for idempotent delivery
    pub channel_id: String,
    pub window_start: Timestamp,
    pub window_end: Timestamp,
    pub aggregator_type: AggregationFunction,
    pub aggregated_value: f64,           // for average/sum/min/max
    pub count: u64,                      // number of contributing events
    pub original_count: u64,             // total raw events (for count aggregates)
    pub min_value: f64,
    pub max_value: f64,
    pub metadata: HashMap<String, String>,
}
```

### 2.4 Persistence Delivery Contract

The v1 PostgreSQL sink is reached through a bounded RabbitMQ delivery path.

- `event_id` uniquely identifies a raw event.
- `aggregate_id` uniquely identifies an aggregate window.
- The sink must enforce uniqueness for both identifiers.
- RabbitMQ messages are acknowledged only after the sink confirms a successful, idempotent write.
- A lost acknowledgement may produce a retry; duplicate delivery must not duplicate PostgreSQL rows.
- Queue saturation produces backpressure or a proactive recovery spill before allocating beyond the process budget.

The future sink abstraction must preserve these delivery guarantees for every supported driver.

---

## 3. Protocol Specifications

### 3.1 Ingest Protocol (HTTP/Protobuf)

#### REST Endpoints

| Endpoint | Method | Purpose | Request Format |
|----------|--------|---------|----------------|
| `/ingest` | POST | Accept log events | JSON or protobuf encoded body |
| `/channels/{channel_id}/config` | GET | Retrieve channel config | - |
| `/channels/{channel_id}` | PUT | Create/update channel definition | ChannelDefinition JSON |
| `/channels/{channel_id}/schema` | POST | Register new message type schema | MessageSchemaDef JSON |
| `/metrics` | GET | System metrics (buffer sizes, throughput) | - |

#### Ingest Request Format

```protobuf
message IngestRequest {
    oneof content_type {
        bytes json_payload = 1;       // raw JSON event(s)
        bytes proto_payload = 2;      // protobuf-encoded event(s)
    }
}
```

#### Response Codes

- `202 Accepted` - Event durably queued for sink delivery
- `400 Bad Request` - Invalid payload or schema
- `404 Not Found` - Channel doesn't exist
- `503 Service Unavailable` - Admission rejected because bounded queues or memory budget are exhausted

---

### 3.2 Internal Communication Protocol

#### Ring Buffer Message Format

```c
struct RingBufferMsg {
    uint64_t timestamp_ns;           // monotonic clock start
    uint64_t event_id;               // unique identifier per channel
    uint8_t severity_level;          // enum value (0-5)
    uint32_t payload_size;           // protobuf bytes
    uint8_t payload[...];            // actual log data
    uint32_t checksum;               // CRC32 for integrity
} __attribute__((packed));
```

---

## 4. Algorithm Specifications

### 4.1 Aggregation Algorithms

#### Time-Based Window Aggregation (per T)

```
Algorithm: TimeWindowAggregator(channel_id, window_size_seconds, agg_function)
Input: Stream of LogEvent{timestamp, severity_level, value}
Output: AggregatedLog

WHILE messages arriving DO
    IF current_timestamp - last_flush >= window_size_seconds OR buffer.size >= MAX_BYTES (50MB) THEN
        flush_window(channel_id)
        
        FOR each entry in flushed_entries DO
            accumulator = agg_function(entry.value)
            publish(AggregatedLog(
                channel_id=entry.channel_id,
                window_start=entry.window_start,
                window_end=entry.window_end,
                aggregated_value=accumulator.result,
                count=accumulator.count,
                metadata=aggregate_metadata(entries))
            ) TO aggregation_output_queue
        
        clear flushed_entries from buffer
    END IF
END WHILE
```

#### N-Events Window Aggregation (per N)

```
Algorithm: EventCountWindowAggregator(channel_id, n_events, agg_function)
Input: Stream of LogEvent{timestamp, severity_level, value}
Output: AggregatedLog

WHILE messages arriving DO
    buffer.push(new_event)
    
    IF buffer.count >= n_events OR current_timestamp - buffer.first_timestamp > MAX_TIME (2 hours) THEN
        flush_window(channel_id)
        
        // Compute aggregate over all buffered entries
        accumulator = agg_function(buffer.entries)
        
        publish(AggregatedLog(...)) TO output_queue
        
        clear buffer
    END IF
END WHILE
```

#### Overall/Streaming Aggregation

```
Algorithm: ContinuousAggregator(channel_id, agg_function)
Input: Unbounded stream of LogEvent
Output: Streaming AggregatedLog (real-time updates)

INITIALIZE:
    accumulator = new_accumulator(agg_function)
    window_start = current_timestamp()

WHILE messages arriving DO
    buffer.push(new_event)
    
    IF new_event.severity_level >= channel.threshold OR 
       should_flush_on_threshold(channel.config) THEN
        // Immediate persist - don't wait for aggregation
        publish_confirmed_to_rabbitmq(new_event, channel.rabbitmq_config)
        continue
    
    END IF
    
    accumulator = accumulator.update(new_event.value)
    
    IF buffer.size >= MAX_BYTES (50MB) THEN
        flush_continuous_aggregate(channel_id, accumulator)
        accumulator.reset()
    END IF
END WHILE

ON_CHANNEL_STARTUP:
    // Only reset after the actor has drained and durably handed off its state.
    reset_accumulator_after_handoff(channel_id)
```

---

### 4.2 Threshold Routing Decision Tree

```
FUNCTION determine_route(log_event, channel_definition) -> RouteDecision
    
    IF channel_definition.aggregator IS NULL OR EMPTY THEN
        // No aggregator means every event is persisted immediately.
        RETURN RouteDecision {
            action: DIRECT_PERSIST,
            destination: channel_definition.sink_config,
            reason: NO_AGGREGATOR_DEFINED
        }
    
    END IF

    IF log_event.severity_level >= channel_definition.severity_threshold THEN
        // High-severity events are persisted individually and still contribute
        // to the same aggregate as every other event in the channel.
        RETURN RouteDecision {
            action: DIRECT_PERSIST_AND_AGGREGATE,
            destination: channel_definition.sink_config,
            aggregator: channel_definition.aggregator
        }
    END IF
    
    // Evaluate aggregation window triggers
    window_trigger = evaluate_window_trigger(
        channel_definition.aggregator,
        current_timestamp(),
        buffer_stats
    )
    
    RETURN RouteDecision {
        action: BUFFER_FOR_AGGREGATION,
        aggregator: channel_definition.aggregator,
        estimated_flush_time: estimate_flush_time(channel_definition.aggregator)
    }
```

---

## 5. Error Handling Strategy

### 5.1 Failure Domains

| Component | Failure Mode | Recovery Strategy |
|-----------|--------------|-------------------|
| RabbitMQ Connection | Connection lost | Exponential backoff reconnect (1s, 2s, 4s, ... max 60s) |
| Buffer Full | Ring buffer at capacity | Evict oldest entries, alert operator via metrics endpoint |
| Schema Validation Fail | Payload doesn't match schema | Return 400 Bad Request with validation error details |
| Aggregation Timeout | Window exceeds MAX_TIME | Force flush remaining buffered entries, log warning |
| Persist Failure | PostgreSQL unavailable or unreachable | Retry through RabbitMQ, then move to the dead-letter queue |

### 5.2 Error Propagation Model

```rust
pub enum ProcessingError {
    ParseError(String, Vec<u8>),                    // Failed to parse payload
    ChannelNotFound(String),                        // Unknown channel_id
    SchemaValidationError(String, FieldPath),       // Protobuf validation failed
    BufferFull(String),                             // Ring buffer at capacity
    PersistError(String, Option<LogEvent>),         // Failed to write to the configured external sink
    AggregationOverflow(OverflowError),              // Accumulator numeric overflow
}

pub struct OverflowError {
    pub accumulator_type: AggregationFunction,
    pub max_value: f64,
    pub new_value: f64,
    pub operation: String,                         // "add", "multiply", etc.
}
```

### 5.3 Metrics for Error Monitoring

| Metric | Type | Alert Threshold | Description |
|--------|------|-----------------|-------------|
| `buffer_utilization` | Gauge | >80% | Buffer size / MAX_BYTES * 100 |
| `persist_error_rate` | Counter/Rate | >1%/min | Persist failures per minute |
| `schema_validation_failures` | Counter | >10/hour | Validation errors (potential config issues) |
| `channel_creation_latency` | Histogram | p99 > 500ms | Time to create new channel on first event |

---

## 6. Configuration Format

### 6.1 Channel Configuration JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "channel_id": {
      "type": "string",
      "pattern": "^[a-z][a-z0-9_.-]*$",
      "description": "Unique channel identifier (lowercase, alphanumeric, dots/hyphens/underscores only)"
    },
    "severity_threshold": {
      "type": "string",
      "enum": ["trace", "debug", "info", "warning", "error", "critical"]
    },
    "rabbitmq_config": {
      "type": "object",
      "properties": {
        "connection_string": {"type": "string"},
        "exchange_name": {"type": "string"},
        "routing_key_prefix": {"type": "string"},
        "queue_ttl_seconds": {"type": "integer"}
      }
    },
    "sink_config": {
      "type": "object",
      "required": ["driver", "connection_string"],
      "properties": {
        "driver": {"const": "postgres"},
        "connection_string": {"type": "string"}
      },
      "description": "PostgreSQL persistence sink in v1; future versions will support additional sink drivers"
    },
    "aggregator": {
      "oneOf": [
        {
          "properties": {
            "function": {"enum": ["average", "average_duration_between_events", "max", "min", "sum", "count"]},
            "granularity": {"enum": ["per_n_events", "per_t_time", "overall"]},
            "params": {"type": "object"},
            "rolling": {"type": "boolean", "default": false}
          }
        },
        {"type": "null"}
      ]
    }
  },
  "required": ["channel_id", "severity_threshold", "rabbitmq_config", "sink_config"]
}
```

---

## 7. Deployment Considerations

### 7.1 Startup Procedure

1. **Initialize Hot Path Engine**
   - Load global channel registry (from disk or empty if dynamic-only)
   - Initialize schema cache with LRU policy (100MB max)
   
2. **Establish RabbitMQ Connection Pool**
   - Create bounded RabbitMQ and PostgreSQL delivery resources shared by channel actors
   - Subscribe to channel management queues for hot-reload events
   
3. **Start Channel Actors**
   - Start bounded Tokio tasks for configured channels
   - Initialize safe owner-local buffers for existing channels

### 7.2 Channel Lifecycle Management

```
CHANNEL LIFE CYCLE:

[CREATED] ←──────────────────────┐
      ↓                          │ (no activity)
[PERSISTED TO DISK] ─────────────┤
      ↓                          │
[ACTIVE / PROCESSING]            │ (>24h no messages)
      ↓                          ▼
[METRICS COLLECTED]       [EVICT FROM MEMORY]
      ↓                          (persist config to disk if configured)
    ...                            (cold storage)

RELOAD TRIGGER:
  - New channel_id appears in /channels/{id}/config endpoint
  - RabbitMQ management API publishes channel-registered event
  - Config hot-reload file (if enabled) modified
```

### 7.3 Shutdown Procedure

1. **Graceful Drain Phase**
   - Stop accepting new ingest requests
   - Wait for active aggregation windows to complete (max 5 minutes)
   
2. **Checkpoint State**
   - Flush or durably hand off every channel mailbox and aggregation window
   - Persist channel registry configuration if configured
   
3. **Cleanup**
   - Close RabbitMQ connections
   - Close RabbitMQ and PostgreSQL resources only after successful drain or explicit durable recovery handoff

---

## Appendix A: Message Formats

### Protobuf Schema for Log Events

```protobuf
syntax = "proto3";

package hotlog;

message LogEvent {
  Timestamp timestamp = 1;
  string channel_id = 2;
  SeverityLevel severity_level = 3;
  bytes payload = 4;
  map<string, string> metadata = 5;
}

enum SeverityLevel {
  SEVERITY_UNKNOWN = 0;
  SEVERITY_TRACE = 1;
  SEVERITY_DEBUG = 2;
  SEVERITY_INFO = 3;
  SEVERITY_WARNING = 4;
  SEVERITY_ERROR = 5;
  SEVERITY_CRITICAL = 6;
}

message AggregatedLog {
  string channel_id = 1;
  Timestamp window_start = 2;
  Timestamp window_end = 3;
  AggregationFunction aggregator_type = 4;
  float aggregated_value = 5;
  uint64 count = 6;
  uint64 original_count = 7;
  float min_value = 8;
  float max_value = 9;
  map<string, string> metadata = 10;
}

enum AggregationFunction {
  AGGREGATION_UNKNOWN = 0;
  AGGREGATION_AVERAGE = 1;
  AGGREGATION_MAX = 2;
  AGGREGATION_MIN = 3;
  AGGREGATION_SUM = 4;
  AGGREGATION_COUNT = 5;
}

enum GranularityType {
  GRANULARITY_UNKNOWN = 0;
  GRANULARITY_PER_N_EVENTS = 1;
  GRANULARITY_PER_T_TIME = 2;
  GRANULARITY_OVERALL = 3;
}
```

### JSON Ingest Format (Multiple Events)

```json
[
  {
    "timestamp": "2026-08-01T12:00:00.123456Z",
    "channel_id": "app.production.api",
    "severity_level": "info",
    "payload": "{\"user_id\": \"u123\", \"action\": \"login\", \"latency_ms\": 42}",
    "metadata": {"host": "api-01.example.com"}
  },
  {
    "timestamp": "2026-08-01T12:00:00.789012Z",
    "channel_id": "app.production.api",
    "severity_level": "error",
    "payload": "{\"user_id\": \"u456\", \"action\": \"checkout_failed\", \"reason\": \"payment_declined\"}",
    "metadata": {"host": "api-01.example.com"}
  }
]
```
