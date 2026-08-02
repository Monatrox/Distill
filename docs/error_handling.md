# Error Handling Specification - RabbitMQ HotLog Aggregator

## 1. Error Classification Model

### 1.1 Error Categories

```
┌─────────────────────────────────────────────────────────────┐
│                    ERROR CLASSIFICATION                       │
├──────────────────┬───────────────────────────────────────────┤
│ Parse Errors     │ Invalid JSON/protobuf, malformed payloads │
├──────────────────┼───────────────────────────────────────────┤
│ Channel Errors   │ Unknown channel_id, invalid configuration │
├──────────────────┼───────────────────────────────────────────┤
│ Schema Errors    │ Validation failures, type mismatches      │
├──────────────────┼───────────────────────────────────────────┤
│ Buffer Errors    │ Memory limits exceeded, buffer full       │
├──────────────────┼───────────────────────────────────────────┤
│ Network Errors   │ RabbitMQ connection lost/unavailable      │
├──────────────────┼───────────────────────────────────────────┤
│ Aggregation      │ Overflow conditions, timeout exceeded     │
│ Errors           │                                           │
└──────────────────┴───────────────────────────────────────────┘
```

---

## 2. Error Types and Definitions

### 2.1 ParseError

**When**: Payload cannot be deserialized or validated

**Triggers**:
- Invalid JSON syntax
- Protobuf schema version mismatch
- Missing required fields
- Type mismatches (string vs integer, etc.)

**Recovery Strategy**: 
- Return 400 Bad Request immediately
- Include detailed validation error in response body
- Log error with payload sample (masked for privacy)

**Response Format**:
```json
{
  "error_code": "PARSE_ERROR",
  "message": "Invalid JSON structure",
  "details": ["Missing required field: timestamp"],
  "payload_sample": "{\"user_id\": \"u123\"}"
}
```

### 2.2 ChannelNotFoundError

**When**: Channel configuration not found or invalid channel_id format

**Triggers**:
- Channel doesn't exist in registry
- Invalid channel_id (contains uppercase, spaces, etc.)
- Channel was deleted after last message

**Recovery Strategy**:
- Return 404 Not Found
- For hot-path operations: reject the event, increment the error counter, and never treat an unknown channel as an allowed drop path
- Metrics endpoint exposes error rate for monitoring

### 2.3 SchemaValidationError

**When**: Payload structure doesn't match defined schema

**Triggers**:
- Unknown field in payload
- Field type mismatch (string provided where integer expected)
- Nested object missing required fields
- Array item count exceeds definition

**Recovery Strategy**:
- Return 400 Bad Request with field-path details
- Include the runtime descriptor validation error and field path
- Log for schema evolution review

**Response Format**:
```json
{
  "error_code": "SCHEMA_VALIDATION_ERROR",
  "message": "Field validation failed",
  "field_path": ["user", "email"],
  "expected_type": "string",
  "actual_value": "null"
}
```

### 2.4 BufferFullError

**When**: Per-channel ring buffer reaches capacity (50MB default)

**Triggers**:
- New event pushed when buffer.size == MAX_BYTES
- Aggregation window full before flush completes

**Recovery Strategy**:
- Apply backpressure or proactively spill recoverable data before the limit
- Evict only after a durable handoff is confirmed
- Increment buffer_utilization metric (alert if >80%)
- Write evicted entries to the configured recovery file first when they cannot be merged or delivered to the external sink

**Metrics Trigger**:
```
ALERT IF: buffer_utilization > 80% AND duration > 60s
ACTION: Page on-call engineer, review channel configuration
```

### 2.5 PersistError

**When**: Failed to deliver a message through RabbitMQ or write it to the configured PostgreSQL sink

**Triggers**:
- Connection lost to RabbitMQ server
- Queue full (policies not yet configured)
- Exchange doesn't exist or permissions denied
- Memory limit exceeded on broker

**Recovery Strategy**:
- Retry with exponential backoff: 1s, 2s, 4s, ... max 60s
- After max retries: move to dead-letter handling
- Log error with correlation_id for tracing

**Dead-Letter Handling**:
```rust
struct DeadLetterPolicy {
    max_retries: u32,              // default: 10
    backoff_base_seconds: Duration,// default: 1s
    max_backoff_seconds: Duration, // default: 60s
    
    // Dead-letter destination
    dlq_exchange_name: String,     // optional
    dlq_queue_name: String,        // optional
}

impl ChannelDefinition {
    fn get_deadletter_policy(&self) -> &DeadLetterPolicy {
        // Use global default if not specified in channel config
    }
}
```

### 2.6 AggregationOverflowError

**When**: Accumulator exceeds numeric limits

**Triggers**:
- Sum accumulator overflows i64/f64 range
- Average computation with NaN/Infinity
- Min/Max overflow (NaN comparison issues)
- Count exceeds u64::MAX (unlikely but possible)

**Recovery Strategy**:
- For Sum: reject non-finite values and use explicit finite-range checks before applying a saturating policy; Rust's `f64` type has no `saturating_add` method
- For Average: handle division by zero (return 0.0), check for NaN
- For Min/Max: initialize with proper sentinel values
- Log overflow event with accumulator type and value

**Code Example**:
```rust
impl AccumulatorSum {
    fn add(&mut self, value: f64) -> Result<(), OverflowError> {
        if !value.is_finite() {
            return Err(OverflowError::non_finite(value));
        }

        let new_sum = self.sum + value;
        if !new_sum.is_finite() {
            return Err(OverflowError::overflow(self.sum, value));
        }

        self.sum = new_sum;
        Ok(())
    }
}
```

### 2.7 AggregationTimeoutError

**When**: Aggregation window exceeds MAX_TIME without flush

**Triggers**:
- Time-based window: current - last_flush > MAX_TIME (default 2h)
- Event-count window: first_timestamp + N_hours exceeded

**Recovery Strategy**:
- Force-flush remaining buffered entries
- Log warning with channel_id and elapsed time
- Reset window counters

**Code Example**:
```rust
fn check_and_flush_window(channel_id: &str, buffer: &ChannelBuffer) {
    let now = Instant::now();
    
    if now >= buffer.timeout_deadline {
        tracing::warn!(
            error.type = "aggregation_timeout",
            channel.id = %channel_id,
            elapsed_minutes = now.duration_since(buffer.last_flush_timestamp).as_secs() / 60,
            "forcing aggregation flush"
        );
        
        force_flush_buffer(channel_id, buffer);
    }
}
```

### 2.8 ChannelReloadError

**When**: Attempted to reload existing channel configuration

**Triggers**:
- New config rejects currently buffered entries (schema change)
- Aggregator function incompatible with buffered data format
- Queue/exchange already exists with conflicting settings

**Recovery Strategy**:
- For schema incompatibilities: persist buffered entries before reload
- For queue conflicts: skip creation, log warning, continue existing behavior
- Reject config changes that would cause immediate failures

---

## 3. Error Propagation Model

### 3.1 Internal Error Representation (Rust)

```rust
pub enum ProcessingError {
    Parse(ParseError),
    ChannelNotFound(ChannelNotFoundError),
    SchemaValidation(SchemaValidationError),
    BufferFull(BufferFullError),
    Persist(PersistError),
    AggregationOverflow(AggregationOverflowError),
    AggregationTimeout(AggregationTimeoutError),
}

// Helper function to convert to HTTP response
impl ProcessingError {
    pub fn http_response(&self) -> HttpResponse {
        match self {
            ProcessingError::Parse(e) => HttpResponse::BadRequest().json(
                ErrorEnvelope::from_parse_error(e)
            ),
            
            ProcessingError::ChannelNotFound(e) => HttpResponse::NotFound().json(
                ErrorEnvelope::from_channel_not_found(e)
            ),
            
            ProcessingError::SchemaValidation(e) => HttpResponse::BadRequest().json(
                ErrorEnvelope::from_schema_error(e)
            ),
            
            ProcessingError::BufferFull(e) => {
                // Evict oldest, try again
                if let Some(recovered) = self.evict_and_retry() {
                    HttpResponse::Ok().json(recovered)
                } else {
                    HttpResponse::ServiceUnavailable().json(
                        ErrorEnvelope {
                            error_code: "BUFFER_FULL".to_string(),
                            message: "Ring buffer at capacity, evicted oldest entries"
                                .to_string(),
                            details: vec![e.to_string()],
                        }
                    )
                }
            },
            
            ProcessingError::Persist(e) => {
                if e.retry_count < e.max_retries {
                    // Retry with backoff
                    self.retry_with_backoff()
                } else {
                    // Move to dead-letter queue
                    self.send_to_deadletter_queue()
                }
            },
            
            ProcessingError::AggregationOverflow(e) => HttpResponse::BadRequest().json(
                ErrorEnvelope {
                    error_code: "AGGREGATION_OVERFLOW".to_string(),
                    message: format!("Numeric overflow in {} accumulator", 
                                     e.accumulator_type),
                    details: vec![e.to_string()],
                }
            ),
            
            ProcessingError::AggregationTimeout(e) => HttpResponse::Accepted().json(
                // Force-flush result already published
                AggregatedLog {
                    channel_id: e.channel_id,
                    window_start: e.window_start,
                    window_end: Timestamp::now(),
                    aggregator_type: e.aggregator_type,
                    aggregated_value: e.last_aggregate_value,
                    count: e.event_count,
                    original_count: e.original_event_count,
                    min_value: e.min_value,
                    max_value: e.max_value,
                    metadata: e.metadata,
                }
            ),
        }
    }
}
```

### 3.2 HTTP Error Envelope Format

```json
{
  "error_code": "PARSE_ERROR",
  "message": "Invalid JSON structure - missing required field: timestamp",
  "details": [
    "Missing required field: timestamp",
    "Field 'payload' must be valid Protobuf or JSON object"
  ],
  "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2026-08-01T12:34:56.789Z"
}
```

---

## 4. Metrics for Error Monitoring

### 4.1 Prometheus Metrics

| Metric Name | Type | Labels | Description | Alert Condition |
|-------------|------|--------|-------------|-----------------|
| `hotlog_errors_total` | Counter | `error_type`, `channel_id` | Total errors by type and channel | - |
| `hotlog_buffer_utilization_bytes` | Gauge | `channel_id` | Current buffer size in bytes | `> 40MB (80%)` |
| `hotlog_buffer_utilization_percent` | Gauge | `channel_id` | Buffer utilization percentage | `> 80` for >60s |
| `hotlog_persist_errors_total` | Counter | `error_type`, `retry_count` | Persist failures by retry attempt | Rate > 1%/min |
| `hotlog_schema_validation_failures_total` | Counter | `field_path` | Schema validation errors | Rate > 10/hour |
| `hotlog_reconnect_attempts_total` | Counter | `channel_id`, `reason` | RabbitMQ reconnection attempts | - |
| `hotlog_dead_letter_sent_total` | Counter | `channel_id`, `reason` | Messages sent to dead-letter queue | Rate > 1%/min |

### 4.2 Alert Rules (Prometheus)

```yaml
# Buffer exhaustion alert
groups:
  - name: hotlog-alerts
    rules:
      # High buffer utilization
      - alert: HotLogBufferExhaustion
        expr: sum(hotlog_buffer_utilization_bytes{channel_id!=""}) by (channel_id) 
               > on(channel_id) group_left max(hotlog_buffer_utilization_bytes) * 0.8
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Channel {{ $labels.channel_id }} buffer at 80% capacity"
          description: "Buffer utilization is {{ printf \"%.0f\" (100 * value / 52428800) }}% on channel {{ $labels.channel_id }}"

      # Persist errors exceeding threshold
      - alert: HotLogHighPersistErrorRate
        expr: rate(hotlog_persist_errors_total[5m]) / 
               rate(hotlog_ingest_requests_total[5m]) * 100 > 1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High persist error rate on {{ $labels.channel_id }}"
          description: "{{ $value }}% of messages failing to persist (threshold: 1%)"

      # Schema validation failures
      - alert: HotLogSchemaValidationErrors
        expr: rate(hotlog_schema_validation_failures_total[5m]) > 1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High schema validation error rate"
          description: "{{ $value }} failures per minute on {{ $labels.field_path }}"

      # RabbitMQ connection issues
      - alert: HotLogRabbitMQReconnects
        expr: increase(hotlog_reconnect_attempts_total[1h]) > 5
        for: 30m
        labels:
          severity: critical
        annotations:
          summary: "Frequent RabbitMQ reconnection attempts"
          description: "{{ $value }} reconnect attempts in last hour on {{ $labels.channel_id }}"
```

### 4.3 Logging Strategy

**Structured logging format**:
```json
{
  "level": "WARN",
  "timestamp": "2026-08-01T12:34:56.789Z",
  "service": "hotlog-aggregator",
  "channel_id": "app.production.api",
  "event_type": "persist_failure",
  "message": "Failed to persist log after 3 retries",
  "error": "Connection refused to RabbitMQ: amqp:connection_closed",
  "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "retry_count": 3,
  "backoff_until": "2026-08-01T12:35:12.789Z"
}
```

**Log levels by error type**:
- `ERROR`: Critical errors (parse failures, persist after max retries)
- `WARN`: Recoverable errors (buffer full with eviction, connection lost but reconnected)
- `INFO`: Normal operations (retry attempts, dead-letter sends)

---

## 5. Dead-Letter Queue Handling

### 5.1 Dead-Letter Policy Configuration

```rust
struct DeadLetterPolicy {
    // Retention settings
    retention_hours: u32,         // default: 72h
    
    // Retry configuration for initial failures
    max_retries: u32,             // default: 10
    backoff_base_seconds: Duration,// default: 1s
    max_backoff_seconds: Duration, // default: 60s
    
    // Dead-letter destination
    dlq_exchange: String,         // optional - if None, use original exchange
    dlq_queue_prefix: String,     // optional - queue name template
    
    // Processing mode
    auto_acknowledge: bool,       // false until replay/archive succeeds
}

impl Default for DeadLetterPolicy {
    fn default() -> Self {
        DeadLetterPolicy {
            retention_hours: 72,
            max_retries: 10,
            backoff_base_seconds: Duration::from_secs(1),
            max_backoff_seconds: Duration::from_secs(60),
            dlq_exchange: None,     // inherit from original if not specified
            dlq_queue_prefix: "dlq.".to_string(),
            auto_acknowledge: false,
        }
    }
}
```

### 5.2 Dead-Letter Message Format

```protobuf
message DeadLetterMessage {
  string original_channel_id = 1;      // Original channel that produced this
  Timestamp original_timestamp = 2;    // When original event was received
  string error_type = 3;               // e.g., "PersistError", "ParseError"
  string error_message = 4;            // Human-readable error description
  
  oneof failure_data {
    bytes payload = 5;                 // Original log payload (if parseable)
    AggregatedLog aggregated_result = 6;// Aggregated data if applicable
  }
  
  map<string, string> metadata = 7;
  uint32 retry_count = 8;              // How many times this was retried
}
```

### 5.3 DLQ Processing (Optional Feature)

**Purpose**: Re-process messages in DLQ when original issues are resolved

**Trigger Options**:
1. Manual - Admin invokes DLQ processing via API
2. Scheduled - Cron job processes DLQ periodically
3. Event-driven - New config for channel triggers DLQ re-processing

**Processing Pipeline**:
```
Dead-Letter Queue → Parse Error Messages → Update Channel Config 
    ↓ (if error was transient)
Re-queue to Original Channel Exchange → Monitor for Success
    ↓ (if error persisted)
Move to Archive Queue → Generate report for on-call team
```

---

## 6. Operational Procedures

### 6.1 Error Escalation Matrix

| Condition | Response Time | Action Required |
|-----------|---------------|-----------------|
| Buffer >90% and >5min | 15 min | Review channel configs, consider increasing MAX_BYTES |
| Persist error rate >2%/min | 30 min | Check RabbitMQ cluster health, review queues |
| Schema validation errors >20/hour | 1 hour | Investigate payload format changes in source systems |
| RabbitMQ reconnection attempts >10/hour | 30 min | Review connection pool settings, check network stability |

### 6.2 Recovery Checklist (Buffer Exhaustion)

```
[ ] Check buffer utilization metric - identify affected channels
[ ] Review channel configurations - are thresholds appropriate?
[ ] Check RabbitMQ queue depths - is the queue backing up?
[ ] Verify upstream log producers aren't generating excessive volume
[ ] Consider temporary config change: increase MAX_BYTES for affected channel(s)
[ ] After recovery, analyze error logs for root cause
[ ] Update runbook with lessons learned
```

### 6.3 Recovery Checklist (Persist Failure Burst)

```
[ ] Check RabbitMQ cluster health (rabbitmqctl status)
[ ] Review queue message rates vs consumer rates
[ ] Check disk space on RabbitMQ nodes
[ ] Verify network connectivity between aggregator and broker
[ ] If queues full: temporarily increase rabbitMQ queue_max_length policy
[ ] After recovery: analyze persist_error_rate metric for patterns
[ ] Consider adding more RabbitMQ consumers if rate-limited
```

---

## 7. Testing Strategy for Error Handling

### 7.1 Unit Tests

**Buffer Full**:
- Push to ring buffer until capacity reached
- Verify oldest entries evicted correctly
- Check memory usage stays within bounds

**Persist Error Retry**:
- Mock RabbitMQ connection with failure sequence
- Verify exponential backoff timing (1s, 2s, 4s, ...)
- Confirm dead-letter queue receive after max retries

**Aggregation Overflow**:
- Feed extremely large values to sum accumulator
- Verify saturating arithmetic prevents overflow
- Check NaN handling in average computation

### 7.2 Integration Tests

**RabbitMQ Connection Drops**:
```bash
# Test script: simulate connection loss
1. Start aggregator with RabbitMQ connected
2. Send 100 events successfully (track event_ids)
3. Kill RabbitMQ service
4. Continue sending events (verify they queue locally)
5. Restart RabbitMQ
6. Verify all queued events published successfully
7. Assert no data loss for tracked event_ids
```

**Schema Validation**:
```bash
# Test script: validate schema errors
1. Define channel with strict Protobuf schema
2. Send valid events (verify 200 OK)
3. Send invalid payload (missing required field)
4. Verify 400 Bad Request response with error details
5. Send different type for same field (string vs integer)
6. Verify 400 Bad Request with type mismatch details
```

### 7.3 Load Tests for Error Conditions

**Buffer Exhaustion Simulation**:
```bash
# Generate burst to trigger buffer full condition
1. Configure channel with 1MB MAX_BYTES (test limit)
2. Send 10k events in <1 second
3. Verify oldest entries evicted
4. Check evicted entries persisted to disk (if configured)
5. Send additional events after burst
6. Verify no data loss for non-evicted entries
```

**Persist Failure Storm**:
```bash
# Simulate RabbitMQ unavailability
1. Start aggregator connected to RabbitMQ
2. Mock network disconnect on RabbitMQ port
3. Send 10k events during disconnect (verify queuing)
4. Restore network connectivity
5. Verify all queued events published within 60s
6. Check dead-letter queue for any failures after retry max
```

---

## Appendix A: Error Code Reference

| Error Code | HTTP Status | Recoverable? | Alert Threshold |
|------------|-------------|--------------|-----------------|
| PARSE_ERROR | 400 | No (immediate response) | Rate >10/hour |
| CHANNEL_NOT_FOUND | 404 | No (event rejected) | N/A |
| SCHEMA_VALIDATION_ERROR | 400 | No (immediate response) | Rate >20/hour |
| BUFFER_FULL | 503 | Yes (evict oldest) | Utilization >80% for >60s |
| PERSIST_ERROR (retry < max) | - | Yes (internal retry) | Rate >1%/min |
| AGGREGATION_OVERFLOW | 400 | Partial (clamp values) | N/A |
| AGGREGATION_TIMEOUT | 200 | Yes (force flush) | Window >2x MAX_TIME |
| CHANNEL_RELOAD_ERROR | - | Varies (config-dependent) | On first occurrence only |

---
