# RabbitMQ HotLog Aggregator - Architecture Overview

## Core Philosophy

A dynamic, hot-path logging aggregator built in Rust where different log categories ("channels") have independent storage semantics without code changes or redeployment. The service runs as a Docker container with external data stores for logs and aggregates.

---

## System Boundaries

### External Interfaces

- **Ingest Endpoint**: Accepts structured log events (JSON/Protobuf)
- **RabbitMQ Cluster**: Message broker used for buffering, routing, retries, and dead-letter handling; it is not the persistence destination
- **Config Store**: External configuration management (files, database, or external API)
- **External Data Store**: User-specified persistence sink for logs and aggregates. PostgreSQL is the v1 implementation; the final system will support pluggable sinks such as MSSQL, MySQL, SQLite, Seq, and others.

### Internal Components

1. **Hot Path Engine** - Rust core processing messages at minimum latency
2. **Channel Manager** - Dynamically creates/manages bounded channel actors and their state
3. **Aggregation Engine** - Computes rolling aggregates for all events in configured channels
4. **Schema Registry** - Loads, caches, and validates runtime Protobuf descriptors

---

## Deployment Architecture

### Docker Containerization

The Distill application is built in its own Docker container. Docker Compose supplies RabbitMQ for local development; the configured external sink remains outside the Distill deployment:

```yaml
# docker-compose.yml example
version: '3.8'

services:
  hotlog-aggregator:
    image: hotlog-aggregator:latest
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080"       # HTTP ingest & metrics
    environment:
      - RABBITMQ_URI=amqp://user:password@rabbitmq:5672
      - CONFIG_PATH=/app/config/
      - LOG_DIR=/app/logs/
    volumes:
      - ./config:/app/config:ro  # External config (host volume)
      - hotlog-logs:/app/logs    # External data store mount
    deploy:
      resources:
        limits:
          memory: 512M           # Initial v1 guardrail; tune with measured channel concurrency
```

### External Data Store Requirement

**Critical Design Decision**: All log and aggregate data must be persisted to the external sink configured when a channel is created, NOT within RabbitMQ or the Docker container's writable layer.

**Why External Storage?**
- Survives container restarts (stateless container, stateful storage)
- Allows horizontal scaling with shared datastore
- Enables zero-downtime deployments (swap containers without data loss)
- Simplifies backup strategies (back up datastore, not containers)

#### Connection String Pattern

When creating a channel, users specify the external datastore connection string:

```json
{
  "channel_id": "app.production.api",
  "severity_threshold": 4,
  "rabbitmq_config": {
    "host": "rabbitmq.example.com",
    "port": 5672,
    "username": "hotlog_user"
  },
  "aggregator": {
    "function": "average"
  },
  "sink_config": {                // External persistence sink connection
    "connection_string": "postgresql://user:pass@db.example.com:5432/hotlogs",
      "driver": "postgres",         // PostgreSQL in v1; future sinks include mysql, mssql, sqlite, seq, etc.
    "database": "hotlogs"
  }
}
```

**Datastore Responsibilities:**
- Raw log events (before aggregation)
- Aggregated results
- Historical rollups for compliance/archival
- Queryable historical data

The connection string in `sink_config.connection_string` points to the external sink where all persisted logs and aggregates will be written.

---

## Key Design Principles

### External Interfaces

- **Ingest Endpoint**: Accepts structured log events (JSON/Protobuf)
- **RabbitMQ Cluster**: Message broker for buffering, routing, retries, and dead-letter handling
- **Config Store**: External configuration management (files, database, or external API)

### Internal Components

1. **Hot Path Engine** - Rust core processing messages at minimum latency
2. **Channel Manager** - Dynamically creates/manages per-channel state
3. **Aggregation Engine** - Computes aggregates for all events; the threshold controls additional raw persistence
4. **Schema Registry** - Loads, caches, and validates runtime Protobuf descriptors

---

## Key Design Principles

| Principle | Description |
|-----------|-------------|
| **Zero-Downtime Channel Addition** | New log categories added without redeployment |
| **Per-Channel Autonomy** | Each channel has independent thresholds, destinations, aggregation rules |
| **Dynamic Schema Handling** | Runtime Protobuf descriptors loaded per message type |
| **Hot Path Optimization** | Rust core handles classification and routing at maximum throughput |
| **Memory-Bounded Aggregation** | Safe owner-local buffers (50 MiB/3600s default) plus a 512 MiB process budget prevent memory bloat |

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

**Direct Path**: Message meets threshold → publish durably to RabbitMQ → also contribute to the channel's aggregate  
**Aggregation Path**: Every event with an aggregator contributes to the buffer → aggregate compute → publish summary → drop originals  
**No-Aggregator Path**: Message below threshold without an aggregator → immediate persistence to the configured external sink

Direct persistence is delivered through a durable RabbitMQ path. The ingest request is accepted only after publication confirmation; the consumer acknowledges a message only after the configured external sink confirms an idempotent write. Failed writes are republished with bounded exponential backoff and then routed to a durable dead-letter exchange. Every event has a stable event ID; aggregate windows have stable aggregate IDs so retries cannot create duplicate records.

---

## RabbitMQ Integration

### Channel Creation Strategy

- **Dynamic Provisioning**: RabbitMQ channels/queues/exchanges created per-channel definition
- **Deterministic Naming**: `<channel_prefix>_rabbit_<timestamp>/<hash>` ensures uniqueness while allowing reuse
- **Hot Reload**: New channel definitions activate immediately without restart
- **No Static Dependencies**: Startup doesn't depend on pre-existing queues

### Queue Topology

```
Channel Config → durable topic exchange → shared durable persistence queue → internal Tokio consumer → external sink

The consumer also publishes exhausted deliveries to the durable `distill.dead-letter` exchange and queue. Channel exchanges are declared and bound when channels are created, so REST-created channels use the same path as startup-configured channels.
```

---

## Protobuf Schema Generation

### Dynamic Schema Pipeline

1. **User Defines Structure** → Channel config includes message type definition
2. **On-First-Use Resolution** → Runtime descriptors are loaded and cached per channel using `prost-reflect`
3. **Validation Layer** → All payloads validated against schema before processing
4. **No Code Changes** → New message types handled transparently

### Schema Storage Strategy

- Per-channel schema registry (in-memory cache + optional disk persistence)
- Versioned descriptors with migration hooks for backward compatibility
- No runtime `protoc` process is required in v1
