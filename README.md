# Distill

### Keep the signal. Lose the noise.

Distill is a self-hosted, structured logging service for teams that want more control than “store everything forever” and more insight than “grep a mountain of text later.”

Send Distill the events that matter. Keep important events intact. Distill the rest into meaningful, durable aggregate metrics—counts, averages, minimums, maximums, sums, and event-duration measurements—then persist the result in a supported sink.

It is a focused place between application logging and long-term observability:

```text
application events
        │
        ▼
   Distill channels
        │
        ├── important events → preserved individually
        ├── routine events   → distilled into aggregates
        └── durable delivery → configured sink
```

## Why Distill?

Logs are valuable because they contain detail. Logs are expensive because they contain detail.

Distill lets you decide, channel by channel, what deserves a permanent seat at the table:

- Keep every error and critical event for investigation.
- Preserve warnings while turning routine activity into compact counts.
- Track latency, size, temperature, or throughput with rolling averages.
- Measure the minimum, maximum, sum, or average of a numeric property.
- Calculate the average time between events.
- Store everything for an audit stream when completeness matters more than compression.

The result is a logging system that can answer both questions that matter:

> “What exactly happened?”

and:

> “What was happening overall?”

Distill is especially useful for services with high-volume, repetitive events where retaining every ordinary event would be wasteful, but losing operational trends would be worse.

## The core idea: channels with intent

A channel is a named stream with its own retention and processing behavior. You decide what each channel means, then give it a policy.

For example:

| Channel | Policy | Result |
|---|---|---|
| `orders.production` | Warning threshold + count | Errors remain searchable; routine order activity becomes a compact metric |
| `checkout.latency` | Average per minute | One-minute latency trends instead of millions of request events |
| `security.audit` | No aggregator | Every event is retained individually |
| `workers.jobs` | Maximum and count | Spot spikes and failures without storing every heartbeat |

Each event remains a structured JSON value with application-defined properties. Distill does not require your business data to fit a tiny predefined schema.

## What Distill does

### Intentional persistence

A channel without an aggregator persists every event. A channel with an aggregator can preserve events at or above a severity threshold while aggregating lower-severity events.

With a warning threshold:

```text
trace/debug/info       → aggregate
warning/error/critical → persist individually and aggregate
```

This is not blind log deletion. It is an explicit policy for deciding which detail to preserve and which detail to summarize.

### Meaningful aggregation

Built-in aggregators include:

- `count` — how many events occurred;
- `average` — average numeric `payload.value`;
- `sum` — total numeric `payload.value`;
- `min` and `max` — numeric bounds;
- `average_duration_between_events` — average interval between event timestamps.

Aggregates can be emitted for the overall active window, every N events, or every T seconds. Rolling aggregation can merge successive windows into one expanding series.

### Durable, bounded delivery

Distill uses RabbitMQ as a durable delivery boundary and a per-channel sink as the persistence destination. Built-in drivers are `postgres`, `mysql`, `sqlserver` (also accepted as `mssql`), and `http`.

An event is accepted only after RabbitMQ confirms publication. The internal consumer acknowledges delivery only after the configured sink confirms an idempotent write. Transient failures are retried with bounded backoff; exhausted deliveries go to a durable dead-letter path for investigation.

### Sink configuration

Channels select their sink when they are created:

```json
{
  "name": "payments",
  "sink_config": {
    "driver": "mysql",
    "connection_string": "mysql://user:password@db/logs"
  }
}
```

The HTTP driver posts each event or aggregate as JSON to the configured URL. Custom headers can be supplied for authentication. A `2xx` response, or `409 Conflict` for an already-known record, acknowledges delivery; other responses are retried or dead-lettered by the normal delivery path.

```json
{
  "name": "payments",
  "sink_config": {
    "driver": "http",
    "connection_string": "http://payments-sink:8081/distill",
    "headers": {
      "Authorization": "Bearer replace-me"
    }
  }
}
```

HTTP receivers should deduplicate using `X-Distill-Record-Id`. The record type is supplied in `X-Distill-Record-Type` as either `event` or `aggregate`.

The service also uses bounded mailboxes, buffers, and process memory accounting. A database outage should create controlled backpressure and a visible delivery problem—not an unbounded memory spiral.

### Custom properties, without compromise

Your application owns the event shape:

```json
{
  "event_id": "0c0c8e36-0f93-4d42-8c2d-b4faec8d6f17",
  "timestamp": "2026-08-02T15:04:05Z",
  "channel_id": "orders.production",
  "severity_level": "error",
  "payload": {
    "message": "Payment authorization failed",
    "order_id": "ord-123",
    "payment_provider": "stripe",
    "retryable": true,
    "latency_ms": 842
  },
  "metadata": {
    "service": "checkout-api",
    "host": "checkout-01",
    "correlation_id": "trace-abc"
  }
}
```

`payload` may contain arbitrary JSON. `metadata` is available for string-valued operational context. Schemas are optional: begin with flexible JSON, then add runtime validation when a channel’s contract becomes stable.

## Quick start

### 1. Start Distill and RabbitMQ

Requirements:

- Docker and Docker Compose;
- PostgreSQL, MySQL, SQL Server, or an HTTP receiver for the configured sink.

Start the local service:

```powershell
docker compose up --build
```

Compose publishes Distill through Caddy at `https://localhost`; it is the only publicly exposed Distill service. Caddy uses a local development certificate for `localhost`, so trust its local CA or use an HTTP client’s development-only certificate bypass. Set `DISTILL_PUBLIC_HOST` to a real DNS name for automatic ACME certificates in a production deployment. Administrative routes are not published, and RabbitMQ’s ports are bound to host loopback only.

The Compose file uses separate development keys for ingestion and administration. Replace both before using the service outside a local test environment.

### 2. Configure a sink with the CLI

The CLI writes a sink manifest without storing database credentials in it. Set the secret in the environment used by the Distill process:

```powershell
$env:DISTILL_POSTGRES_URL = "postgresql://distill:password@host.docker.internal:5432/logs"
distill sink create postgres-primary `
  --driver postgres `
  --connection-string-env DISTILL_POSTGRES_URL `
  --file sinks.json `
  --default
```

Point the service at the manifest:

```powershell
$env:DISTILL_SINKS_CONFIG_PATH = "./sinks.json"
```

For an advanced/custom integration, use the HTTP sink and keep its bearer token outside the manifest:

```powershell
$env:DISTILL_COLLECTOR_TOKEN = "replace-me"
distill sink create custom-http `
  --driver http `
  --url https://collector.example.com/distill `
  --bearer-token-env DISTILL_COLLECTOR_TOKEN `
  --file sinks.json `
  --default
```

See [the complete CLI guide](docs/cli.md) for secret files, deployment manifests, sink listing, and security guidance.

### 3. Create a channel

Channels reference a named sink, or use the configured default when `sink_id` is omitted. Credentials and connection strings do not belong in channel definitions.

```powershell
$channel = @'
{
  "channel_id": "orders.production",
  "severity_threshold": "warning",
  "rabbitmq_config": {
    "connection_string": "amqp://guest:guest@rabbitmq:5672/%2f",
    "exchange_name": "distill.orders.production",
    "routing_key_prefix": "orders."
  },
  "sink_id": "postgres-primary",
  "aggregator": {
    "function": "count",
    "granularity": "per_t_time",
    "parameter": 60,
    "rolling": false
  },
  "retention_hours": 720,
  "compression_enabled": false,
  "buffer_bytes": 52428800,
  "ttl_seconds": 3600,
  "mailbox_capacity": 1024,
  "ordering": "first_come_first_served",
  "allowed_lateness_ms": 5000
}
'@

Invoke-WebRequest `
  -Method Put `
  -Uri http://127.0.0.1:8081/channels/orders.production `
  -Headers @{ "X-API-Key" = "change-me-admin-in-development" } `
  -ContentType "application/json" `
  -Body $channel
```

The channel setup connects to the named sink, initializes the required tables when applicable, registers the RabbitMQ exchange, and starts the channel actor.

### A simpler channel request

For a friendlier self-hosted setup, configure `default_sink_id` in the sink manifest. Distill will generate the RabbitMQ exchange and routing key from the channel name and apply the selected preset:

```powershell
$env:DISTILL_SINKS_CONFIG_PATH = "./sinks.json"
```

Then create a channel with only its intent and any overrides you need:

```powershell
$request = '{
  "name": "checkout.latency",
  "preset": "metrics",
  "retention_hours": 720
}'

Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8081/channels `
  -Headers @{ "X-API-Key" = "change-me-admin-in-development" } `
  -ContentType "application/json" `
  -Body $request
```

Available presets are `raw_events`, `errors_only`, `metrics`, `audit`, and `count`. Presets are starting points, not restrictions: send `severity_threshold`, `aggregator`, buffering, ordering, retention, or other supported properties to override them. Set `"aggregator": null` explicitly when a preset’s aggregation should be disabled.

Use `POST /channels/validate` with the same request shape to resolve and validate a channel without registering it.

### 3. Send a structured event

```powershell
$event = @'
[
  {
    "event_id": "0c0c8e36-0f93-4d42-8c2d-b4faec8d6f17",
    "timestamp": "2026-08-02T15:04:05Z",
    "channel_id": "orders.production",
    "severity_level": "info",
    "payload": {
      "message": "Order created",
      "order_id": "ord-123",
      "total_cents": 4999,
      "region": "us-central"
    },
    "metadata": {
      "service": "orders-api",
      "host": "orders-01"
    }
  }
]
'@

Invoke-RestMethod `
  -Method Post `
  -Uri https://localhost/ingest `
  -Headers @{ "X-API-Key" = "change-me-ingest-in-development" } `
  -ContentType "application/json" `
  -Body $event
```

Expected response:

```json
{
  "accepted": 1
}
```

`accepted` means the event was accepted into Distill’s durable delivery path. It does not promise that the PostgreSQL row is already queryable at the instant the HTTP response arrives.

For a single event, the channel-bound endpoint removes the repeated `channel_id` field and can generate the timestamp and event ID:

```powershell
$event = '{
  "severity": "error",
  "message": "Payment authorization failed",
  "attributes": {
    "order_id": "ord-123",
    "retryable": true
  },
  "metadata": {
    "service": "checkout-api"
  }
}'

Invoke-RestMethod `
  -Method Post `
  -Uri https://localhost/channels/orders.production/events `
  -Headers @{ "X-API-Key" = "change-me-ingest-in-development" } `
  -ContentType "application/json" `
  -Body $event
```

Use `POST /channels/{channel_id}/test` with this same simple event shape to see whether the event would be directly persisted, aggregated, or both without enqueueing it.

## Choosing a channel policy

### Persist every event

Use `"aggregator": null` when every event must be retained individually:

```json
"aggregator": null
```

This is a good fit for audit trails, security events, incident timelines, and low-volume business events.

### Count activity

```json
{
  "function": "count",
  "granularity": "per_t_time",
  "parameter": 300
}
```

This produces a count every five minutes. It is useful for request volume, job throughput, queue activity, and operational heartbeats.

### Track a numeric measure

```json
{
  "function": "average",
  "granularity": "per_t_time",
  "parameter": 60
}
```

The aggregator reads a finite numeric value from `payload.value`. For example:

```json
{
  "payload": {
    "value": 184.5,
    "unit": "milliseconds",
    "route": "/checkout"
  }
}
```

The same pattern supports `sum`, `min`, and `max`.

### Measure time between events

```json
{
  "function": "average_duration_between_events",
  "granularity": "per_n_events",
  "parameter": 100
}
```

This uses event timestamps rather than `payload.value`, making it useful for measuring intervals between jobs, state changes, or recurring signals.

### Preserve high-severity detail while aggregating routine traffic

Set a threshold such as `warning` and configure an aggregator. Every event contributes to the aggregate; warning, error, and critical events are also persisted individually.

That gives you a compact view of normal behavior and a detailed trail when something important happens.

## HTTP API

Ingestion and metrics requests require the ingest key and use the public listener. Management requests require the separate admin key and are only served by the loopback-only administrative listener. Invalid or missing credentials return `401 Unauthorized`.

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness check |
| `GET` | `/metrics` | Prometheus-style service metrics; requires the ingest key |
| `POST` | `/ingest` | Ingest one or more JSON events |
| `POST` | `/ingest` with Protobuf | Ingest a Protobuf `IngestRequest` |
| `POST` | `/v1/logs` | Ingest OTLP/HTTP JSON log records |
| `POST` | `http://127.0.0.1:8081/channels` | Create a channel from a preset and overrides |
| `POST` | `http://127.0.0.1:8081/channels/validate` | Validate and resolve a channel request |
| `PUT` | `http://127.0.0.1:8081/channels/{channel_id}` | Create or replace a channel |
| `GET` | `http://127.0.0.1:8081/channels/{channel_id}/config` | Inspect sanitized effective configuration |
| `POST` | `https://localhost/channels/{channel_id}/events` | Ingest one simple event for a channel |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/test` | Preview validation and routing for an event |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/schema` | Register or replace a runtime schema |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/pause` | Stop accepting events while retaining the channel |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/resume` | Resume a paused channel |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/flush` | Flush the active aggregate window |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/archive` | Flush and stop accepting events |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/clone` | Create a new channel from an existing definition |

### JSON ingestion

`POST /ingest` accepts a JSON array of events. Each event requires:

- `timestamp` — ISO-8601 UTC timestamp;
- `channel_id` — an existing channel;
- `severity_level` — `unknown`, `trace`, `debug`, `info`, `warning`, `error`, or `critical`;
- `payload` — any JSON value.

`event_id` is optional, but stable UUIDs are strongly recommended. They allow idempotent retries when a response is lost after successful admission. `metadata` is optional and is a string-to-string map.

### OpenTelemetry Logs

Distill also accepts OTLP/HTTP JSON at `POST /v1/logs`. Supply the destination through `X-Distill-Channel` or the OTLP resource attribute `distill.channel`. OTLP resource and record attributes are preserved as structured event properties, and trace/span identifiers are retained in metadata.

```http
POST /v1/logs
Content-Type: application/json
X-API-Key: change-me-ingest-in-development
X-Distill-Channel: orders.production
```

The adapter feeds the same validation, severity routing, aggregation, RabbitMQ delivery, and PostgreSQL persistence path as native ingestion.

### Protobuf ingestion

Send `application/protobuf` or `application/x-protobuf` containing the `IngestRequest` envelope defined in `src/proto.rs`. Protobuf is useful when producers already use generated contracts or when compact wire representation matters.

### Runtime schemas

Channels can register a runtime Protobuf descriptor at:

```http
POST /channels/{channel_id}/schema
```

When a schema is configured, Distill validates the incoming payload against it. A schema mismatch returns `400`; malformed events should not be retried indefinitely.

## Reliability model

Distill is designed around a clear handoff contract:

1. The HTTP layer validates and routes the event.
2. The channel actor classifies it for direct persistence, aggregation, or both.
3. Distill publishes the event or aggregate to RabbitMQ.
4. RabbitMQ confirms durable publication.
5. The internal consumer writes to the channel’s configured sink.
6. The consumer acknowledges only after an idempotent sink write succeeds.

If the configured sink is temporarily unavailable, messages are retried with bounded backoff. Exhausted messages are sent to the durable `distill.dead-letter` exchange and queue for operator investigation.

Because writes are idempotent, producers should retry an event using the same `event_id` when a request times out.

## Configuration

### HTTP resource controls

The server applies the following process-wide defaults to both listeners. Pass a value of `0` to disable an individual control. The same values can be provided through the listed environment variables for container deployments.

```powershell
distill `
  --request-body-limit-bytes 2097152 `
  --request-timeout-seconds 30 `
  --max-concurrent-requests 128 `
  --max-requests-per-minute 6000
```

- `--request-body-limit-bytes` / `DISTILL_REQUEST_BODY_LIMIT_BYTES`: 2 MiB default.
- `--request-timeout-seconds` / `DISTILL_REQUEST_TIMEOUT_SECONDS`: 30 seconds default.
- `--max-concurrent-requests` / `DISTILL_MAX_CONCURRENT_REQUESTS`: 128 default.
- `--max-requests-per-minute` / `DISTILL_MAX_REQUESTS_PER_MINUTE`: 6000 default, shared across the process in a rolling one-minute window.

Distill requires:

| Variable | Required | Description |
|---|---:|---|
| `RABBITMQ_URI` | Yes | Process-level RabbitMQ connection |
| `DISTILL_INGEST_API_KEY` | Yes | API key for public ingestion endpoints |
| `DISTILL_ADMIN_API_KEY` | Yes | Separate API key for loopback-only management endpoints; must differ from the ingest key |
| `DISTILL_BIND_ADDRESS` | No | Public ingestion/metrics bind address; defaults to `127.0.0.1:8080` |
| `DISTILL_ADMIN_BIND_ADDRESS` | No | Loopback-only administrative bind address; defaults to `127.0.0.1:8081` and rejects non-loopback addresses |
| `DISTILL_REQUEST_BODY_LIMIT_BYTES` | No | Maximum HTTP request body size; defaults to `2097152`, `0` disables |
| `DISTILL_REQUEST_TIMEOUT_SECONDS` | No | Maximum HTTP request duration; defaults to `30`, `0` disables |
| `DISTILL_MAX_CONCURRENT_REQUESTS` | No | Maximum simultaneous HTTP requests; defaults to `128`, `0` disables |
| `DISTILL_MAX_REQUESTS_PER_MINUTE` | No | Process-wide rolling request allowance; defaults to `6000`, `0` disables |
| `DISTILL_CONFIG_PATH` | No | JSON file containing startup channel definitions |
| `DISTILL_SINKS_CONFIG_PATH` | No | JSON sink manifest containing named sinks and secret references |
| `DISTILL_DEFAULT_SINK_CONNECTION_STRING` | No | Legacy PostgreSQL fallback for simplified channel requests |
| `DISTILL_EXCHANGE_NAME_PREFIX` | No | Generated exchange prefix; defaults to `distill` |

Startup configuration uses this shape:

```json
{
  "channels": [
    {
      "channel_id": "security.audit",
      "severity_threshold": "info",
      "rabbitmq_config": {
        "connection_string": "amqp://guest:guest@rabbitmq:5672/%2f",
        "exchange_name": "distill.security.audit",
        "routing_key_prefix": "security."
      },
      "sink_id": "postgres-primary",
      "aggregator": null,
      "retention_hours": 2160,
      "compression_enabled": false,
      "buffer_bytes": 52428800,
      "ttl_seconds": 3600,
      "mailbox_capacity": 1024,
      "ordering": "first_come_first_served",
      "allowed_lateness_ms": 5000
    }
  ]
}
```

Keep API keys and database credentials in environment variables, secret stores, or deployment configuration. Do not commit them to source control or place them in event payloads.

## Build and test locally

Run the Rust test suite:

```powershell
cargo test
```

Build the production container:

```powershell
docker build -t distill .
```

The container listens on port `8080` and runs as an unprivileged `distill` user.

## When Distill is a good fit

Choose Distill when you want:

- structured events instead of unsearchable text blobs;
- explicit control over what is stored in detail;
- compact aggregate metrics derived from high-volume event streams;
- per-channel processing policies;
- PostgreSQL ownership of the persisted data;
- a self-hosted deployment with familiar infrastructure;
- durable handoff and bounded behavior during downstream failures.

Distill may not be the right tool when you need a full hosted log-search product, arbitrary ad-hoc query analytics across every raw event, or a turnkey multi-tenant SaaS control plane. It is deliberately focused: decide what matters, preserve that detail, and distill the rest into numbers you can use.

## Roadmap direction

The communication surface is evolving toward a friendlier layered experience:

- simple channel creation with defaults and presets;
- full configuration as an advanced escape hatch;
- schema-optional channels with progressive validation;
- first-party client libraries;
- OpenTelemetry/OTLP ingestion;
- channel selection outside event payloads;
- richer responses and resolved configuration;
- generated integration snippets;
- sample-event testing and lifecycle operations.

The guiding goal is simple:

> Create a channel with intent. Send it rich events. Keep the detail that matters. Get the metrics that explain the rest.

## Documentation

- [.NET SDK](clients/dotnet/README.md)
- [.NET `ILogger` provider](clients/dotnet/Distill.Extensions.Logging/README.md)
- [Communication surface improvements](docs/communication_surface_improvements.md)
- [C#/.NET integration guide](docs/csharp_integration.md)
- [Technical specification](docs/technical_specification.md)
- [Architecture overview](docs/architecture_overview.md)
- [Error handling](docs/error_handling.md)
- [Development guidelines](docs/development_guidelines.md)
