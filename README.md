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
- Track latency, size, temperature, or throughput with rolling averages across all supported sinks.
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

The HTTP driver posts each event or aggregate as JSON to the configured URL. Custom headers can be supplied for authentication. Only a `2xx` response acknowledges delivery. `409 Conflict` means an identity/revision conflict and is never a duplicate-success response. See the receiver contract below.

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

HTTP receivers atomically apply the contract below using `X-Distill-Record-Id`, `X-Distill-Record-Type`, and `X-Distill-Update-Revision`. Distill computes rolling snapshots; receivers do not merge windows themselves.

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

### 1. Start unconfigured Distill and RabbitMQ

Requirements:

- Docker with Compose, or Podman 5.0+ with a Compose provider;
- PostgreSQL, MySQL, SQL Server, or an HTTP receiver for the configured sink.

Start the local service with either runtime:

```powershell
docker compose up --build
# or
podman compose up --build
```

Both runtimes use the same `Dockerfile` and `docker-compose.yml`; no application changes are required.

Compose publishes Distill through Caddy at `https://localhost`; it is the only publicly exposed Distill service. Caddy uses a local development certificate for `localhost`, so trust its local CA or use an HTTP client’s development-only certificate bypass. Set `DISTILL_PUBLIC_HOST` to a real DNS name for automatic ACME certificates in a production deployment. Administrative routes are not published, and RabbitMQ’s ports are bound to host loopback only.

The Compose file uses separate development keys for ingestion and administration. Replace both before using the service outside a local test environment.

Compose intentionally includes no database or default sink. It remains unready and rejects logs until a sink-backed channel is configured.

### 2. Register a sink and channel at runtime

Applications can provision configuration during deployment/startup through the private admin listener. No Distill restart is required. This HTTP example needs an existing conforming receiver; its host/port must pass the configured outbound destination policy.

```http
POST http://127.0.0.1:8081/sinks
X-API-Key: your-config-key
Content-Type: application/json

{
  "name": "payments-receiver",
  "version": "1",
  "config": {
    "driver": "http",
    "connection_string": "https://collector.example.com/records",
    "headers": { "Authorization": "Bearer receiver-secret" }
  }
}
```

The response contains `sink_id: "payments-receiver@1"`. Submit secrets over a protected connection; they are not returned by configuration reads. The receiver must be reachable from the Distill container. Registering identical content again succeeds; changing content under the same sink identity returns `409`. Use a new version for a new destination.

```http
POST http://127.0.0.1:8081/channels
X-API-Key: your-config-key
Content-Type: application/json

{
  "name": "orders.production",
  "version": "2026.11",
  "preset": "count",
  "sink_id": "payments-receiver@1"
}
```

Channel name plus version is immutable. Identical declarations reconcile safely; changed configuration requires a new version. Producers explicitly send `version` alongside `channel_id`; old and new versions coexist. Use `?version=2026.11` on channel configuration/lifecycle/flush admin routes. Unversioned channels remain supported as a separate identity, without implicit latest-version routing.

For database sinks, run explicit schema migrations before registration:

```sh
distill sink migrate postgres-primary --file sinks.json
```

See [the CLI guide](docs/cli.md) for optional startup manifests and secret references. There is no required default sink. Runtime configuration is persisted in Distill's internal state store.

### A simpler channel request

For a friendlier self-hosted setup, configure `default_sink_id` in the sink manifest. Distill will generate the RabbitMQ exchange and routing key from the channel name and apply the selected preset:

```powershell
$env:DISTILL_SINKS_CONFIG_PATH = "./sinks.json"
```

Then create a channel with only its intent and any overrides you need:

```powershell
$request = '{
  "name": "checkout.latency",
  "preset": "metrics"
}'

Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8081/channels `
  -Headers @{ "X-API-Key" = "change-me-admin-in-development" } `
  -ContentType "application/json" `
  -Body $request
```

Available presets are `raw_events`, `errors_only`, `metrics`, `audit`, and `count`. Presets are starting points, not restrictions: send `severity_threshold`, `aggregator`, buffering, ordering, or other supported properties to override them. Set `"aggregator": null` explicitly when a preset’s aggregation should be disabled. Startup configuration is optional. Runtime definitions, versions and lifecycle state survive process/container replacement when the state volume is retained.

Use `POST /channels/validate` with the same request shape to resolve and validate a channel without registering it.

### 3. Send a structured event

```powershell
$event = @'
[
  {
    "event_id": "0c0c8e36-0f93-4d42-8c2d-b4faec8d6f17",
    "timestamp": "2026-08-02T15:04:05Z",
    "channel_id": "orders.production",
    "version": "2026.11",
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

Ingestion and metrics requests require the ingest key and use the public listener. Management requests require the configured admin key (the ingest key by default) and are only served by the loopback-only administrative listener. Invalid or missing credentials return `401 Unauthorized`.

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Process liveness, including idle startup |
| `GET` | `/ready` | Active channel and broker-consumer/publisher readiness |
| `POST` | `http://127.0.0.1:8081/sinks` | Register an immutable versioned destination |
| `GET` | `/metrics` | Prometheus-style service metrics; requires the ingest key |
| `POST` | `/ingest` | Ingest one or more JSON events |
| `POST` | `/ingest` with Protobuf | Ingest a Protobuf `IngestRequest` |
| `POST` | `/v1/logs` | Ingest OTLP/HTTP JSON log records |
| `POST` | `http://127.0.0.1:8081/channels` | Create a channel from a preset and overrides |
| `POST` | `http://127.0.0.1:8081/channels/validate` | Validate and resolve a channel request |
| `PUT` | `http://127.0.0.1:8081/channels/{channel_id}` | Reconcile an immutable channel definition |
| `GET` | `http://127.0.0.1:8081/channels/{channel_id}/config` | Inspect sanitized effective configuration |
| `POST` | `https://localhost/channels/{channel_id}/events` | Ingest one simple event for a channel |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/test` | Preview validation and routing for an event |
| `POST` | `http://127.0.0.1:8081/channels/{channel_id}/schema` | Legacy unversioned schema registration; include schema in versioned declarations |
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

`event_id` is optional, but a stable UUID is required for retry-safe contributions. Reuse the entire original event, including timestamp, on retry. They allow idempotent retries when a response is lost after successful admission. `metadata` is optional and is a string-to-string map.

### OpenTelemetry Logs

Distill also accepts OTLP/HTTP JSON at `POST /v1/logs`. Supply the destination through `X-Distill-Channel` or the OTLP resource attribute `distill.channel`. Set `distill.version` for versioned channels and `distill.event_id` to a stable UUID for retry-safe OTLP requests. OTLP resource and record attributes are preserved as structured event properties, and trace/span identifiers are retained in metadata.

```http
POST /v1/logs
Content-Type: application/json
X-API-Key: change-me-ingest-in-development
X-Distill-Channel: orders.production
```

The adapter feeds the same validation, severity routing, aggregation, RabbitMQ delivery, and PostgreSQL persistence path as native ingestion.

### Protobuf ingestion

Send `application/protobuf` or `application/x-protobuf` containing the `IngestRequest` envelope defined in `src/core/proto.rs`. Protobuf is useful when producers already use generated contracts or when compact wire representation matters.

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
6. The consumer acknowledges after an idempotent sink write succeeds or after a confirmed handoff to retry/DLQ storage.

If the configured sink is temporarily unavailable, messages move through durable quorum TTL retry queues configured for RabbitMQ at-least-once dead-lettering; consumer tasks never sleep while holding the shared delivery path. Exhausted or malformed deliveries move to the durable `distill.dead-letter` exchange and queue with bounded failure metadata and original broker bytes. Use `distill dlq list` to inspect metadata and `distill dlq replay --id <message-id>` or `--all` only after destination recovery. Replay is intentionally limited to valid, exhausted direct-persistence records; poison and aggregate-input records remain retained because replaying their unchanged bytes is unsafe. Default CLI output never prints payloads.

Delivery, retry, dead-letter, and aggregate-generation queues cap at 1,000,000 messages or 1 GiB with `reject-publish`. A full queue rejects publication, so ingest returns non-success instead of `202`. Configure RabbitMQ disk alarms and alert on queue depth, oldest-message age, DLQ count, and broker disk watermark; a full DLQ leaves original delivery recoverable rather than discarding failure.

Direct event and aggregate-output sink writes are idempotent by their stable record IDs. Producers should retry an event using the same `event_id` when a request times out. Source IDs and content fingerprints are committed atomically with aggregate checkpoints in SQLite before source acknowledgement. Pending aggregate snapshots use a durable outbox and duplicate-safe sink updates. Aggregate-input CLI replay remains disabled pending its separate recovery contract; export is available.

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
| `DISTILL_ADMIN_API_KEY` | No | Management key; defaults to the ingest key. A separate key is optional. |
| `DISTILL_STATE_PATH` | No | Durable internal SQLite path; default `data/state.sqlite3`, image `/var/lib/distill/state.sqlite3` |
| `DISTILL_STATE_MAX_BYTES` | No | Main SQLite DB cap, default 1 GiB; WAL/journal and filesystem overhead require additional disk |
| `DISTILL_MAX_CHANNELS` / `DISTILL_MAX_SINKS` | No | Retained channel/version and registered-sink ceilings; default 500 each |
| `DISTILL_AUTO_MIGRATE` | No | Default `false`; explicit migrations required. `true` is for development fixtures. |
| `DISTILL_SINK_ALLOWLIST` | No | Explicit HTTP destination host:port/CIDR policy for private targets |
| `DISTILL_BIND_ADDRESS` | No | Public ingestion/metrics bind address; defaults to `127.0.0.1:8080` |
| `DISTILL_ADMIN_BIND_ADDRESS` | No | Administrative bind address; defaults to loopback `127.0.0.1:8081` |
| `DISTILL_ALLOW_ADMIN_NETWORK` | No | Set `true` only for a private network endpoint such as Aspire service discovery; permits a non-loopback administrative bind |
| `DISTILL_REQUEST_BODY_LIMIT_BYTES` | No | Maximum HTTP request body size; defaults to `2097152`, `0` disables |
| `DISTILL_REQUEST_TIMEOUT_SECONDS` | No | Maximum HTTP request duration; defaults to `30`, `0` disables |
| `DISTILL_SHUTDOWN_TIMEOUT_SECONDS` | No | Maximum confirmed aggregate flush duration; defaults to `30` and must be positive |
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

## HTTP receiver contract

All deliveries carry `X-Distill-Contract-Version: 1`, a record type (`event` or `aggregate`), record ID, and update revision. The JSON body is the event or aggregate. A receiver must commit before returning success:

- Events: first ID stores the event; identical repeated content succeeds; different content under the same ID returns `409`. Use globally unique event UUIDs, including across channels sharing a destination.
- Aggregates: `aggregate_id` is stable for a rolling series, `update_revision` increases, and the body is a complete cumulative snapshot. Atomically replace only with a newer revision. Equal revision/equal content and older revisions succeed without changing stored state. Equal revision/different content returns `409`.
- Return `2xx` only after these effects are durable. `408`, `429`, transport failures and `5xx` are retried; other `4xx` fail into the DLQ. A receiver must never return success for an unapplied required write.

Run the standard-library reference receiver:

```sh
DISTILL_RECEIVER_TOKEN=dev-token python3 examples/http_receiver.py --help
```

It provides authenticated `POST /records` and `GET /records/{id}` plus `/health` using SQLite. GET is a reference/testing inspection endpoint; Distill's current write contract does not require receivers to host its configuration or recovery catalog. Developer-owned implementations can use any storage that meets the observable contract. Run `scripts/test-sink-contracts.sh` for the PostgreSQL, MySQL, SQL Server and reference HTTP contract suite. `scripts/test-http-only.sh` exercises the actual server with runtime registration, rolling snapshots and restart/deduplication using only RabbitMQ and the HTTP receiver.

## State, recovery and V1 deployment limits

Run one active Distill instance per dedicated RabbitMQ vhost. Retain both RabbitMQ data and Distill's private state volume during container replacement. The state directory must be owned by the service user with no group/other access; new directories/files are created with restrictive permissions. It contains resolved sink credentials, configuration, processed event identities, checkpoints and pending outputs. Back it up as sensitive transactional state together with the broker; do not delete it as a cache. A file lock rejects concurrent processes sharing the same SQLite state.

Deduplication history currently has no automatic age eviction. Its guarantee lasts while that state is retained; restoring an older database or deleting state ends that guarantee for omitted IDs. Capacity exhaustion fails processing visibly and keeps source work recoverable through broker retry/DLQ. Monitor state size and broker/DLQ depth. The 1 GiB main-database ceiling is a safety limit, not a measured workload recommendation; allow extra WAL/journal and disk headroom.

SQLite WAL requires local-disk-compatible persistence. Azure Container Apps/Azure Files compatibility is **not validated** and remains a production release gate. Multiple logger replicas, automatic revision retirement, retention/compression, and cross-version aggregate merging are deferred. Old channel versions and their queues remain retained and count toward configured caps. Credential rotation and stronger secret storage are a fast follow; current confidentiality relies on protected transport, private storage and safe diagnostics.

The current changes are under verification. See [the readiness handoff](docs/production-readiness-handoff.md) for executed evidence and remaining gates before declaring V1 production ready. Existing pre-V1 rolling delta messages are incompatible with cumulative snapshots: drain them with the old revision before upgrading. There is no automatic queue/schema/data downgrade.

## Build and test locally

Run the Rust test suite:

```powershell
cargo test
```

Build the production container with either runtime:

```powershell
docker build -t distill .
# or
podman build -t distill .
```

The container listens on port `8080` and runs as an unprivileged `distill` user.

### Real Aspire application qualification

The local DistillTesting sample can run managed PostgreSQL, application-registered HTTP-only, and external-reference scenarios. Its AppHost requires `UserSecretsId` so generated credentials survive local runs. Set a unique `VerificationRun` for isolated test volumes and `VerificationOutput` to a JSON path inside a private directory. `VerificationSink=http` selects HTTP-only mode. The sample enables verification endpoints only in Development.

After starting the updated AppHost, run:

```sh
python3 scripts/test-aspire-application.py --endpoints /private/endpoints.json --output /private/evidence.json
```

The driver validates both producer versions and persisted values, restarts the isolated logger/broker, and exercises concurrent registration/ingestion. `--previous-output` additionally checks recovered data and retries old IDs. The optional Playwright driver `scripts/test-aspire-ui.cjs` clicks the actual counter and renders weather; it requires an installed Playwright module/browser. See the [qualification handoff](docs/production-readiness-handoff.md) and [safe evidence summary](docs/production-readiness-local-evidence.json) for the executed local scenarios and remaining release gates.

Back up broker, SQLite state, destinations and stable credential configuration coherently. A tested local procedure stops the deployment, exports the volumes, saves their **container UID/GID/mode**, and imports into new volumes before restoring that metadata and starting the same resource identities. The tested Podman export omitted volume-root metadata; preserve it separately. Keep archives and credential backups private. Production-target/off-host restore still needs its own rehearsal.

## When Distill is a good fit

Choose Distill when you want:

- structured events instead of unsearchable text blobs;
- explicit control over what is stored in detail;
- compact aggregate metrics derived from high-volume event streams;
- per-channel processing policies;
- ownership of persisted data in a supported sink;
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
