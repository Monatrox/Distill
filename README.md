# Distill

### Keep the signal. Lose the noise.

Distill is a self-hosted structured logging and aggregation service written in Rust. Preserve important events individually, summarize routine activity, and deliver both to PostgreSQL, MySQL, SQL Server, or an HTTP receiver you control.

```text
Applications → authenticated HTTP → confirmed RabbitMQ admission
                                      ├─ individual events → sink
                                      └─ aggregate inputs → durable checkpoints → snapshots → sink
```

**Status:** V1 implementation, version `0.1.0`. Local and hosted CI cover Rust, .NET, real sink contracts, migrations, and HTTP-only recovery. Production-target storage/restore and further fault/soak testing remain release gates. Build from source for the setup below; it does not require a published image or NuGet package.

## Why Distill?

- Choose a policy for each channel: retain every event, or aggregate all events while retaining high-severity detail.
- Compute `count`, `average`, `sum`, `min`, `max`, and `average_duration_between_events`.
- Use cumulative rolling snapshots across all four supported sinks.
- Register sinks and immutable channel versions at runtime without restarting Distill. Old and new producer versions can coexist.
- Send native JSON, Protobuf, simple channel-bound events, or OTLP/HTTP JSON logs.
- Integrate through the .NET client, `ILogger` provider, and Aspire hosting/client helpers.
- Recover configuration, aggregate contributions, checkpoints, and pending outputs after process replacement.

Distill is a delivery and aggregation service. Query retained events and aggregates in your destination; it does not include a log-search UI or a general analytics API. SQLite holds internal recovery state, not your default log destination.

## Quick start

This local demo builds Distill and starts RabbitMQ plus the bundled authenticated HTTP receiver. No external database or locally installed Rust/.NET SDK is needed.

Requirements: Git, Docker with Compose or Podman with a Compose provider, a Bash-compatible shell and `curl`. On Windows, use WSL or Git Bash for these commands. Ports `8080`, `8081`, and `8765` must be free. The first build downloads images and Rust dependencies.

### 1. Start the demo

```sh
git clone https://github.com/Monatrox/Distill.git
cd Distill
docker compose -f examples/compose.quickstart.yml up --build -d
```

For Podman, use `podman compose` in place of `docker compose` throughout this walkthrough. The demo publishes all three HTTP endpoints on host loopback only:

| Address | Purpose | Development credential |
|---|---|---|
| `http://127.0.0.1:8080` | Ingestion, metrics, health and readiness | `X-API-Key: dev-key` for ingest/metrics |
| `http://127.0.0.1:8081` | Administration and OpenAPI | `X-API-Key: dev-key-admin` for management |
| `http://127.0.0.1:8765` | Reference receiver and record inspection | `Authorization: Bearer dev-receiver-token` |

These are disposable development credentials. Keep this plaintext demo on a trusted local machine; use protected transport and deployment-managed credentials for a real installation.

Wait for Distill to respond:

```sh
curl --fail --retry 20 --retry-delay 1 --retry-connrefused \
  http://127.0.0.1:8080/health
curl -i http://127.0.0.1:8080/ready
```

`/health` returns `200`. On a fresh demo, `/ready` returns `503`: starting with no configured channels is valid, but the service is not ready for ingestion yet.

### 2. Register a destination and channel

```sh
curl --fail-with-body -sS http://127.0.0.1:8081/sinks \
  -H 'X-API-Key: dev-key-admin' \
  -H 'Content-Type: application/json' \
  --data '{
    "name": "demo-receiver",
    "version": "1",
    "config": {
      "driver": "http",
      "connection_string": "http://receiver:8765/records",
      "headers": { "Authorization": "Bearer dev-receiver-token" }
    }
  }'
```

The response is `201` with `{"sink_id":"demo-receiver@1"}`. The URL uses the receiver's container hostname; `localhost` inside Distill would refer to the Distill container. The demo explicitly allowlists `receiver:8765` through `DISTILL_SINK_ALLOWLIST`.

Create a channel that preserves each event:

```sh
curl --fail-with-body -sS http://127.0.0.1:8081/channels \
  -H 'X-API-Key: dev-key-admin' \
  -H 'Content-Type: application/json' \
  --data '{
    "name": "orders.production",
    "version": "1",
    "preset": "raw_events",
    "sink_id": "demo-receiver@1"
  }'

curl --fail --retry 20 --retry-delay 1 http://127.0.0.1:8080/ready
```

Channel creation returns `201` with the effective sanitized configuration. Readiness becomes `200` once its broker paths are healthy. Identical sink/channel declarations reconcile successfully; changing content under the same identity returns `409`. Use a new version for changed configuration.

### 3. Send an event and inspect its destination

```sh
curl --fail-with-body -sS http://127.0.0.1:8080/ingest \
  -H 'X-API-Key: dev-key' \
  -H 'Content-Type: application/json' \
  --data '[{
    "event_id": "00000000-0000-4000-8000-000000000001",
    "timestamp": "2026-11-14T00:00:00Z",
    "channel_id": "orders.production",
    "version": "1",
    "severity_level": "info",
    "payload": { "message": "Order created", "order_id": "ord-123" },
    "metadata": { "service": "orders-api" }
  }]'
```

A successful response has HTTP status `202` and this shape; `request_id` is generated per request:

```json
{
  "accepted": 1,
  "rejected": 0,
  "request_id": "00000000-0000-4000-8000-000000000000",
  "durability": "queued",
  "errors": []
}
```

`202` means RabbitMQ confirmed admission; it does not mean the sink has already committed the record. Inspect the receiver after delivery:

```sh
curl --fail-with-body -sS \
  -H 'Authorization: Bearer dev-receiver-token' \
  http://127.0.0.1:8765/records/00000000-0000-4000-8000-000000000001
```

Repeat that GET if it initially returns `404`. Retrying the identical ingest request preserves one destination record. Real producers should generate a unique event UUID and current timestamp once, then reuse the original ID, timestamp, version, and payload on retries.

For a single channel-bound event, put `version` in the body and omit `channel_id`:

```sh
curl --fail-with-body -sS http://127.0.0.1:8080/channels/orders.production/events \
  -H 'X-API-Key: dev-key' \
  -H 'Content-Type: application/json' \
  --data '{
    "version": "1",
    "event_id": "00000000-0000-4000-8000-000000000002",
    "timestamp": "2026-11-14T00:00:01Z",
    "severity": "error",
    "message": "Payment authorization failed",
    "attributes": { "order_id": "ord-123", "retryable": true }
  }'
```

Simple events can generate an ID and timestamp when omitted, but supplying stable values is necessary for retry-safe HTTP requests.

### 4. Stop or restart

```sh
docker compose -f examples/compose.quickstart.yml down
```

Named volumes retain RabbitMQ data, Distill recovery state, and receiver records. Starting the same Compose project again restores configuration and data; do not change retained broker credentials or project identity casually. `down --volumes` deliberately deletes the demo's stored data and ends its deduplication history.

For troubleshooting, use `docker compose -f examples/compose.quickstart.yml ps` and `logs distill rabbitmq receiver`.

## Choosing a channel policy

A channel without an aggregator persists every event, regardless of its severity threshold. When an aggregator is configured, **every event contributes to the aggregate**; events at or above the threshold are also persisted individually:

```text
warning threshold + aggregator:
trace/debug/info       → aggregate
warning/error/critical → individual event and aggregate
```

Presets supply defaults, which you can override when creating a new channel version:

| Preset | Aggregation | Also preserves individual events |
|---|---|---|
| `raw_events` | None | All events |
| `audit` | None | All events |
| `errors_only` | Count, 300-second window | Warning and above |
| `count` | Count, 300-second window | Info and above |
| `metrics` | Average, 60-second window | Error and above |

Despite its name, `errors_only` summarizes lower-severity events rather than dropping them. Set `"aggregator": null` explicitly to disable a preset's aggregator. A channel has one aggregate function; use separate channels for separate measures.

For example, register another channel against the same sink:

```sh
curl --fail-with-body -sS http://127.0.0.1:8081/channels \
  -H 'X-API-Key: dev-key-admin' \
  -H 'Content-Type: application/json' \
  --data '{
    "name": "checkout.latency",
    "version": "1",
    "preset": "metrics",
    "sink_id": "demo-receiver@1",
    "aggregator": {
      "function": "average",
      "granularity": "per_n_events",
      "parameter": 2,
      "rolling": true
    }
  }'
```

Send events with a finite numeric `payload.value`, such as `{"value":184.5,"unit":"milliseconds"}`. The `average`, `sum`, `min`, and `max` functions read this field; `count` counts events, and `average_duration_between_events` uses event timestamps.

Granularities are `per_n_events`, `per_t_time` (parameter in seconds), and `overall`. Time windows are measured from the first event timestamp rather than aligned calendar buckets. Explicit flush, shutdown, buffer/TTL handling can also emit a window; an `overall` window is not an unlimited retention policy. Rolling mode produces a complete cumulative snapshot with a stable aggregate ID and increasing revision. All four sinks support this contract.

## HTTP API

The default native-process listeners are `127.0.0.1:8080` for ingestion/operations and `127.0.0.1:8081` for administration. The demo publishes equivalent loopback host ports. A non-loopback administrative bind requires `DISTILL_ALLOW_ADMIN_NETWORK=true`; keep it private.

| Listener | Method | Path | Purpose |
|---|---|---|---|
| Public | `GET` | `/health` | Liveness; no key required |
| Public | `GET` | `/ready` | Active configuration and broker readiness; no key required |
| Public | `GET` | `/metrics` | Metrics; ingest key required |
| Public | `POST` | `/ingest` | Native JSON array or Protobuf ingestion |
| Public | `POST` | `/channels/{channel_id}/events` | One simple event |
| Public | `POST` | `/v1/logs` | OTLP/HTTP JSON logs |
| Admin | `POST` | `/sinks` | Register an immutable versioned destination |
| Admin | `POST` | `/channels` | Create/reconcile a channel using a preset and overrides |
| Admin | `POST` | `/channels/validate` | Preview configuration validation without registration |
| Admin | `PUT` | `/channels/{channel_id}` | Reconcile a complete explicit channel definition |
| Admin | `GET` | `/channels/{channel_id}/config` | Read sanitized effective configuration |
| Admin | `POST` | `/channels/{channel_id}/test` | Preview event validation/routing without enqueueing |
| Admin | `POST` | `/channels/{channel_id}/schema` | Legacy unversioned schema registration |
| Admin | `POST` | `/channels/{channel_id}/pause`, `/resume`, `/flush`, `/archive`, `/clone` | Channel lifecycle operations |
| Admin | `GET` | `/swagger-ui`, `/openapi.json` | Interactive API documentation and contract |

Ingestion uses `X-API-Key` with the ingest key. Management uses the admin key, which defaults to the ingest key if not configured separately. Missing/invalid credentials return `401`, with throttling after repeated authentication failures. OpenAPI documentation is on the private admin listener.

Use `X-API-VERSION: 1` to pin the API contract. This is separate from a channel's `version`. Channel name plus version selects an immutable definition. Native and simple events carry `version` in their bodies; configuration, test, lifecycle, and clone operations select it with `?version=1`. Omitting the version selects the distinct unversioned channel, never the latest version.

Native JSON requires `timestamp`, `channel_id`, `severity_level`, and `payload`. Valid severities are `unknown`, `trace`, `debug`, `info`, `warning`, `error`, and `critical`. `payload` may be any JSON value; `metadata` is an optional string-to-string map. Use an explicit stable UUID in `event_id` for retries.

Batches return `202` when all items are admitted, or `207` with accepted/rejected counts and indexed errors when admission is partial. Retry only rejected transient items with their original content. In the current implementation an empty or completely rejected batch returns `400`; inspect the error body for the underlying cause. Request parsing, authentication, and resource limits can reject a request before batch processing.

For OTLP, use `X-Distill-Channel` or resource attribute `distill.channel`. Supply `distill.version` for versioned channels and a stable per-record `distill.event_id` for retry-safe records. The adapter requires `resourceLogs`, `scopeLogs`, and record timestamps (`timeUnixNano` or `observedTimeUnixNano`), and returns Distill's ingest response. It supports OTLP/HTTP JSON, not gRPC or binary OTLP. All supported sinks receive the resulting events.

Native Protobuf uses `application/protobuf` or `application/x-protobuf` with the envelope defined in [src/core/proto.rs](src/core/proto.rs). For payload validation, include a `message_schema` descriptor in the versioned channel declaration. The legacy schema endpoint only applies to unversioned channels; changing a versioned schema requires a new channel version.

## .NET and Aspire

`Distill.Client` targets .NET 8; `Distill.Extensions.Logging` targets .NET 8/9/10; `Distill.Aspire` targets .NET 9/10 and uses Aspire 13.6.5. Use project references for source development, or build the `0.1.0` packages into a local/private feed. The setup does not assume public NuGet availability.

With `Distill.Client` referenced, this example uses the running demo:

```csharp
using Distill.Client;

using var distill = DistillClient.Create(
    new Uri("http://127.0.0.1:8080/"),
    apiKey: "dev-key",
    adminApiKey: "dev-key-admin",
    adminEndpoint: "http://127.0.0.1:8081/");

await distill.ReconcileChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Version = "1",
    SinkId = "demo-receiver@1",
    Preset = ChannelPreset.RawEvents
});

var entry = new SimpleEvent
{
    Severity = DistillSeverity.Info,
    Message = "Order created",
    Payload = new { order_id = "ord-456" }
};
await distill.WriteAsync("orders.production", entry);
```

Reconciliation binds subsequent client writes to that explicit channel version. Reuse the `SimpleEvent` object on retry to retain its generated ID/timestamp. `EnsureChannelAsync` remains create-only. The logging provider performs ingestion only; reconcile destinations/channels explicitly before serving application traffic.

Aspire can build Distill with `.WithLocalBuild(...)`, provision a managed RabbitMQ resource and persistent volumes, and propagate public/admin endpoints and keys through `.WithReference(...)`. Configure a stable `UserSecretsId` for a local AppHost so generated credentials survive volume-preserving replacements. Native sink schemas need explicit migration unless development-only auto-migration is selected. See the [Aspire guide](clients/dotnet/Distill.Aspire/README.md) for container and external-instance modes.

## Configuration and other setup options

Only `RABBITMQ_URI` and `DISTILL_INGEST_API_KEY` are required for a native server process. Idle startup without a sink or channel is valid. The process does not automatically load a dotenv file: set variables in its environment or use your deployment's environment-file support. [.env.example](.env.example) is a starting point for native-process configuration, not a replacement for the demo's fixed Compose configuration.

| Variable | Default / purpose |
|---|---|
| `RABBITMQ_URI` | Required broker URI; use a dedicated vhost for each active Distill instance |
| `DISTILL_INGEST_API_KEY` | Required ingest/metrics credential |
| `DISTILL_ADMIN_API_KEY` | Defaults to the ingest key; a separate key is optional |
| `DISTILL_BIND_ADDRESS` | `127.0.0.1:8080` |
| `DISTILL_ADMIN_BIND_ADDRESS` | `127.0.0.1:8081` |
| `DISTILL_ALLOW_ADMIN_NETWORK` | `false`; explicitly permits a private non-loopback admin bind |
| `DISTILL_STATE_PATH` | Native `data/state.sqlite3`; image `/var/lib/distill/state.sqlite3` |
| `DISTILL_STATE_MAX_BYTES` | 1 GiB main DB cap; allow extra disk for WAL/journal and filesystem overhead |
| `DISTILL_MAX_CHANNELS` / `DISTILL_MAX_SINKS` | 500 each; retained channel versions count toward the channel cap |
| `DISTILL_AUTO_MIGRATE` | `false`; development-only automatic native sink DDL when enabled |
| `DISTILL_SINK_ALLOWLIST` | Explicit private HTTP destination host:port/CIDR policy |
| `DISTILL_REQUEST_BODY_LIMIT_BYTES` | 2 MiB; `0` disables |
| `DISTILL_REQUEST_TIMEOUT_SECONDS` | 30; `0` disables |
| `DISTILL_MAX_CONCURRENT_REQUESTS` | 128; `0` disables |
| `DISTILL_MAX_REQUESTS_PER_MINUTE` | 6,000 per process over a rolling minute; `0` disables |
| `DISTILL_SHUTDOWN_TIMEOUT_SECONDS` | 30; must be positive |
| `DISTILL_CONFIG_PATH` | Optional startup channel JSON file |
| `DISTILL_SINKS_CONFIG_PATH` | Optional named sink manifest with environment/file secret references |
| `DISTILL_DEFAULT_SINK` / `DISTILL_SINK__{NAME}__*` | Environment-based startup sinks, used by Aspire |
| `DISTILL_DEFAULT_SINK_CONNECTION_STRING` | Legacy PostgreSQL fallback for simplified channel requests |
| `DISTILL_EXCHANGE_NAME_PREFIX` | `distill` for generated exchanges |
| `DISTILL_API_DEFAULT_VERSION` | `1` |

Startup manifests must be present in the server's filesystem and their environment/file secret references must resolve in the server process. A configured default sink allows channel requests to omit `sink_id`; setting a variable in a client shell does not reconfigure an already running container. Runtime API registrations are persisted independently in SQLite.

Database drivers are `postgres`, `mysql`, and `sqlserver` (alias `mssql`). Before registration, use a [sink manifest and the explicit migration CLI](docs/cli.md):

```sh
cargo run --locked -- sink migrate postgres-primary --file sinks.json
```

Normal startup performs no automatic database schema migration by default. HTTP sinks require a receiver implementing the contract below; they do not require PostgreSQL.

### Root Compose stack with Caddy

The root `docker-compose.yml` is a separate development stack: `docker compose up --build` starts RabbitMQ, Distill and Caddy, with ingestion at `https://localhost`. Trust Caddy's local development CA before sending HTTPS requests. The stack has no receiver, database, default sink or startup channel.

Its admin listener is bound to loopback **inside the Distill container** and is not published on the host. Provision through a client sharing that network namespace, or explicitly configure a protected admin bind and loopback host mapping as demonstrated by the quick-start file. Do not send the demo's host-admin requests to the unmodified root stack.

`DISTILL_PUBLIC_HOST` configures Caddy's hostname. A real DNS name and reachable ports can enable ACME certificates, but choosing a hostname alone does not qualify this development stack for production. RabbitMQ's published root-stack ports are bound to host loopback.

## HTTP receiver contract

All deliveries include `X-Distill-Contract-Version: 1`, `X-Distill-Record-Type` (`event` or `aggregate`), and `X-Distill-Record-Id`. Aggregate deliveries also include `X-Distill-Update-Revision`; direct events do not carry that revision header. The body is the event or aggregate JSON.

A receiver must commit atomically before returning success:

- **Events:** first ID stores the event. Identical repeated content succeeds; conflicting content under the same ID returns `409`. Use globally unique event IDs across channels sharing a destination.
- **Aggregates:** the body is a complete snapshot. Apply a newer revision, accept equal revision/equal content and stale revisions without changing stored state, and reject equal revision/different content with `409`.
- Return `2xx` only after the required durable effect. `408`, `429`, transport failures and `5xx` are retried; other `4xx` are terminal and handed to the DLQ. `409` is never a duplicate-success response.

[examples/http_receiver.py](examples/http_receiver.py) implements this contract with Python's standard library and SQLite. Its authenticated `GET /records/{id}` is a demo inspection endpoint, not a required receiver API. Run it independently with `DISTILL_RECEIVER_TOKEN` set and `--database` pointing to private persistent storage; `--help` lists its options.

## Reliability and deployment limits

A `202` follows a mandatory persistent RabbitMQ publish confirmation. Aggregate inputs commit source identity/content fingerprints and contribution checkpoints in SQLite before acknowledgement. Pending aggregate outputs use a transactional outbox; sink revision handling absorbs duplicate or out-of-order snapshot delivery. Accepted work retains its original channel version and destination.

Transient destination failures move through durable quorum TTL retry queues; exhausted or malformed work goes to the durable DLQ. A shared persistence consumer can wait up to a sink operation timeout before handing off a failure, so a failing destination can temporarily delay other work. Delivery, retry, DLQ and generation queues are capped at 1,000,000 messages or 1 GiB with `reject-publish`. Monitor disk, queue depth/age, state capacity, retry/DLQ counts and delivery errors.

Use `distill dlq list` for metadata, `replay` for eligible exhausted event/aggregate-output records, and protected `export` for original bytes. General poison repair and unchanged aggregate-input replay are unsupported. Default inspection omits payloads; exports and backups can contain application secrets. See the [CLI guide](docs/cli.md) before recovery or queue maintenance.

- Run **one active Distill process per dedicated RabbitMQ vhost**, including deployment transitions. Multiple active replicas and zero-downtime logger replacement are deferred; overlapping producer versions are supported.
- Retain broker data and Distill's private SQLite state together. The state includes resolved destination secrets, configuration, deduplication identities, checkpoints and pending outputs. Preserve service-account ownership and restrictive permissions during backup/restore.
- Deduplication history has no automatic age eviction. Deleting state or restoring an older backup ends the guarantee for omitted IDs. Capacity failures fail closed; plan for state growth and WAL/disk headroom.
- SQLite WAL requires local-disk-compatible storage. Azure Container Apps/Azure Files compatibility and production-target restore remain unvalidated. Hosted CI and local restore evidence do not establish those deployment guarantees.
- Retention/compression, automatic version retirement, cross-version aggregate merging and multi-sink fan-out are deferred. Old versions and queues remain retained and count toward caps. Each channel targets one sink.
- `/health` establishes liveness. `/ready` checks active configuration and broker paths; it does not universally probe each sink. Destination failures appear through delivery errors/retries/DLQ.
- Pre-V1 rolling delta envelopes must drain under the old implementation before upgrade. Existing queues with incompatible declaration arguments require maintenance. There is no automatic queue/schema/data downgrade.

Use protected ingress/admin transport and deployment-managed credentials. Stronger secret storage and credential rotation remain follow-up work; current confidentiality relies on private storage, protected transport and redacted diagnostics. See [Security](docs/security.md).

## Build and test locally

CI uses Rust `1.98.0` with rustfmt and Clippy. Native builds need a C toolchain for bundled native dependencies. The Dockerfile builds with Rust `1.97` and needs no host Rust installation. For the full .NET contract/package suite, install the .NET 8, 9 and 10 SDKs.

```sh
rustup toolchain install 1.98.0 --profile minimal --component rustfmt --component clippy
rustup override set 1.98.0
cargo build --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

Four external-service contract tests are ignored by the ordinary Rust run. With Docker available, execute:

```sh
cargo fetch --locked
DISTILL_CONTAINER_ENGINE=docker bash scripts/test-sink-contracts.sh
DISTILL_CONTAINER_ENGINE=docker bash scripts/test-http-only.sh
DISTILL_CONTAINER_ENGINE=docker bash scripts/test-durable-aggregate-input.sh
```

Use `DISTILL_CONTAINER_ENGINE=podman` for Podman. The sink/HTTP harnesses use offline Cargo commands, so fetch dependencies first. The broker harness builds its own image. The Aspire application/browser drivers require a separate application fixture not shipped in this repository.

```sh
dotnet run --project clients/dotnet/Distill.Client.ContractTests
dotnet pack clients/dotnet/Distill.Client -c Release -o artifacts
dotnet pack clients/dotnet/Distill.Extensions.Logging -c Release -o artifacts
dotnet pack clients/dotnet/Distill.Aspire -c Release -o artifacts
bash scripts/test-packages.sh artifacts
```

To build only the service image:

```sh
docker build -t distill:local .
# Or: podman build -t distill:local .
```

The runtime image runs as unprivileged UID `10001`. Only `DISTILL_STATE_PATH` is configured by the image; set broker/key configuration and the appropriate listener binds when launching it. The tag-triggered release workflow gates GHCR image publication on CI/broker checks; NuGet packages are CI artifacts until explicitly published.

## Documentation

- [CLI guide](docs/cli.md)
- [.NET SDK](clients/dotnet/README.md)
- [.NET logging provider](clients/dotnet/Distill.Extensions.Logging/README.md)
- [Aspire integration](clients/dotnet/Distill.Aspire/README.md)
- [C#/.NET integration guide](docs/csharp_integration.md)
- [Security and credential handling](docs/security.md)
- [Documentation index](docs/README.md)

Distill is licensed under the [MIT license](LICENSE).
