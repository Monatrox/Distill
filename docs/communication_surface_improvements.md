# Distill Communication Surface Improvements

## Summary

Distill should make the common path—create a useful channel and write the first log—possible without requiring users to understand RabbitMQ, PostgreSQL connection strings, Protobuf descriptors, or Distill's internal routing model.

This document collects potential improvements to the ways users create channels and write logs. It is intended as a decision document: ideas are grouped by interaction surface, with benefits, trade-offs, and an implementation-oriented product direction.

## 1. Current friction

The current API is powerful but infrastructure-heavy.

### Channel creation

Creating or updating a channel currently requires a complete `ChannelDefinition`, including:

- a constrained channel identifier;
- a severity threshold;
- RabbitMQ connection and exchange settings;
- a PostgreSQL sink connection string;
- optional aggregation rules;
- optional runtime schema descriptors;
- buffer, TTL, mailbox, ordering, and lateness settings.

This is appropriate as an advanced configuration contract, but it is a difficult first-run experience. Most users want to answer a simpler question: “What kind of events should this channel retain?”

### Log writing

Writers currently need to:

- know the `/ingest` endpoint;
- include `channel_id` in every event;
- provide the expected JSON or Protobuf shape;
- understand the severity enum;
- supply the global `X-API-Key` header;
- implement batching, retries, and shutdown flushing themselves;
- understand that a successful response means durable handoff to Distill, not necessarily completed database insertion.

The current surface is documented in `src/http.rs`, `src/domain.rs`, `src/proto.rs`, and `docs/csharp_integration.md`.

## 2. Product principles

Any future communication surface should follow these principles:

1. **Easy first event** — a new user should reach a successful first write in minutes.
2. **Progressive disclosure** — simple users see presets and defaults; advanced users can still control every relevant setting.
3. **Infrastructure hiding** — RabbitMQ topology and sink connection details should not be required for ordinary channel creation.
4. **Safe defaults** — convenience features must not silently discard logs or weaken delivery guarantees.
5. **Interoperability** — use familiar logging standards and formats where possible.
6. **Operational clarity** — users should be able to tell whether an event was accepted, queued, persisted, aggregated, rejected, or quarantined.
7. **Least privilege** — writers should receive write access only to the channels they need.
8. **Stable advanced contract** — the full channel definition remains available for infrastructure-as-code and automation.

## 3. Ideas for channel creation

### 3.1 Opinionated channel presets

Offer templates such as:

- `raw-events` — persist every event individually;
- `errors-only` — persist warning, error, and critical events individually;
- `metrics` — aggregate numeric values;
- `audit` — retain every event with longer retention and stricter validation;
- `count` — count events over a configurable time window.

Example:

```json
{
  "name": "orders.production",
  "preset": "errors-only"
}
```

The service would expand the preset into a complete internal definition.

**Benefit:** removes the need to understand aggregation, thresholds, and low-level defaults.

**Trade-off:** presets become product contracts and must be versioned or clearly documented when their defaults change.

### 3.2 Separate user settings from infrastructure settings

Most users should not need to provide RabbitMQ exchange names, routing-key prefixes, or broker connection details. Those can be service-level settings, generated automatically, or managed by an administrator.

Simplified request:

```json
{
  "name": "orders.production",
  "mode": "errors-only",
  "retention": "30d"
}
```

Advanced users can retain an explicit infrastructure configuration mode.

**Benefit:** substantially reduces the number of concepts required at channel creation time.

**Trade-off:** infrastructure operators lose some per-channel control unless an advanced mode remains available.

### 3.3 Named destinations instead of raw connection strings

Replace ordinary use of embedded sink connection strings with named destinations:

```json
{
  "name": "orders.production",
  "sink": "production-logs"
}
```

The destination registry can resolve credentials through environment variables, a secret manager, or administrator-managed configuration.

**Benefits:**

- fewer secrets in API requests and configuration files;
- portable channel definitions;
- easier sink rotation;
- clearer ownership of infrastructure.

**Trade-off:** requires a destination-management concept and an administrative workflow.

### 3.4 Interactive channel wizard

Provide a web or CLI wizard that asks:

1. What should the channel be called?
2. What kind of data will it contain?
3. Should everything be retained or only important events?
4. Should values be aggregated?
5. How long should data be retained?
6. Where should data be stored?

Before submission, show a plain-language behavior preview such as:

> Stores errors individually, aggregates lower-severity events into five-minute counts, and retains data for 30 days.

**Benefit:** translates implementation settings into user outcomes.

**Trade-off:** a UI or guided CLI is additional product surface to maintain.

### 3.5 `POST /channels` for simple creation

Keep the current idempotent `PUT /channels/{channel_id}` for explicit configuration management, but add a simpler create operation:

```http
POST /channels
```

```json
{
  "name": "orders.production",
  "preset": "raw-events"
}
```

Return the created channel, its effective configuration summary, and instructions for sending events.

### 3.6 Human-readable names, descriptions, and metadata

Keep the stable machine identifier, but add:

- display name;
- description;
- owning team;
- service name;
- environment;
- application or repository URL;
- aliases;
- creation and last-modification metadata.

This prevents users from having to memorize identifiers such as `app.production.api`.

### 3.7 Hierarchical defaults

Allow defaults to be inherited at several levels:

```text
global defaults
  → organization defaults
    → environment defaults
      → channel overrides
```

For example, production channels could inherit 90-day retention and a shared destination while an audit channel overrides retention and validation.

**Benefit:** makes large-scale configuration manageable.

**Trade-off:** inheritance can make effective configuration harder to understand. Always expose a fully resolved configuration view.

### 3.8 Create from the first event

For development environments, an unknown channel could return a proposed definition:

```json
{
  "error": "channel_not_found",
  "channel": "orders.production",
  "suggested_creation": {
    "preset": "raw-events"
  }
}
```

An optional development-only auto-create mode could create channels on demand. Production should require explicit creation or an approved policy.

### 3.9 Progressive schema adoption

Allow channels to start without a schema and introduce validation later:

1. create a schemaless channel;
2. observe real payloads;
3. generate or upload a schema;
4. run in warn-only mode;
5. enforce validation when ready.

Useful schema modes:

- disabled;
- observe;
- validate and reject;
- validate and quarantine.

This avoids making runtime Protobuf descriptors a prerequisite for experimentation.

### 3.10 Dry-run and validation operations

Add operations such as:

```http
POST /channels/validate
POST /channels/{channel_id}/test
```

They could verify:

- sink connectivity and permissions;
- schema validity;
- aggregation compatibility;
- naming rules;
- effective routing behavior;
- whether a sample event would be persisted or aggregated.

This is particularly useful before replacing an active channel definition.

## 4. Ideas for writing logs

### 4.1 Native client libraries

Provide first-party clients for the ecosystems most likely to integrate with Distill:

- .NET;
- JavaScript/TypeScript;
- Python;
- Go;
- Rust;
- Java.

Clients should handle:

- API-key or token authentication;
- event IDs and timestamps;
- batching;
- compression;
- retries with jitter;
- backpressure;
- graceful shutdown flushing;
- severity mapping;
- connection reuse;
- partial-batch errors.

The existing C# integration guide can become the reference implementation for this work.

### 4.2 OpenTelemetry and other standard protocols

Support familiar standards so applications do not need a Distill-specific logging integration:

- OpenTelemetry Logs;
- OTLP over HTTP;
- OTLP over gRPC;
- Syslog;
- GELF;
- JSON Lines;
- CloudEvents.

OpenTelemetry is a particularly strong candidate because resource attributes, scopes, traces, and spans can map naturally into Distill metadata.

### 4.3 Local agent or sidecar

Provide an agent that accepts events through:

- stdout/stderr capture;
- Unix domain sockets;
- Windows named pipes;
- localhost HTTP;
- local TCP;
- file tailing.

The agent can buffer, batch, compress, retry, and forward to Distill. Applications then write locally and do not need to know the remote service address or handle transient network failures.

This is especially useful for legacy applications and containerized workloads.

### 4.4 Simple channel-bound event endpoint

Keep `/ingest` for advanced and high-throughput clients, but add:

```http
POST /channels/orders.production/events
```

```json
{
  "severity": "error",
  "message": "Payment authorization failed",
  "attributes": {
    "order_id": "1234"
  }
}
```

Distill can add the timestamp and event ID automatically.

This avoids repeating `channel_id` in every event and provides a self-explanatory integration path.

### 4.5 JSON Lines ingestion

Support newline-delimited JSON for scripts, shell pipelines, and batch jobs:

```text
{"severity":"info","message":"started"}
{"severity":"error","message":"failed","order_id":"123"}
```

Example:

```powershell
Get-Content app.log | distill ingest --channel app.production
```

### 4.6 First-party CLI

Useful commands include:

```text
distill channel create orders.production --preset errors-only
distill channel list
distill channel inspect orders.production
distill channel test orders.production
distill log orders.production --severity error --message "Payment failed"
distill ingest orders.production events.jsonl
```

The CLI should generate example payloads and client configuration automatically.

### 4.7 Channel-bound and scoped ingest tokens

Replace the single global API key as the only normal integration model with:

- per-channel write tokens;
- read-only management tokens;
- scoped permissions;
- token expiration;
- token rotation;
- separate create, manage, and write permissions.

A client could then use only the access it needs:

```text
DISTILL_CHANNEL=orders.production
DISTILL_TOKEN=...
```

This reduces blast radius when a credential is exposed.

### 4.8 Select the channel outside the event payload

Support channel selection through:

- URL path;
- `X-Distill-Channel` header;
- client configuration;
- token binding;
- source metadata.

For example:

```http
POST /channels/orders.production/events
```

The event body does not need to contain `channel_id`. The existing envelope remains available for multi-channel batches.

### 4.9 Automatic routing rules

Agents and OpenTelemetry collectors could route events based on metadata:

```json
{
  "routes": [
    { "when": "service == 'orders'", "channel": "orders" },
    { "when": "environment == 'production'", "channel": "production" }
  ]
}
```

Routing requires explicit precedence rules and a safe default channel for unmatched events. A preview or test operation should show the selected route before deployment.

### 4.10 Automatic batching, compression, and asynchronous delivery

The client-facing API should make reliable delivery automatic:

- accept one event or many;
- batch transparently;
- compress large requests;
- retry transient failures;
- expose local queue depth;
- flush during graceful shutdown;
- preserve stable event IDs.

Users should not need to know that RabbitMQ confirmation is part of the acceptance contract.

## 5. Feedback, discovery, and operational clarity

### 5.1 Richer ingest responses

The current response reports only an accepted count. A richer response could include:

```json
{
  "accepted": 10,
  "rejected": 0,
  "request_id": "...",
  "durability": "queued",
  "channel": "orders.production"
}
```

For partial batches:

```json
{
  "accepted": 8,
  "rejected": 2,
  "errors": [
    {
      "index": 3,
      "code": "invalid_payload",
      "message": "payload.value must be numeric"
    }
  ]
}
```

The response should clearly distinguish “accepted for durable handoff” from “already queryable in the sink.”

### 5.2 Resolved channel configuration and capabilities

The channel inspection endpoint should return a sanitized effective configuration summary:

- channel name and description;
- preset or mode;
- severity behavior;
- aggregation behavior;
- schema status;
- retention;
- named destination, never the connection string;
- example payload;
- supported content types;
- recent health and delivery status.

The effective view is important if defaults or inheritance are introduced.

### 5.3 Generated integration snippets

For every channel, provide copyable snippets for:

- curl;
- PowerShell;
- C#;
- Python;
- JavaScript;
- OpenTelemetry;
- Docker Compose;
- Kubernetes.

This turns channel creation into an immediately usable workflow.

### 5.4 Test console and event explorer

A small UI or CLI command should allow users to:

- select a channel;
- enter a sample event;
- view validation results;
- see whether it will be persisted or aggregated;
- inspect the normalized event;
- send a test event;
- view the request ID and delivery state.

This makes severity thresholds and aggregation behavior observable rather than theoretical.

### 5.5 Explicit lifecycle operations

Useful management operations include:

- pause ingestion;
- resume ingestion;
- flush an aggregate;
- archive a channel;
- clone a channel;
- rotate a sink;
- export configuration;
- delete a channel with safeguards.

These are more discoverable than overloading `PUT` for creation and replacement.

## 6. Suggested surface design

An approachable API could have three layers.

### Layer 1: simple users

```http
POST /channels
POST /channels/{channel}/events
```

Simple JSON events, presets, generated defaults, and channel-bound tokens.

### Layer 2: integrated applications

```http
POST /ingest
POST /otlp/v1/logs
```

Batch JSON, Protobuf, OpenTelemetry, compression, and client-library support.

### Layer 3: operators and automation

```http
PUT /channels/{channel}
POST /channels/validate
POST /channels/{channel}/schema
GET /channels/{channel}/config
```

Full channel definitions, explicit schemas, named destinations, infrastructure-as-code, and lifecycle control.

The layers should share the same internal channel model and delivery guarantees.

## 7. Practical product direction

The recommended implementation sequence is:

### Phase 1: remove first-run friction

1. Add channel presets and sensible defaults.
2. Add `POST /channels` for simple creation.
3. Add `POST /channels/{channel}/events` for simple event writing.
4. Add a CLI that wraps channel creation and event submission.
5. Return richer validation and ingest responses.
6. Generate curl, PowerShell, and C# examples for each channel.

### Phase 2: make integrations reliable

1. Ship a first-party .NET client, followed by the other high-value languages.
2. Add batching, retries, compression, and shutdown flushing to clients.
3. Add per-channel scoped write tokens.
4. Add JSON Lines ingestion.
5. Add dry-run channel tests and sample-event routing previews.

### Phase 3: reduce infrastructure coupling

1. Introduce named destinations.
2. Hide RabbitMQ topology behind service-level configuration.
3. Add resolved configuration and capability views.
4. Add progressive schema modes.
5. Add lifecycle operations such as pause, flush, clone, and archive.

### Phase 4: reach existing observability ecosystems

1. Add OpenTelemetry/OTLP ingestion.
2. Provide a local agent or sidecar.
3. Add metadata-based routing rules.
4. Add a web wizard and event test console.

## 8. Prioritization matrix

| Idea | User value | Implementation effort | Recommendation |
|---|---:|---:|---|
| Presets | High | Low | Do early |
| Simple channel-bound event endpoint | High | Low | Do early |
| CLI | High | Medium | Do early |
| Rich responses | High | Low | Do early |
| Generated snippets | Medium | Low | Do early |
| Native client libraries | High | Medium | Do early for .NET |
| Scoped channel tokens | High | Medium | Do early |
| JSON Lines | Medium | Low | Do early |
| Named destinations | High | Medium | Do next |
| Hide RabbitMQ settings | High | Medium | Do next |
| Dry-run/test endpoint | Medium | Medium | Do next |
| Progressive schemas | Medium | Medium | Do next |
| OpenTelemetry/OTLP | High | High | Strategic |
| Local agent/sidecar | High | High | Strategic |
| Routing rules | Medium | High | Strategic |
| Web wizard | Medium | High | Strategic |

## 9. Risks and design questions

Before implementation, decide:

- whether simple channel creation should be available to all API-key holders or only administrators;
- whether auto-created channels are allowed outside development;
- how preset versions are represented and migrated;
- whether a channel may have multiple writers with separate tokens;
- how channel deletion and sink rotation preserve existing data;
- what unmatched automatic-routing events should do;
- whether partial batch acceptance is preferable to all-or-nothing requests;
- which fields are mandatory in the simple event format;
- how message-oriented events map to aggregation functions that require `payload.value`;
- whether OpenTelemetry becomes the canonical normalized event model;
- how to expose queueing and persistence status without promising synchronous sink visibility.

## 10. Success measures

The improvements should be evaluated with measurable outcomes:

- time from installation to first accepted event;
- number of configuration fields required for the first channel;
- percentage of users using presets versus raw definitions;
- integration time for a new application;
- percentage of events sent through a supported standard or client library;
- retry and delivery failure rates;
- invalid-channel and invalid-payload rates;
- number of credentials with unnecessarily broad write access;
- support questions related to RabbitMQ, sink configuration, and schema setup.

The strongest initial success criterion is simple: a new user should be able to create a channel and send a valid log with a short command or a small JSON request, while advanced operators retain the full existing configuration contract.

## 11. Decisions and rationale

This section records the current product decisions made while reviewing the proposals above.

### 11.1 Defaults and presets are approved

Distill should provide 3.1-style defaults and presets to simplify channel creation, provided they do not interfere with the full configuration model.

The intended configuration flow is:

```text
simple request
    → apply built-in defaults or preset
    → apply explicit user overrides
    → produce the effective full ChannelDefinition
```

For example:

```json
{
  "name": "orders.production",
  "preset": "raw-events",
  "retention_hours": 720,
  "buffer_bytes": 104857600
}
```

The preset supplies a starting point, while advanced users retain access to all relevant channel properties through the complete configuration contract.

Presets should therefore be additive convenience layers, not restricted channel types. Their behavior should be inspectable through the resolved configuration endpoint, and preset changes should be versioned or documented clearly.

### 11.2 Custom properties remain first-class

Most valuable logs are expected to contain application-specific properties that cannot be represented by a universal default event shape. The simplified communication surface must preserve that behavior.

Simple events should be able to contain arbitrary JSON payloads and attributes, for example:

```json
{
  "severity": "error",
  "message": "Payment failed",
  "attributes": {
    "order_id": "123",
    "payment_provider": "stripe",
    "retryable": true
  }
}
```

Schemas are optional and progressive. They may describe or validate custom properties when explicitly configured, but defaults and presets must not impose a fixed payload shape.

The product should distinguish between:

- the generic event envelope, which can have stable fields such as severity, timestamp, message, and attributes;
- the application payload, which remains extensible and user-defined;
- aggregate-specific values or dimensions, which may require explicit aggregation configuration.

### 11.3 A single API key is acceptable for the initial self-hosted product

Per-channel or per-writer tokens from idea 4.7 are intentionally deferred.

The primary deployment assumption is self-hosted usage, where the operator controls the service boundary and a global API key is not considered a significant early-product risk. Requiring users to generate and manage hundreds of channel tokens could create more operational friction than it removes, especially for on-demand logging across many channels.

The initial product should therefore retain the simpler global API-key model. Authentication should still be kept behind a replaceable boundary so that hosted, multi-tenant, or externally managed deployments can introduce a different authorization model later without changing the basic ingestion contract.

Future hosted behavior will need to be designed separately rather than assumed to be a direct extension of the self-hosted model.

### 11.4 Named destinations are deferred

Named destinations from idea 3.3 are considered a useful long-term convenience but unnecessary for the early build.

The intended use case is on-demand logging where persisted logs are created deliberately, either as raw events or through aggregation. Ordinary users are not expected to need to discover or select from a large destination catalog, so hiding sink connection details is not currently a major usability requirement.

The early build may continue to use explicit sink configuration as part of the advanced channel definition. A future destination abstraction can still be introduced if it becomes valuable for:

- reducing repeated configuration;
- rotating sinks;
- centralizing secrets;
- supporting multiple deployment environments;
- enabling hosted or multi-tenant operation.

The internal sink interface should remain sufficiently separated from the user-facing channel model to leave this option open, but named destinations should not be required for the initial communication-surface work.

### 11.5 Resulting early-build scope

The current preferred scope is:

1. Simple channel creation through `POST /channels`.
2. Built-in defaults and presets with explicit overrides.
3. Full advanced channel configuration as an escape hatch.
4. Schema-optional channels with progressive validation.
5. Arbitrary application payloads and custom attributes.
6. Channel selection outside the event payload where practical.
7. First-party client libraries and standard integrations.
8. Rich ingest responses and partial-batch error reporting.
9. Resolved channel configuration and capability inspection.
10. Generated integration snippets.
11. A test console or equivalent sample-event workflow.
12. Explicit lifecycle operations such as pause, resume, flush, clone, and archive.
13. A single global API key for the initial self-hosted deployment model.

The following remain deliberately deferred:

- per-channel or per-writer token management;
- named destinations as a required user-facing abstraction;
- hosted or multi-tenant authorization behavior;
- more elaborate automatic routing rules, pending validation of the simpler channel-selection mechanisms.

The guiding outcome remains: a user should be able to create a useful channel and send a custom-property-rich log without learning Distill's internal RabbitMQ topology, while operators retain complete control when they need it.
