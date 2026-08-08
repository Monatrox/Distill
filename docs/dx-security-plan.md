# Distill Developer Experience + Security Improvement Plan

**Status:** Phase 1 (security items 1-4) complete as of 2026-08-08  
**Created:** 2026-08-08

## Overview

This plan combines communication-surface developer experience (DX) improvements with the security hardening items that are cheap enough to bundle into the same pass. The existing `POST /channels` preset API, `POST /channels/{id}/events`, lifecycle operations, `POST /channels/validate`, `POST /channels/{id}/test`, and OTLP ingestion are already shipped. This plan focuses on the remaining gaps.

## 1. CLI Expansion

The current CLI (`src/infra/cli.rs`) supports only `sink create`, `sink list`, `secret file-write`, and `secret env-write`. Add channel and event subcommands.

### 1.1 Channel create

```
distill channel create <NAME> [--preset <PRESET>] [--severity-threshold <LEVEL>]
    [--retention-hours <HOURS>] [--buffer-bytes <BYTES>]
    [--base-url <URL>]
```

Sends `POST /channels` to the admin API with the given preset and overrides. Prints the created channel config and a curl-able event example.

**Files:** `src/infra/cli.rs` — new `Command::Channel` variant, `ChannelCommand` enum, `CreateChannel` args struct.

### 1.2 Channel list

```
distill channel list [--base-url <URL>]
```

Lists channel IDs + lifecycle state. Since `GET /channels` doesn't exist yet, add a `GET /channels` endpoint returning a summary list. Use `Arc<RwLock<HashMap<String, ChannelRecord>>>` from the registry.

**Files:**
- `src/http/mod.rs` — register `GET /channels` on admin router
- `src/http/channels.rs` — new `list_channels` handler
- `src/http/dto.rs` — new `ChannelSummary` DTO
- `src/infra/cli.rs` — `ListChannels` subcommand

### 1.3 Channel inspect

```
distill channel inspect <NAME> [--base-url <URL>]
```

Wraps `GET /channels/{channel_id}/config`. Pretty-prints the resolved config.

**Files:** `src/infra/cli.rs` — `InspectChannel` args struct.

### 1.4 Event submission

```
distill event <CHANNEL> [--severity <LEVEL>] [--message <TEXT>]
    [--attribute KEY=VALUE]... [--base-url <URL>] [--api-key <KEY>]
```

Sends a single `POST /channels/{channel_id}/events` using the ingest API key. Automatically sets `event_id` (UUID) and `timestamp` (now) if omitted.

### 1.5 JSON Lines ingestion

```
distill ingest <CHANNEL> [--file <PATH>] [--base-url <URL>] [--api-key <KEY>]
```

Reads newline-delimited JSON from stdin or `--file`, batches into sets of up to 100, and posts to `/ingest`. Reports accepted/rejected counts.

**Files:** `src/infra/cli.rs` — `SendEvent`, `IngestJsonl` args structs.

### 1.6 HTTP client helper

All CLI networking commands need a shared reqwest client. Since the CLI subcommands currently run without a server, they need their own HTTP client. Extract a small helper:

```rust
fn distill_client(base_url: &str, api_key: &str) -> reqwest::Client { ... }
fn resolve_base_url(provided: Option<&str>) -> String {
    provided.unwrap_or("http://127.0.0.1:8081").to_string()
}
```

The admin subcommands default to `:8081` (admin port); ingest subcommands default to `:8080` (public port).

**File:** `src/infra/cli.rs` or a new `src/infra/client.rs`.

---

## 2. Integration Snippets in Channel Config Responses

### 2.1 Add `examples` to `ChannelConfigResponse`

```rust
pub struct ChannelConfigResponse {
    // ... existing fields ...
    pub examples: ChannelExamples,
}

pub struct ChannelExamples {
    pub curl_write: String,
    pub powershell_write: String,
    pub curl_config: String,
    pub client_config: HashMap<String, serde_json::Value>,
}
```

The examples are generated from the channel ID, ingest endpoint, and configuration. The response includes copy-paste-ready commands and JSON payload examples populated with the channel's actual `channel_id`.

**Files:** `src/http/dto.rs` — new structs + `ChannelExamples::for_channel()` constructor.

### 2.2 Channel-bound write snippet in CLI

When `distill channel create` returns the channel config, print the curl and PowerShell write examples directly to the terminal.

---

## 3. Progressive Schema Modes

Currently schemas are either absent (optional) or enforced (when a `MessageSchemaDefinition` is registered, `validate_payload` is called). Add explicit schema modes so users can adopt validation incrementally.

### 3.1 SchemaMode enum

```rust
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMode {
    #[default]
    Disabled,       // No schema, no validation
    Observe,        // Schema loaded, violations logged but not rejected
    Reject,         // Violations return 400
    Quarantine,     // Violations routed to dead-letter queue
}
```

Add `schema_mode: SchemaMode` to `ChannelDefinition` (defaulting to `Disabled` when no schema is present, `Reject` when a schema is set without an explicit mode).

### 3.2 Wire `SchemaMode` into validation

In `src/core/registry.rs:submit_event`, after schema validation:
- `Reject`: current behavior (return error on failure)
- `Observe`: log warning, continue
- `Quarantine`: route to dead-letter exchange
- `Disabled`: skip validation entirely

**Files:**
- `src/domain/channel.rs` — add `SchemaMode` field to `ChannelDefinition`
- `src/core/registry.rs` — branch on `schema_mode` in `submit_event`
- `src/http/dto.rs` — add `schema_mode` to `CreateChannelRequest`, `PublicChannelConfig`, `ChannelConfigResponse`

---

## 4. Richer Event Test Endpoint

`POST /channels/{channel_id}/test` currently returns route, severity threshold, and schema status. Add:

- `normalized_event`: the `LogEvent` as it would be submitted (with generated `event_id`, `timestamp`, merged payload/attributes)
- `schema_validation`: validation result or null
- `severity_passes`: whether the event meets the severity threshold
- `aggregation`: what would happen if the event is aggregated (aggregate key, function)

**Files:** `src/http/dto.rs` — extend `EventTestResponse`; `src/http/channels.rs` — enrich `test_event` handler.

---

## 5. Simple Channel Summaries

Add `GET /channels` — returns a list of `ChannelSummary` objects (channel_id, lifecycle, preset-equivalent description, severity threshold, active).

This supports the CLI `distill channel list` command and provides a discoverability entry point.

**Files:**
- `src/http/channels.rs` — new `list_channels` handler
- `src/http/dto.rs` — `ChannelSummary` DTO
- `src/http/mod.rs` — register route

---

## 6. Security: Sink URL SSRF Prevention

### 6.1 URL policy validation

Replace the current `reqwest::Url::parse()` discarding with a `validate_sink_url()` function in `src/persistence/http.rs` that enforces:

| Check | Rule |
|---|---|
| Scheme | Must be `http` or `https` |
| Host | Must NOT be a raw IP address (v4 or v6) unless on an explicit allowlist |
| Host | Must NOT resolve to loopback (`127.0.0.0/8`, `::1`) |
| Host | Must NOT resolve to link-local (`169.254.0.0/16`, `fe80::/10`) |
| Host | Must NOT resolve to private ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fc00::/7`) |
| Host | Must NOT resolve to multicast or reserved ranges |
| Redirects | Disabled (`reqwest::redirect::Policy::none()`) |
| Port | Must be 80, 443, 8080, or 8443 unless on an explicit allowlist |

Add an optional operator configuration for an explicit hostname/IP/port allowlist via env var `DISTILL_SINK_ALLOWLIST`:

```
DISTILL_SINK_ALLOWLIST=internal-api.corp:9090,10.20.0.0/16
```

Format: comma-separated entries, each being `host[:port]` or `cidr[:port]`.

### 6.2 Validation location

The URL validation runs in `SinkConfig::validate()` (when `driver == "http"`) and again in `HttpSink::connect()` (belt-and-suspenders). `reqwest` redirect policy is set to `none()`.

**Files:**
- `src/persistence/http.rs` — `validate_sink_url()` function, redirect policy, parsed URL stored
- `src/domain/sink.rs` — call URL validation from `SinkConfig::validate()`
- `src/infra/config.rs` — parse `DISTILL_SINK_ALLOWLIST`

---

## 7. Security: Hard Upper Bounds

Add per-field maximums to `ChannelDefinition::validate()` and `CreateChannelRequest::to_definition()`:

| Field | Current check | New max | Rationale |
|---|---|---|---|
| `buffer_bytes` | Must be > 0 | `256 MiB` (268,435,456) | One channel shouldn't consume the 512 MiB process budget |
| `mailbox_capacity` | Must be > 0 | `65,536` | Prevents unbounded mpsc channel allocation |
| `ttl_seconds` | None | `86,400` (24 hours) | Prevents effectively-never-flush configurations |
| `retention_hours` | None | `21,600` (900 days) | Reasonable maximum for on-demand logging |
| `channel_id` length | None | 128 characters | Prevents abuse of exchange/routing-key names |
| Global channel count | None | Operator-configured via `DISTILL_MAX_CHANNELS` (default 500) | Prevents unbounded actor/RabbitMQ resource creation |
| Schema `descriptor_set_base64` | None (100 MiB cache limit) | 1 MiB decoded | Prevent large schema blobs from exhausting memory |

Add a `MAX_SCHEMA_DESCRIPTOR_BYTES` constant (1 MiB raw, checked before decoding in `SchemaRegistry::register()`).

The global channel count check runs at the top of `ChannelRegistry::register_channel()`:

```rust
let channel_count = self.channels.read().await.len();
if channel_count >= self.max_channels {
    return Err(DistillError::InvalidConfiguration(
        format!("maximum channel count ({}) reached", self.max_channels)
    ));
}
```

**Files:**
- `src/domain/channel.rs` — add max constants, extend `validate()`
- `src/domain/mod.rs` — add `MAX_BUFFER_BYTES`, `MAX_MAILBOX_CAPACITY`, `MAX_TTL_SECONDS`, `MAX_RETENTION_HOURS`, `MAX_CHANNEL_ID_LENGTH`
- `src/core/registry.rs` — add `max_channels` field, check in `register_channel()`
- `src/core/schema.rs` — add `MAX_SCHEMA_DESCRIPTOR_BYTES` check
- `src/infra/config.rs` — `DISTILL_MAX_CHANNELS` default 500
- `src/http/dto.rs` — validate overrides in `to_definition()` before building `ChannelDefinition`

---

## 8. Security: Failed-Authentication Throttling

Add a per-auth-type (ingest/admin) failed-attempt counter. After 10 consecutive failures within a 60-second window, subsequent attempts from any source receive `429 Too Many Requests` for the remainder of the window. This is a coarse global throttle — sufficient for a self-hosted single-key model without per-IP tracking.

Implementation: a `tokio::sync::Mutex<FailedAuthWindow>` in `AppState`, updated in `auth.rs` on failure, checked before the constant-time comparison.

**Files:**
- `src/http/auth.rs` — extend `authenticate` with throttle check
- `src/http/mod.rs` — add `FailedAuthWindow` to `AppState`

---

## 9. Error Message Improvements

### 9.1 Better channel_not_found

When `POST /channels/{channel_id}/events` hits a missing channel, the current response is a generic `CHANNEL_NOT_FOUND`. Extend it with:

```json
{
  "error_code": "CHANNEL_NOT_FOUND",
  "message": "channel 'orders.production' does not exist",
  "suggested_creation": {
    "preset": "raw_events",
    "admin_endpoint": "POST /channels"
  }
}
```

**File:** `src/core/errors.rs` — add optional `channel_id` and `suggestions` to `ChannelNotFound` variant; `src/http/ingest.rs` and `src/http/channels.rs` — pass channel_id in errors.

### 9.2 Unknown field rejection

Currently, unknown JSON fields in `CreateChannelRequest` are silently ignored (serde default). Add `#[serde(deny_unknown_fields)]` on `CreateChannelRequestWire` to catch typos like `"bufffer_bytes"`.

**File:** `src/http/dto.rs` — add `#[serde(deny_unknown_fields)]` to `CreateChannelRequestWire`.

---

## 10. Implementation Sequence

| # | Item | Effort | Files touched |
|---|---|---|---|
| 1 | Hard upper bounds (Sec 7) | Small | `domain/channel.rs`, `domain/mod.rs`, `core/registry.rs`, `infra/config.rs`, `http/dto.rs` |
| 2 | Sink URL SSRF prevention (Sec 6) | Medium | `persistence/http.rs`, `domain/sink.rs`, `infra/config.rs` |
| 3 | Failed-auth throttling (Sec 8) | Small | `http/auth.rs`, `http/mod.rs` |
| 4 | Unknown field rejection (Sec 9.2) | Trivial | `http/dto.rs` |
| 5 | Channel summaries endpoint (Sec 5) | Small | `http/channels.rs`, `http/dto.rs`, `http/mod.rs` |
| 6 | Integration snippets (Sec 2) | Small | `http/dto.rs` |
| 7 | Richer test endpoint (Sec 4) | Small | `http/channels.rs`, `http/dto.rs` |
| 8 | Better channel_not_found (Sec 9.1) | Small | `core/errors.rs`, `http/ingest.rs`, `http/channels.rs` |
| 9 | HTTP client helper (Sec 1.6) | Small | `infra/cli.rs` or `infra/client.rs` |
| 10 | CLI channel create (Sec 1.1) | Small | `infra/cli.rs` |
| 11 | CLI channel list/inspect (Sec 1.2-1.3) | Small | `infra/cli.rs` |
| 12 | CLI event submission (Sec 1.4) | Small | `infra/cli.rs` |
| 13 | CLI JSON Lines ingest (Sec 1.5) | Medium | `infra/cli.rs` |
| 14 | Progressive schema modes (Sec 3) | Medium | `domain/channel.rs`, `core/registry.rs`, `http/dto.rs` |

Security items (1-4) are listed first because they protect the surface before the new DX endpoints widen it. Items 5-8 are the server-side DX additions. Items 9-14 are CLI and schema work.

## 11. Not in Scope

- **Scoped per-channel tokens** — deferred per `communication_surface_improvements.md:11.3`
- **Named destinations** — deferred per `communication_surface_improvements.md:11.4`
- **First-party client libraries (.NET, Python, etc.)** — separate effort
- **OpenTelemetry agent/sidecar** — separate effort
- **RabbitMQ retry queue isolation** — significant broker topology change, deserves its own plan
- **Transactional channel replacement** — significant registry refactor
- **CI scanning (cargo audit, dependency policy)** — process change, not code
- **Web wizard / UI** — separate product track
- **Auto-create channels** — explicitly deferred; production creates should be explicit
