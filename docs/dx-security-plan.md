# Distill Developer Experience + Security Improvement Plan

**Last updated:** 2026-08-08

---

## Phase 1 — Security Hardening ✅ *Complete*

| # | Item | Status | Files changed |
|---|---|---|---|
| 1 | Hard upper bounds on resource fields | Done | `domain/mod.rs`, `domain/channel.rs`, `core/registry.rs`, `core/schema.rs`, `infra/config.rs` |
| 2 | Sink URL SSRF prevention | Done | `persistence/policy.rs` (new), `persistence/http.rs`, `persistence/mod.rs`, `domain/sink.rs`, `infra/config.rs`, `core/registry.rs` |
| 3 | Failed-authentication throttling | Done | `http/auth.rs`, `http/mod.rs` |
| 4 | Unknown field rejection on create-channel requests | Done | `http/dto.rs` |

### 1. Hard upper bounds

Constants in `src/domain/mod.rs:73`:

| Field | Max | Rationale |
|---|---|---|
| `buffer_bytes` | 256 MiB | Half the 512 MiB process budget |
| `mailbox_capacity` | 65,536 | Bounds mpsc channel allocation |
| `ttl_seconds` | 86,400 (24h) | Prevents never-flush configurations |
| `retention_hours` | 21,600 (900 days) | Generous ceiling for on-demand logging |
| `channel_id` | 128 chars | Bounds broker topology names |

Enforced in `ChannelDefinition::validate()` (`src/domain/channel.rs:44`). Global channel count defaults to 500, configurable via `DISTILL_MAX_CHANNELS` (0 = disabled), checked in `ChannelRegistry::register_channel()` (`src/core/registry.rs:150`). Schema descriptor limit: 1 MiB decoded in `SchemaRegistry::register()` (`src/core/schema.rs:44`).

### 2. Sink URL SSRF prevention

New `src/persistence/policy.rs` module:

- **`SinkAllowlist`** — parsed from `DISTILL_SINK_ALLOWLIST` (comma-separated `host[:port]`, `cidr[:port]`, `:port` entries)
- **`validate_sink_url()`** — enforces:
  - scheme `http`/`https` only
  - default ports 80, 443, 8080, 8443 (others require allowlist)
  - raw IP addresses require allowlisted CIDR
  - hostnames resolve to public addresses only (loopback, link-local, private, multicast, reserved, documentation ranges all denied) — allowlisted hostnames bypass resolution
- **Redirects disabled** in `HttpSink::connect()` (`src/persistence/http.rs:34`)
- **Scheme invariant** in `SinkConfig::validate()` (`src/domain/sink.rs:39`) as a first-pass guard

Threaded through `Settings` → `ChannelRegistry` → `build_sink`.

### 3. Failed-auth throttling

Per-listener `FailedAuthWindow` in `src/http/mod.rs:76`:
- 10 failures within 60 seconds → `429 Too Many Requests`
- Successful authentication resets the window
- Ingest and admin throttles are independent via separate `AuthGuard` instances

### 4. Unknown field rejection

`#[serde(deny_unknown_fields)]` on `CreateChannelRequestWire` (`src/http/dto.rs:34`) — typos like `"bufffer_bytes"` now return `400` instead of being silently ignored.

---

## Phase 2 — Server-Side DX ⏳ *Next*

| # | Item | Effort | Files |
|---|---|---|---|
| 5 | `GET /channels` list endpoint | Small | `http/channels.rs`, `http/dto.rs`, `http/mod.rs` |
| 6 | Integration snippets in channel config responses | Small | `http/dto.rs` |
| 7 | Richer `POST /channels/{id}/test` response | Small | `http/channels.rs`, `http/dto.rs` |
| 8 | Better `channel_not_found` errors | Small | `core/errors.rs`, `http/ingest.rs`, `http/channels.rs` |

### 5. Channel summaries endpoint

Add `GET /channels` returning a list of `ChannelSummary` objects (channel_id, lifecycle, preset-equivalent description, severity threshold, active). Supports the CLI `distill channel list` command.

### 6. Integration snippets

`ChannelConfigResponse` gets an `examples` field with copy-paste-ready curl and PowerShell write commands and a machine-readable client config map — all populated from the channel's actual `channel_id`.

### 7. Richer test endpoint

`POST /channels/{channel_id}/test` response adds:
- `normalized_event` — the fully assembled `LogEvent`
- `schema_validation` — validation result or null
- `severity_passes` — whether the event meets the severity threshold
- `aggregation` — what would happen (aggregate key, function)

### 8. Better `channel_not_found` errors

Extend the error response with the channel identifier and a `suggested_creation` block pointing to presets.

---

## Phase 3 — CLI ⏳ *Pending*

| # | Item | Effort | Files |
|---|---|---|---|
| 9 | HTTP client helper | Small | `infra/cli.rs` or `infra/client.rs` |
| 10 | CLI channel create | Small | `infra/cli.rs` |
| 11 | CLI channel list/inspect | Small | `infra/cli.rs` |
| 12 | CLI event submission | Small | `infra/cli.rs` |
| 13 | CLI JSON Lines ingest | Medium | `infra/cli.rs` |

Add channel and event subcommands to `distill` CLI (`src/infra/cli.rs`):

- `distill channel create <NAME> [--preset <PRESET>] [options]` — wraps `POST /channels`
- `distill channel list [--base-url <URL>]` — wraps `GET /channels`
- `distill channel inspect <NAME> [--base-url <URL>]` — wraps `GET /channels/{id}/config`
- `distill event <CHANNEL> [--message <TEXT>] [options]` — wraps `POST /channels/{id}/events`
- `distill ingest <CHANNEL> [--file <PATH>]` — JSON Lines batching to `/ingest`

All networking commands share a reqwest client helper.

---

## Phase 4 — Progressive Schema Modes ⏳ *Pending*

| # | Item | Effort | Files |
|---|---|---|---|
| 14 | SchemaMode enum + validation wiring | Medium | `domain/channel.rs`, `core/registry.rs`, `http/dto.rs` |

### SchemaMode enum

```rust
pub enum SchemaMode {
    Disabled,    // No schema, no validation
    Observe,     // Schema loaded, violations logged but not rejected
    Reject,      // Violations return 400 (current behavior)
    Quarantine,  // Violations routed to dead-letter queue
}
```

Add `schema_mode` to `ChannelDefinition`, `CreateChannelRequest`, and `PublicChannelConfig`. Wire into `submit_event` branch logic.

---

## Not in Scope (deferred or separate efforts)

| Item | Reason |
|---|---|
| Scoped per-channel tokens | Deferred per `communication_surface_improvements.md:11.3` |
| Named destinations | Deferred per `communication_surface_improvements.md:11.4` |
| First-party client libraries (.NET, Python, etc.) | Separate product track |
| OpenTelemetry agent/sidecar | Separate product track |
| RabbitMQ retry queue isolation | Significant broker topology change, separate plan |
| Transactional channel replacement | Significant registry refactor, separate plan |
| CI scanning (cargo audit, dependency policy) | Process change, not code |
| Web wizard / UI | Separate product track |
| Auto-create channels | Explicitly deferred; production creates must be explicit |
