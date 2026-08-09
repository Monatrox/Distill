# Distill Security

**Status:** Current security decisions and planned follow-ups  
**Last reviewed:** 2026-08-03

This document describes the security boundaries currently implemented by Distill. It is an operational guide, not a penetration-test report or a guarantee that the service is safe for every deployment.

## Current security decisions

### Separate public and administrative surfaces

Distill runs two HTTP listeners:

- The public listener handles ingestion, metrics, and administrative operations (channel creation, lifecycle, schema, validation, and test operations).
- The administrative listener handles the same administrative operations on a loopback-only port for local access.

Administrative operations are guarded by the admin API key on both listeners. The loopback admin listener is retained for direct process access and is never exposed externally.

### Separate credentials by privilege

Public ingestion and administrative operations use different required credentials:

- `DISTILL_INGEST_API_KEY` authenticates ingestion and metrics.
- `DISTILL_ADMIN_API_KEY` authenticates administrative operations.

Startup rejects configurations where the keys are equal. Keys are compared in constant time and are never included in debug output. Invalid or missing credentials return `401 Unauthorized`.

This is still a static shared-key model. It provides privilege separation, but not caller identity, per-client revocation, expiration, or granular channel permissions.

### Authentication occurs before body extraction

Authentication is implemented as route middleware. Rejected requests do not reach handlers that extract `Bytes` or JSON bodies. Body limits, request timeouts, concurrency limits, rate limits, and tracing remain active around the authenticated routes.

The health endpoint is intentionally unauthenticated and bypasses the request resource controls so local liveness checks remain reliable. It returns only a success status.

### Failed-authentication throttling

Each listener throttles failed authentication attempts. After 10 failures within a 60-second window, subsequent attempts receive `429 Too Many Requests` until a successful authentication resets the window. The throttle is process-wide per listener (ingest and admin track separately) and does not distinguish source addresses.

### HTTP sink destinations are restricted

Outbound HTTP sink destinations are validated against an operator-controlled policy before a sink client is built:

- the scheme must be `http` or `https`;
- redirects are disabled on the sink client;
- the port must be 80, 443, 8080, or 8443 unless allowlisted;
- raw IP addresses are rejected unless an allowlisted CIDR covers them;
- hostnames must resolve only to public addresses unless the hostname is allowlisted — loopback, link-local, private, multicast, reserved, NAT, and documentation ranges are denied;
- operators may approve internal destinations with `DISTILL_SINK_ALLOWLIST` using `host[:port]`, `cidr[:port]`, or `:port` entries.

DNS is resolved when the sink is built (channel registration). Allowlisted hostnames bypass resolution entirely.

### Channel resource settings have hard upper bounds

Channel definitions enforce hard maximums: `buffer_bytes` at 256 MiB, `mailbox_capacity` at 65,536, `ttl_seconds` at 24 hours, `retention_hours` at 900 days, channel identifiers at 128 characters, and decoded schema descriptors at 1 MiB. The total registered channel count defaults to 500 and is configurable with `DISTILL_MAX_CHANNELS` (use 0 to disable).

### Request resource controls are bounded by default

The process applies these defaults to authenticated routes on both listeners:

| Control | Default | Disable |
|---|---:|---:|
| Request body size | 2 MiB | `0` |
| Request timeout | 30 seconds | `0` |
| Concurrent requests | 128 | `0` |
| Requests per rolling minute | 6,000 process-wide | `0` |

They can be configured with CLI flags or their corresponding `DISTILL_*` environment variables. The rate and concurrency controls are process-wide rather than per IP address or per credential.

### TLS and deployment boundaries

The Compose stack places Caddy in front of Distill. Caddy publishes ports 80 and 443 and proxies to the internal Distill listener. For `localhost`, Caddy uses a local development certificate; a real configured hostname enables automatic ACME certificates.

Distill should not be exposed directly over plaintext HTTP outside a trusted local network. Production deployments should use a trusted certificate, managed secrets, and an appropriately restricted network policy.

RabbitMQ broker and management ports in the supplied Compose stack bind to host loopback. The development credentials and Compose configuration are not production secrets.

### Metrics access

`/metrics` is protected with the ingest credential. It is intentionally kept on the public listener for now because the service may be monitored through the same edge path. Operators should still restrict it at the proxy or network layer when metrics do not need to be publicly reachable.

## Known limitations

The following are known limitations of the current security model:

- HTTP sink destinations remain operator-configurable and can still reach internal services when an operator explicitly approves them through `DISTILL_SINK_ALLOWLIST`; the policy restricts but does not eliminate that capability.
- API keys do not identify individual clients and do not support independent rotation or targeted revocation.
- Rate limiting is process-wide and does not distinguish clients, credentials, channels, or source addresses; failed-authentication throttling is similarly address-agnostic.
- Administrative resource settings have hard upper bounds, but per-credential quotas and total sink counts are still unquotaed.
- RabbitMQ retry handling can still allow a failing sink to delay unrelated delivery work.
- Channel replacement is not yet a fully transactional state swap across schemas, sinks, broker topology, and actors.
- The Compose Caddy setup is a development-friendly deployment pattern, not a complete production ingress policy.
- Dependency and container scanning must be part of CI and release review; source tests alone are not a vulnerability assessment.

## Planned security work

Potential follow-up work, in priority order:

1. Replace arbitrary HTTP sink URLs with named operator-managed sinks, or enforce strict scheme, DNS, port, IP-range, redirect, and header policies. (Scheme, port, IP-range, redirect, and resolution policies are now enforced; header allowlists, per-sink secret storage, and named destinations remain.)
2. Introduce key IDs, hashed key storage, scoped permissions, expiration, overlapping rotation, and targeted revocation.
3. Add audit events for administrative actions and authentication failures without logging credentials or payload secrets.
4. Make rate limits configurable per credential or source identity, with explicit `Retry-After` behavior and failed-authentication throttling. (A coarse per-listener failed-authentication throttle is now in place; per-identity rate limits remain.)
5. Add hard quotas for channels, sinks, schemas, mailbox capacity, buffers, and other resource-affecting settings. (Hard upper bounds and the channel-count limit are enforced; per-identity quotas and sink-count limits remain.)
6. Move RabbitMQ retries into durable retry queues and isolate unhealthy destinations.
7. Make channel replacement transactional and add rollback-focused tests.
8. Add CI checks for `cargo audit`, dependency policy, container image scanning, security integration tests, and deployment configuration validation.
9. Re-evaluate whether metrics should move to a dedicated private listener or monitoring credential instead of sharing the ingest surface.

## Reporting and operational expectations

Do not commit real API keys, broker credentials, database credentials, certificates, or sink bearer tokens. Use a secret manager, protected environment, or restricted secret file. Rotate development credentials before sharing a deployment.

Security-sensitive changes should include tests for both the allowed and denied paths, update this document when a boundary changes, and record any new externally reachable listener or proxy route.

The earlier source review in [`review-findings.md`](review-findings.md) records the baseline findings that led to several of the controls above. Its findings should be read together with this document; items marked as implemented here may remain present in that historical review for traceability.
