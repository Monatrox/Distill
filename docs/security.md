# Distill Security

**Status:** Current security decisions and planned follow-ups  
**Last reviewed:** 2026-08-03

This document describes the security boundaries currently implemented by Distill. It is an operational guide, not a penetration-test report or a guarantee that the service is safe for every deployment.

## Current security decisions

### Separate public and administrative surfaces

Distill runs two HTTP listeners:

- The public listener handles ingestion and metrics.
- The administrative listener handles channel creation, replacement, schema, lifecycle, validation, and test operations.

The administrative bind address must be loopback-only. Non-loopback values are rejected during startup. The administrative port is not published by the supplied Compose stack.

The loopback restriction applies to the Distill process network namespace. A reverse proxy, host-level port forward, or service-mesh rule can still expose it if an operator explicitly configures one.

### Separate credentials by privilege

Public ingestion and administrative operations use different required credentials:

- `DISTILL_INGEST_API_KEY` authenticates ingestion and metrics.
- `DISTILL_ADMIN_API_KEY` authenticates administrative operations.

Startup rejects configurations where the keys are equal. Keys are compared in constant time and are never included in debug output. Invalid or missing credentials return `401 Unauthorized`.

This is still a static shared-key model. It provides privilege separation, but not caller identity, per-client revocation, expiration, or granular channel permissions.

### Authentication occurs before body extraction

Authentication is implemented as route middleware. Rejected requests do not reach handlers that extract `Bytes` or JSON bodies. Body limits, request timeouts, concurrency limits, rate limits, and tracing remain active around the authenticated routes.

The health endpoint is intentionally unauthenticated and bypasses the request resource controls so local liveness checks remain reliable. It returns only a success status.

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

- HTTP sink destinations are configurable and can make outbound requests to arbitrary URLs. This remains an SSRF risk for anyone who obtains administrative access.
- API keys do not identify individual clients and do not support independent rotation or targeted revocation.
- Rate limiting is process-wide and does not distinguish clients, credentials, channels, or source addresses.
- Administrative resource settings and total channel/sink counts need stronger global quotas and hard upper bounds.
- RabbitMQ retry handling can still allow a failing sink to delay unrelated delivery work.
- Channel replacement is not yet a fully transactional state swap across schemas, sinks, broker topology, and actors.
- The Compose Caddy setup is a development-friendly deployment pattern, not a complete production ingress policy.
- Dependency and container scanning must be part of CI and release review; source tests alone are not a vulnerability assessment.

## Planned security work

Potential follow-up work, in priority order:

1. Replace arbitrary HTTP sink URLs with named operator-managed sinks, or enforce strict scheme, DNS, port, IP-range, redirect, and header policies.
2. Introduce key IDs, hashed key storage, scoped permissions, expiration, overlapping rotation, and targeted revocation.
3. Add audit events for administrative actions and authentication failures without logging credentials or payload secrets.
4. Make rate limits configurable per credential or source identity, with explicit `Retry-After` behavior and failed-authentication throttling.
5. Add hard quotas for channels, sinks, schemas, mailbox capacity, buffers, and other resource-affecting settings.
6. Move RabbitMQ retries into durable retry queues and isolate unhealthy destinations.
7. Make channel replacement transactional and add rollback-focused tests.
8. Add CI checks for `cargo audit`, dependency policy, container image scanning, security integration tests, and deployment configuration validation.
9. Re-evaluate whether metrics should move to a dedicated private listener or monitoring credential instead of sharing the ingest surface.

## Reporting and operational expectations

Do not commit real API keys, broker credentials, database credentials, certificates, or sink bearer tokens. Use a secret manager, protected environment, or restricted secret file. Rotate development credentials before sharing a deployment.

Security-sensitive changes should include tests for both the allowed and denied paths, update this document when a boundary changes, and record any new externally reachable listener or proxy route.

The earlier source review in [`review-findings.md`](review-findings.md) records the baseline findings that led to several of the controls above. Its findings should be read together with this document; items marked as implemented here may remain present in that historical review for traceability.
