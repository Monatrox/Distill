# Security and Resilience Review Findings

**Review date:** 2026-08-03  
**Scope:** The current Distill Rust service, Docker development deployment, HTTP API, RabbitMQ delivery path, and persistence adapters.  
**Method:** Focused source review plus `cargo test --all-targets` and `cargo clippy --all-targets -- -D warnings`.

This is a source-level review, not a penetration test or a complete dependency-vulnerability assessment. `cargo audit` was not available in the review environment; add it to CI before release.

## Priority summary

| Priority | Finding | Main consequence |
|---|---|---|
| High | Shared API key provides ingest and administration privileges | Credential compromise enables channel/sink reconfiguration and SSRF-capable requests |
| High | Plain HTTP/AMQP development deployment | API keys and log data can be exposed on a shared network |
| High | RabbitMQ retry sleeps in the only consumer | One failing sink can stall persistence for all channels |
| Medium/High | Unbounded administrative resource settings | Authenticated callers can exhaust memory, connections, queues, or actors |
| Medium | Metrics endpoint is unauthenticated | Workload and failure information is exposed for reconnaissance |
| Medium | Static shared credential model | No scoped permissions, caller identity, rotation, or targeted revocation |

## 1. Shared API key crosses trust boundaries

**Priority:** High

The same `X-API-Key` check protects normal ingestion and all channel-management endpoints. An integration that only needs to submit logs therefore also receives the ability to create channels, change sinks, register schemas, flush channels, and clone channel definitions.

Evidence:

- [`src/http/auth.rs`](../src/http/auth.rs) validates one configured API key.
- [`src/http/mod.rs`](../src/http/mod.rs) registers both ingest and channel-management routes.
- [`src/http/channels.rs`](../src/http/channels.rs) allows authenticated callers to create or change channel definitions.

This combines two roles that should normally be distinct:

1. **Ingest writers** submit events to an existing channel.
2. **Administrators** create channels and configure delivery destinations.

The impact is amplified by HTTP sinks: a caller with the shared key can supply a destination URL and headers through a channel definition. That allows requests from Distill's network identity to arbitrary reachable HTTP services.

### Recommendation

- Introduce separate credentials or signed tokens for `ingest` and `admin` permissions.
- Scope ingest credentials to an explicit allowlist of channel IDs.
- Require an administrative role for sink, schema, lifecycle, clone, and validation endpoints.
- Record credential ID, actor, action, channel ID, and outcome in an audit log. Never log the credential itself.

## 2. HTTP sink configuration permits SSRF-style access

**Priority:** High when any non-fully-trusted caller can administer channels; otherwise Medium.

The HTTP sink accepts a user-supplied URL and arbitrary headers. URL parsing verifies syntax, but does not restrict hostnames, IP address ranges, schemes, redirects, or ports.

Evidence:

- [`src/http/dto.rs`](../src/http/dto.rs) converts a request-supplied sink configuration into a channel definition.
- [`src/persistence/http.rs`](../src/persistence/http.rs) parses and posts directly to the configured endpoint.

An attacker with administrative access, or an ingest client under the current shared-key model, could target services reachable only from the Distill container or host—for example private control-plane APIs or metadata services. Redirect following can also defeat a simple hostname-only validation strategy.

### Recommendation

- Prefer named, operator-managed sinks over arbitrary per-request sink URLs.
- If dynamic HTTP destinations are required, enforce an allowlist of schemes, DNS names, ports, and resolved IP ranges.
- Deny loopback, link-local, private, multicast, and reserved address ranges unless explicitly approved.
- Disable redirects or validate each redirect target with the same policy.
- Restrict configurable request headers to an allowlist; keep bearer tokens in server-managed secret storage.

## 3. Development deployment is insecure outside local use

**Priority:** High outside an isolated developer workstation.

The supplied Compose configuration exposes the service over plain HTTP and uses development RabbitMQ and API-key credentials. It also exposes RabbitMQ's broker and management ports.

Evidence:

- [`docker-compose.yml`](../docker-compose.yml) binds service ports and sets `change-me-in-development`.
- [`.env.example`](../.env.example) shows an `amqp://` URL and a static API-key variable.

When this deployment is used on a shared network, API keys and log payloads can be intercepted or replayed. The RabbitMQ management surface and default-style credentials also expand the attack surface.

### Recommendation

- Treat the Compose file as local-development-only and state that prominently in deployment documentation.
- Put the HTTP service behind TLS termination with a trusted certificate.
- Use `amqps://` and per-environment RabbitMQ credentials with least-privilege permissions and a dedicated vhost.
- Do not publish RabbitMQ broker or management ports unless a private network policy requires it.
- Inject secrets through a platform secret manager; do not commit real values in Compose files or `.env` files.

## 4. Sink retries block all persistence delivery

**Priority:** High availability risk.

RabbitMQ consumption uses QoS 1 and performs exponential backoff with `sleep` directly in the consumer loop. While a failing message waits, the only consumer performs no work for any other channel.

Evidence:

- [`src/broker/mod.rs`](../src/broker/mod.rs) configures `basic_qos(1, ...)`.
- [`src/broker/mod.rs`](../src/broker/mod.rs) sleeps during `retry_delivery`.

A permanently failing sink can delay every message behind it for increasing periods, eventually creating backlogs and potentially exhausting RabbitMQ disk or application capacity. A malicious or compromised administrative client can induce this through an unavailable HTTP sink.

### Recommendation

- Replace in-process sleep with RabbitMQ retry queues using message TTL and dead-letter routing, or a delayed-message exchange.
- Keep retry state in message headers or a durable envelope and process retries independently.
- Consider isolation by destination or tenant so one unhealthy sink cannot block unrelated delivery.
- Add queue-depth, retry-count, dead-letter, and sink-failure alerts.

## 5. Administrative settings lack hard upper bounds and quotas

**Priority:** Medium/High

Channel definitions expose resource-affecting values such as mailbox capacity, buffer capacity, TTL, schemas, and sink configuration. Current validation rejects zero capacities but does not impose safe maximums.

Evidence:

- [`src/domain/channel.rs`](../src/domain/channel.rs) validates only positive buffer and mailbox capacities.
- [`src/runtime/channel.rs`](../src/runtime/channel.rs) creates a Tokio channel using the requested mailbox capacity.
- [`src/http/channels.rs`](../src/http/channels.rs) permits creation and replacement of channel definitions through the API.

Very large mailbox capacities can trigger excessive allocation or a runtime failure. Repeated channel creation can also consume actors, RabbitMQ exchanges/queues, database connections, schemas, and sink clients.

### Recommendation

- Define and enforce maximum values for mailbox capacity, buffer bytes, TTL, schema size, number of aggregators, and request payload size.
- Limit the total number of channels and sinks globally and per administrative identity.
- Rate-limit channel-management operations and make create/update operations idempotent.
- Validate limits before allocating resources or registering RabbitMQ topology.

## 6. Metrics are publicly reachable

**Priority:** Medium

`/metrics` does not require authentication. Its current content is aggregate-only, but it exposes event, persistence, aggregate, and error volume to any network client able to reach the service.

Evidence:

- [`src/http/mod.rs`](../src/http/mod.rs) registers `/metrics`.
- [`src/http/ingest.rs`](../src/http/ingest.rs) returns metrics without calling `authorize`.
- [`src/runtime/metrics.rs`](../src/runtime/metrics.rs) exposes service activity counters.

### Recommendation

- Bind metrics to a loopback or private monitoring interface, or protect it with a monitoring-specific credential or mTLS.
- Keep labels free of channel IDs, payload values, URLs, and user-controlled strings.

## 7. Static shared credentials have no rotation or attribution

**Priority:** Medium

The constant-time comparison in [`src/http/auth.rs`](../src/http/auth.rs) is correct, but the model is still a single long-lived secret. It does not identify the caller, support per-client revocation, expire keys, or safely rotate one client at a time.

### Recommendation

- Store hashed API keys with key IDs, scopes, expiration, and optional channel restrictions.
- Support overlapping active keys during rotation.
- Add audit events for successful and failed administrative actions, with redacted request context.
- Apply rate limits to failed authentication attempts and channel-management actions.

## 8. Channel updates can leave partially changed runtime state

**Priority:** Medium reliability gap.

During channel registration, schema state is changed before all subsequent sink, broker, flush, and actor steps finish. A later failure can leave the schema registry changed while the prior channel remains active. Updating an existing channel also relies on asynchronous flushing and replacement without a transactional update boundary.

Evidence:

- [`src/http/mod.rs`](../src/http/mod.rs) updates schema state and then performs sink/broker/channel registration.

### Recommendation

- Build and validate all replacement resources first.
- Register broker topology and sinks before swapping the active channel record.
- Swap state atomically only after successful preparation.
- On failure, retain the old channel and schema unchanged; add tests for each failure point.

## 9. Coverage and supply-chain checks are incomplete

**Priority:** Medium process gap.

The current unit suite passes, but it has no dedicated coverage for adversarial authorization, SSRF prevention, endpoint limits, retry isolation, RabbitMQ poison messages, or failed channel replacement. The review environment also did not have `cargo-audit` installed.

### Recommendation

- Add integration tests for distinct ingest/admin identities, restricted sinks, limits, and retry queues.
- Add a test RabbitMQ deployment to verify dead-letter and retry behavior.
- Run `cargo audit`, `cargo deny`, formatting, Clippy with warnings denied, and tests in CI.
- Add dependency update and security-advisory review to the release process.

## Suggested remediation order

1. Do not expose the development Compose stack beyond local development; deploy behind TLS with managed secrets.
2. Split the shared API key into scoped ingest and administrative identities.
3. Remove arbitrary user-supplied HTTP sink destinations, or implement strict destination and redirect controls.
4. Replace sleeping retries with durable RabbitMQ retry queues and add delivery isolation.
5. Add hard resource limits, quotas, rate limits, and metrics access controls.
6. Make channel replacement transactional and add adversarial integration tests.
