# Production Readiness Blockers

This document tracks work required before Distill can make a production-ready v1 release. Priorities assume v1 targets Aspire, the C# SDK, RabbitMQ, PostgreSQL, and custom HTTP/API endpoint sinks.

## Recommended v1 scope

Keep the first supported path narrow:

- PostgreSQL and custom HTTP/API endpoint sinks;
- one default sink per Distill resource for the initial golden path;
- one Aspire-managed RabbitMQ resource;
- channels provisioned by Aspire rather than application logging code;
- `Distill.Client` and `Distill.Extensions.Logging`;
- published, version-pinned NuGet packages and container image.

HTTP sink support is mandatory: it is the extension point that lets teams implement persistence against unsupported production systems. Defer additional first-party database drivers, retention, compression, and multi-sink orchestration until their end-to-end behavior is implemented and tested. Remove or mark those capabilities experimental in documentation until then.

## Confirmed v1 decisions

- Publish private versioned images to `ghcr.io/monatrox/distill`; GitHub Actions publishes on `v*` tags. Consumers must provide GHCR pull credentials.
- Use version `0.1.0` for server, image, and all NuGet packages. NuGet packages remain CI build artifacts; do not publish them yet.
- Cap each RabbitMQ delivery and retry queue at 1 GiB or 1,000,000 messages, whichever comes first. Use `reject-publish`; failed admission must not return `202`.
- Run PostgreSQL migrations explicitly before production deployment. Permit automatic migration only with an opt-in development setting.
- Support Azure Container Apps deployment through Aspire publish as production golden path. Compose remains development-only.

## P0 — Correctness and durability

### 1. Aggregate-only events are acknowledged before durable handoff

**Evidence:** `src/core/channel.rs` stores aggregate-only events in `EventBuffer`; RabbitMQ publication occurs only when the aggregate flushes. `src/http/ingest.rs` still reports `durability: "queued"` and returns `202 Accepted`.

**Impact:** Process crash can lose events already reported as durably accepted.

**Minimal resolution:**

1. Publish every accepted event to RabbitMQ before returning `202`.
2. Move aggregation behind the durable broker boundary, or introduce a durable aggregate-input queue consumed by the aggregator.
3. Return `202` only after publisher confirmation.
4. Add an integration test that kills Distill after acceptance but before aggregate flush, restarts it, and verifies the event contributes to the eventual aggregate.

**Done when:** Every `202` response has a tested recovery path after immediate process termination.

### 2. Graceful shutdown does not flush aggregate buffers

**Evidence:** `run_actor` in `src/core/channel.rs` exits when its command channel closes and only logs that it stopped. `src/lib.rs` stops HTTP servers but does not flush all registered channels.

**Impact:** Normal container replacement or deployment shutdown can lose buffered aggregate events.

**Minimal resolution:**

1. Add a registry shutdown method that stops admission and flushes every active channel.
2. Invoke it after HTTP shutdown begins and before process exit.
3. Bound shutdown with a configurable timeout; fail visibly when durable handoff cannot complete.
4. Test SIGTERM with a non-empty aggregate buffer.

**Done when:** SIGTERM either durably flushes all accepted data or exits with an explicit failure after the configured timeout.

### 3. Dynamic channel and lifecycle state is not durable

**Evidence:** `ChannelRegistry` stores channels in an in-memory `HashMap`. API-created channels, schemas, and paused/archived states are not written back to configuration.

**Impact:** Restart removes runtime-created channels. Existing RabbitMQ messages may then retry or dead-letter because their sink/channel mapping no longer exists.

**Minimal resolution:**

1. For v1, document Aspire/startup configuration as supported channel provisioning workflow.
2. Keep runtime channel mutation, but clearly mark it ephemeral unless a durable control-plane store is added.
3. Restore startup channel definitions and sink mappings before starting RabbitMQ consumption.
4. Add restart tests with queued messages and lifecycle changes.

**Done when:** Restart restores every startup-configured channel before queued delivery resumes; runtime channel and lifecycle mutations are documented as ephemeral.

### 4. RabbitMQ consumer does not recover after connection failure

**Evidence:** `RabbitConsumer::connect` spawns one consumer task. If `consume` returns, it logs `distill.rabbit.consumer.stopped` and is not restarted.

**Impact:** Process remains alive and healthy while persistence delivery has permanently stopped.

**Minimal resolution:**

1. Supervise the consumer task with bounded reconnect backoff.
2. Re-declare topology and resume consumption after reconnect.
3. Track consumer state for readiness and metrics.
4. Test broker restart during active delivery.

**Done when:** RabbitMQ restart recovers automatically without restarting Distill or losing acknowledged events.

### 5. One failing sink blocks all delivery

**Evidence:** `src/broker/mod.rs` uses prefetch `1` and sleeps inside the only consumer before republishing a retry.

**Impact:** One unavailable sink stalls every channel for up to the retry delay.

**Minimal resolution:**

1. Replace in-consumer sleeps with durable RabbitMQ retry queues using TTL and dead-letter routing.
2. Preserve retry count in message headers or the existing envelope.
3. Acknowledge the original only after retry publication is confirmed.
4. Test that a healthy channel continues while another sink fails.

**Done when:** Failure of one destination does not stop delivery to unrelated healthy destinations.

### 6. Poison messages can requeue forever

**Evidence:** If envelope deserialization fails in `process_delivery`, `retry_delivery` fails to deserialize the same payload, then the consumer negatively acknowledges it with `requeue: true`.

**Impact:** One malformed message can loop forever and block the prefetch-one consumer.

**Minimal resolution:**

1. Treat malformed envelopes as non-retryable.
2. Publish raw bytes plus failure metadata to the dead-letter exchange.
3. Confirm dead-letter publication, then acknowledge the original.
4. Add malformed JSON and unknown-version envelope tests.

**Done when:** Poison messages reach DLQ once and later valid messages continue processing.

### 7. Rolling aggregates are inconsistent across sink drivers

**Evidence:** PostgreSQL merges rolling windows transactionally. MySQL and SQL Server overwrite the existing record with the latest window snapshot. HTTP receivers get repeated snapshots with the same stable aggregate ID, while current documentation also recommends deduplicating that ID; a receiver can therefore mistake later rolling windows for duplicates.

**Impact:** Rolling totals, averages, sums, and counts lose prior windows or become ambiguous outside PostgreSQL.

**Minimal resolution:**

1. For v1, support rolling aggregates only with PostgreSQL and reject them for HTTP, MySQL, and SQL Server.
2. Document HTTP aggregate delivery as non-rolling window records for v1.
3. Later, define an explicit HTTP rolling upsert/merge contract and port PostgreSQL merge semantics to other database drivers.

**Done when:** Rolling aggregates are rejected for every non-PostgreSQL sink, and PostgreSQL rolling-aggregate contract tests pass.

### 8. Retention and compression settings have no behavior

**Evidence:** `retention_hours` and `compression_enabled` are parsed, validated, and returned, but persistence code does not enforce either setting.

**Impact:** Operators can believe data is expiring or compressed when it is not.

**Minimal resolution:**

1. Remove these fields from v1 SDK examples and production claims, or return a validation error when set.
2. Reintroduce them only with database jobs/policies and end-to-end tests.

**Done when:** Configuration cannot claim unsupported retention or compression behavior.

## P0 — Aspire integration

### 9. Aspire PostgreSQL and HTTP sink configuration does not match the server

**Evidence:** `Distill.Aspire` emits `DISTILL_SINK__{NAME}__*` and `DISTILL_DEFAULT_SINK`. Server configuration reads `DISTILL_SINKS_CONFIG_PATH` and legacy `DISTILL_DEFAULT_SINK_CONNECTION_STRING` only.

**Impact:** Mandatory custom HTTP sinks do not work through Aspire as documented. PostgreSQL works only through the legacy fallback.

**Minimal resolution:**

1. Add explicit Aspire golden paths for `WithPostgresSink(...)` and `WithHttpSink(...)`.
2. Make the server consume the configuration Aspire emits, or have Aspire produce/mount the existing sink manifest format; keep one format as the source of truth.
3. For HTTP, accept an Aspire resource endpoint reference as well as an external `Uri`, and pass bearer tokens through secret parameter resources rather than literal strings.
4. Automatically allowlist the exact referenced Aspire resource host/port. Keep arbitrary private-network destinations blocked unless operators explicitly allowlist them.
5. Do not claim external-mode sink provisioning; Aspire can reference an external Distill instance but cannot mutate that instance's sink configuration through resource environment annotations.
6. Contract-test both sink drivers from AppHost declaration through successful delivery.

**Done when:** A fresh AppHost can select PostgreSQL or a custom HTTP/API receiver without manual files, duplicated secrets, or undocumented variables.

### 9a. HTTP sink wire contract and security policy are incomplete

**Evidence:** `src/persistence/http.rs` posts one event or aggregate with `X-Distill-Record-Type` and `X-Distill-Record-Id`, accepts any `2xx` or `409`, and disables redirects. Destination validation resolves hostnames before `reqwest` performs its own connection resolution. No versioned receiver contract or end-to-end receiver conformance test exists.

**Impact:** Teams building mandatory custom persistence receivers can interpret retries, aggregate updates, authentication, and success semantics differently. Internal Aspire receiver endpoints are blocked by default unless allowlisted. Separate DNS resolution creates a possible validation/connection race for non-allowlisted hostnames.

**Minimal resolution:**

1. Publish a versioned HTTP receiver contract covering payload schemas, record types, stable IDs, retry behavior, accepted status codes, timeouts, size limits, and authentication headers.
2. Require idempotency by `X-Distill-Record-Id`; define `409` as “this exact record was already durably accepted.” Reject rolling HTTP aggregates in v1 so later revisions cannot be confused with retries.
3. Add a small reference receiver/sample and conformance tests for event, aggregate, retry, duplicate, timeout, `4xx`, `5xx`, and DLQ behavior.
4. Support secret-backed bearer/custom headers without returning or logging their values.
5. Bind validated DNS results to the outbound connection, or require operator allowlisting for hostnames whose resolution cannot be safely pinned.
6. Load-test one-request-per-record delivery; add batching only if measurements show it is required.

**Done when:** An independent team can implement a secure, idempotent receiver from the published contract and pass the conformance suite.

### 10. Aspire API keys do not flow through resource references

**Evidence:** `AddDistill` configures keys on the Distill resource, while consumers must manually repeat `Distill__ApiKey`. The nullable `apiKey` overload can also start a server missing required keys.

**Impact:** Default setup either fails startup or requires duplicated secrets in application code.

**Minimal resolution:**

1. Create Aspire secret parameter resources for ingest and admin keys.
2. Inject both into Distill; inject only ingest key into consuming applications.
3. Make missing key configuration impossible or generate development-only values explicitly.
4. Keep admin credentials out of application logging processes.

**Done when:** `.WithReference(distill)` supplies endpoint and ingest credentials without plaintext duplication.

### 11. Management endpoint boundary is contradictory

**Evidence:** `public_router` merges administrative routes, and `Caddyfile` proxies every public path. Documentation says administrative routes are loopback-only. A second loopback admin router also exists.

**Impact:** Operators cannot know which security model is real; administrative routes are publicly reachable when the public listener is exposed.

**Minimal resolution:**

1. Remove admin routes from `public_router`.
2. Keep management on a private endpoint, or intentionally expose it behind a separately documented network/authentication policy.
3. Update Caddy, OpenAPI location, Aspire provisioning, and documentation to match one model.
4. Add route tests proving admin endpoints are absent from the public listener.

**Done when:** Public and administrative surfaces have one documented, tested boundary.

### 12. Aspire health check uses an administrative operation

**Evidence:** `DistillHealthCheck` calls `GetChannelAsync("application")`. Without an admin key, the client sends the ingest key and receives `401`.

**Impact:** Default Aspire dashboard health can be unhealthy even when ingestion works.

**Minimal resolution:**

1. Probe `/health` or a new dependency-aware `/ready` endpoint with a dedicated client method.
2. Do not require channel existence or admin credentials for basic resource health.
3. Register health checks against the Distill resource itself, not only consuming applications.

**Done when:** Default AppHost shows correct Distill health without admin credentials in the consumer.

### 13. Default Aspire image is not publishable or reproducible

**Evidence:** Integration defaults to `distill:latest`; no image publishing workflow exists. `.WithLocalBuild()` depends on locating a source-tree Dockerfile.

**Impact:** NuGet consumers cannot run the integration without cloning/building Distill manually.

**Minimal resolution:**

1. Publish private versioned images to `ghcr.io/monatrox/distill` from GitHub Actions `v*` tags.
2. Make package version select the matching image tag.
3. Keep `.WithLocalBuild()` as an explicit development override.
4. Document required GHCR pull credentials and test authenticated image pull and startup from a clean machine.

**Done when:** Installing the NuGet package plus documented GHCR credentials is sufficient to launch the matching Distill image.

### 14. Aspire resource uses a fixed host port

**Evidence:** `WithHttpEndpoint(DefaultHttpPort, DefaultHttpPort, "http")` fixes host and target ports to `8080`.

**Impact:** Parallel AppHosts or an existing service on port 8080 cause avoidable conflicts.

**Minimal resolution:**

1. Set only target port `8080` and let Aspire allocate the host port.
2. Resolve consumer addresses exclusively through service discovery.

**Done when:** Two AppHosts can run concurrently without manual port changes.

### 15. Aspire RabbitMQ resource lacks production credentials and persistence

**Evidence:** Integration creates a RabbitMQ container using `guest:guest`; Compose and Aspire setup do not configure durable storage for RabbitMQ data.

**Impact:** Container replacement can lose queued messages, and default credentials are unsuitable outside local development.

**Minimal resolution:**

1. Use Aspire parameter resources for non-default RabbitMQ credentials.
2. Add a persistent volume for local/Aspire deployments.
3. Allow referencing an existing `IResourceWithConnectionString` RabbitMQ resource instead of always creating one.
4. Label unmanaged/ephemeral mode as development-only.

**Done when:** Broker recreation preserves accepted messages and no production example uses default credentials.

### 16. No end-to-end Aspire golden-path test or sample

**Impact:** Compile success does not prove image, networking, secrets, sink initialization, channel provisioning, health, and logging work together.

**Minimal resolution:**

1. Add one sample containing AppHost and one minimal API service.
2. Test: start stack, wait healthy, send structured log, verify PostgreSQL row.
3. Run test with supported Docker runtime; run Podman smoke test before releases if Podman remains advertised.

**Done when:** CI proves clean-machine setup using only documented steps.

## P0 — C# SDK and logging provider

### 17. Presets always serialize `aggregator: null`

**Evidence:** `CreateChannelRequest.Aggregator` uses `JsonIgnoreCondition.Never`. Default requests therefore include `"aggregator": null`, which tells the server to disable the preset aggregator.

**Impact:** `metrics`, `count`, and `errors_only` presets silently behave differently through C#.

**Minimal resolution:**

1. Restore three states: omitted, configured, and explicitly disabled.
2. Simplest public API: omit `Aggregator` when null and add an explicit `DisableAggregation` option handled by a custom converter/request DTO.
3. Add serialization contract tests for every preset and override combination.

**Done when:** Default `metrics` request omits `aggregator`, while explicit disable emits `aggregator: null`.

### 18. Logger wake-up logic can strand queued records

**Evidence:** Capacity-one wake signal is drained, one batch per channel is processed, then the worker waits without checking whether queues still contain records.

**Impact:** A finite burst larger than `BatchSize` can remain buffered indefinitely.

**Minimal resolution:**

1. After processing a batch, continue draining that queue until empty, yielding between batches if needed.
2. Remove the separate wake channel if `PeriodicTimer` plus queue reads can cover batching more simply.
3. Add a test that enqueues more than two batches and then produces no further logs.

**Done when:** Every admitted event is delivered or explicitly reported dropped without needing a later log event.

### 19. Logger timestamps are captured during delivery

**Evidence:** `QueuedLog` stores no timestamp; `ToDistillEvent` assigns `DateTimeOffset.UtcNow` when flushing.

**Impact:** Queue delay changes event chronology and duration-based aggregates.

**Minimal resolution:**

1. Capture timestamp and stable event ID inside `Log()` before enqueueing.
2. Preserve both across retries.
3. Add a delayed-flush test verifying original event time.

**Done when:** Event timestamp represents log-call time, not transport time.

### 20. Queue overflow is not observable

**Evidence:** Default policy drops records, but no drop counter, callback, or diagnostic event identifies the loss.

**Impact:** Applications can lose logs silently under load.

**Minimal resolution:**

1. Count dropped records by channel and overflow policy.
2. Add one rate-limited diagnostic callback or `Meter` counter.
3. Document that `Block` can stall application threads and should not be the default.

**Done when:** Operators can detect and alert on SDK-side drops.

### 21. Partial batch rejection is treated as success

**Evidence:** HTTP `207 Multi-Status` is successful to `HttpClient`; provider invokes `DeliverySuccess` without checking `IngestResponse.Rejected` or item errors.

**Impact:** Rejected log records are silently discarded.

**Minimal resolution:**

1. Inspect `Rejected` and `Errors` after every ingestion call.
2. Report rejected items through delivery-failure/partial-failure diagnostics.
3. Retry only errors explicitly classified as retryable, preserving event IDs.

**Done when:** Every rejected item is surfaced and no validation error is blindly retried.

### 22. Automatic channel provisioning fails silently and crosses privilege boundaries

**Evidence:** Startup probing catches broad exceptions, creation supports only statically known channels, and admin credentials/endpoints are not wired by default.

**Impact:** Missing channels can cause log loss with little diagnostic information; application processes need excessive privileges to self-provision.

**Minimal resolution:**

1. Move channel provisioning to Aspire/AppHost configuration.
2. Remove automatic provisioning from the logging provider for production use.
3. Surface startup validation failures through standard logging/health diagnostics.

**Done when:** Application logger requires ingest permission only and missing channels fail visibly before traffic begins.

### 23. HTTP client ownership and configuration are unclear

**Evidence:** Factory methods create `HttpClient` instances, and a separate admin client can be created internally. `DistillClient` does not dispose owned clients or integrate with `IHttpClientFactory`.

**Impact:** Connection lifetime, DNS refresh, timeouts, and shutdown behavior are hard to control.

**Minimal resolution:**

1. Make the primary registration use `IHttpClientFactory`/typed clients.
2. Accept separate public/admin `HttpClient` instances when admin support is needed.
3. Set documented request timeouts and avoid creating undisposed clients internally.
4. Keep direct constructor for callers that own their client.

**Done when:** DI registration has managed client lifetimes and no hidden owned `HttpClient`.

### 24. SDK uses unchecked strings for closed protocol values

**Evidence:** Presets, severity, aggregation function, granularity, and ordering are public strings.

**Impact:** Typos compile and fail only at runtime.

**Minimal resolution:**

1. Add string-enum types with JSON converters for closed server values.
2. Keep raw advanced APIs only as an explicit escape hatch.
3. Contract-test enum wire values against OpenAPI.

**Done when:** Normal channel creation is compile-time guided and server-compatible.

### 25. SDK does not pin API version

**Evidence:** Server supports `X-API-VERSION`; C# requests do not set it.

**Impact:** Client behavior can drift when server defaults change.

**Minimal resolution:**

1. Send the SDK-supported API version on every request.
2. Expose an advanced override only if needed.
3. Test unsupported-version error handling.

**Done when:** SDK/server compatibility is explicit on the wire.

## P1 — Server operations and observability

### 26. Health endpoint is liveness-only but described as readiness

**Evidence:** `/health` always returns `200` and checks no dependencies.

**Impact:** Orchestrators can route traffic to a process with a dead broker consumer or unavailable sink.

**Minimal resolution:**

1. Keep `/health` as liveness.
2. Add `/ready` checking publisher, supervised consumer, loaded channel configuration, and required sink connectivity.
3. Remove readiness when consumer supervision fails.

**Done when:** Orchestration stops routing ingestion to an instance unable to honor its acceptance contract.

### 27. Metrics are inaccurate and incomplete

**Evidence:** Persist counters increment both after RabbitMQ publication in `src/core/channel.rs` and after sink writes in `src/broker/mod.rs`. Error counter is never incremented.

**Impact:** Dashboards overcount persistence and cannot reliably alert on failures.

**Minimal resolution:**

1. Rename broker-admission and sink-persistence counters separately.
2. Increment sink-persisted only after sink confirmation.
3. Increment error/retry/DLQ/drop counters at their actual failure points.
4. Add queue depth, consumer state, buffer bytes, and channel count metrics.

**Done when:** A deterministic integration test produces expected metric deltas.

### 28. Queue growth has no application-defined ceiling

**Evidence:** Durable delivery and dead-letter queues are declared without maximum length/bytes or retention policy.

**Impact:** Extended sink outage can consume RabbitMQ disk until broker alarms stop the system.

**Minimal resolution:**

1. Document required RabbitMQ disk alarms and queue policies.
2. Cap each delivery and retry queue at 1 GiB or 1,000,000 messages, whichever comes first, with `reject-publish` overflow. Failed broker admission must fail ingestion rather than return `202`.
3. Alert on queue depth, oldest message age, disk watermark, retry depth, and DLQ count.

**Done when:** Sink outage produces bounded, observable behavior with a documented operator response.

### 29. Dead-letter messages have no supported replay workflow

**Evidence:** Exhausted messages reach a durable DLQ, but no command/API safely inspects or replays them.

**Impact:** “Durable failure” becomes permanent manual RabbitMQ surgery.

**Minimal resolution:**

1. Add a minimal CLI command to list counts and replay selected/all DLQ messages.
2. Preserve original exchange, routing key, retry count, and failure reason.
3. Make replay idempotent and test it against PostgreSQL.

**Done when:** Operator can recover from a resolved sink outage using documented tooling.

### 30. Channel replacement is not transactional

**Evidence:** Schema state can change before sink and broker setup completes. Replacement flushes and mutates several resources without one commit boundary.

**Impact:** Failed replacement can leave schema/runtime state inconsistent.

**Minimal resolution:**

1. Validate and construct new sink, schema, and topology before changing active state.
2. Atomically swap the channel record only after preparation succeeds.
3. Retain old channel and schema on failure.
4. Add failure-injection tests for each preparation step.

**Done when:** Every failed replacement leaves the prior channel fully operational.

### 31. Database schema changes are ad hoc

**Evidence:** Sink connection runs `CREATE TABLE`/`ALTER TABLE` directly, with different upgrade behavior by driver and no migration ledger.

**Impact:** Production deployments require DDL privileges and upgrades are difficult to audit or roll forward safely.

**Minimal resolution:**

1. For PostgreSQL v1, add versioned, idempotent migrations.
2. Run migrations explicitly before production deployment; permit automatic migration only through an opt-in development setting. Document separate migration and runtime database privileges.
3. Test upgrade from the previous released schema.

**Done when:** Database upgrade path is repeatable and tested without broad runtime DDL privileges.

### 32. Production deployment guidance is incomplete

**Impact:** Development Compose may be mistaken for a supported production topology.

**Minimal resolution:**

1. Mark Compose as development-only.
2. Document TLS termination, secret injection, RabbitMQ persistence/credentials, PostgreSQL backup, resource requests/limits, readiness, shutdown timeout, and monitoring.
3. Provide Azure Container Apps deployment instructions through Aspire publish.

**Done when:** Operator can deploy the supported v1 topology without inferring security or durability settings.

## P1 — Packaging, testing, and release process

### 33. C# packages have inconsistent versions and incomplete metadata

**Evidence:** NuGet defaults to `1.0.0`; Rust package is `0.1.0`. Generated packages lack useful author/license/readme metadata.

**Minimal resolution:**

1. Use release version `0.1.0` across server, image, and NuGet packages.
2. Add repository URL, MIT license expression/file, `Monatrox` as author, package readmes, and release notes.
3. Add `LICENSE` at repository root.
4. Produce symbol packages and deterministic builds.

**Done when:** Package metadata and image tags identify one matching release.

### 34. `Distill.Extensions.Logging` cannot be packed

**Evidence:** `dotnet pack` fails with `NU5039` because declared `README.md` is not placed in the package.

**Minimal resolution:**

1. Correct the README pack path.
2. Add a CI pack-and-inspect step for all packages.
3. Install produced packages into a clean temporary consumer project.

**Done when:** All three packages pack and restore together from an empty local feed.

### 35. No C# automated tests

**Impact:** Serialization, routing, batching, overflow, disposal, health, and Aspire environment contracts are unprotected.

**Minimal resolution:**

1. Add one SDK test project and one Aspire integration test project.
2. Prioritize tests for blockers 9–25 rather than broad unit coverage.
3. Run tests against the generated OpenAPI contract and one real PostgreSQL/RabbitMQ stack.

**Done when:** Known SDK/Aspire regressions fail CI before package creation.

### 36. No CI or release pipeline

**Minimal resolution:**

1. Add CI for Rust format, Clippy, tests, .NET build/test/pack, container build, and golden-path integration test.
2. Add dependency vulnerability checks (`cargo audit` or `cargo deny`, plus NuGet audit).
3. Publish private GHCR images only after all gates pass; upload NuGet packages as CI build artifacts without publishing them.
4. Generate provenance/SBOM if required by target users.

**Done when:** Releases are reproducible and cannot bypass required checks.

### 37. Rust formatting currently fails

**Evidence:** `cargo fmt --check` reports differences in `src/core/errors.rs` and `src/http/openapi.rs`.

**Minimal resolution:** Run `cargo fmt` and enforce `cargo fmt --check` in CI.

**Done when:** Clean checkout passes formatting without modifications.

### 38. OpenAPI and hand-written C# models can drift

**Evidence:** Server OpenAPI is generated at runtime, while C# request/response models are maintained independently. Current tests verify route coverage but not C# compatibility.

**Minimal resolution:**

1. Check in or generate a release OpenAPI artifact.
2. Add contract tests serializing C# requests and deserializing representative server responses.
3. Fail CI on breaking API changes unless API version changes.

**Done when:** Server contract and published SDK are verified together for every release.

### 39. Documentation contains contradictory or stale claims

**Examples:** loopback-only administration despite public routing; supported Aspire named/custom sinks; durable acceptance for aggregate-only events; retention/compression behavior; automatic channel provisioning and health behavior.

**Minimal resolution:**

1. Update README and package docs after choosing supported v1 scope.
2. Label planned/experimental features explicitly.
3. Make every quick-start command part of a runnable documentation smoke test.

**Done when:** Clean-machine users can follow one documented path without relying on source knowledge.

## Release acceptance checklist

Production-ready v1 should not ship until all items below pass:

- [ ] Fresh Aspire AppHost with documented GHCR pull credentials pulls a versioned image and starts without cloning Distill source.
- [ ] `.WithReference(distill)` supplies service discovery and ingest credentials.
- [ ] AppHost provisions either PostgreSQL or HTTP sink plus channels without giving application code admin credentials.
- [ ] Structured `ILogger` event reaches PostgreSQL with original timestamp and properties.
- [ ] Structured event and aggregate reach a reference HTTP receiver with correct record IDs, authentication, retry, and duplicate behavior.
- [ ] Internal Aspire HTTP receiver works through an exact destination allowlist; arbitrary private destinations remain blocked.
- [ ] Burst larger than two SDK batches fully drains.
- [ ] SDK preset serialization matches server behavior.
- [ ] Immediate Distill termination after `202` does not lose accepted events.
- [ ] SIGTERM flushes aggregate buffers.
- [ ] RabbitMQ restart recovers automatically.
- [ ] Poison and exhausted messages reach DLQ without blocking healthy delivery.
- [ ] Distill restart restores channel/sink state before consuming backlog.
- [ ] `/health` and `/ready` report liveness and readiness accurately.
- [ ] Metrics distinguish admission, sink persistence, retries, drops, and DLQ delivery.
- [ ] NuGet packages pack, install, and run from a clean local feed.
- [ ] Rust and .NET tests, formatting, linting, vulnerability scans, package builds, image build, and end-to-end test pass in CI.
- [ ] Documentation describes only implemented and tested v1 behavior.

## Implementation order

Work in this order to avoid implementing behavior that v1 later removes:

1. **Lock v1 surface first** — blockers 3, 7, 8, and 22. Startup/Aspire is supported channel provisioning workflow; runtime channel mutations remain documented as ephemeral; PostgreSQL-only rolling aggregates; reject retention/compression; no logger self-provisioning. *In progress: scope and validation complete; startup recovery testing remains.*
2. **Define contracts and boundaries** — blockers 9, 9a, 10, 11, 12, 14, 17, 24, 25, and 38. One configuration format, public/admin split, health API, HTTP receiver wire contract, SDK serialization/version types.
3. **Make broker acceptance durable** — blockers 1, 2, 4, 5, 6, 28, and 29. Durable event handoff, shutdown flush, reconnect supervision, retry/DLQ topology, poison handling, queue caps, replay.
4. **Finish runtime correctness** — blockers 18, 19, 20, 21, 23, 26, 27, 30, and 31. SDK drain/timestamps/failures/client ownership; readiness/metrics; transactional replacement; explicit migrations.
5. **Ship golden path** — blockers 13, 15, 16, and 32. Versioned image, RabbitMQ persistence/credentials, Aspire sample plus end-to-end test, Azure publish docs.
6. **Release guardrails and docs** — blockers 33–39. Run blocker 37 now; package metadata/pack fix; CI gates; contract checks; remove stale claims.

Build tests with each phase. Phase 5 golden-path test becomes release gate in phase 6.
