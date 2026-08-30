# Production Readiness Blockers

This document tracks work required before Distill can make a production-ready v1 release. Priorities assume v1 targets Aspire, the C# SDK, RabbitMQ, PostgreSQL, and custom HTTP/API endpoint sinks.

## Recommended v1 scope

Keep the first supported path narrow:

- PostgreSQL and custom HTTP/API endpoint sinks;
- one default sink per Distill resource for the initial golden path;
- one Aspire-managed RabbitMQ resource;
- finite channels explicitly provisioned by the owning application during startup;
- `Distill.Client` and `Distill.Extensions.Logging`;
- published, version-pinned NuGet packages and container image.

HTTP sink support is mandatory: it is the extension point that lets teams implement persistence against unsupported production systems. Defer additional first-party database drivers, retention, compression, and multi-sink orchestration until their end-to-end behavior is implemented and tested. Remove or mark those capabilities experimental in documentation until then.

## Confirmed v1 decisions

- Publish private versioned images to `ghcr.io/monatrox/distill`; GitHub Actions publishes on `v*` tags. Consumers must provide GHCR pull credentials.
- Use version `0.1.0` for server, image, and all NuGet packages. NuGet packages remain CI build artifacts; do not publish them yet.
- Cap each RabbitMQ delivery and retry queue at 1 GiB or 1,000,000 messages, whichever comes first. Use `reject-publish`; failed admission must not return `202`.
- Run PostgreSQL migrations explicitly before production deployment. Permit automatic migration only with an opt-in development setting.
- Support Azure Container Apps deployment through Aspire publish as production golden path. Compose remains development-only.

## Current-branch architecture decisions

- An accepted ingest is represented by one routed, persistent RabbitMQ message. The route flags and channel generation travel with that message so `PersistAndAggregate` does not require two independently fallible admission publications.
- Ingestion returns `202` only after a mandatory publish receives an explicit broker acknowledgement. An unroutable or negatively acknowledged publish fails admission.
- Aggregate inputs use durable, ordered queues isolated by channel generation. Their broker delivery is acknowledged only after any direct persistence finishes and the resulting aggregate publication is broker-confirmed.
- A channel replacement prepares its sink, schema, exchange, generation queue, and actors before it stops the old event actor. Already accepted work remains pinned to the old generation and sink; newly accepted work uses the activated generation.
- Startup-configured definitions derive stable generations. Runtime replacements deliberately receive fresh generations, but runtime mutations remain ephemeral until the control plane and generation lifecycle are persisted.
- Shutdown stops admission, drains the HTTP listeners, and then flushes every in-process aggregate generation. `DISTILL_SHUTDOWN_TIMEOUT_SECONDS` bounds this work and defaults to 30 seconds.
- Malformed broker deliveries are confirmed into the DLQ before the original is acknowledged. Structured failure metadata, envelope versioning, and safe replay of eligible records are implemented; real-broker evidence and a repair path for ineligible records remain required before release.
- Retry delay queues are quorum queues configured for RabbitMQ at-least-once dead-lettering. Replay is limited to valid exhausted direct-persistence records; poison and aggregate-input records remain retained until a safe transformation/deduplication contract exists.

## Current status

**Implemented and locally verified:** blocker 8 (unsupported retention/compression rejected), 17 (three-state aggregation serialization), 22 (explicit fail-fast application provisioning), 24 (typed C# protocol values), and 25 (SDK API-version pinning). Rust formatting is clean and the new broker-durability workflow enforces it, but that workflow still needs execution evidence before blocker 37 can close.

**Partially implemented:** blocker 1 publishes one routed event with mandatory publisher confirmation before `202`; isolated generation queues hold aggregate acknowledgements until confirmed aggregate publication. The crash/restart script passed under Podman before the latest quorum-retry and permission corrections and must be rerun. Blocker 2 has the same prior local SIGTERM evidence and also needs rerun/CI evidence. Blocker 4 now reconnects shared and generation consumers after a bounded delay, but lacks state, shutdown-aware backoff, jitter, publisher reconnect, and broker-restart coverage. Blocker 5 uses quorum TTL retry queues with at-least-once dead-lettering, but needs RabbitMQ isolation and saturation coverage. Blocker 6 versions envelopes, records byte-bounded failure metadata, and provides bounded metadata-only DLQ tooling; broker integration coverage remains. Blocker 28 applies `reject-publish` limits to delivery, retry, DLQ, and generation queues, but needs broker assertions and generation retirement. Blocker 29 safely replays eligible direct-persistence records but deliberately retains poison and aggregate inputs; PostgreSQL replay coverage remains. Blocker 30 remains incomplete. Blockers 7, 9, 11, 12, and 14 have core implementation but lack complete contract/integration coverage.

**Current critical path:** run blocker 1's Docker-backed crash/restart test in CI. Then finish broker recovery/retry/limits/replay and generation retirement (4, 5, 28, 29, 30), and close partial blockers with integration tests. All unlisted blockers remain open.

**Verification snapshot (2026-08-30):** `cargo test --all-targets` passes 49 tests; CLI selector/error smoke checks, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, `git diff --check`, and shell validation pass. The revised broker script could not run because neither Podman nor Docker was accessible. Current retry, DLQ, replay, reconnect, overflow, crash/restart, SIGTERM, and retirement scenarios therefore still require real-broker evidence.

## P0 — Correctness and durability

### 1. Durable aggregate acceptance lacks crash/restart verification

**Status:** Partial. Each accepted event is one mandatory, publisher-confirmed broker message. Aggregate routes use ordered, per-channel-generation queues whose acknowledgements wait for confirmed aggregate publication. `scripts/test-durable-aggregate-input.sh` starts RabbitMQ/PostgreSQL, kills Distill after confirmed direct, aggregate-only, and `PersistAndAggregate` admissions, restarts it, and verifies direct events persist once plus each aggregate contains two events.

**Remaining work:** Rerun the revised Podman/Docker-backed check and preserve it as CI evidence. Prior evidence predates the retry-topology correction.

**Done when:** Every `202` response has a tested recovery path after immediate process termination.

### 2. Graceful aggregate flushing lacks SIGTERM coverage

**Status:** Partial. `ChannelRegistry::shutdown` stops admission and gives every active aggregate generation a configurable, 30-second-default window to publish with broker confirmation after both listeners stop. The script covers successful and unavailable-broker SIGTERM cases, but its current revision lacks executed container evidence.

**Remaining risk:** Prior Podman evidence predates the latest broker and fixture corrections; rerun locally and in CI.

**Minimal resolution:**

1. Preserve the SIGTERM integration check as CI evidence.

**Done when:** SIGTERM either durably flushes all accepted data or exits with an explicit failure after the configured timeout.

### 3. Dynamic channel and lifecycle state is not durable

**Status:** Open. Stable generations let unchanged startup-configured definitions reconnect to their durable aggregate queues. API-created definitions, lifecycle changes, and the fresh generations assigned to runtime replacements are not persisted. The broker consumer also starts before startup definitions finish registering.

**Evidence:** `ChannelRegistry` stores channels in an in-memory `HashMap`. API-created channels, schemas, and paused/archived states are not written back to configuration.

**Impact:** Restart removes runtime-created channels and cannot recreate runtime replacement generations. Existing RabbitMQ messages may retry or dead-letter because their generation-specific sink, actor, or queue consumer no longer exists.

**Minimal resolution:**

1. Document explicit application startup declarations as the supported channel provisioning workflow.
2. Keep runtime channel mutation, but clearly mark it ephemeral until a durable control-plane store is added.
3. Persist ensured definitions and active generations, or otherwise restore channel definitions and generation-specific sink mappings before RabbitMQ resumes backlog consumption.
4. Add restart tests with queued messages and lifecycle changes.

**Done when:** Restart restores every startup-configured channel before queued delivery resumes, and runtime mutation is either disabled on the production boundary or persisted with its active generation mappings.

### 4. RabbitMQ consumer does not recover after connection failure

**Status:** Partial. Shared persistence and generation consumers reconnect on fresh connections after a bounded two-second delay, re-declaring their required topology, confirms, and QoS. Publisher reconnect, consumer state, jittered exponential backoff, shutdown cancellation, and real broker-restart coverage remain.

**Remaining risk:** Process health still does not expose consumer degradation, and the publisher remains fail-closed rather than reconnecting.

**Minimal resolution:**

1. Add exponential backoff with jitter and shutdown cancellation to the current supervisor.
2. Track consumer state for readiness and metrics.
3. Reconnect the publisher or verify its fail-closed admission behavior throughout broker restart.
4. Test broker restart during active delivery.

**Done when:** RabbitMQ restart recovers automatically without restarting Distill or losing acknowledged events.

### 5. Shared direct-persistence retries block unrelated channels

**Status:** Partial. Per-channel-generation aggregate queues isolate aggregate ingestion and preserve ordered actor admission. Direct persistence now hands failures to durable, capped TTL retry queues and acknowledges only after confirmed retry publication. RabbitMQ isolation coverage remains.

**Evidence:** `src/broker/mod.rs` routes failed delivery through confirmed, TTL-based retry queues; real-broker isolation coverage remains.

**Remaining risk:** Sink-call latency can still hold the shared prefetch-one consumer briefly, and retry/DLQ saturation isolation lacks broker coverage.

**Minimal resolution:**

1. Test quorum TTL/DLX transfer and prove a healthy channel continues while another sink fails or its retry queue is saturated.

**Done when:** Failure of one destination does not stop delivery to unrelated healthy destinations.

### 6. Poison-message DLQ handling lacks a complete recovery contract

**Status:** Partial. Malformed envelopes and payloads are published to the DLQ before acknowledgement; envelopes are versioned, failure reasons are bounded by UTF-8 bytes, and CLI inspection/replay is async, constant-memory, and payload-free by default. Poison and aggregate-input replay is intentionally ineligible. Broker integration coverage remains.

**Remaining risk:** Confirmed DLQ/replay and full-DLQ recovery have not been exercised against RabbitMQ. Unsafe records need a separate repair/export contract rather than unchanged replay.

**Minimal resolution:**

1. Add malformed JSON/unsupported-version/full-DLQ broker integration tests; prove later valid delivery continues or the supervised consumer recovers after capacity returns.
2. Define safe inspection/repair for ineligible poison records without exposing payloads by default.

**Done when:** Poison messages reach the DLQ once with safe diagnostic/version metadata, later valid messages continue processing, and operators have a tested replay path.

### 7. Rolling aggregates are inconsistent across sink drivers

**Status:** Partial. Channel registration rejects rolling aggregates for every non-PostgreSQL sink. PostgreSQL rolling contract coverage remains.

**Evidence:** PostgreSQL merges rolling windows transactionally. MySQL and SQL Server overwrite the existing record with the latest window snapshot. HTTP receivers get repeated snapshots with the same stable aggregate ID, while current documentation also recommends deduplicating that ID; a receiver can therefore mistake later rolling windows for duplicates.

**Impact:** Rolling totals, averages, sums, and counts lose prior windows or become ambiguous outside PostgreSQL.

**Minimal resolution:**

1. For v1, support rolling aggregates only with PostgreSQL and reject them for HTTP, MySQL, and SQL Server.
2. Document HTTP aggregate delivery as non-rolling window records for v1.
3. Later, define an explicit HTTP rolling upsert/merge contract and port PostgreSQL merge semantics to other database drivers.

**Done when:** Rolling aggregates are rejected for every non-PostgreSQL sink, and PostgreSQL rolling-aggregate contract tests pass.

### 8. Unsupported retention and compression must remain rejected

**Status:** Implemented. `retention_hours` and enabled `compression_enabled` return v1 validation errors; production documentation no longer advertises either behavior.

**Future work:** Reintroduce these fields only with database jobs/policies and end-to-end tests.

**Done when:** Configuration cannot claim unsupported retention or compression behavior.

## P0 — Aspire integration

### 9. Aspire sink configuration lacks complete golden-path coverage

**Status:** Partial. Server parses `DISTILL_SINK__{NAME}__*` and `DISTILL_DEFAULT_SINK`; Aspire emits that format. Explicit HTTP/PostgreSQL golden-path APIs, private receiver allowlisting, secret-backed headers, and end-to-end contract coverage remain.

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
2. Inject both into Distill and inject the ingest key into consuming applications.
3. Inject the admin key only into applications that explicitly opt into startup channel provisioning.
4. Make missing key configuration impossible or generate development-only values explicitly.
5. Do not expose admin credentials to `DistillLoggerProvider`; only the startup provisioning call should use them.

**Done when:** `.WithReference(distill)` supplies the endpoint and required credentials without plaintext duplication, and ingest-only consumers never receive the admin key.

### 11. Management boundary lacks complete contract coverage

**Status:** Partial. Public router no longer merges administrative routes; Aspire exposes a non-external private admin endpoint. Route tests and final Caddy/OpenAPI/documentation alignment remain.

**Remaining risk:** Caddy, OpenAPI, and route coverage can drift from the now-private administrative boundary.

**Minimal resolution:**

1. Remove admin routes from `public_router`.
2. Keep management on a private endpoint, or intentionally expose it behind a separately documented network/authentication policy.
3. Update Caddy, OpenAPI location, Aspire provisioning, and documentation to match one model.
4. Add route tests proving admin endpoints are absent from the public listener.

**Done when:** Public and administrative surfaces have one documented, tested boundary.

### 12. Aspire health registration and readiness semantics are incomplete

**Status:** Partial. `DistillHealthCheck` now probes `/health` through `CheckHealthAsync` without admin credentials. Resource-level registration and final liveness/readiness split remain.

**Remaining risk:** Resource-level health and liveness/readiness semantics can still diverge.

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

### 14. Dynamic Aspire host ports lack parallel-AppHost coverage

**Status:** Partial. Aspire specifies target port `8080` only; host port allocation is dynamic. Parallel-AppHost coverage remains.

**Remaining risk:** A regression could reintroduce fixed host ports without parallel-AppHost coverage.

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

### 17. Aggregation override serialization must remain three-state

**Status:** Implemented. Null aggregators are omitted; `DisableAggregation = true` emits `"aggregator": null`. Runnable contract checks cover omitted and explicit-null behavior.

**Regression risk:** Serialization changes could again make presets silently disable aggregation.

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

### 22. Channel provisioning must be explicit, fail-fast, and separate from the logger worker

**Evidence:** The former `DistillLoggerProvider` startup probe caught broad exceptions and continued after provisioning failures. It also mixed admin operations into the logging transport and only inspected `DefaultChannel` plus preconfigured `ChannelOptions`; it was not true first-use provisioning.

**Impact:** Missing channels could cause log loss with little diagnostic information. Removing provisioning entirely, however, regresses modular-monolith DX where the application intentionally owns a finite logging contract.

**Minimal resolution:**

1. Keep implicit provisioning out of `DistillLoggerProvider`.
2. Provide `DistillClient.EnsureChannelAsync` for explicit application startup provisioning in `Program.cs`.
3. Require a finite list of definitions, named/default operator-provisioned sinks, and server-enforced channel limits.
4. Fail application startup when any required channel cannot be ensured.
5. Use the admin key only for startup provisioning; continue using the ingest key for normal logging.
6. Document that ensure creates missing channels but does not mutate existing definitions.

**Done when:** Owning applications can provision their declared channels before serving traffic, failures stop startup, and logger categories cannot implicitly create arbitrary resources.

### 23. HTTP client ownership and configuration are unclear

**Evidence:** Factory methods create `HttpClient` instances, and a separate admin client can be created internally. `DistillClient` does not dispose owned clients or integrate with `IHttpClientFactory`.

**Impact:** Connection lifetime, DNS refresh, timeouts, and shutdown behavior are hard to control.

**Minimal resolution:**

1. Make the primary registration use `IHttpClientFactory`/typed clients.
2. Accept separate public/admin `HttpClient` instances when admin support is needed.
3. Set documented request timeouts and avoid creating undisposed clients internally.
4. Keep direct constructor for callers that own their client.

**Done when:** DI registration has managed client lifetimes and no hidden owned `HttpClient`.

### 24. Typed SDK protocol values must remain wire-compatible

**Status:** Implemented. C# enum types serialize with snake_case wire values; raw full-definition API remains an explicit escape hatch.

**Regression risk:** Enum wire values can drift from the server contract without OpenAPI contract coverage.

**Minimal resolution:**

1. Add string-enum types with JSON converters for closed server values.
2. Keep raw advanced APIs only as an explicit escape hatch.
3. Contract-test enum wire values against OpenAPI.

**Done when:** Normal channel creation is compile-time guided and server-compatible.

### 25. SDK API-version pinning must remain enforced

**Status:** Implemented. C# requests send supported API version `1`; runnable contract check asserts the header.

**Regression risk:** Supported client and server API versions can drift without OpenAPI contract coverage.

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

**Status:** Partial; the 1 GiB/1,000,000-message `reject-publish` policy is applied to application delivery, retry, DLQ, and generation queues. Broker assertions, operator alarms, and safe generation retirement remain.

**Evidence:** Application queues now declare maximum length/bytes and `reject-publish`; replacing pre-existing queues requires an explicit drain/recreate maintenance step. Replaced generations still have no retirement or queue-deletion workflow.

**Remaining risk:** Per-queue limits lack real-broker assertions, the number of retry/generation queues can still grow, and operator alarms/retirement are incomplete.

**Minimal resolution:**

1. Assert queue caps and queue-full admission behavior against RabbitMQ.
2. Define when drained retry and channel generations are retired without discarding accepted work.
3. Document and test required disk alarms plus queue depth, oldest-message, retry-generation, channel-generation, and DLQ alerts.

**Done when:** Sink outage produces bounded, observable behavior with a documented operator response, and drained generations do not leave unbounded queues or in-memory bindings.

### 29. Dead-letter recovery lacks complete integration and repair coverage

**Status:** Partial. The local async CLI passively inspects safe metadata and replays one selected or all eligible retry-exhausted direct records with confirmation before acknowledgement. It scans without retaining payload bodies, separately reports confirmed, intentionally retained, and malformed records, and does not redeclare application queues. Poison and aggregate inputs remain retained without failing successful all-eligible replay; integration coverage remains.

**Impact:** “Durable failure” becomes permanent manual RabbitMQ surgery.

**Minimal resolution:**

1. Test eligible replay idempotency against PostgreSQL and partial broker failure.
2. Define a safe repair/export workflow for poison and aggregate-input records without unchanged replay.

**Done when:** Operator can recover from a resolved sink outage using documented tooling.

### 30. Channel replacement is not fully transactional or restart-safe

**Status:** Partial. Registration is serialized and validates/builds the sink, schema, exchange, generation queue, and actors before touching the active record. Queued work remains pinned to its generation-specific sink. The old event actor is then stopped and flushed before schema/consumer activation and the record swap.

**Remaining gaps:** A failure while stopping or flushing the old actor has no rollback, prepared actors/queues are not cleaned up, and old generations are retained indefinitely. Runtime replacement generations are not restored across process restart.

**Impact:** Most preparation failures leave the prior channel operational, but a late replacement failure can stop its event actor or leak generation resources. A restart can strand backlog accepted under an ephemeral runtime generation.

**Minimal resolution:**

1. Add rollback or a recoverable commit protocol for failures after the old actor begins stopping.
2. Retire prepared resources when activation fails and retire old generations only after their queues and actor buffers are durably drained.
3. Persist or deterministically restore runtime generation mappings before consuming backlog after restart.
4. Add failure-injection and concurrent-ingest tests for every preparation and activation boundary.

**Done when:** Every failed replacement leaves the prior channel fully operational, every accepted event remains bound to the intended sink across replacement and restart, and drained generations are reclaimed safely.

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

### 37. Rust formatting lacks CI enforcement

**Status:** Partial. `cargo fmt --check` passes locally and `.github/workflows/broker-durability.yml` enforces it on pushes and pull requests; the new workflow has not yet produced execution evidence.

**Remaining work:** Preserve a successful workflow run as CI evidence.

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
- [ ] AppHost provisions either PostgreSQL or HTTP sink; the owning application explicitly ensures its finite channels before serving traffic.
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
- [ ] Channel replacement preserves old-generation delivery, rolls back every injected failure, and retires drained generation queues without data loss.
- [ ] `/health` and `/ready` report liveness and readiness accurately.
- [ ] Metrics distinguish admission, sink persistence, retries, drops, and DLQ delivery.
- [ ] NuGet packages pack, install, and run from a clean local feed.
- [ ] Rust and .NET tests, formatting, linting, vulnerability scans, package builds, image build, and end-to-end test pass in CI.
- [ ] Documentation describes only implemented and tested v1 behavior.

## Implementation order

Work in this order to avoid implementing behavior that v1 later removes:

1. **Lock v1 surface first** — blockers 3, 7, 8, and 22. Owning applications explicitly provision finite channels at startup; runtime channel mutations remain documented as ephemeral; PostgreSQL-only rolling aggregates; reject retention/compression; no logger self-provisioning. *In progress: retention/compression and explicit provisioning are implemented; rolling and restart integration coverage remain.*
2. **Define contracts and boundaries** — blockers 9, 9a, 10, 11, 12, 14, 17, 24, 25, and 38. One configuration format, public/admin split, health API, HTTP receiver wire contract, SDK serialization/version types. *In progress: server/Aspire sink environment contract, private admin route, dynamic host port, typed SDK values, aggregation serialization, and API version pinning are implemented; secrets, HTTP receiver contract, resource-level health, OpenAPI artifact, and integration coverage remain.*
3. **Make broker acceptance durable** — blockers 1, 2, 4, 5, 6, 28, and 29. Durable event handoff, shutdown flush, reconnect supervision, retry/DLQ topology, poison handling, queue caps, generation retirement, replay. *In progress: bounded listener shutdown/aggregate flush, poison-message DLQ bypass, one confirmed admission publication, and isolated durable aggregate input are implemented; run aggregate crash/restart coverage in Docker CI before accepting final semantics. Dispatch prompt: `docs/next-production-blockers-dispatch.md`.*
4. **Finish runtime correctness** — blockers 18, 19, 20, 21, 23, 26, 27, 30, and 31. SDK drain/timestamps/failures/client ownership; readiness/metrics; replacement rollback and restart-safe generations; explicit migrations. *In progress: replacement resources are staged and queued work retains its generation-specific sink; rollback, durable runtime generations, retirement, and failure-injection coverage remain.*
5. **Ship golden path** — blockers 13, 15, 16, and 32. Versioned image, RabbitMQ persistence/credentials, Aspire sample plus end-to-end test, Azure publish docs.
6. **Release guardrails and docs** — blockers 33–39. Rust formatting is clean and included in the new broker workflow; execute and extend CI, fix package metadata/pack, add contract checks, and remove stale claims.

Build tests with each phase. Phase 5 golden-path test becomes release gate in phase 6.
