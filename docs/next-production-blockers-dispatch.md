# Dispatch Prompt: Broker Durability and Recovery

Use the following prompt to dispatch the next production-readiness work.

---

You are working in the Distill repository. Implement and verify the next production-readiness phase: broker durability and recovery. Use `docs/production-readiness-blockers.md` as the source of truth, focusing on blockers 1, 2, 4, 5, 6, 28, and 29, plus only the generation-retirement portion of blocker 30.

Preserve the current acceptance contract and architecture decisions:

- A successful ingest is represented by one persistent routed message and returns `202` only after a mandatory RabbitMQ publish receives an explicit acknowledgement.
- `PersistAndAggregate` must not become two independently fallible admission publications.
- Aggregate input remains ordered and isolated by channel generation.
- A broker delivery is acknowledged only after its next durable handoff is publisher-confirmed.
- Work accepted under an old channel generation remains bound to that generation and sink.
- Payloads, credentials, authorization headers, connection strings, and API keys must never appear in logs or operator-facing error messages.

## Current implementation handoff

This phase is **partial**. Do not mark any listed blocker resolved without its required real-broker evidence.

### Implemented

- `src/broker/mod.rs`
  - Broker envelopes now carry `version: 1`; missing versions are accepted as v1 so existing queued messages drain, while unsupported versions are DLQed.
  - Sink failures move to per-destination quorum TTL retry queues using RabbitMQ at-least-once dead-lettering. Retry admission receives a publisher confirm before the original delivery is acknowledged. No consumer sleeps for retry delay.
  - Delivery, retry, DLQ, and aggregate-generation queues declare `x-max-length=1000000`, `x-max-length-bytes=1073741824`, and `x-overflow=reject-publish`.
  - Malformed and exhausted deliveries are wrapped once in a DLQ record containing bounded failure metadata, original exchange/routing data, retry count, known envelope/record IDs, timestamp, and base64 original bytes. Logs do not print payloads.
  - Consumer, retry, registration, and actor-flush logs retain stable error codes while omitting potentially sensitive dynamic text. Shared and generation consumers reconnect after failure with a bounded delay, but full state/readiness and shutdown-aware backoff remain open.
  - `DeadLetterClient` connects passively, scans in constant memory, and supports metadata-only listing plus confirmed replay of `--id` or `--all`. Only structurally valid, retry-exhausted direct persistence records are eligible; poison and aggregate-input records remain retained without making successful all-eligible replay fail, while malformed DLQ wrappers produce a non-zero repair signal.
- `src/infra/cli.rs`: DLQ commands run on the application's existing Tokio runtime, Clap enforces exactly one replay selector, and default output omits payload and failure text.
- `scripts/test-durable-aggregate-input.sh`: supports Podman first and Docker fallback, uses fully qualified images and broker network aliases, and keeps its credential-bearing configuration owner-only. It covers direct, aggregate-only, and `PersistAndAggregate` crash/restart recovery plus SIGTERM flush/failure behavior, but must be rerun after the quorum-retry and permission changes.
- `.github/workflows/broker-durability.yml`: `durable-handoff` runs formatting, Clippy, tests, script syntax, and the complete durable-handoff integration script.
- `src/core/channel.rs` and `src/http/channels.rs`: aggregate-flush and registration logs also omit dynamic error text; registration retains its stable error code.
- `Dockerfile`, `README.md`, `docs/cli.md`, and `docs/production-readiness-blockers.md` document current behavior and queue upgrade caveat.

### Executed evidence

Passed locally:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets            # 49 tests
bash -n scripts/test-durable-aggregate-input.sh
cargo run --quiet -- dlq replay --help
git diff --check
```

The prior version of the integration script passed under Podman, but that evidence predates the quorum-retry and owner-only configuration corrections. Neither Podman nor Docker is accessible in the current review environment, and GitHub Actions has not yet supplied execution evidence. Treat all current real-broker acceptance criteria as unverified until the revised script runs.

### Important limits and risks

- Existing RabbitMQ queues declared without new `x-*` arguments cannot be redeclared compatibly. Drain/replay backlog, then recreate queues during maintenance; no automatic queue migration exists.
- The shared and generation consumers reconnect with a bounded fixed delay, but they do not yet expose consumer state or cancel their delay explicitly during shutdown. `RabbitPublisher` remains one-shot and fails admission safely when unavailable.
- Old channel generations are not retired. Do not add forced queue deletion; safe retirement needs drain, actor flush, no in-flight delivery, and empty-queue proof.
- Retry isolation, at-least-once DLX behavior, DLQ/replay, overflow, reconnect, poison, and retirement remain untested against a real broker after the corrections. Docker/Podman and CI execution remain unverified.
- Aggregate-input DLQ replay remains disabled until durable source-event deduplication exists. Do not describe aggregate replay as idempotent merely because sink record IDs are stable.
- Consumer logs now provide safe error codes and reconnect, but consumer health is not exposed. Do not treat process liveness as broker-consumer health.
- The shutdown integration scenario intentionally stops the active Distill container before starting the next one. Do not run overlapping instances against the same generation queue when asserting which actor owns an unacknowledged delivery.

### Next work, in order

1. Run `durable-handoff` in GitHub Actions and preserve its result. Run the same script with Docker daemon access when available. Keep blockers 1 and 2 partial until that evidence is recorded.
2. Complete consumer supervision with bounded exponential backoff and jitter, shutdown cancellation, and explicit starting/healthy/reconnecting/stopped state. Reconnect the publisher or retain verified fail-closed admission. Add RabbitMQ-restart integration coverage before changing blocker 4 beyond partial.
3. Add isolated real-broker coverage for retry TTL/DLX routing, a failing destination not delaying healthy work, malformed/unsupported/exhausted poison delivery, metadata-only DLQ listing, confirmed replay to PostgreSQL, queue arguments, and queue-full `202` rejection. Use bounded, observable waits; do not add consumer sleeps.
4. Define bounded DLQ capacity/retention and operator response without silent loss. Document broker disk alarms and queue-depth/oldest-message alerts.
5. Implement race-safe generation retirement only after proof that admission moved, actor buffer flushed, in-flight deliveries reached zero, and generation queue is empty. Defer and surface cleanup when proof fails; never force-delete. Then add concurrent-ingest retirement coverage.
6. Update blocker statuses only from executed evidence. Current status: blockers 1, 2, 4, 5, 6, 28, 29, and 30 remain partial.

Before editing, inspect the current uncommitted changes and preserve unrelated user work. Follow `AGENTS.md`; use CodeGraph first when available, and follow the mandatory Rust skill before changing Rust files.

## Remaining required work

Do not reimplement already-delivered confirmed routed admission, quorum retry topology, envelope versioning/DLQ records, queue caps, bounded DLQ CLI, crash/restart coverage, or SIGTERM coverage. Extend them only with real-broker evidence or a defect fix. Aggregate-input replay is intentionally not delivered.

1. Supervise shared persistence and every generation consumer. On connection/channel/consumer failure, reconnect with bounded exponential backoff and jitter; re-declare compatible topology; restore consumers; cancel promptly at shutdown; and expose truthful consumer state. Ensure publisher unavailability cannot produce `202`.
2. Prove retry isolation, TTL/DLX routing, poison/DLQ behavior, metadata-only listing, confirmed PostgreSQL replay, queue arguments, and overflow admission behavior against RabbitMQ. Add DLQ retention/capacity policy, disk-alarm guidance, and operator response.
3. Implement safe generation retirement. Remove a generation from consumer, actor, sink maps, and broker topology only after admission moves, actor flush succeeds, no delivery is in flight, and queue emptiness is proven. Add concurrent-ingest coverage; defer cleanup when proof fails.
4. Preserve complete CI and Docker evidence for durable-handoff. Update operator/integrator docs and `docs/production-readiness-blockers.md` only from executed evidence.

Do not expand this dispatch into full transactional replacement rollback, persistence of runtime channel definitions/generations, general `/ready` endpoint design, the complete metrics overhaul, SDK work, or database migrations. Leave blocker 30 partial unless all of its separate acceptance criteria are actually completed.

## Acceptance criteria

### Durable acceptance and shutdown

- Every returned `202` has a confirmed RabbitMQ handoff; unroutable, negatively acknowledged, timed-out, closed-channel, and queue-overflow publications return a safe non-success response.
- Immediate process termination after `202` loses no direct or aggregate input and causes no duplicate persisted record or aggregate contribution after restart.
- SIGTERM stops new admission, drains in-flight HTTP requests, and flushes all in-process aggregate generations within `DISTILL_SHUTDOWN_TIMEOUT_SECONDS`.
- A shutdown timeout exits non-successfully and emits a concise operational error without sensitive event or credential data.

### Broker recovery and isolation

- Restarting RabbitMQ during active direct and aggregate delivery does not require restarting Distill.
- All required exchanges, queues, bindings, confirms, QoS settings, and consumers are restored after reconnect without incompatible redeclaration errors.
- Reconnect loops use bounded backoff with jitter, do not spin, and stop promptly during application shutdown.
- Consumer state distinguishes starting, healthy, reconnecting/degraded, and stopped states and can be consumed by later readiness/metrics work.
- A destination that repeatedly fails does not delay successful delivery to an unrelated healthy destination beyond normal scheduling latency.
- FIFO behavior is preserved wherever the channel ordering contract requires it.

### Retry, DLQ, and replay safety

- Retry delays are broker-managed and durable; no consumer task sleeps while holding the shared delivery path.
- The original delivery is acknowledged only after confirmed retry or DLQ publication. A failed handoff leaves the original recoverable.
- Retry count survives broker and process restarts and terminates at the documented maximum.
- Malformed JSON, unsupported envelope versions, invalid routed payloads, and exhausted valid messages enter the DLQ once, while a later valid message continues processing.
- DLQ metadata includes envelope version when known, failure category, bounded/sanitized reason, retry count, original exchange/routing key, message identifier when known, and failure timestamp.
- CLI listing is metadata-only by default. Replay supports an explicit selection and all-eligible mode, gives actionable exit codes/output, and never acknowledges a DLQ message before confirmed republish.
- Replaying the same eligible direct-persistence item is safe against duplicate sink writes using the existing stable event/aggregate identifiers; aggregate-input replay remains ineligible.

### Capacity and lifecycle bounds

- Automated RabbitMQ assertions prove delivery, aggregate-input, and retry queues use both `x-max-length=1000000` and `x-max-length-bytes=1073741824` with `x-overflow=reject-publish` or an equivalent policy whose behavior is tested.
- Queue-full admission fails visibly and predictably; tests prove it cannot produce `202`.
- DLQ capacity/retention behavior, broker disk alarms, and the operator response are documented and tested where automation is practical.
- Repeated channel replacements do not grow in-memory generation handles or RabbitMQ generation queues without bound.
- A concurrent-ingest retirement test proves no accepted message is lost, rerouted to the replacement sink, or deleted while in flight.
- If a generation cannot be proven drained, cleanup is deferred and surfaced operationally rather than forced.

### Developer experience and compatibility

- Existing public ingest payloads and successful response shapes remain compatible unless a versioned change is explicitly justified and documented.
- New configuration has secure production defaults, validation at startup, clear environment-variable names, and actionable errors that identify the setting without echoing secrets.
- CLI help contains safe examples and clearly explains selection, dry-run/preview behavior if provided, confirmation semantics, and partial failure recovery.
- Legacy queued messages are either migrated through a confirmed handoff or supported until drained; they are never silently dropped.
- Unit tests cover state machines and metadata validation. Docker integration tests cover real RabbitMQ confirms, overflow, retry TTL/dead-lettering, reconnect, poison delivery, replay, crash/restart, SIGTERM, and generation retirement.
- Tests use bounded waits and deterministic readiness checks rather than arbitrary long sleeps.

## Required verification

Run and report, at minimum:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
bash -n scripts/test-durable-aggregate-input.sh
git diff --check
```

Also run every new Docker-backed broker test in an environment with Docker daemon access. If Docker is unavailable, do not mark the integration acceptance criteria complete: report the exact unverified scenarios and leave the associated blocker partial.

Finish with a second regression and security review of the complete diff. Confirm that direct persistence, aggregate-only, and `PersistAndAggregate` behavior remain correct; publisher confirmations are checked on every durable transition; no consumer can silently die while health remains falsely positive; queue cleanup cannot delete accepted work; retries and replay cannot expose secrets or payloads; and the documentation status matches executed evidence.

The handoff must list changed files, acceptance criteria satisfied, commands and integration scenarios executed, unresolved risks, and the exact blocker statuses after the work. Do not claim a blocker is resolved based solely on static review or unit tests when its acceptance criteria require RabbitMQ, PostgreSQL, process termination, or reconnect behavior.

---
