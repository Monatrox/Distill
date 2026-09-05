# Production Readiness Continuation Handoff

This is the durable, session-to-session handoff for the active production-readiness work. Read this file first when resuming. Use `docs/production-readiness-blockers.md` for the complete release backlog and `docs/next-production-blockers-dispatch.md` for the focused broker-phase acceptance contract.

Last updated: 2026-09-05

## Repository snapshot

- Branch: `production-readiness`
- Tested implementation commit: `df28e18936b8aba1aa8b2d768987d8435fc067e5` (`Harden broker durability and recovery`).
- Local verification below was repeated immediately before that commit.
- Repository instructions: follow `AGENTS.md`; because `.codegraph/` exists, use CodeGraph before grep/find or broad code reading. Read and follow the mandatory `ms-rust` skill before modifying any `.rs` file.

Files in the tested implementation:

| File | Current work |
| --- | --- |
| `.github/workflows/ci.yml` | New automatic push/PR Rust and script validation workflow. |
| `.github/workflows/broker-durability.yml` | Manual Docker-backed broker integration workflow and redacted failure artifacts. |
| `docs/cli.md` | DLQ bounds, alarms, operator response, and safe recovery guidance. |
| `docs/next-production-blockers-dispatch.md` | Focused broker phase handoff, evidence, limits, and next work. |
| `docs/production-readiness-blockers.md` | Evidence-based blocker status updates. |
| `docs/production-readiness-handoff.md` | This durable continuation record. |
| `scripts/test-durable-aggregate-input.sh` | Expanded real-service integration harness. |
| `src/broker/mod.rs` | Publisher/consumer recovery, DLQ/replay, bounded queues, and generation retirement. |
| `src/core/registry.rs` | Bounded registry shutdown and consumer supervision integration. |

`docs/cli.md` currently produces a Git warning that CRLF will be replaced by LF the next time Git touches it. `git diff --check` still passes.

## Implemented working state

The current diff implements the following behavior:

1. RabbitMQ admission remains one persistent routed message. A `202` is returned only after mandatory routing and an explicit publisher confirmation. `PersistAndAggregate` remains a single admission publication.
2. Publishers reconnect after a failed publish for subsequent requests. They deliberately do **not** automatically replay the ambiguous failed request: a lost confirmation may mean RabbitMQ accepted it, so replay could duplicate an aggregate contribution.
3. Shared and generation consumers are supervised. They expose internal `starting`, `healthy`, `reconnecting`, and `stopped` states; reconnect with bounded exponential backoff and jitter; restore topology, confirms, QoS, and consumption; and cancel promptly during shutdown.
4. Sink failures use capped per-destination quorum TTL retry queues with RabbitMQ at-least-once dead-lettering. The original delivery is acknowledged only after the next durable retry or DLQ handoff is confirmed.
5. Delivery, retry, DLQ, and generation queues declare `x-max-length=1000000`, `x-max-length-bytes=1073741824`, and `x-overflow=reject-publish`.
6. Broker envelopes are versioned. Missing versions drain as v1; unsupported versions go to the DLQ. Malformed/exhausted deliveries are wrapped once with bounded safe metadata and base64 original bytes. Normal logs and default CLI output do not expose payloads or secrets.
7. The DLQ client scans in constant memory, lists metadata only by default, and replays selected/all eligible exhausted direct-persistence records only after confirmed republish. Poison and aggregate-input records remain retained and intentionally ineligible.
8. Old channel generations retire conservatively: admission moves first; old accepted work remains pinned to its generation and sink; in-flight consumer and sink persistence work is tracked; the old actor is stopped/flushed; and RabbitMQ deletion uses both `if-empty` and `if-unused`. Incomplete proof emits `generation_retirement.deferred` and never force-deletes accepted work.
9. Registry shutdown bounds actor flushing and consumer shutdown with `DISTILL_SHUTDOWN_TIMEOUT_SECONDS`.
10. The integration harness supports Docker and rootless Podman, captures redacted diagnostics, and covers crash/restart, SIGTERM, retry isolation, DLQ/replay, overflow rejection, generation reclamation, queue arguments, and broker reconnect behavior.

## Executed evidence

The following passed against the current worktree:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets                         # 50 tests
cargo run --quiet -- dlq replay --help
bash -n scripts/test-durable-aggregate-input.sh
git diff --check
DISTILL_CONTAINER_ENGINE=podman \
  DISTILL_TEST_ARTIFACT_DIR=/tmp/distill-broker-diagnostics \
  bash scripts/test-durable-aggregate-input.sh
```

The full integration run passed under rootless Podman 5.8.4 with this final result:

```text
durable handoff, retry isolation, DLQ replay, overflow rejection, generation retirement, broker restart, queue limits, and SIGTERM behavior verified
```

That run exercised:

- direct, aggregate-only, and `PersistAndAggregate` crash/restart recovery;
- successful SIGTERM flush and explicit failure when RabbitMQ is unavailable;
- failed HTTP destination isolation while unrelated PostgreSQL persistence succeeds, followed by durable HTTP retry and recovery;
- malformed-message DLQ handoff, later-valid delivery, metadata-only listing, poison retention, and confirmed eligible PostgreSQL replay;
- a small `reject-publish` probe queue proving overflow admission is non-`202`;
- repeated channel replacements proving old accepted aggregate input flushes and two old generation queues are reclaimed;
- delivery, retry, DLQ, and generation queue argument assertions; and
- fail-closed admission while RabbitMQ is stopped, followed by direct and aggregate recovery after RabbitMQ restarts without restarting Distill.

Temporary redacted diagnostics are under `/tmp/distill-broker-diagnostics`; they are not durable repository evidence and may disappear. A GitHub-hosted Docker run has not passed for this revision. The `gh` executable was unavailable locally, and the workflow/diff is uncommitted, so dispatching the remote workflow would not test the current code anyway.

## Exact blocker status

Never close a blocker solely from static review when its acceptance criteria require RabbitMQ, PostgreSQL, process termination, reconnect, or CI evidence.

| Blocker | Status | Proven now | Still required |
| --- | --- | --- | --- |
| 1 | Partial | Confirmed single-message admission and local crash/restart recovery for all route modes. | Successful GitHub-hosted Docker workflow evidence. |
| 2 | Partial | Local successful SIGTERM flush and unavailable-broker timeout/failure. | Successful GitHub-hosted Docker workflow evidence. |
| 4 | Partial | Publisher/consumer supervision and local outage/restart recovery without restarting Distill. | Interrupt already accepted in-flight direct and aggregate delivery; CI evidence. |
| 5 | Partial | Broker-managed retry, failed-destination isolation, and recovered HTTP delivery. | Retry/DLQ saturation coverage and latency/ordering confirmation under stress. |
| 6 | Partial | Malformed DLQ, later-valid progress, metadata-only listing, poison retention, eligible replay. | Unsupported/exhausted poison, full-DLQ recovery, and safe repair/export coverage. |
| 28 | Partial | All queue-class arguments, visible overflow rejection, alarm/operator guidance, repeated generation reclamation. | Drained retry-queue retirement and sustained/concurrent lifecycle coverage. |
| 29 | Partial | Confirmed eligible PostgreSQL replay. | Replay idempotency, partial broker failure, and safe repair/export of ineligible records. |
| 30 | Partial | Conservative retirement and old-input flush across repeated replacements. | Concurrent-ingest retirement, late-failure rollback, prepared-resource cleanup, and runtime generation persistence. |
| 37 | Partial | Automatic CI workflow exists and local checks pass. | A successful workflow run on the hosted environment. |

The broader readiness document records blockers 8, 17, 22, 24, and 25 as implemented and locally verified. Blockers 7, 9, 11, 12, and 14 have core implementation but incomplete contract/integration evidence. All other unlisted blockers remain open.

## Next work in priority order

1. Commit or otherwise publish the current revision, manually run `.github/workflows/broker-durability.yml`, preserve the successful run URL/artifact status in this file and the blocker document, then decide whether the workflow is reliable enough for automatic/nightly execution.
2. Extend the harness to stop RabbitMQ while already accepted direct and aggregate deliveries are genuinely in flight, then prove automatic recovery without a Distill restart or loss.
3. Add real-broker cases for unsupported envelope versions, exhausted valid messages, full retry/DLQ queues, replay idempotency, and partial failure during replay.
4. Design and implement a safe export/repair path for intentionally ineligible poison and aggregate-input records. It must not expose payloads by default or replay aggregate input until durable source-event deduplication exists.
5. Exercise generation retirement during sustained concurrent ingest, proving no accepted input is lost, sent to the replacement sink, or deleted while in flight; prove incomplete drain proof is surfaced and deferred.
6. Continue the remaining production blockers only after the focused broker phase has evidence matching its acceptance criteria.

Re-run after any relevant change:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo run --quiet -- dlq replay --help
bash -n scripts/test-durable-aggregate-input.sh
git diff --check
DISTILL_CONTAINER_ENGINE=podman \
  DISTILL_TEST_ARTIFACT_DIR=/tmp/distill-broker-diagnostics \
  bash scripts/test-durable-aggregate-input.sh
```

## Safety and architecture invariants

- Never return `202` without one persistent, mandatory, explicitly confirmed RabbitMQ handoff.
- Never split `PersistAndAggregate` into two independently fallible admission publications.
- Preserve ordered, per-channel-generation aggregate input and keep old accepted work bound to its original generation and sink.
- Acknowledge an original broker delivery only after its next durable handoff is confirmed.
- Never automatically replay an ambiguously confirmed admission publish.
- Never force-delete a generation queue. If empty/in-flight proof is incomplete, defer cleanup and surface it operationally.
- Never expose event payloads, credentials, authorization headers, connection strings, or API keys in logs, diagnostics, errors, or default DLQ output.
- Keep aggregate-input replay disabled until durable source-event deduplication exists. Stable sink record IDs alone do not make aggregate contributions idempotent.
- Existing RabbitMQ queues without the new `x-*` arguments cannot be compatibly redeclared. Drain/replay them and recreate them during maintenance; there is no automatic migration.
- Consumer lifecycle state is currently internal and is not wired into `/health`, readiness, or metrics. Do not claim process liveness proves broker-consumer health.
- Generation retirement currently uses a conservative global delivery-queue empty check. Sustained unrelated delivery traffic can defer cleanup, and deferred cleanup has no automatic retry yet.

## Deliberately out of the focused broker phase

Do not silently expand the current phase into full transactional replacement rollback, persistence of runtime channel definitions/generations, general `/ready` design, the complete metrics overhaul, SDK work, or database migrations. These remain legitimate production blockers but are separate work. Blocker 30 must remain partial until all of its independent acceptance criteria are met.

## Updating this handoff

At the end of each substantial work session, update the date, worktree/base snapshot, changed-file table, executed commands, exact blocker statuses, unresolved risks, and next action. If work is committed, replace the base commit with the tested commit SHA and record any CI run links. Keep claims tied to executed evidence.
