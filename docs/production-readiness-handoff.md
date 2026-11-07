# Production Readiness Continuation Handoff

Last updated: 2026-11-07. Branch: `production-readiness`. This revision records the V1 implementation and local qualification developed from base `a342f4c`; the earlier broker implementation was `df28e18`. Local evidence below is not a production release or hosted CI result.

## Authoritative direction

Read the **Agreed V1 contract** in [production-readiness-blockers.md](production-readiness-blockers.md). It records the user's decisions and supersedes conflicting historical backlog items and [the old broker dispatch](next-production-blockers-dispatch.md).

V1 includes runtime application-led destination registration, immutable explicit channel/configuration versions, overlapping producer versions, durable duplicate-safe aggregate contributions, and the existing rolling functions across PostgreSQL, MySQL, SQL Server and HTTP. Empty startup is valid; shared credentials suffice. SQLite is internal recovery state. Multiple active logger replicas, automatic revision retirement, cross-version merging, producer leases, retention/compression and additional rolling semantics are deferred.

Repository instructions: use CodeGraph before locating/understanding code (`.codegraph/` exists), and read the mandatory `ms-rust` skill before editing Rust. The graph may lag the worktree; use direct reads to confirm changed source.

## Implemented in this worktree

| Area | Result / principal files |
| --- | --- |
| Recovery state | `src/core/state.rs`: SQLite WAL/FULL durability, private directory/files, exclusive process ownership, schema/integrity checks, bounded main DB, catalog, processed IDs, checkpoints and output outbox. |
| Runtime configuration | `src/core/registry.rs`, `src/http/channels.rs`: `POST /sinks`, durable immutable sink IDs, channel name/version reconciliation, explicit conflicts, persisted lifecycle, version-aware admin operations and source routing. Stored mappings restore before shared backlog consumption; unavailable native destinations reconnect lazily. |
| Aggregate correctness | `src/core/channel.rs`, `src/core/aggregation.rs`: source ID/content fingerprint plus checkpoint commit atomically before source acknowledgement; output checkpoint plus outbox commit atomically. Stable aggregate IDs and increasing revisions survive retries/restarts. Rolling snapshots are cumulative; ordered timestamp summaries preserve duration semantics. |
| Sinks | All native drivers atomically reject conflicting event IDs, accept identical duplicates, apply only newer cumulative aggregate revisions, reject conflicting equal revisions and ignore stale revisions. Explicit `sink migrate` schema version 1; normal startup has no DDL unless development auto-migration is selected. SQL Server connections are discarded on failure/cancellation. |
| HTTP | `src/persistence/http.rs`, `policy.rs`: contract headers, only 2xx success, explicit conflict semantics, destination host/port isolation, DNS validation on each connection, proxies disabled. `examples/http_receiver.py` is a durable authenticated reference implementation with query inspection. No PostgreSQL dependency for HTTP-only operation. |
| Broker/operations | Existing confirmed single-message admission retained. Confirmed durable retries/DLQ; bounded sink operation duration; retained version queues; protected JSONL export without broker deletion; liveness/readiness separated; admission, confirmation, retry/DLQ and consumer-health metrics. No automatic retirement. |
| Validation | Runtime schemas validate JSON field types using protobuf reflection; resource ceilings reject additions instead of silently evicting existing validation. Native JSON/protobuf/simple/OTLP carry version identity; OTLP supports stable `distill.event_id`. |
| .NET | Version-aware registration/reconciliation, typed schema declarations, bound ingest/configuration/lifecycle operations, stable simple-event IDs/timestamps, explicit client ownership; logger burst draining, immutable enqueue snapshots, selective partial retries, visible overflow/terminal failures and bounded shutdown. |
| Aspire | Managed RabbitMQ with non-default user, persistent broker/state volumes, generated shared secret or explicit key, endpoint/key propagation, logical external references, HTTP factory integration and readiness checks. No derived admin secret. |
| Distribution | Version 0.1.0 package metadata/license; fresh package consumer smoke test; repeatable broker and four-sink harnesses; Rust/.NET/sink CI and release workflow gating private GHCR image publication. No image or NuGet publication was performed. |
| Documentation | Root README, CLI and .NET/Aspire guides updated for the agreed contracts. Historical numbered backlog retained and clearly marked superseded where incompatible. |

## Verification

Passed locally during this implementation:

```sh
cargo fmt --check
cargo clippy --offline --all-targets -- -D warnings
cargo test --offline --all-targets
# Four external-service tests are ignored here and executed by the next command.
DISTILL_CONTAINER_ENGINE=podman bash scripts/test-sink-contracts.sh
DISTILL_CONTAINER_ENGINE=podman bash scripts/test-http-only.sh

dotnet run --project clients/dotnet/Distill.Client.ContractTests --no-restore -p:BuildInParallel=false -m:1 -nr:false
dotnet pack clients/dotnet/Distill.Client -c Release -o artifacts --no-restore -p:BuildInParallel=false -m:1 -nr:false
dotnet pack clients/dotnet/Distill.Extensions.Logging -c Release -o artifacts --no-restore -p:BuildInParallel=false -m:1 -nr:false
dotnet pack clients/dotnet/Distill.Aspire -c Release -o artifacts --no-restore -p:BuildInParallel=false -m:1 -nr:false
bash scripts/test-packages.sh artifacts

bash -n scripts/test-durable-aggregate-input.sh scripts/test-sink-contracts.sh scripts/test-packages.sh scripts/test-http-only.sh
python3 -m py_compile examples/http_receiver.py
git diff --check
```

The external sink run passed all four drivers against real PostgreSQL/MySQL/SQL Server containers and the reference HTTP receiver. It verifies first write, identical duplicate, newer snapshot, stale snapshot, conflicting equal revision, actual final count/revision, and direct-event duplicate/conflicting-ID behavior. Rust tests exercise transaction rollback, catalog recovery/exclusive ownership, source redelivery/restart, persistent output identity and cumulative rolling values. .NET checks cover burst delivery, timestamp/ID stability, selective partial retry, drops, bounded disposal, transport ownership, version binding and Aspire resource wiring. Package tests restore a fresh consumer without project references or cached Distill packages.

The full-server HTTP-only suite also passed with RabbitMQ and the reference receiver, without a native database or startup catalog. It verifies idle liveness/unreadiness, one shared key for runtime sink/channel registration, rolling snapshots, forced process replacement, restored catalog/checkpoints and duplicate-source protection. The fixture uses an explicit disposable Erlang cookie because this Podman host rejected the image's default cookie file ownership; application credentials remain separate from that fixture cookie.

The expanded broker harness passed under rootless Podman with this result:

```text
durable handoff, retry isolation, DLQ replay, overflow rejection, persistent configuration, overlapping versions, duplicate protection, in-flight broker restart, queue limits, and SIGTERM behavior verified
```

It starts idle, registers sinks/channels through the API, replaces processes while retaining SQLite, retains and routes overlapping producer versions, and verifies duplicate contributions do not change totals. A real PostgreSQL table lock proves both direct and PersistAndAggregate deliveries are in flight before RabbitMQ is restarted; recovery produces exactly one direct row per ID and the expected aggregate count without restarting Distill. Protected export verifies original bytes, mode 0600 and broker retention. Earlier fixture failures (wrong metadata column and package-consumer source globbing into dependency caches) were corrected; both suites subsequently passed.

The earlier unit run passed 58 tests; all four external-service tests passed separately. An additional comparison exercises all six rolling functions with overlapping/equal timestamps. Final small follow-up edits redact HTTP sink Debug/header values and startup dependency errors, and describe version queries/conflicts in OpenAPI; the Rust checks were rerun for those edits.
Temporary redacted diagnostics: `/tmp/distill-broker-final`. Package outputs: ignored `artifacts/`. Neither is durable hosted CI evidence. Hosted workflows have not run against this implementation revision at the time of local qualification. NuGet restore audit identified an Aspire dependency advisory; MessagePack was explicitly moved to patched 2.5.303, and restores passed with audit enabled.

## Actual Aspire / DistillTesting qualification

The user-authorized integration application is `/run/media/monatrox/M2 2TB/Dev/Rider/DistillTesting`. It has no Git repository. Original edited files were backed up privately under `/tmp/distilltesting-before-v1-bn0485f6`. The AppHost, startup declarations and ServiceDefaults were updated to Aspire 13.6/.NET 10, shared generated credentials, explicit version reconciliation, dynamic producer ports and current audited telemetry dependencies. Development-only verification endpoints are gated by both Development and `Distill:VerificationEnabled`.

Executed against rootless Podman and actual Blazor applications:

- PostgreSQL 16 with an operator-provided startup sink, plus application-created immutable channel versions 1 and 2.
- HTTP-only with a Python 3.13 reference receiver and application-led `RegisterSinkAsync`, with no PostgreSQL resource.
- A separate AppHost referencing the existing logger through `DistillExternalResource`, without creating another logger/broker/destination container.
- Distinct producer endpoints and explicitly asserted version identities. All six rolling functions, stable revisions, identical-source retries, SDK direct ingestion and `ILogger` bursts verified through persisted data.
- Forced logger and managed RabbitMQ restarts while applications remained alive; complete AppHost replacement with retained data volumes and credentials.
- Four concurrent configuration/ingest requests admitting 2,400 unique aggregate source events per scenario, followed by exact persisted values and an empty DLQ. This is a bounded integration workload, not a sustained-throughput SLA.
- Real Chromium counter clicks and weather rendering for both versions, with zero page errors. Five clicks per version produced count 5, sum 15, average 3, minimum 1, maximum 5 and valid duration aggregates. Small per-N windows were explicitly flushed after recovery; a durable checkpoint is intentionally not a completed output before its flush condition.
- Managed and external manifest generation through the actual Aspire publisher (`--publisher manifest --output-path ...`). Managed output retains image build, state volume and shared secret references; external output uses a value resource and secret parameter without an image-less container.
- Coordinated offline exports of all six logger/broker/destination volumes into private archives, then import into six new volumes. Every source volume was preserved. Restored deployments passed prior-result checks, replay of old IDs, browser aggregate checks, restarts and concurrent ingest.

Local volume storage was Btrfs. An initial restore correctly failed closed because `podman volume export` omitted volume-root directory metadata: imported state roots were owned by the host user with mode 0755. The successful procedure records container UID/GID/mode with the backup, reapplies them to imported volumes, and preserves the AppHost credential configuration. Backup credentials and archives are sensitive; they remain in the private temporary directory and are not repository/CI artifacts. An off-host or Azure restore is not established by this local result.

Real-host testing found and fixed issues absent from the earlier wire/model tests:

1. HttpClientFactory/resilience bootstrap depended on ILoggerFactory while constructing the Distill provider. Transport resolution is deferred; internal transport HTTP logging is disabled to prevent recursive logging.
2. DI could dispose HTTP clients before logger draining. A hosted shutdown service drains first; disposal interrupts batching delays rather than sleeping through its deadline. Per-channel overrides now reject settings such as zero-sized batches before starting a worker.
3. Failed DLQ handoff could leave a redelivered message attached to the initial shared channel retained by `RabbitConsumer`. Consumer channels are explicitly closed before reconnection. The previously failing full-DLQ recovery scenario now passes.
4. The sample lacked `UserSecretsId` and advertised the same fixed producer port twice. Generated credentials now persist across AppHost replacements, and each producer has an allocated port. The verifier rejects shared endpoints or missing version coverage.

Additional executed evidence: 59 Rust unit tests, all four real sink contracts, legacy PostgreSQL/MySQL/SQL Server upgrades through the actual `sink migrate` CLI with sentinel data preserved, configured SQLite-capacity exhaustion/rollback/recovery, full-DLQ/unsupported-envelope recovery, .NET host startup/shutdown and package checks. The DB fixtures wait for final TCP listeners rather than temporary initialization servers.

Safe, durable summary: [production-readiness-local-evidence.json](production-readiness-local-evidence.json). Private detailed results, resource logs, browser screenshots, manifests, archives and restore metadata: `/tmp/distill-aspire-actual-nhMQuLAn`. Earlier `invalid-shared-port-evidence.json` is intentionally invalidated and must not be used as version-overlap evidence. Successful broker diagnostics target: `/tmp/distill-broker-capacity-fixed` (failure artifacts are written only on failure).

The repeatable application driver is `scripts/test-aspire-application.py`; real browser driver is `scripts/test-aspire-ui.cjs`. The external sample is required for these two drivers and is not automatically available on hosted CI. Local qualification preceded this implementation commit; no hosted CI result or release publication was recorded during that qualification.

## Remaining release gates

1. **Production deployment storage:** local Podman/Btrfs persistence and restore passed. Azure Container Apps/Azure Files compatibility and the actual production storage/credential arrangement remain unvalidated. Execute the target deployment and restore there, or formally select the qualified local-storage hosting path.
2. **Hosted release evidence:** review the implementation commit and execute CI/broker workflows on that exact revision. Local real-AppHost and manifest tests passed; their sample checkout is external to this repository, so hosted automation needs an equivalent supplied fixture if that coverage is required there.
3. **Remaining fault/soak coverage:** full DLQ, unsupported-envelope recovery, concurrent configuration/ingest and configured SQLite-capacity recovery now pass. Full retry-queue saturation, broker interruption during CLI replay, actual filesystem ENOSPC/corruption recovery and sustained workload/latency sizing remain unproven. The shared persistence consumer can wait up to a sink timeout before handing off failures.
4. **Production operations/upgrade rehearsal:** native legacy-schema upgrades and coordinated local backup/restore passed. Rehearse target-specific restore, key handling and any pre-V1 queue/data transition needed by an existing installation. Pre-V1 rolling delta envelopes must drain under the old implementation; revision-zero rolling outputs remain deliberately rejected. Existing queues without the declared arguments need maintenance rather than incompatible redeclaration.

Unchanged replay scope: general poison repair and unchanged aggregate-input CLI replay remain deferred. Protected export is supported; durable source deduplication alone does not authorize an untested repair workflow.

Fast follow after V1: stronger secret references/encryption and rotation lifecycle; optional scoped credentials; explicit safe retirement/deletion and leases only if needed; additional rolling semantics. Current SQLite contains resolved destination secrets under filesystem access controls. Never expose them through configuration readback, diagnostics or default DLQ output.

## Operational invariants

- `202` means mandatory persistent broker confirmation, not immediate sink visibility. Clients retain stable IDs, timestamps and payloads on ambiguous retry.
- PersistAndAggregate remains one admission publication; direct persistence is itself duplicate-safe before source aggregation is checkpointed.
- A source acknowledgement follows durable local contribution state. Output acknowledgement/removal follows broker confirmation; sink snapshots absorb duplicate publication.
- Channel name/version is immutable; no implicit latest-version routing. Already accepted work keeps its generation/destination. No automatic queue/history deletion.
- Preserve both broker and SQLite state. One active process per dedicated broker vhost; SQLite locking alone cannot protect different state files from accidental duplicate logger replicas.
- `/health` is liveness. `/ready` checks active config and broker paths; it does not universally probe every sink. Delivery errors/retry/DLQ expose destination failures.
- Storage corruption/capacity/errors must fail closed for work requiring dedup/checkpoints. Never silently fall back to in-memory processing.
- Never print payloads, credentials, connection strings, headers or keys in errors/logs/default inspection. Export is explicitly sensitive, owner-only and non-destructive.

## Next action

Review the implementation and local qualification evidence. Choose/validate the production storage target, run hosted CI on the exact revision, and close the remaining fault/soak and target-operation gates above. Local Aspire, external references, manifest publication, migration and restore evidence no longer need to be treated as missing. Do not label this a production-certified release solely from local results.
