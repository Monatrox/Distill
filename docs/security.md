# Security and credential handling

This guide describes the V1 deployment boundaries. See the [project README](../README.md) for setup and release limits.

## Authentication and network boundaries

The public listener serves ingestion and authenticated metrics. Configuration, sink registration and channel administration are on the separate administrative listener, which defaults to loopback. A non-loopback administrative bind requires `DISTILL_ALLOW_ADMIN_NETWORK=true`; keep it on a protected private network.

`DISTILL_INGEST_API_KEY` is required. `DISTILL_ADMIN_API_KEY` is optional and defaults to the ingest key; a shared key is supported. Operators may configure a separate administrative key. Keys use constant-time comparison, authentication precedes request-body extraction, and failed authentication is throttled per listener. Static keys do not provide individual caller identities, scoped permissions or independent client revocation.

`/health` and `/ready` are unauthenticated operational endpoints. Liveness does not establish destination health; readiness checks active configuration and broker paths rather than universally probing every sink. Restrict metrics and operational endpoints at the network or proxy boundary as appropriate.

Use protected transport for ingest and administration. Compose includes development credentials and a local Caddy certificate setup; replace those credentials before using a deployment outside local testing. RabbitMQ and native destinations need their own protected credentials and network access controls.

## Credential and recovery storage

Use environment variables, a secret manager or restricted secret files for real credentials. The [CLI guide](cli.md) documents manifest secret references and local secret helpers. Submit runtime destination credentials only over a protected administrative connection. Configuration readback omits those secrets.

Distill's internal SQLite database contains resolved destination credentials as well as configuration, deduplication identities, checkpoints and pending outputs. It is sensitive transactional state. The state directory must be private to the service account; preserve its ownership and restrictive permissions during backup and restore. Current confidentiality relies on filesystem permissions and protected transport; stronger secret storage and rotation remain follow-up work.

Retain broker data, SQLite state and stable deployment credential configuration together. Run one active Distill process per dedicated RabbitMQ vhost. SQLite WAL requires suitable local-disk semantics; Azure Container Apps/Azure Files storage and target-specific restore remain unvalidated release gates.

DLQ payloads and explicitly exported records can contain application secrets. Default inspection omits payloads and credential-bearing failure text. Export creates an owner-only file without deleting broker work; store exports, backups and test endpoint files privately.

## HTTP destinations and resource limits

HTTP sink destinations are constrained by scheme, host/address and port policy. Redirects and environment proxies are disabled. DNS policy is checked when building a sink and for new connections. Private targets require explicit approval through `DISTILL_SINK_ALLOWLIST`; approve only destinations the deployment should reach. An allowlist grants access from Distill's network identity and does not replace destination authentication.

Request size, duration, concurrency and rate have configurable limits. Channel, sink, schema and state capacity are also bounded. These controls do not provide tenant isolation or per-caller quotas. Monitor delivery errors, retry/DLQ depth and state/disk usage.

## Repository hygiene

Commit only placeholders and disposable test credentials. The values in `.env.example`, Compose and integration fixtures are examples, not production credentials. Keep real environment files, secret files, private keys, recovery databases, exports and backups out of source control.

Git ignores local environment files (while retaining `.env.example`), `.secrets/` and `secrets/`, private key/certificate files, SQLite recovery files, root local manifests, and `docs/archive/`. The container build context also excludes these local resources. Ignore rules do not protect files already tracked by Git and do not erase earlier commits. Keep other sensitive files outside the repository or add an explicit ignore rule before creating them.

Before publishing, scan both the working files and all available Git history with a secret scanner and review findings. No automated scan proves the absence of every possible secret. If a real credential was committed, revoke or rotate it first; removing a file or adding an ignore rule does not invalidate the credential or remove historical copies.
