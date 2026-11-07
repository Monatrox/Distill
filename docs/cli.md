# Distill CLI Guide

The Distill CLI manages sink manifests and local secret references.

## Overview

Build and install the CLI from this repository with:

```powershell
cargo install --path .
```

During development, use `cargo run --` before the command:

```powershell
cargo run -- sink list --file sinks.json
```

Running `distill` with no subcommand starts the Distill server. Manifest and secret commands are local operations. `sink migrate` connects to the named database; DLQ commands connect to RabbitMQ.

```text
distill sink create ...
distill sink list ...
distill sink migrate <sink-id> --file sinks.json
distill secret file-write ...
distill secret env-write ...
distill dlq list
distill dlq replay --id <message-id>
distill dlq export --id <message-id> --file protected.jsonl
distill
```

The CLI never requires secrets as positional arguments. Sink manifests contain either a non-secret endpoint or a reference to an environment variable or secret file.

## Runtime configuration and migrations

The preferred application deployment workflow is `POST /sinks` followed by versioned `POST /channels`; neither needs a logger redeploy. See the root README for request contracts. The manifest below is an optional operator-provided startup path. Changing the manifest itself takes effect at restart; runtime registrations persist independently in SQLite.

Database DDL is disabled during normal startup/registration by default. Resolve manifest secret references and explicitly apply the current schema before registering a database sink:

```sh
distill sink migrate postgres-primary --file sinks.json
```

The command is idempotent and records schema version 1. `DISTILL_AUTO_MIGRATE=true` enables automatic DDL for disposable development fixtures. It is not required in production.

## Protected DLQ export

```sh
distill dlq export --id <message-id> --file /private/recovery.jsonl
# Or explicitly select every item:
distill dlq export --all --file /private/recovery.jsonl
```

Export creates a new owner-only JSONL file containing raw original broker bytes encoded for recovery. It never prints bodies or acknowledges/deletes broker records. Protect the containing directory and exported file as sensitive application data. Export does not authorize unchanged poison or aggregate-input replay.

## Sink manifest

The server reads the manifest named by `DISTILL_SINKS_CONFIG_PATH`:

```powershell
$env:DISTILL_SINKS_CONFIG_PATH = "C:\ProgramData\Distill\sinks.json"
```

Example manifest:

```json
{
  "default_sink_id": "postgres-primary",
  "sinks": [
    {
      "sink_id": "postgres-primary",
      "driver": "postgres",
      "connection_string_env": "DISTILL_POSTGRES_URL"
    },
    {
      "sink_id": "custom-http",
      "driver": "http",
      "connection_string": "https://collector.example.com/distill",
      "bearer_token_env": "DISTILL_COLLECTOR_TOKEN"
    }
  ]
}
```

`connection_string` is appropriate for non-secret HTTP endpoints. Database connection strings should use `connection_string_env` or `connection_string_file`.
Database sinks intentionally reject raw `connection_string` values in the manifest so credentials are not accidentally committed.

## Create a PostgreSQL sink

Set the connection string in the environment of the Distill process:

```powershell
$env:DISTILL_POSTGRES_URL = "postgresql://distill:password@postgres:5432/logs?sslmode=require"
```

Create or replace the named sink:

```powershell
distill sink create postgres-primary `
  --driver postgres `
  --connection-string-env DISTILL_POSTGRES_URL `
  --file sinks.json `
  --default
```

The resulting manifest contains only the variable name. The password is not written to `sinks.json`.

For a mounted secret file:

```powershell
distill sink create postgres-primary `
  --driver postgres `
  --connection-string-file /run/secrets/distill_postgres_url `
  --file sinks.json `
  --default
```

The secret file should contain one connection string and should be readable only by the Distill service account.

## Create an HTTP sink

The HTTP URL is not secret. Keep its bearer token in the service environment:

```powershell
$env:DISTILL_COLLECTOR_TOKEN = "replace-with-a-secret"

distill sink create custom-http `
  --driver http `
  --url https://collector.example.com/distill `
  --bearer-token-env DISTILL_COLLECTOR_TOKEN `
  --file sinks.json `
  --default
```

Static, non-secret headers may be supplied with `--header`:

```powershell
distill sink create custom-http `
  --driver http `
  --url https://collector.example.com/distill `
  --bearer-token-env DISTILL_COLLECTOR_TOKEN `
  --header X-Tenant=payments `
  --file sinks.json
```

Do not put bearer tokens or passwords in `--header` values. Use `--bearer-token-env` or a secret provider instead.

## List sinks

```powershell
distill sink list --file sinks.json
```

Output contains only identifiers, drivers, and the default marker:

```text
postgres-primary    postgres (default)
custom-http         http
```

## Dead-letter recovery

Inspect dead-letter count and safe metadata without exposing payloads:

```powershell
distill dlq list --limit 100
```

After resolving the destination failure, replay one message or every eligible message:

```powershell
distill dlq replay --id 6cdd38e0-3bd4-47fb-b014-4a4791d228ad
# Review listed metadata first; this acknowledges each item only after RabbitMQ confirms republish.
distill dlq replay --all
```

DLQ commands require `RABBITMQ_URI` with read access to the DLQ and write access to original destination exchanges. They inspect the existing DLQ passively and do not redeclare unrelated application queues, so they remain usable during queue-upgrade recovery. Output contains DLQ ID, timestamp, failure category, retry count, envelope version, original record ID when known, original exchange, and routing key—never original event payload, failure text, or credentials.

Only structurally valid, retry-exhausted direct event and aggregate-output records are eligible. Malformed, unsupported-version, and aggregate-input records remain in the DLQ because replaying unchanged poison data or an aggregate input without the separately validated aggregate replay contract is unsupported. Listing and replay scan without retaining message bodies in process memory. Replay reports confirmed, intentionally retained, and invalid record counts. `--all` succeeds when every eligible record was confirmed even if intentionally ineligible records remain; malformed DLQ wrappers produce a non-zero exit for repair. An explicit `--id` also exits non-zero when that record is missing or ineligible.

## Local secret helpers

The CLI can prompt without echoing a value and write a local secret file:

```powershell
distill secret file-write --file .secrets/distill_postgres_url
```

It can also create a dotenv-style file for a local deployment:

```powershell
distill secret env-write `
  --name DISTILL_POSTGRES_URL `
  --file .env.distill
```

These commands refuse to overwrite existing files unless `--force` is supplied. On Unix, generated files receive mode `0600`. On Windows, protect the file with the account or service ACLs appropriate to the deployment.

The CLI does not modify the parent shell's environment. Load the generated file through Docker Compose, the service manager, or the deployment platform. Do not commit it to source control.

## Channels and the default sink

Channels should reference a sink by name, or omit the reference to use `default_sink_id`:

```json
{
  "channels": [
    {
      "channel_id": "orders.production",
      "sink_id": "postgres-primary",
      "severity_threshold": "warning",
      "rabbitmq_config": {
        "connection_string": "amqp://guest:guest@rabbitmq:5672/%2f",
        "exchange_name": "distill.orders.production",
        "routing_key_prefix": "orders."
      },
      "aggregator": null
    }
  ]
}
```

When the channel uses the configured default, omit `sink_id`. The channel definition never contains a connection string or credential.

## Deployment guidance

For production, prefer a secret manager, Docker secret, Kubernetes Secret, or service-manager credential injection. Use `connection_string_file` for mounted secrets and `connection_string_env` when the deployment platform already manages environment secrets.

The sink manifest is safe to version-control only when it contains references and non-secret endpoints. Review HTTP URLs and egress policy because the HTTP sink can deliver data to any administrator-approved destination.

## Queue limits and upgrades

Distill declares delivery, retry, dead-letter, and aggregate-generation queues with `x-max-length=1000000`, `x-max-length-bytes=1073741824`, and `x-overflow=reject-publish`. Retry queues are quorum queues with at-least-once dead-lettering; RabbitMQ 3.10 or newer with the `stream_queue` feature flag is required. RabbitMQ disk alarms remain mandatory.

The DLQ deliberately has no time-based retention: records remain until an operator confirms replay, export/repair, or approved disposal. Its one-million-message/1-GiB limits bound disk growth. When the DLQ rejects a publish, Distill does not acknowledge the original delivery; the supervised consumer becomes degraded and retries after capacity is restored, preventing silent loss.

Production monitoring must alert before the hard limits are reached:

- RabbitMQ memory or disk alarms: page immediately and stop nonessential publishers; restore capacity before changing Distill queues.
- Any DLQ depth: investigate the failure category and destination. Warn at 100,000 messages or 100 MiB and page at 800,000 messages or 800 MiB.
- Oldest DLQ message: warn after 15 minutes and page after one hour unless a documented incident exception applies.
- Delivery, retry, or generation queues: alert on sustained growth and oldest-message age; a growing retry generation identifies an unhealthy destination.

At an alert, stop or rate-limit new admission if broker capacity is threatened, repair the destination, inspect metadata with `distill dlq list`, and replay only eligible records. Never increase limits merely to clear an alarm, and never purge or delete a non-empty DLQ. Back up/export ineligible records under the deployment's data-handling policy before an approved repair or disposal action.

Existing queues declared without these arguments require a maintenance migration because RabbitMQ rejects incompatible redeclaration. Stop new admission, keep the previous Distill release running until delivery, generation, and retry queues drain, and use the passively connected DLQ CLI to replay eligible records after destination recovery. Do not delete a non-empty DLQ: if ineligible records remain, preserve the queue and defer migration until an approved backup/export-and-repair workflow can retain them. Once every affected queue is proven empty and the broker is backed up, stop the previous release, recreate those queues, and start the new release. There is no automatic queue migration.

## Exit codes and automation

Successful commands exit with code `0`. Invalid arguments, invalid manifests, broker failures, and filesystem failures return a non-zero exit code and write a human-readable error to stderr. `sink create` is idempotent: creating an existing `sink_id` replaces its definition in the selected manifest.
