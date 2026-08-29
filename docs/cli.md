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

Running `distill` with no subcommand starts the Distill server. Administrative commands are local file operations and do not require RabbitMQ, PostgreSQL, or the server to be running.

```text
distill sink create ...
distill sink list ...
distill secret file-write ...
distill secret env-write ...
distill
```

The CLI never requires secrets as positional arguments. Sink manifests contain either a non-secret endpoint or a reference to an environment variable or secret file.

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

## Exit codes and automation

Successful commands exit with code `0`. Invalid arguments, invalid manifests, and filesystem failures return a non-zero exit code and write a human-readable error to stderr. `sink create` is idempotent: creating an existing `sink_id` replaces its definition in the selected manifest.
