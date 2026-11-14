# Distill.Aspire

`Distill.Aspire` provides Aspire hosting and client integration for Distill structured logging.

## AppHost usage

### External Distill (no container)

Configure an existing Distill instance by providing the endpoint:

```csharp
var distill = builder.AddDistill("distill",
    endpoint: new Uri("http://distill.example.com:8080"),
    apiKey: "your-api-key");

var myService = builder.AddProject<Projects.MyService>("my-service")
    .WithReference(distill);
```

### Container-mode Distill

Launch Distill with a managed RabbitMQ resource, non-default broker user, generated credentials, and persistent broker/state volumes. A generated shared Distill key is used if `apiKey` is omitted. Configure a `UserSecretsId` in the AppHost project so generated broker/API credentials persist across local runs when their data volumes are retained. Production supplies stable credentials through its secret configuration. The image defaults to private `ghcr.io/monatrox/distill:0.1.0`; authenticate your container runtime to the registry.
Override with `.WithImage(...)` for a custom image.

#### Automatic local build (recommended for development)

`.WithLocalBuild()` finds the repo's `Dockerfile` by walking up from the
current directory and delegates its build to Aspire. Aspire detects a running
Docker or Podman runtime, keeping image build and container execution on the
same engine.

Podman 5.0 or newer is required. Detection needs no Distill-specific setup.
When both runtimes are available, force one before starting the AppHost:

```powershell
$env:ASPIRE_CONTAINER_RUNTIME = "podman" # or "docker"
```

```csharp
var distill = builder.AddDistill("distill", apiKey: "dev-key")
    .WithLocalBuild();
```

Provide explicit paths if the Dockerfile is not under the current directory:

```csharp
.WithLocalBuild(dockerfile: "../Dockerfile", context: "../")
```

#### Pre-built image

```csharp
var distill = builder.AddDistill("distill", apiKey: "dev-key");

// (Optional) use a custom registry image
// distill.WithImage("ghcr.io/myorg/distill", "1.0.0");
```

### Register a PostgreSQL sink

```csharp
var postgres = builder.AddPostgres("pg").AddDatabase("distill");

var distill = builder.AddDistill("distill", apiKey: "dev-key")
    .WithSink(postgres, name: "primary");  // waits for PostgreSQL; primary becomes default
```

For an existing PostgreSQL server, keep its connection string in AppHost configuration or a secret provider:

```csharp
var postgres = builder.AddConnectionString("distill-postgres");

var distill = builder.AddDistill("distill", apiKey: "dev-key")
    .WithSink(postgres, name: "primary");
```

Set `ConnectionStrings__distill-postgres` through user secrets or deployment secret configuration. Do not hardcode it in `AppHost.cs`.

For multiple named sinks:

```csharp
var distill = builder.AddDistill("distill", apiKey: "dev-key")
    .WithSink(new Dictionary<string, IResourceBuilder<IResourceWithConnectionString>>
    {
        ["primary"] = postgres,
        ["analytics"] = analyticsDb,
    }, driver: "postgres", defaultSinkName: "primary");
```

`WithSink` passes the connection expression only to the Distill container at startup. The value is never sent to consuming applications. Distill connects it before becoming ready; logs remain rejected until an active channel references the sink.

`WithSink` configures container-mode Distill. For an externally managed instance, consuming applications can register sinks through the runtime API; `.WithSink` remains a container startup helper.

### Register a custom sink (HTTP, etc.)

```csharp
var distill = builder.AddDistill("distill", apiKey: "dev-key")
    .WithSink(new DistillSinkSpecification
    {
        Name = "http-collector",
        Driver = "http",
        ConnectionString = "https://collector.example.com/distill",
        BearerToken = "my-token",
        IsDefault = true
    });
```

Multiple named custom sinks:

```csharp
.WithSink(new Dictionary<string, DistillSinkSpecification>
{
    ["primary"] = new() { Name = "primary", Driver = "postgres", ConnectionString = "..." },
    ["analytics"] = new() { Name = "analytics", Driver = "postgres", ConnectionString = "..." },
}, defaultSinkName: "primary");
```

## Consumer usage

Wire up the Distill client and logging provider by calling `AddDistillClient`:

```csharp
builder.AddDistillClient("distill", options =>
{
    options.DefaultChannel = "production.errors";
    options.MinimumLevel = LogLevel.Warning;
});
```

This registers `DistillClient` with factory-managed HTTP transports, adds the `ILogger` provider, and registers a readiness health check. Reconcile the application's destinations/channels explicitly before serving traffic. Configure `DefaultVersion` or let the client bind versions through reconciliation.

When using `.WithReference(distill)`, Aspire sets `ConnectionStrings__distill` to the Distill endpoint automatically. The concrete `.WithReference(distill)` extension also supplies the shared ingest/configuration key and private admin endpoint. Consumers do not need to derive or repeat an admin key:

```csharp
var myService = builder.AddProject<Projects.MyService>("my-service")
    .WithReference(distill);
```

In the consuming application's `Program.cs`, provision channels after `Build()` and before serving traffic:

```csharp
var app = builder.Build();
var distill = app.Services.GetRequiredService<DistillClient>();

await distill.ReconcileChannelAsync(new CreateChannelRequest
{
    Name = "production.errors",
    Version = "1",
    SinkId = "primary",
    Preset = ChannelPreset.RawEvents
});

app.Run();
```

Provisioning is explicit and fail-fast. `DistillLoggerProvider` remains an ingest-only transport and never creates channels from arbitrary logger categories.

### Standalone usage (without Aspire hosting)

The extension reads configuration directly from environment variables or `appsettings.json`:

```json
{
  "Distill": {
    "Endpoint": "http://localhost:8080",
    "ApiKey": "change-me-ingest",
    "AdminApiKey": "change-me-admin"
  }
}
```

## Health check

`DistillHealthCheck` calls `/ready`, which requires active channel configuration and healthy broker paths. The container startup dependency uses `/health` so idle Distill can start before applications reconcile their channels; this avoids a provisioning deadlock. Sink outages are visible through delivery failures/retry/DLQ rather than a universal live database probe.

## Sink environment variables

When sinks are registered via `.WithSink(...)`, the integration sets the following startup-only environment variables on the Distill container:

- `DISTILL_SINK__{NAME}__DRIVER` — sink driver type
- `DISTILL_SINK__{NAME}__CONNECTION_STRING` — connection string (for database sinks)
- `DISTILL_SINK__{NAME}__URL` — endpoint URL (for HTTP sinks)
- `DISTILL_SINK__{NAME}__BEARER_TOKEN` — bearer token (for HTTP sinks)
- `DISTILL_DEFAULT_SINK` — name of the default sink

## Runtime registration and deployment limits

`WithSink` is optional; application startup can call `RegisterSinkAsync` and `ReconcileChannelAsync` without changing the logger deployment. External mode also accepts an Aspire secret `ParameterResource` for the shared key and an explicit `adminEndpoint`. Ensure that destination addresses are reachable from Distill, and explicitly allowlist private HTTP receiver hosts/ports through `DISTILL_SINK_ALLOWLIST`. Native database schemas require the explicit `distill sink migrate` workflow unless development-only auto-migration is enabled.

Run only one active Distill instance per dedicated broker vhost. Persist `/var/lib/distill` on local-disk-compatible storage. An actual deployed Aspire/Azure end-to-end run and Azure SQLite storage compatibility remain release gates; local resource-model tests do not establish deployment compatibility.

The client resolves its HTTP transports lazily so the logging provider can be constructed alongside HttpClientFactory/resilience defaults without a logger-factory dependency cycle. Distill transport HTTP loggers are disabled to avoid logging its own delivery requests. A hosted shutdown service drains logging before dependency injection disposes the HTTP client wrappers.
