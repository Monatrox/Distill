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

Launch a Distill container. The image defaults to `distill:latest`.
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

`WithSink` configures container-mode Distill. For an externally managed Distill instance, use its deployment sink manifest and secret store instead.

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

This registers `DistillClient` as a singleton, adds the Distill `ILogger` provider, and registers a health check for the Aspire dashboard.

When using `.WithReference(distill)`, Aspire sets `ConnectionStrings__distill` to the Distill endpoint automatically. Set the ingest key on every consumer. Applications that explicitly provision their own finite channel set also need the admin key during startup:

```csharp
var myService = builder.AddProject<Projects.MyService>("my-service")
    .WithReference(distill)
    .WithEnvironment("Distill__ApiKey", "dev-key")
    .WithEnvironment("Distill__AdminApiKey", "dev-key-admin");
```

In the consuming application's `Program.cs`, provision channels after `Build()` and before serving traffic:

```csharp
var app = builder.Build();
var distill = app.Services.GetRequiredService<DistillClient>();

await distill.EnsureChannelAsync(new CreateChannelRequest
{
    Name = "production.errors",
    Preset = "raw_events"
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

`DistillHealthCheck` calls `/health`. It is healthy only when Distill has an active channel backed by a connected sink. The check is visible in the Aspire dashboard.

## Sink environment variables

When sinks are registered via `.WithSink(...)`, the integration sets the following startup-only environment variables on the Distill container:

- `DISTILL_SINK__{NAME}__DRIVER` — sink driver type
- `DISTILL_SINK__{NAME}__CONNECTION_STRING` — connection string (for database sinks)
- `DISTILL_SINK__{NAME}__URL` — endpoint URL (for HTTP sinks)
- `DISTILL_SINK__{NAME}__BEARER_TOKEN` — bearer token (for HTTP sinks)
- `DISTILL_DEFAULT_SINK` — name of the default sink
