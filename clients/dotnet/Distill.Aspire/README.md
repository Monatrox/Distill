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
current directory, checks if `distill:latest` exists locally, and runs
`docker build` if it doesn't. The first build compiles the Rust binary;
subsequent builds are fast thanks to Docker layer caching.

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
    .WithSink(postgres);  // single sink → becomes default
```

For multiple named sinks:

```csharp
var distill = builder.AddDistill("distill", apiKey: "dev-key")
    .WithSink(new Dictionary<string, IResourceBuilder<IResourceWithConnectionString>>
    {
        ["primary"] = postgres,
        ["analytics"] = analyticsDb,
    }, driver: "postgres", defaultSinkName: "primary");
```

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

When using `.WithReference(distill)`, Aspire sets `ConnectionStrings__distill` to the Distill endpoint automatically. The API key should be set on the consumer project via `WithEnvironment`:

```csharp
var myService = builder.AddProject<Projects.MyService>("my-service")
    .WithReference(distill)
    .WithEnvironment("Distill__ApiKey", "dev-key");
```

### Standalone usage (without Aspire hosting)

The extension reads configuration directly from environment variables or `appsettings.json`:

```json
{
  "Distill": {
    "Endpoint": "http://localhost:8080",
    "ApiKey": "change-me"
  }
}
```

## Health check

`DistillHealthCheck` probes the Distill endpoint by calling `GetChannelAsync("application")`. Any HTTP response (including 404) is treated as healthy. Transport failures produce an unhealthy result. The check is visible in the Aspire dashboard.

## Sink environment variables

When sinks are registered via `.WithSink(...)`, the integration sets the following environment variables on the Distill container:

- `DISTILL_SINK__{NAME}__DRIVER` — sink driver type
- `DISTILL_SINK__{NAME}__CONNECTION_STRING` — connection string (for database sinks)
- `DISTILL_SINK__{NAME}__URL` — endpoint URL (for HTTP sinks)
- `DISTILL_SINK__{NAME}__BEARER_TOKEN` — bearer token (for HTTP sinks)
- `DISTILL_DEFAULT_SINK` — name of the default sink
