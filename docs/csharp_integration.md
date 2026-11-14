# C#/.NET integration guide

Start the [root README's local demo](../README.md#quick-start) to obtain a working Distill instance and reference HTTP destination. C# applications communicate with Distill over HTTP; they do not need broker or destination database credentials for ingestion.

## Packages and supported targets

| Package | Targets | Purpose |
|---|---|---|
| `Distill.Client` | .NET 8 | Direct HTTP client and configuration APIs |
| `Distill.Extensions.Logging` | .NET 8/9/10 | Batched `ILogger` delivery |
| `Distill.Aspire` | .NET 9/10, Aspire 13.6.5 | Hosting, service discovery, client and health wiring |

Current package version: `0.1.0`. Use source project references or build packages into a configured local/private feed. These instructions do not assume public NuGet availability. See the [SDK guide](../clients/dotnet/README.md) for project-reference setup.

## Direct client

This example uses the demo's endpoints, credentials and previously registered sink:

```csharp
using Distill.Client;

using var distill = DistillClient.Create(
    new Uri("http://127.0.0.1:8080/"),
    apiKey: "dev-key",
    adminApiKey: "dev-key-admin",
    adminEndpoint: "http://127.0.0.1:8081/");

await distill.ReconcileChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Version = "1",
    SinkId = "demo-receiver@1",
    Preset = ChannelPreset.RawEvents
});

var entry = new SimpleEvent
{
    Severity = DistillSeverity.Info,
    Message = "Order created",
    Payload = new { order_id = "ord-456" }
};
var response = await distill.WriteAsync("orders.production", entry);
Console.WriteLine($"Accepted: {response?.Accepted}, rejected: {response?.Rejected}");
```

The admin endpoint is separate from ingestion even when both listeners use a shared key. `DISTILL_ADMIN_API_KEY` is optional on the server and defaults to the ingest key; a deployment may select separate keys as the demo does.

`ReconcileChannelAsync` requires an explicit configuration version. Identical definitions reconcile; changing a definition under the same name/version conflicts. Reconciliation binds subsequent client writes and configuration/lifecycle operations to that version. Old and new producer versions coexist. `EnsureChannelAsync` remains create-only and does not reconcile changed definitions.

Applications may call `RegisterSinkAsync` before channel reconciliation, or reference a sink provisioned by the operator. Destination credentials must reach Distill through protected configuration; they are not returned by configuration readback. The SDK guide includes runtime sink registration. Database sinks require explicit migration before use unless development-only auto-migration is enabled.

For batch ingestion, set `EventId`, `Timestamp`, `ChannelId`, `Version`, `SeverityLevel` and `Payload` on each `DistillEvent`. A `202` confirms broker admission, not immediate destination visibility. Partial admission returns accepted/rejected counts with indexed errors; retry only rejected transient records. Reuse the same `SimpleEvent` object or complete original batch event on retry, including its ID, timestamp, version and payload. Server-generated IDs on fresh HTTP retries do not provide deduplication.

The direct client does not automatically retry. `DistillApiException` exposes status and machine-readable error information. Avoid printing raw response bodies or credentials in application diagnostics. A completely rejected batch currently returns `400` with the underlying cause in the error body; do not assume every delivery outage appears as `503`.

`DistillClient.Create(...)` owns its created transports. Constructors accepting caller-provided `HttpClient` instances leave those transports owned by the caller. See the SDK guide for ownership and schema declarations.

## ILogger integration

Reference `Distill.Extensions.Logging` and configure one finite channel set during startup. With the demo running:

```csharp
using Distill.Client;
using Distill.Extensions.Logging;

var builder = WebApplication.CreateBuilder(args);
using var distill = DistillClient.Create(
    new Uri("http://127.0.0.1:8080/"),
    apiKey: "dev-key",
    adminApiKey: "dev-key-admin",
    adminEndpoint: "http://127.0.0.1:8081/");
await using var provider = new DistillLoggerProvider(distill, new DistillLoggerOptions
{
    DefaultChannel = "application.production",
    DefaultVersion = "1",
    MinimumLevel = LogLevel.Information
});
builder.Logging.AddProvider(provider);

var app = builder.Build();
await distill.ReconcileChannelAsync(new CreateChannelRequest
{
    Name = "application.production",
    Version = "1",
    SinkId = "demo-receiver@1",
    Preset = ChannelPreset.RawEvents
});
app.MapGet("/", (ILogger<Program> logger) =>
{
    logger.LogInformation("Application request received");
    return "OK";
});
await app.RunAsync();
```

The provider only ingests; it does not automatically create channels from logger categories. Reconcile every channel selected by category routing before serving traffic, and let reconciliation conflicts fail startup.

The provider captures identity/timestamp and an immutable event snapshot when enqueuing, drains bursts, and selectively retries partial failures. Queue overflow and terminal delivery failures are observable through callbacks. The sample owns the provider and client explicitly: `await using` drains the provider before `using` disposes its client. The convenience `builder.Logging.AddDistill(...)` helper also exists, but callers must arrange the provider/client disposal order; externally supplied instances are not a substitute for explicit ownership. Shutdown is bounded and cannot promise delivery beyond its deadline. Configure batching, queue capacity, overflow, retries and shutdown for the workload. See the [logging provider guide](../clients/dotnet/Distill.Extensions.Logging/README.md) for options.

## Aspire integration

Use `AddDistill(...).WithLocalBuild(...)` to build from a source checkout rather than assuming a registry image is available. Managed hosting creates RabbitMQ with generated credentials, persists broker/state volumes, and propagates ingest/admin endpoints and keys through `.WithReference(distill)`. Set a stable `UserSecretsId` in the AppHost project for credential continuity across local restarts.

In the consuming application, `builder.AddDistillClient("distill", ...)` registers the client, logger and readiness health check. Build the app, obtain `DistillClient` from DI, register/reconcile its sinks and versioned channels, then start serving traffic. A shared configuration/ingest credential is supported; separate mandatory keys are not required.

Native database schemas still need explicit migration. With application-led HTTP registration, allowlist private destination hosts/ports and use an address reachable from the Distill container, not the producer's own loopback address. External-reference mode requires reachable public/admin endpoints without launching another logger. See the [Aspire guide](../clients/dotnet/Distill.Aspire/README.md) for concrete hosting and reference examples.

`/health` is process liveness. `/ready` requires active configuration and healthy broker paths; it is not a universal destination probe. Actual local Aspire applications have been qualified, but production-target/Azure storage and restore remain release gates. Retain Distill's private recovery state and stable credentials, and run one active logger process per dedicated broker vhost.
