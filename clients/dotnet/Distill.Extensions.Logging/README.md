# Distill.Extensions.Logging

`Distill.Extensions.Logging` connects the standard .NET `ILogger` abstraction to Distill.

It is intentionally a separate package from `Distill.Client`: applications that want direct control can use the base client without taking a logging-provider dependency.

Register a destination and reconcile versioned channels during application startup. The provider only sends events; the explicit client configuration APIs can register destinations, while the logger transport itself does not manage sink credentials. Modular applications may explicitly ensure their finite channel set in `Program.cs` before serving traffic.

## Registration

```csharp
using Distill.Client;
using Distill.Extensions.Logging;

using var distill = DistillClient.Create(
    new Uri("http://localhost:8080/"),
    apiKey: Environment.GetEnvironmentVariable("DISTILL_INGEST_API_KEY")!,
    adminApiKey: Environment.GetEnvironmentVariable("DISTILL_ADMIN_API_KEY"),
    adminEndpoint: "http://localhost:8081/");

var options = new DistillLoggerOptions
{
    DefaultChannel = "application.production",
    DefaultVersion = "1",
    MinimumLevel = LogLevel.Information,
    BatchSize = 100,
    FlushInterval = TimeSpan.FromSeconds(1),
    MaxRetries = 3
};
await using var provider = new DistillLoggerProvider(distill, options);
builder.Logging.AddProvider(provider);
```

Before `app.RunAsync()`, reconcile application-owned channels with an admin-enabled `DistillClient`:

```csharp
var app = builder.Build();

await distill.ReconcileChannelAsync(new CreateChannelRequest
{
    Name = "application.production",
    Version = "1",
    SinkId = "application-receiver@1",
    Preset = ChannelPreset.RawEvents
});

await app.RunAsync();
```

`ReconcileChannelAsync` binds the client to the declared immutable version. Register the named sink first through `RegisterSinkAsync` (see the SDK README). Let reconciliation conflicts stop startup; use a new version for changed configuration. `EnsureChannelAsync` remains create-only.

The sample explicitly owns the provider and HTTP client. Keep the client alive until the provider has drained: declaring `await using` for the provider after `using` for the client gives that disposal order. The convenience `builder.Logging.AddDistill(...)` helper is also available when your application arranges their lifetimes. See the complete [web application example](../../../docs/csharp_integration.md#ilogger-integration) for a runnable setup.

Existing logging code continues to work:

```csharp
logger.LogInformation("Order {OrderId} created", orderId);
logger.LogError(exception, "Payment failed for order {OrderId}", orderId);
```

Structured state, scopes, categories, event IDs, and exception details are preserved as Distill properties.

## Channel routing

Route logger categories to different Distill channels:

```csharp
options.ChannelResolver = category =>
    category.StartsWith("Microsoft.", StringComparison.Ordinal)
        ? "platform.production"
        : "application.production";
```

## Delivery behavior

The provider uses a bounded queue and batches records through `DistillClient.IngestAsync`. Failed batches are retried three times by default using the same event IDs. The default overflow policy is `DropNewest`, which avoids blocking application threads. For critical audit paths, use the explicit asynchronous `DistillClient` API when delivery failure must be actionable.

Set `DeliveryFailure` to observe batches that exhaust their retries:

```csharp
options.DeliveryFailure = context => Console.Error.WriteLine(context.Exception);
```

The provider drains bursts continuously, snapshots timestamp/ID/payload at enqueue time, and retries only transiently rejected records from a partial response. `DeliveryDropped` and dropped/failed counters make overflow and terminal failures observable. Records accepted by the in-process queue are not yet durable server admissions.

Disposal interrupts any batching delay and drains within `ShutdownTimeout` (30 seconds by default), then reports remaining failures rather than hanging indefinitely. Aspire integration drains through a hosted shutdown service before HTTP transports are disposed. Applications should preserve stable event IDs when implementing their own retry behavior through the base client.
