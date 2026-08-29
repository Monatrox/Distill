# Distill.Extensions.Logging

`Distill.Extensions.Logging` connects the standard .NET `ILogger` abstraction to Distill.

It is intentionally a separate package from `Distill.Client`: applications that want direct control can use the base client without taking a logging-provider dependency.

Provision the Distill sink before starting the application. The provider only sends events; it does not receive, store, or manage PostgreSQL/HTTP sink credentials. Modular applications may explicitly ensure their finite channel set in `Program.cs` before serving traffic.

## Registration

```csharp
using Distill.Client;
using Distill.Extensions.Logging;

var distill = DistillClient.Create(
    new Uri("http://localhost:8080/"),
    apiKey: Environment.GetEnvironmentVariable("DISTILL_API_KEY")!,
    adminApiKey: Environment.GetEnvironmentVariable("DISTILL_ADMIN_API_KEY"));

builder.Logging.AddDistill(distill, options =>
{
    options.DefaultChannel = "application.production";
    options.MinimumLevel = LogLevel.Information;
    options.BatchSize = 100;
    options.FlushInterval = TimeSpan.FromSeconds(1);
    options.MaxRetries = 3;
});
```

Before `app.Run()`, ensure application-owned channels with an admin-enabled `DistillClient`:

```csharp
var app = builder.Build();

await distill.EnsureChannelAsync(new CreateChannelRequest
{
    Name = "application.production",
    Preset = ChannelPreset.RawEvents
});

app.Run();
```

`EnsureChannelAsync` creates missing channels and leaves existing definitions unchanged. Let provisioning failures stop startup. Keep sink credentials in Distill deployment configuration, not application channel requests.

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

The provider flushes accepted records during disposal. Applications should preserve stable event IDs when implementing their own retry behavior through the base client.
