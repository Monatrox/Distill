# Distill.Client

`Distill.Client` is a small dependency-free .NET client for the Distill HTTP API.

It targets .NET 8 and supports:

- simplified channel creation with presets and overrides;
- full advanced channel definitions;
- simple channel-bound event writes;
- batch event ingestion;
- sample-event routing tests;
- resolved channel configuration;
- pause, resume, flush, archive, and clone operations.

## Example

```csharp
using Distill.Client;

var client = DistillClient.Create(
    new Uri("http://localhost:8080/"),
    apiKey: "change-me-in-development");

await client.CreateChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = "errors_only",
    RetentionHours = 720
});

// To disable a preset aggregator and persist every event, use:
// DisableAggregator = true

IngestResponse? response = await client.WriteAsync(
    "orders.production",
    new SimpleEvent
    {
        Severity = "error",
        Message = "Payment authorization failed",
        Payload = new
        {
            order_id = "ord-123",
            provider = "stripe"
        },
        Attributes = new Dictionary<string, object?>
        {
            ["retryable"] = true,
            ["attempt"] = 2
        }
    });

Console.WriteLine($"Accepted: {response?.Accepted}");
```

For high-volume producers, use `IngestAsync` with stable `EventId` values. If a request times out after admission, retrying the same event ID allows Distill’s idempotent sink write to absorb duplicate delivery.

## Add the project

```xml
<ProjectReference Include="..\\Distill.Client\\Distill.Client.csproj" />
```

Build it locally with:

```powershell
dotnet build clients/dotnet/Distill.Client/Distill.Client.csproj
```

The client intentionally does not auto-retry requests: callers should choose retry policy appropriate to their workload and preserve stable event IDs when retrying.
