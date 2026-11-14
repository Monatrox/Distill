# Distill.Client

`Distill.Client` is a small dependency-free .NET client for the Distill HTTP API.

For drop-in integration with existing `ILogger` applications, see the companion [Distill.Extensions.Logging provider](Distill.Extensions.Logging/README.md).

It targets .NET 8 and supports:

- runtime sink registration and versioned startup reconciliation;
- full advanced channel definitions;
- simple channel-bound event writes;
- batch event ingestion;
- sample-event routing tests;
- resolved channel configuration;
- pause, resume, flush, archive, and clone operations.

## Example

```csharp
using Distill.Client;

using var client = DistillClient.Create(
    new Uri("http://localhost:8080/"),
    apiKey: "dev-key",
    adminApiKey: "dev-key-admin",
    adminEndpoint: "http://localhost:8081/");

// These destination values match the root README's local demo.
await client.RegisterSinkAsync(new RegisterSinkRequest
{
    Name = "demo-receiver",
    Version = "1",
    Config = new SinkConfig
    {
        Driver = "http",
        ConnectionString = "http://receiver:8765/records",
        Headers = new Dictionary<string, string>
        {
            ["Authorization"] = "Bearer dev-receiver-token"
        }
    }
});
await client.ReconcileChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Version = "1",
    SinkId = "demo-receiver@1",
    Preset = ChannelPreset.RawEvents
});

IngestResponse? response = await client.WriteAsync(
    "orders.production",
    new SimpleEvent
    {
        Severity = DistillSeverity.Error,
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

`ReconcileChannelAsync` requires an explicit configuration version, creates or verifies an identical immutable definition, and binds subsequent writes from that client to that version. Different content under the same identity returns a conflict. `EnsureChannelAsync` remains create-only; it does not reconcile changes. A different version must be reconciled by the corresponding producer instance. Include `MessageSchema` (fully qualified `MessageType`, schema `Version`, and base64 `FileDescriptorSet` in `DescriptorSetBase64`) to deploy payload schema changes with that immutable channel version. Configuration/lifecycle methods automatically target the bound version.

For high-volume producers, use `IngestAsync` with stable `EventId`, timestamp and payload values. Retry the original event after an ambiguous failure; changing content under the same ID is a conflict. `SimpleEvent` generates its timestamp and ID when constructed, so reuse the object on retry. Inspect rejected indexes on partial responses rather than resending accepted items.

Clients constructed from caller-provided `HttpClient` instances leave them owned by the caller. `DistillClient.Create(...)` owns and disposes the transports it creates or explicitly takes ownership of. Use separate ingest/admin transports when the listeners differ. `CheckReadinessAsync` checks `/ready`; `/health` means only process liveness.

## Add the project

For source development, add a project reference using the path to your checkout:

```sh
dotnet add ./MyApp/MyApp.csproj reference ../Distill/clients/dotnet/Distill.Client/Distill.Client.csproj
```

Adjust paths for your directory layout. XML `ProjectReference` paths are relative to the consuming `.csproj`.

CI produces `Distill.Client`, `Distill.Extensions.Logging` and `Distill.Aspire` packages at version `0.1.0`. Build them using the root README's package commands and restore them from a local/private feed; public NuGet publication is not automatic.

The client does not automatically retry requests. Choose a caller retry policy and preserve the original event identity/content after ambiguous failures. The example above uses the root README's development endpoints and credentials; replace them with deployment configuration outside the demo.
