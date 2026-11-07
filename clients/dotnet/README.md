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
    apiKey: "change-me-ingest");

// Use a protected admin endpoint when it differs from the ingest endpoint.
await client.RegisterSinkAsync(new RegisterSinkRequest
{
    Name = "orders-receiver",
    Version = "1",
    Config = new SinkConfig
    {
        Driver = "http",
        ConnectionString = "https://collector.example.com/records"
    }
});
await client.ReconcileChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Version = "1",
    SinkId = "orders-receiver@1",
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

`ProjectReference` paths are resolved relative to the consuming `.csproj`, not relative to the solution file. For a quick diagnosis, use the absolute path first:

```xml
<ItemGroup>
  <ProjectReference Include="A:\Dev\Rust\Projects\Distill\clients\dotnet\Distill.Client\Distill.Client.csproj" />
</ItemGroup>
```

Once that works, replace the absolute path with a relative path calculated from the consuming project's directory. In PowerShell, verify the result before adding it:

```powershell
$consumerProject = "A:\path\to\MyApp.csproj"
$reference = "..\..\Distill\clients\dotnet\Distill.Client\Distill.Client.csproj"
$consumerDirectory = Split-Path -Parent $consumerProject
[IO.Path]::GetFullPath((Join-Path $consumerDirectory $reference))
Test-Path (Join-Path $consumerDirectory $reference)
```

The final command must return `True`. For local source development use a `ProjectReference`. Release CI also produces `Distill.Client`, `Distill.Extensions.Logging`, and `Distill.Aspire` packages at version `0.1.0`; these can be restored from a private feed. Publication to NuGet is not automatic:

```xml
<ProjectReference Include="..\..\Distill\clients\dotnet\Distill.Client\Distill.Client.csproj" />
```

If using an IDE, add `Distill.Client.csproj` as an existing project in the solution and then add it as a project reference. Reload the solution after correcting the path.

```xml
<ProjectReference Include="..\Distill.Client\Distill.Client.csproj" />
```

Build it locally with:

```powershell
dotnet build clients/dotnet/Distill.Client/Distill.Client.csproj
```

The client intentionally does not auto-retry requests: callers should choose retry policy appropriate to their workload and preserve stable event IDs when retrying.
