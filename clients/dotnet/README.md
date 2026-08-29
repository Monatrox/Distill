# Distill.Client

`Distill.Client` is a small dependency-free .NET client for the Distill HTTP API.

For drop-in integration with existing `ILogger` applications, see the companion [Distill.Extensions.Logging provider](Distill.Extensions.Logging/README.md).

It targets .NET 8 and supports:

- simplified channel creation and explicit startup provisioning;
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
    apiKey: "change-me-ingest",
    adminApiKey: "change-me-admin");

await client.EnsureChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = "raw_events"
});

// To disable a preset aggregator and persist every event, set Aggregator to null:
// Aggregator = null

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

`EnsureChannelAsync` is intended for explicit application startup provisioning. It creates a missing channel, leaves an existing channel unchanged, and requires administration credentials. Provisioning errors should fail startup rather than allowing logs to target missing channels.

For high-volume producers, use `IngestAsync` with stable `EventId` values. If a request times out after admission, retrying the same event ID allows Distill’s idempotent sink write to absorb duplicate delivery.

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

The final command must return `True`. The reference should be a `ProjectReference`, not a `Reference` or `PackageReference`:

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
