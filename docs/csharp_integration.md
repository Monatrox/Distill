# C#/.NET Integration Guide

This guide covers all three ways to integrate a .NET application with Distill.

It targets **.NET 8 or later** and assumes you have:

- an HTTP address for the Distill service;
- the Distill API key;
- a sink provisioned with the Distill CLI or deployment system.

---

## 1. Integration model

The C# application is an ingest client. It sends events over HTTP and never connects to RabbitMQ or writes the sink database directly.

```text
C# application
    │ HTTP + X-API-Key
    ▼
Distill channel actor
    │ confirmed durable publish
    ▼
RabbitMQ
    │ internal consumer; ack after sink success
    ▼
Configured Distill sink
```

A successful HTTP response means "durably handed to Distill," not that the record is already queryable in the sink.

---

## 2. Choosing an integration path

| Layer | Package | When to use |
|---|---|---|
| **Direct client** | `Distill.Client` | You need full control over channel lifecycle and event payloads. Zero external dependencies. |
| **ILogger provider** | `Distill.Extensions.Logging` | You want drop-in integration with `Microsoft.Extensions.Logging`. Handles batching, retries, and channel routing. |
| **Aspire integration** | `Distill.Aspire` | You're building an Aspire distributed application. Wires the client, logger, and health check from configuration automatically. |

---

## 3. Distill.Client — direct HTTP API

The `Distill.Client` package is a lightweight, dependency-free wrapper over the Distill REST API.

### 3.1 Installation

Reference from your project:

```xml
<PackageReference Include="Distill.Client" Version="1.0.0" />
```

### 3.2 Create a client

```csharp
using Distill.Client;

// Factory — creates a new HttpClient internally
var distill = DistillClient.Create(
    new Uri("http://localhost:8080/"),
    apiKey);

// Constructor injection — useful with IHttpClientFactory or DI
var distill = new DistillClient(
    httpClient,   // must have BaseAddress set
    apiKey);
```

### 3.3 Channel management

Channels must exist before ingestion. The channel identifier must contain only lowercase ASCII letters, digits, `.`, `-`, or `_`:

```csharp
var channel = await distill.CreateChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = "raw_events",
    SinkId = "postgres-primary",
    Aggregator = null   // null = disable preset aggregator; omit = use preset default
});
```

Other channel operations:

```csharp
// Advanced channel definition (raw object — caller must know the server schema)
await distill.CreateOrReplaceChannelAsync("orders.production", advancedDefinition);

// Dry-run validation
var validation = await distill.ValidateChannelAsync(request);

// Read effective config
var config = await distill.GetChannelAsync("orders.production");

// Pause / resume / flush / archive / clone
await distill.PauseChannelAsync("orders.production");
await distill.ResumeChannelAsync("orders.production");
await distill.FlushChannelAsync("orders.production");
await distill.ArchiveChannelAsync("orders.production");
await distill.CloneChannelAsync("source-channel", "target-channel");
```

`SinkId` may be omitted when the default sink is configured. The application never receives the sink connection string.

### 3.4 Write an event

```csharp
IngestResponse? response = await distill.WriteAsync(
    "orders.production",
    new SimpleEvent
    {
        Severity = "error",
        Message = "Payment authorization failed",
        Payload = new { order_id = "ord-123", provider = "stripe" },
        Attributes = new Dictionary<string, object?>
        {
            ["retryable"] = true,
            ["attempt"] = 2
        }
    });
```

### 3.5 Batch ingest

```csharp
IngestResponse? response = await distill.IngestAsync(new[]
{
    new DistillEvent
    {
        EventId = Guid.NewGuid(),
        Timestamp = DateTimeOffset.UtcNow,
        ChannelId = "orders.production",
        SeverityLevel = "info",
        Payload = new { order_id = "ord-123", operation = "created" },
        Metadata = new Dictionary<string, string>
        {
            ["service"] = "checkout-api",
            ["correlation_id"] = Activity.Current?.Id ?? "unknown"
        }
    }
});
```

Use stable `EventId` values so Distill's idempotent sink write can absorb retries.

### 3.6 Test routing

```csharp
EventTestResponse? test = await distill.TestEventAsync(
    "orders.production",
    eventToTest);
```

### 3.7 Error handling

```csharp
try
{
    await distill.IngestAsync(events);
}
catch (DistillApiException ex)
{
    Console.WriteLine($"Status: {ex.StatusCode}");
    Console.WriteLine($"Error code: {ex.ErrorCode}");
    Console.WriteLine($"Body: {ex.ResponseBody}");
}
```

| Status | Meaning |
|---|---|
| `400` | Invalid request or schema payload |
| `404` | The requested channel does not exist |
| `422` | Invalid channel definition |
| `503` | Distill or its external delivery path is unavailable |

Retry `503` and transport failures. Do not retry `400`, `404`, or `422` without correcting the request.

---

## 4. Distill.Extensions.Logging — ILogger provider

Drop in the `Microsoft.Extensions.Logging` provider and send structured logs through Distill without changing your existing `ILogger` code.

### 4.1 Installation

```xml
<PackageReference Include="Distill.Extensions.Logging" Version="1.0.0" />
```

### 4.2 Registration

```csharp
using Distill.Client;
using Distill.Extensions.Logging;

var distill = DistillClient.Create(
    new Uri("http://localhost:8080/"),
    Environment.GetEnvironmentVariable("DISTILL_API_KEY")!);

builder.Logging.AddDistill(distill, options =>
{
    options.DefaultChannel = "application.production";
    options.MinimumLevel = LogLevel.Information;
});
```

Now all `ILogger` calls are forwarded to Distill:
```csharp
logger.LogInformation("Order {OrderId} created", orderId);
logger.LogError(exception, "Payment failed for order {OrderId}", orderId);
```

Structured state, scopes, categories, event IDs, and exception details are preserved as Distill payload and metadata.

### 4.3 Channel routing

Route logger categories to different Distill channels:

```csharp
options.ChannelResolver = category =>
    category.StartsWith("Microsoft.", StringComparison.Ordinal)
        ? "platform.production"
        : "application.production";
```

When the resolver returns null or whitespace, the provider falls back to `DefaultChannel` and invokes `ChannelResolutionFailed`:

```csharp
options.ChannelResolutionFailed =
    (category, reason) => Console.Error.WriteLine($"'{category}': {reason}");
```

### 4.4 Per-channel configuration

Override batching, retry, and overflow behavior per channel:

```csharp
options.ChannelOptions["errors.critical"] = new PerChannelOptions
{
    BatchSize = 1,
    FlushInterval = TimeSpan.Zero,
    OverflowPolicy = DistillOverflowPolicy.Block
};

options.ChannelOptions["telemetry.trace"] = new PerChannelOptions
{
    QueueCapacity = 500,
    OverflowPolicy = DistillOverflowPolicy.DropOldest
};
```

Fields in `PerChannelOptions` are all nullable; when null, the global value from `DistillLoggerOptions` is used.

### 4.5 Category-level log levels

Set different minimum levels for namespace prefixes:

```csharp
options.CategoryLevels["Microsoft"] = LogLevel.Warning;
options.CategoryLevels["Microsoft.AspNetCore"] = LogLevel.Error;
options.CategoryLevels["MyApp.Services"] = LogLevel.Debug;
```

Resolution walks the dotted path: `"Microsoft.AspNetCore.Hosting"` checks
`"Microsoft.AspNetCore.Hosting"` → `"Microsoft.AspNetCore"` → `"Microsoft"`
before falling back to `MinimumLevel`.

### 4.6 Delivery callbacks

Observe delivery outcomes per channel:

```csharp
options.DeliverySuccess = context =>
{
    Console.WriteLine($"Delivered {context.EventCount} events to {context.Channel}"
        + $" (accepted: {context.Response.Accepted})");
};

options.DeliveryFailure = context =>
{
    Console.Error.WriteLine(
        $"Failed to deliver {context.EventCount} events to {context.Channel}"
        + $" after {context.Attempts} attempts: {context.Exception.Message}");
};
```

### 4.7 Auto-channel creation

The provider validates channels at startup and can create missing channels automatically:

```csharp
// Global factory — receives the channel ID
options.ChannelFactory = channelId => new CreateChannelRequest
{
    Name = channelId,
    Preset = "raw_events"
};

// Per-channel factory — overrides the global one for specific channels
options.ChannelOptions["errors.critical"].ChannelFactory = () => new CreateChannelRequest
{
    Name = "errors.critical",
    Preset = "errors_only"
};

// Observe provisioning failures
options.ChannelProvisioningFailure =
    (channelId, ex) => Console.Error.WriteLine($"Failed to provision '{channelId}': {ex.Message}");
```

### 4.8 Batching and retries

Configure globally or per channel:

```csharp
// Global defaults
options.BatchSize = 100;           // events per batch
options.FlushInterval = TimeSpan.FromSeconds(1);  // max wait before partial batch
options.MaxRetries = 3;            // retry attempts before DeliveryFailure
options.RetryDelay = TimeSpan.FromSeconds(1);
options.QueueCapacity = 8192;      // per-channel queue depth before overflow
options.OverflowPolicy = DistillOverflowPolicy.DropNewest;
```

Each channel has its own bounded queue. A debug flood on one channel cannot starve critical-error delivery on another because each channel's overflow policy is enforced independently.

The worker drains batches from each channel's queue. After the batch is sent, the `DeliverySuccess` or `DeliveryFailure` callback fires.

### 4.9 Graceful shutdown

The provider implements `IAsyncDisposable`. During shutdown it completes all channel writers, flushes remaining records, then cancels the background worker:

```csharp
await provider.DisposeAsync();
```

For synchronous `IDisposable` compatibility, `Dispose()` delegates to `DisposeAsync().AsTask().GetAwaiter().GetResult()`.

---

## 5. Distill.Aspire — Aspire integration

The `Distill.Aspire` package provides hosting extensions for the AppHost and a
client extension that wires the Distill client, logging provider, and health
check from configuration.

For full API details see `clients/dotnet/Distill.Aspire/README.md`. The essential
patterns are:

**External Distill instance:**
```csharp
var distill = builder.AddDistill("distill",
    endpoint: new Uri("http://localhost:8080"),
    apiKey: "your-api-key");
```

**Container-mode with auto-build and a PostgreSQL sink:**
```csharp
var postgres = builder.AddPostgres("pg").AddDatabase("distill");

var distill = builder.AddDistill("distill", apiKey: "dev-key")
    .WithLocalBuild()          // builds distil:latest from repo Dockerfile
    .WithSink(postgres);       // single sink → becomes default
```

**Consumer project:**
```csharp
builder.AddDistillClient("distill");  // reads ConnectionStrings__distill
```

`AddDistillClient` registers `DistillClient` as a singleton, adds the
`ILogger` provider, and registers a health check. Configuration is read
from `Distill__Endpoint` / `ConnectionStrings__distill` with a fallback
to `http://localhost:8080`, and `Distill__ApiKey` for the API key.

---

## 6. Protobuf ingestion

When you need wire efficiency, the ingestion endpoint also accepts `application/protobuf` or `application/x-protobuf`:

```protobuf
syntax = "proto3";

message IngestRequest {
  repeated ProtoLogEvent events = 1;
}

message ProtoLogEvent {
  string event_id = 1;
  int64 timestamp_millis = 2;
  string channel_id = 3;
  int32 severity_level = 4;
  bytes payload = 5;
  map<string, string> metadata = 6;
}
```

Severity values: `0=unknown`, `1=trace`, `2=debug`, `3=info`, `4=warning`, `5=error`, `6=critical`. The `payload` bytes should contain UTF-8 JSON when schema validation is configured; otherwise Distill preserves non-JSON bytes as base64.

Generate C# types with `Google.Protobuf` and `Grpc.Tools`:

```xml
<PackageReference Include="Google.Protobuf" Version="3.*" />
<PackageReference Include="Grpc.Tools" Version="2.*" PrivateAssets="All" />
```

```csharp
var request = new IngestRequest();
request.Events.Add(new ProtoLogEvent
{
    EventId = Guid.NewGuid().ToString(),
    TimestampMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
    ChannelId = "orders.production",
    SeverityLevel = 3,
    Payload = ByteString.CopyFromUtf8("{\"order_id\":\"ord-123\"}")
});

using var content = new ByteArrayContent(request.ToByteArray());
content.Headers.ContentType = new MediaTypeHeaderValue("application/protobuf");
using var message = new HttpRequestMessage(HttpMethod.Post, "/ingest") { Content = content };
message.Headers.Add("X-API-Key", apiKey);

using var response = await client.SendAsync(message);
response.EnsureSuccessStatusCode();
```

---

## 7. Aggregation

### 7.1 Disabling aggregation

Set `Aggregator` to `null` to persist every event:

```csharp
var channel = await distill.CreateChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = "raw_events",
    Aggregator = null
});
```

### 7.2 Aggregator configuration

Supported v1 aggregators:

| function | description | requires `payload.value` |
|---|---|---|
| `average` | Numeric average | yes |
| `sum` | Numeric total | yes |
| `min` | Minimum numeric value | yes |
| `max` | Maximum numeric value | yes |
| `count` | Event count | no |
| `average_duration_between_events` | Average interval between adjacent timestamps | no |

Granularity options: `overall`, `per_n_events` (with `parameter`), `per_t_time` (with `parameter` in seconds).

Add `"rolling": true` to merge windows into one expanding aggregate record.

### 7.3 Aggregation and severity together

Events at or above `severity_threshold` are persisted individually AND contribute to the aggregate. Events below the threshold only contribute to the aggregate. If no aggregator is set, all events are persisted regardless of severity.

---

## 8. Runtime schema validation

If a channel has a registered `message_schema`, Distill validates the payload against the Protobuf descriptor:

```csharp
var schema = new
{
    message_type = "Orders.OrderCreated",
    version = "1.0.0",
    descriptor_set_base64 = Convert.ToBase64String(fileDescriptorSetBytes)
};

using var response = await client.PostAsJsonAsync(
    "/channels/orders.production/schema", schema);
response.EnsureSuccessStatusCode(); // 204
```

A schema mismatch returns `400`. Fix the producer or register the correct descriptor.

---

## 9. Delivery semantics

The HTTP response reports admission to Distill's delivery path:

1. Distill validates the event and channel.
2. The channel actor buffers or publishes to RabbitMQ.
3. RabbitMQ confirms the durable publication.
4. Distill returns success.
5. The internal consumer writes to the configured sink and acknowledges only after success.

Sink outages may create a backlog after the HTTP request succeeds. The consumer retries transient failures with bounded backoff. Exhausted messages go to the dead-letter exchange. Because writes are idempotent, the C# producer may safely retry using the same `event_id` after a timeout.

Recommended producer policy:

- use one stable `event_id` per logical event;
- use UTC timestamps from `DateTimeOffset.UtcNow`;
- retry only transport failures and `503` responses;
- do not retry `400`, `404`, or `422` without correcting the request;
- use bounded client-side retry with jitter;
- avoid logging payloads that contain secrets or personal data.

---

## 10. Operational checklist

Before enabling production logging, verify:

- Distill can reach RabbitMQ and the configured sink.
- The channel was created with the intended threshold and aggregator.
- The channel's `sink_id` is the intended named sink.
- The API key is stored as a secret and transmitted only over TLS.
- `/health` is monitored.
- `/metrics` is scraped (received, persisted, aggregate, error counters).
- RabbitMQ durable queues and the dead-letter queue have disk/retention monitoring.
- A test event can be traced from the C# client to the selected sink.

---

## 11. Source-level extension points

With full source access:

- add a typed generated C# contract package matching `src/proto.rs`;
- add an authenticated TLS deployment policy at the HTTP ingress;
- extend the sink registry for additional drivers;
- add integration tests covering `HttpClient`, RabbitMQ, PostgreSQL, retries, and dead-letter recovery.

The C# integration should remain at the HTTP boundary. Directly consuming Distill's internal RabbitMQ queue or writing its PostgreSQL tables bypasses validation, routing, retry, and idempotency guarantees.
