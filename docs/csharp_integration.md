# C#/.NET Integration Guide

This guide describes how a .NET operator integrates an application or library with Distill.

It targets .NET 8 or later and assumes the operator has:

- an HTTP address for the Distill service;
- the Distill API key;
- a sink provisioned separately with the Distill CLI or deployment system; and
- source access when a generated Protobuf client, schema registration, or operational customization is required.

## 1. Integration model

The C# application is an ingest client. It does not connect to RabbitMQ or write Distill's tables directly.

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

RabbitMQ is transport, buffering, retry, and dead-letter infrastructure. The configured Distill sink may be PostgreSQL, MySQL, SQL Server, or HTTP. The C# process should treat a successful HTTP response as “durably handed to Distill,” not as proof that a record is already queryable in the sink.

## 2. Deployment and sink administration

For a local, self-contained Distill environment:

```powershell
docker compose up --build
```

The Compose deployment starts Distill and RabbitMQ. Sink administration is a deployment concern and should be performed before the application starts sending logs. The C# application should not receive or store the sink connection string.

Configure the Distill process with:

```text
RABBITMQ_URI=amqp://user:password@rabbitmq.example.com:5672/%2f
DISTILL_API_KEY=<secret>
DISTILL_BIND_ADDRESS=0.0.0.0:8080
DISTILL_SINKS_CONFIG_PATH=/etc/distill/sinks.json
```

For PostgreSQL, provision a named sink with the CLI:

```powershell
$env:DISTILL_POSTGRES_URL = "postgresql://distill:password@postgres:5432/logs?sslmode=require"
distill sink create postgres-primary `
  --driver postgres `
  --connection-string-env DISTILL_POSTGRES_URL `
  --file sinks.json `
  --default
```

For an HTTP sink, keep the endpoint in the manifest and the bearer token in the Distill process environment:

```powershell
$env:DISTILL_COLLECTOR_TOKEN = "replace-with-a-secret"
distill sink create custom-http `
  --driver http `
  --url https://collector.example.com/distill `
  --bearer-token-env DISTILL_COLLECTOR_TOKEN `
  --file sinks.json `
  --default
```

`RABBITMQ_URI` is the service's broker connection. `DISTILL_SINKS_CONFIG_PATH` points to the named sink manifest. The manifest contains secret references, not database passwords or bearer tokens. `rabbitmq_config.connection_string` remains part of the advanced channel contract for self-description, while the current broker connection is process-level.

When PostgreSQL is provisioned by .NET Aspire, ensure the connection string is reachable from the Distill process. If Distill runs in Docker and PostgreSQL runs on the host, `localhost` in the Aspire connection string points to the container itself; use a host-reachable address such as `host.docker.internal` or attach both services to a shared Docker network. If both run under Compose, use the Compose service name, such as `postgres`.

Never place a sink connection string, bearer token, or API key in source control, event payloads, metadata, or ordinary application logs. Use the Distill CLI, a secret manager, or container secrets. The C# application only needs the Distill base URL and API key.

## 3. Create a channel

Channels must exist before ingestion. The channel identifier must contain only lowercase ASCII letters, digits, `.`, `-`, or `_`.

```csharp
using Distill.Client;

var distill = DistillClient.Create(
    new Uri("http://localhost:8080/"),
    apiKey);

var channel = await distill.CreateChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = "raw_events",
    SinkId = "postgres-primary",
    DisableAggregator = true
});
```

`SinkId` may be omitted when `postgres-primary` is the configured default. The application never receives the PostgreSQL connection string. A successful simplified creation returns the sanitized channel configuration. Use `CreateOrReplaceChannelAsync` only when an operator needs to supply the complete advanced definition.

Common failures are:

| Status | Meaning |
|---|---|
| `400` | Invalid request or schema payload |
| `404` | The requested channel does not exist for a channel-specific operation |
| `422` | Invalid channel definition |
| `503` | Distill or its external delivery path is unavailable |

The channel `severity_threshold` controls additional raw persistence. When an aggregator is configured, every event contributes to the aggregate; events at or above the threshold are also persisted individually. With no aggregator, every event is persisted immediately and must not be treated as discardable.

Supported v1 aggregators include:

```json
{ "function": "average", "granularity": "overall" }
```

This computes the average numeric value from `payload.value` across the active overall window. `count` with `overall` computes the total number of events in that window. `average_duration_between_events` computes the average interval, in seconds, between chronologically adjacent event timestamps; it does not require a numeric `payload.value`. For `per_n_events` or `per_t_time`, add `parameter` with the event count or duration in seconds, respectively.

For example, an average event interval channel can be created with:

```json
{
  "function": "average_duration_between_events",
  "granularity": "per_n_events",
  "parameter": 100
}
```

The first event in a window establishes the starting timestamp but does not form an interval. Thus, 100 events produce 99 intervals. Conditional counting is not yet part of the v1 contract; its predicate syntax and security/performance behavior will be designed separately.

Add `"rolling": true` to any supported aggregator when each emitted window should merge into one expanding aggregate record:

```json
{
  "function": "count",
  "granularity": "per_t_time",
  "parameter": 60,
  "rolling": true
}
```

Rolling aggregates retain the first `window_start`, advance `window_end`, and merge their values and counts. Numeric averages are merged using their counts; sums, minimums, maximums, and counts are merged directly. Rolling duration averages preserve custom metadata including `first_event_timestamp`, `last_event_timestamp`, `interval_count`, and `interval_sum_seconds`, allowing the interval crossing two adjacent windows to be included.

Rolling aggregate identity is stable across Distill restarts, so the same channel and aggregator configuration continues the existing record. Reconfiguring the aggregator creates a new series when its identity changes. Custom metadata is present on every aggregate record but is normally empty unless the aggregation requires it.

### Aggregation setup scenarios

The following fragments are the `aggregator` property of a channel definition sent to `PUT /channels/{channel_id}`. Keep the other required channel properties from the channel-creation example unchanged.

`overall` maintains one running window and is flushed when the channel TTL expires, when the channel is flushed or replaced, or when the bounded buffer requires a flush. With the default `ttl_seconds` of `3600`, “overall” means all inputs received during that active window, not an immutable lifetime statistic.

#### Persist every event without aggregation

Use this when every event, including low-severity events, must be stored individually:

```json
"aggregator": null
```

No `payload.value` is required. Severity still determines the route when an aggregator exists, but with `null` every event uses the direct persistence path.

#### Numeric average

Numeric average reads `payload.value` as a finite JSON number.

```json
"aggregator": {
  "function": "average",
  "granularity": "overall"
}
```

Windowed variants:

```json
"aggregator": {
  "function": "average",
  "granularity": "per_n_events",
  "parameter": 100
}
```

```json
"aggregator": {
  "function": "average",
  "granularity": "per_t_time",
  "parameter": 60
}
```

The first example averages all numeric values in the active overall window. The second flushes after 100 aggregated events. The third flushes after 60 seconds from the first event in the window.

#### Average duration between events

Use event timestamps rather than `payload.value`:

```json
"aggregator": {
  "function": "average_duration_between_events",
  "granularity": "overall"
}
```

The implementation sorts timestamps chronologically before calculating adjacent intervals, so producers do not need to deliver events in timestamp order. The result is seconds as a floating-point value. For `per_n_events`, the first event does not form an interval; `N` events produce `N - 1` intervals.

#### Total event count

Use `count` when the count of all events in each aggregation window is needed:

```json
"aggregator": {
  "function": "count",
  "granularity": "overall"
}
```

Windowed count variants use the same granularity settings:

```json
"aggregator": {
  "function": "count",
  "granularity": "per_n_events",
  "parameter": 1000
}
```

```json
"aggregator": {
  "function": "count",
  "granularity": "per_t_time",
  "parameter": 300
}
```

Count ignores the event payload and counts every event routed to the aggregator. Conditional count—such as counting only events where `payload.status == "failed"`—is not implemented yet.

#### Sum, minimum, and maximum

These functions also require a finite numeric `payload.value`:

```json
"aggregator": { "function": "sum", "granularity": "overall" }
```

```json
"aggregator": { "function": "min", "granularity": "per_t_time", "parameter": 60 }
```

```json
"aggregator": { "function": "max", "granularity": "per_n_events", "parameter": 100 }
```

`sum` totals values, `min` records the smallest value, and `max` records the largest value in each window. All three support `overall`, `per_n_events`, and `per_t_time` with the same `parameter` rules.

#### Severity and aggregation together

Aggregation includes every event in the channel. For example, with `severity_threshold: "warning"` and a numeric average aggregator:

- `warning`, `error`, and `critical` events are persisted individually and contribute to the average;
- `trace`, `debug`, and `info` events must contain a numeric `payload.value` and contribute to the average;
- if no aggregator is configured, all severities are persisted individually.

If a low-severity event lacks the required numeric value for `average`, `sum`, `min`, or `max`, Distill rejects that event rather than silently treating it as zero.

## 4. JSON ingestion

The simplest integration uses `POST /ingest` with an array of events.

```csharp
var events = new[]
{
    new
    {
        event_id = Guid.NewGuid(),
        timestamp = DateTimeOffset.UtcNow,
        channel_id = "orders.production",
        severity_level = "info",
        payload = new
        {
            order_id = "ord-123",
            operation = "created",
            latency_ms = 42
        },
        metadata = new Dictionary<string, string>
        {
            ["service"] = "checkout-api",
            ["host"] = Environment.MachineName,
            ["correlation_id"] = Activity.Current?.Id ?? "unknown"
        }
    }
};

using var request = new HttpRequestMessage(HttpMethod.Post, "/ingest")
{
    Content = JsonContent.Create(events)
};
request.Headers.Add("X-API-Key", apiKey);

using var response = await client.SendAsync(request);
var body = await response.Content.ReadAsStringAsync();
response.EnsureSuccessStatusCode();
// body: { "accepted": 1 }
```

Required event fields:

- `timestamp`: an ISO-8601 UTC timestamp. Every event requires one, regardless of ordering mode.
- `channel_id`: an existing valid channel.
- `severity_level`: `unknown`, `trace`, `debug`, `info`, `warning`, `error`, or `critical`.
- `payload`: any JSON value. Objects are recommended for queryable structured logs.

`event_id` is optional, but a producer should always provide a stable UUID. Distill uses it for idempotent sink writes when RabbitMQ redelivers a message. `metadata` is optional and is a string-to-string map.

## 5. Protobuf ingestion

The endpoint accepts `application/protobuf` or `application/x-protobuf`. The wire contract is:

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

Severity values are `0=unknown`, `1=trace`, `2=debug`, `3=info`, `4=warning`, `5=error`, and `6=critical`. The `payload` bytes should contain UTF-8 JSON when schema validation is configured; otherwise Distill preserves non-JSON bytes as base64 in its JSON representation.

Generate C# types with `Google.Protobuf` and `Grpc.Tools`, serialize `IngestRequest`, and send the bytes:

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
    Payload = ByteString.CopyFromUtf8("{\"order_id\":\"ord-123\",\"operation\":\"created\"}")
});

using var content = new ByteArrayContent(request.ToByteArray());
content.Headers.ContentType = new MediaTypeHeaderValue("application/protobuf");
using var message = new HttpRequestMessage(HttpMethod.Post, "/ingest") { Content = content };
message.Headers.Add("X-API-Key", apiKey);

using var response = await client.SendAsync(message);
response.EnsureSuccessStatusCode();
```

## 6. Runtime schema validation

If a channel has `message_schema`, Distill validates the payload against the registered Protobuf descriptor. An operator with source access should generate a serialized `FileDescriptorSet`, base64-encode it, and register:

```csharp
var schema = new
{
    message_type = "Orders.OrderCreated",
    version = "1.0.0",
    descriptor_set_base64 = Convert.ToBase64String(fileDescriptorSetBytes)
};

using var response = await client.PostAsJsonAsync(
    "/channels/orders.production/schema",
    schema);
response.EnsureSuccessStatusCode(); // 204
```

The descriptor's fully-qualified message name and version must match the payload contract used by the producer. A schema mismatch returns `400`; do not retry malformed payloads indefinitely. Fix the producer or register the correct descriptor.

## 7. Delivery, retries, and producer behavior

The HTTP response reports admission to Distill's delivery path:

1. Distill validates the event and channel.
2. The channel actor either buffers it for aggregation or publishes it to RabbitMQ.
3. RabbitMQ confirms the durable publication.
4. Distill returns a successful response.
5. The internal consumer writes to the configured sink and acknowledges only after success.

Sink outages therefore may cause a backlog after an HTTP request has succeeded. The consumer retries transient failures with bounded backoff. Exhausted messages go to the `distill.dead-letter` exchange and durable dead-letter queue for operator investigation. Because writes are idempotent, the C# producer may safely retry an HTTP request using the same `event_id` after a timeout, although the response may have been lost after successful admission.

Recommended producer policy:

- use one stable `event_id` per logical event;
- use UTC timestamps from `DateTimeOffset.UtcNow`;
- retry only transport failures and `503` responses;
- do not retry `400`, `404`, or `422` without correcting the request;
- use bounded client-side retry with jitter and an application circuit breaker;
- monitor `/health` and `/metrics` separately from application success rates;
- avoid logging full payloads when payloads contain secrets or personal data.

## 8. A reusable .NET client shape

For a library integration, wrap `HttpClient` with `IHttpClientFactory` and expose a small typed API:

```csharp
public sealed record DistillEvent(
    Guid EventId,
    DateTimeOffset Timestamp,
    string ChannelId,
    string SeverityLevel,
    object Payload,
    IReadOnlyDictionary<string, string>? Metadata = null);

public sealed class DistillClient(HttpClient httpClient)
{
    public async Task IngestAsync(
        IReadOnlyCollection<DistillEvent> events,
        CancellationToken cancellationToken = default)
    {
        using var response = await httpClient.PostAsJsonAsync(
            "/ingest", events, cancellationToken);

        if ((int)response.StatusCode == 503)
        {
            throw new HttpRequestException(
                "Distill delivery is temporarily unavailable.", null,
                response.StatusCode);
        }

        response.EnsureSuccessStatusCode();
    }
}
```

Register it with an API key supplied by configuration:

```csharp
services.AddHttpClient<DistillClient>((serviceProvider, client) =>
{
    var configuration = serviceProvider.GetRequiredService<IConfiguration>();
    client.BaseAddress = new Uri(configuration["Distill:BaseUrl"]!);
    client.DefaultRequestHeaders.Add("X-API-Key", configuration["Distill:ApiKey"]!);
});
```

Do not create a new `HttpClient` per event. Batch related events when practical, while keeping request sizes bounded and preserving stable IDs.

## 9. Operational checklist

Before enabling production logging, verify:

- Distill can reach RabbitMQ and the configured sink.
- The channel was created successfully with the intended threshold and aggregator.
- The channel's `sink_id` is the intended named sink, or it intentionally uses the configured default.
- The API key is stored as a secret and transmitted only over TLS in production.
- `/health` is monitored.
- `/metrics` is scraped, especially received, persisted, aggregate, and error counters.
- RabbitMQ durable queues and the dead-letter queue have disk/retention monitoring.
- The selected sink's uniqueness constraints on `event_id` and `aggregate_id` remain intact.
- A test event can be traced from the C# client to the selected sink or downstream HTTP adapter.

## 10. Source-level extension points

With full source access, the most relevant extension points are:

- add a typed generated C# contract package matching `src/proto.rs`;
- add an authenticated TLS deployment policy at the HTTP ingress;
- extend the sink registry for additional drivers while preserving named-sink references;
- add an explicit channel status endpoint exposing delivery health;
- add integration tests covering `HttpClient`, RabbitMQ, PostgreSQL, retries, and dead-letter recovery.

The C# integration should remain at the HTTP boundary. Directly consuming Distill's internal RabbitMQ queue or writing its PostgreSQL tables would bypass validation, routing, retry, and idempotency guarantees.
