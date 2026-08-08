using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Distill.Client;

/// Describes a Distill channel using the simplified creation surface.
public sealed record CreateChannelRequest
{
    /// Gets or initializes the channel identifier.
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    /// Gets or initializes the preset used before applying overrides.
    [JsonPropertyName("preset")]
    public string Preset { get; init; } = "raw_events";

    /// Gets or initializes an optional severity threshold override.
    [JsonPropertyName("severity_threshold")]
    public string? SeverityThreshold { get; init; }

    /// Gets or initializes an optional aggregation override. Set this to null to disable a preset aggregator.
    [JsonIgnore(Condition = JsonIgnoreCondition.Never)]
    [JsonPropertyName("aggregator")]
    public AggregatorSpec? Aggregator { get; init; }

    /// Gets or initializes the named sink used by this channel.
    [JsonPropertyName("sink_id")]
    public string? SinkId { get; init; }

    /// Gets or initializes a legacy inline sink configuration override.
    ///
    /// Prefer <see cref="SinkId"/>. Sink credentials should be provisioned by the Distill CLI
    /// or deployment system, not passed through application configuration.
    [Obsolete("Use SinkId and provision the sink with the Distill CLI.")]
    [JsonPropertyName("sink_config")]
    public SinkConfig? SinkConfig { get; init; }

    /// Gets or initializes an optional retention period in hours.
    [JsonPropertyName("retention_hours")]
    public uint? RetentionHours { get; init; }

    /// Gets or initializes an optional channel buffer size in bytes.
    [JsonPropertyName("buffer_bytes")]
    public int? BufferBytes { get; init; }

    /// Gets or initializes an optional aggregation TTL in seconds.
    [JsonPropertyName("ttl_seconds")]
    public long? TtlSeconds { get; init; }

    /// Gets or initializes an optional channel mailbox capacity.
    [JsonPropertyName("mailbox_capacity")]
    public int? MailboxCapacity { get; init; }

    /// Gets or initializes an optional ordering mode.
    [JsonPropertyName("ordering")]
    public string? Ordering { get; init; }

    /// Gets or initializes an optional allowed lateness period in milliseconds.
    [JsonPropertyName("allowed_lateness_ms")]
    public long? AllowedLatenessMilliseconds { get; init; }
}

/// Configures a channel aggregator.
public sealed record AggregatorSpec
{
    /// Gets or initializes the aggregation function.
    [JsonPropertyName("function")]
    public required string Function { get; init; }

    /// Gets or initializes the aggregation granularity.
    [JsonPropertyName("granularity")]
    public required string Granularity { get; init; }

    /// Gets or initializes the event count or duration parameter.
    [JsonPropertyName("parameter")]
    public ulong? Parameter { get; init; }

    /// Gets or initializes whether windows should merge into a rolling aggregate.
    [JsonPropertyName("rolling")]
    public bool Rolling { get; init; }
}

/// Configures a PostgreSQL sink.
public sealed record SinkConfig
{
    /// Gets or initializes the sink driver name.
    [JsonPropertyName("driver")]
    public string Driver { get; init; } = "postgres";

    /// Gets or initializes the sink connection string.
    [JsonPropertyName("connection_string")]
    public required string ConnectionString { get; init; }
}

/// Represents one structured event sent through the batch ingestion endpoint.
public sealed record DistillEvent
{
    /// Gets or initializes the stable event identifier.
    [JsonPropertyName("event_id")]
    public Guid EventId { get; init; } = Guid.NewGuid();

    /// Gets or initializes the event timestamp.
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;

    /// Gets or initializes the destination channel.
    [JsonPropertyName("channel_id")]
    public required string ChannelId { get; init; }

    /// Gets or initializes the Distill severity name.
    [JsonPropertyName("severity_level")]
    public string SeverityLevel { get; init; } = "info";

    /// Gets or initializes the arbitrary application payload.
    [JsonPropertyName("payload")]
    public object? Payload { get; init; }

    /// Gets or initializes string-valued operational metadata.
    [JsonPropertyName("metadata")]
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}

/// Represents a single event written through the channel-bound endpoint.
public sealed record SimpleEvent
{
    /// Gets or initializes the optional stable event identifier.
    [JsonPropertyName("event_id")]
    public Guid? EventId { get; init; }

    /// Gets or initializes the optional event timestamp.
    [JsonPropertyName("timestamp")]
    public DateTimeOffset? Timestamp { get; init; }

    /// Gets or initializes the severity name.
    [JsonPropertyName("severity")]
    public string Severity { get; init; } = "info";

    /// Gets or initializes the human-readable message.
    [JsonPropertyName("message")]
    public string? Message { get; init; }

    /// Gets or initializes the arbitrary JSON payload.
    [JsonPropertyName("payload")]
    public object? Payload { get; init; }

    /// Gets or initializes arbitrary application attributes.
    [JsonPropertyName("attributes")]
    public IReadOnlyDictionary<string, object?>? Attributes { get; init; }

    /// Gets or initializes string-valued operational metadata.
    [JsonPropertyName("metadata")]
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}

/// Reports the result of an ingestion request.
public sealed record IngestResponse
{
    /// Gets the number of accepted events.
    [JsonPropertyName("accepted")]
    public int Accepted { get; init; }

    /// Gets the number of rejected events.
    [JsonPropertyName("rejected")]
    public int Rejected { get; init; }

    /// Gets the Distill request identifier.
    [JsonPropertyName("request_id")]
    public Guid RequestId { get; init; }

    /// Gets the delivery state reported by Distill.
    [JsonPropertyName("durability")]
    public string? Durability { get; init; }

    /// Gets item-level errors returned for a partial batch.
    [JsonPropertyName("errors")]
    public IReadOnlyList<IngestItemError> Errors { get; init; } = [];
}

/// Describes one rejected event in a batch.
public sealed record IngestItemError
{
    /// Gets the zero-based event index.
    [JsonPropertyName("index")]
    public int Index { get; init; }

    /// Gets the stable Distill error code.
    [JsonPropertyName("error_code")]
    public string ErrorCode { get; init; } = string.Empty;

    /// Gets the human-readable error message.
    [JsonPropertyName("message")]
    public string Message { get; init; } = string.Empty;
}

/// Describes a channel’s sanitized effective configuration.
public sealed record ChannelConfigResponse
{
    /// Gets the channel identifier.
    [JsonPropertyName("channel_id")]
    public string ChannelId { get; init; } = string.Empty;

    /// Gets whether the channel currently accepts events.
    [JsonPropertyName("active")]
    public bool Active { get; init; }

    /// Gets the lifecycle state.
    [JsonPropertyName("lifecycle")]
    public string Lifecycle { get; init; } = string.Empty;

    /// Gets the resolved public configuration.
    [JsonPropertyName("effective")]
    public EffectiveChannelConfig Effective { get; init; } = new();
}

/// Contains the non-secret effective channel settings.
public sealed record EffectiveChannelConfig
{
    /// Gets the channel identifier.
    [JsonPropertyName("channel_id")]
    public string ChannelId { get; init; } = string.Empty;

    /// Gets the named sink selected by the channel, when one is configured.
    [JsonPropertyName("sink_id")]
    public string? SinkId { get; init; }

    /// Gets the configured severity threshold.
    [JsonPropertyName("severity_threshold")]
    public string SeverityThreshold { get; init; } = string.Empty;

    /// Gets the generated or configured RabbitMQ exchange name.
    [JsonPropertyName("rabbitmq_exchange")]
    public string RabbitMqExchange { get; init; } = string.Empty;

    /// Gets the routing-key prefix.
    [JsonPropertyName("routing_key_prefix")]
    public string RoutingKeyPrefix { get; init; } = string.Empty;

    /// Gets the configured sink driver.
    [JsonPropertyName("sink_driver")]
    public string SinkDriver { get; init; } = string.Empty;

    /// Gets the effective aggregator, if one is configured.
    [JsonPropertyName("aggregator")]
    public AggregatorSpec? Aggregator { get; init; }

    /// Gets whether a runtime schema is configured.
    [JsonPropertyName("schema_configured")]
    public bool SchemaConfigured { get; init; }
}

/// Reports how a sample event would be handled.
public sealed record EventTestResponse
{
    /// Gets whether the event is valid.
    [JsonPropertyName("valid")]
    public bool Valid { get; init; }

    /// Gets the channel identifier.
    [JsonPropertyName("channel_id")]
    public string ChannelId { get; init; } = string.Empty;

    /// Gets the selected route.
    [JsonPropertyName("route")]
    public string Route { get; init; } = string.Empty;

    /// Gets the channel severity threshold.
    [JsonPropertyName("severity_threshold")]
    public string SeverityThreshold { get; init; } = string.Empty;

    /// Gets whether schema validation will run.
    [JsonPropertyName("schema_configured")]
    public bool SchemaConfigured { get; init; }
}

/// Reports the result of channel validation.
public sealed record ChannelValidationResponse
{
    /// Gets whether the channel is valid.
    [JsonPropertyName("valid")]
    public bool Valid { get; init; }

    /// Gets the channel identifier.
    [JsonPropertyName("channel_id")]
    public string ChannelId { get; init; } = string.Empty;

    /// Gets the selected preset.
    [JsonPropertyName("preset")]
    public string Preset { get; init; } = string.Empty;

    /// Gets validation warnings.
    [JsonPropertyName("warnings")]
    public IReadOnlyList<string> Warnings { get; init; } = [];

    /// Gets the resolved public configuration.
    [JsonPropertyName("effective")]
    public EffectiveChannelConfig Effective { get; init; } = new();
}

/// Represents an API error returned by Distill.
public sealed class DistillApiException : Exception
{
    /// Initializes an API exception with the HTTP status and Distill error payload.
    public DistillApiException(HttpStatusCode statusCode, string? errorCode, string responseBody)
        : base($"Distill returned {(int)statusCode} ({statusCode}): {errorCode ?? "UNKNOWN_ERROR"}")
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
        ResponseBody = responseBody;
    }

    /// Gets the HTTP status code.
    public HttpStatusCode StatusCode { get; }

    /// Gets the Distill error code, when present.
    public string? ErrorCode { get; }

    /// Gets the raw response body for diagnostics.
    public string ResponseBody { get; }
}
