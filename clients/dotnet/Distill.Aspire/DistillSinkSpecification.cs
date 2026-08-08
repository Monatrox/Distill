namespace Distill.Aspire;

/// Describes a named Distill sink for any supported driver (postgres, http, etc.).
///
/// Use the <c>WithSink</c> extension methods on <c>IResourceBuilder&lt;DistillResource&gt;</c>
/// to register sinks with the Distill resource at configuration time.
public sealed record DistillSinkSpecification
{
    /// Gets or initializes the sink name referenced by channels.
    public required string Name { get; init; }

    /// Gets or initializes the sink driver type (e.g. <c>"postgres"</c>, <c>"http"</c>).
    public required string Driver { get; init; }

    /// Gets or initializes the connection string for database drivers.
    public string ConnectionString { get; init; } = string.Empty;

    /// Gets or initializes the endpoint URL for HTTP drivers.
    public Uri? Url { get; init; }

    /// Gets or initializes the bearer token for HTTP drivers.
    public string? BearerToken { get; init; }

    /// Gets or initializes whether this sink is the default for channels.
    public bool IsDefault { get; init; }
}
