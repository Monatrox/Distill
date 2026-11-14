using Distill.Client;
using Microsoft.Extensions.Logging;

namespace Distill.Extensions.Logging;

/// Controls how the Distill logging provider maps and delivers application logs.
public sealed class DistillLoggerOptions
{
    /// Gets or sets the channel used when no resolver is configured.
    public string DefaultChannel { get; set; } = "application";

    /// Gets or sets a declared version; reconciliation bindings are used when omitted.
    public string? DefaultVersion { get; set; }

    /// Gets or sets the maximum time allowed to finish shutdown delivery.
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// Gets or sets an observable notification for SDK queue/overflow drops.
    public Action<string, long>? DeliveryDropped { get; set; }

    /// Gets or sets the minimum level forwarded to Distill.
    public LogLevel MinimumLevel { get; set; } = LogLevel.Information;

    /// Gets or sets the maximum number of records buffered before the overflow policy applies.
    public int QueueCapacity { get; set; } = 8192;

    /// Gets or sets the maximum number of events sent in one batch.
    public int BatchSize { get; set; } = 100;

    /// Gets or sets how long the provider waits to complete a partial batch.
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// Gets or sets how many times a failed batch is retried after its initial attempt.
    public int MaxRetries { get; set; } = 3;

    /// Gets or sets the delay between failed batch attempts.
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// Gets or sets the behavior when the bounded queue is full.
    public DistillOverflowPolicy OverflowPolicy { get; set; } = DistillOverflowPolicy.DropNewest;

    /// Gets or sets a function that maps an ILogger category to a Distill channel.
    public Func<string, string>? ChannelResolver { get; set; }

    /// Gets or sets per-channel overrides for batch size, retry, and queue behavior.
    public Dictionary<string, PerChannelOptions> ChannelOptions { get; set; } = new(StringComparer.Ordinal);

    /// Gets or sets category-specific minimum log levels.
    public Dictionary<string, LogLevel> CategoryLevels { get; set; } = new(StringComparer.Ordinal);

    /// Gets or sets a callback invoked after a batch exhausts its retry attempts.
    public Action<DeliveryFailureContext>? DeliveryFailure { get; set; }

    /// Gets or sets a callback invoked after a batch is successfully delivered.
    public Action<DeliverySuccessContext>? DeliverySuccess { get; set; }

    /// Gets or sets a callback invoked when the channel resolver returns null or whitespace.
    public Action<string, string>? ChannelResolutionFailed { get; set; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(DefaultChannel);
        if (ShutdownTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(ShutdownTimeout));
        if (QueueCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(QueueCapacity));
        }

        if (BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(BatchSize));
        }

        if (FlushInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(FlushInterval));
        }

        if (MaxRetries < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxRetries));
        }

        if (RetryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(RetryDelay));
        }
        if (ChannelOptions is not null)
        {
            foreach (var (channel, value) in ChannelOptions)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(channel);
                ArgumentNullException.ThrowIfNull(value);
                if (value.BatchSize is <= 0 || value.QueueCapacity is <= 0 || value.MaxRetries is < 0
                    || value.FlushInterval is { } flush && flush <= TimeSpan.Zero
                    || value.RetryDelay is { } delay && delay <= TimeSpan.Zero)
                    throw new ArgumentException("Per-channel batching, queue and retry settings must be valid.", nameof(ChannelOptions));
            }
        }

    }

    internal PerChannelOptions GetChannelOptions(string channelId)
    {
        if (ChannelOptions is not null && ChannelOptions.TryGetValue(channelId, out var opts))
        {
            return opts;
        }

        return DefaultPerChannelOptions;
    }

    internal LogLevel GetCategoryLevel(string category)
    {
        if (CategoryLevels is not null)
        {
            if (CategoryLevels.TryGetValue(category, out var level))
            {
                return level;
            }

            int lastDot = category.Length;
            while ((lastDot = category.LastIndexOf('.', lastDot - 1)) > 0)
            {
                if (CategoryLevels.TryGetValue(category[..lastDot], out level))
                {
                    return level;
                }
            }
        }

        return MinimumLevel;
    }

    private static readonly PerChannelOptions DefaultPerChannelOptions = new();
}

/// Per-channel overrides for the Distill logging provider.
public sealed class PerChannelOptions
{
    public string? Version { get; set; }
    public int? BatchSize { get; set; }
    public TimeSpan? FlushInterval { get; set; }
    public int? MaxRetries { get; set; }
    public TimeSpan? RetryDelay { get; set; }
    public int? QueueCapacity { get; set; }
    public DistillOverflowPolicy? OverflowPolicy { get; set; }
    public LogLevel? MinimumLevel { get; set; }
}

/// Context provided to the delivery-failure callback.
public sealed class DeliveryFailureContext
{
    public required string Channel { get; init; }
    public required int EventCount { get; init; }
    public required int Attempts { get; init; }
    public required Exception Exception { get; init; }
}

/// Context provided to the delivery-success callback.
public sealed class DeliverySuccessContext
{
    public required string Channel { get; init; }
    public required int EventCount { get; init; }
    public required IngestResponse Response { get; init; }
}

/// Determines what happens when the provider's bounded queue is full.
public enum DistillOverflowPolicy
{
    /// Discards the incoming event and keeps events already buffered.
    DropNewest,

    /// Discards the oldest buffered event to make room for the incoming event.
    DropOldest,

    /// Blocks the calling logging thread until the event can be queued.
    Block
}
