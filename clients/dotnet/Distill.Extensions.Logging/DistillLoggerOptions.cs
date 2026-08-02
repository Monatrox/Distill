using Microsoft.Extensions.Logging;

namespace Distill.Extensions.Logging;

/// Controls how the Distill logging provider maps and delivers application logs.
public sealed class DistillLoggerOptions
{
    /// Gets or sets the channel used when no resolver is configured.
    public string DefaultChannel { get; set; } = "application";

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

    /// Gets or sets a function that maps an `ILogger` category to a Distill channel.
    public Func<string, string>? ChannelResolver { get; set; }

    /// Gets or sets a callback invoked after a batch exhausts its retry attempts.
    public Action<Exception>? DeliveryFailure { get; set; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(DefaultChannel);
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
    }
}

/// Determines what happens when the provider’s bounded queue is full.
public enum DistillOverflowPolicy
{
    /// Discards the incoming event and keeps events already buffered.
    DropNewest,

    /// Discards the oldest buffered event to make room for the incoming event.
    DropOldest,

    /// Blocks the calling logging thread until the event can be queued.
    Block
}
