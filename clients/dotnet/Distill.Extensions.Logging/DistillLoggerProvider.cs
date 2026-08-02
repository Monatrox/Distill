using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Distill.Extensions.Logging;

/// Provides Microsoft.Extensions.Logging integration for Distill.
public sealed class DistillLoggerProvider : ILoggerProvider, ISupportExternalScope
{
    private readonly Distill.Client.DistillClient client;
    private readonly DistillLoggerOptions options;
    private readonly Channel<QueuedLog> queue;
    private readonly CancellationTokenSource shutdown = new();
    private readonly Task worker;
    private readonly ConcurrentDictionary<string, DistillLogger> loggers = new();
    private IExternalScopeProvider scopeProvider = new LoggerExternalScopeProvider();

    /// Initializes a provider that batches log records through the supplied Distill client.
    public DistillLoggerProvider(
        Distill.Client.DistillClient client,
        DistillLoggerOptions? options = null)
    {
        this.client = client ?? throw new ArgumentNullException(nameof(client));
        this.options = options ?? new DistillLoggerOptions();
        this.options.Validate();
        queue = Channel.CreateBounded<QueuedLog>(new BoundedChannelOptions(this.options.QueueCapacity)
        {
            FullMode = this.options.OverflowPolicy switch
            {
                DistillOverflowPolicy.DropNewest => BoundedChannelFullMode.DropWrite,
                DistillOverflowPolicy.DropOldest => BoundedChannelFullMode.DropOldest,
                DistillOverflowPolicy.Block => BoundedChannelFullMode.Wait,
                _ => throw new ArgumentOutOfRangeException()
            },
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        worker = Task.Run(ProcessQueueAsync);
    }

    /// Creates a logger for the specified category.
    public ILogger CreateLogger(string categoryName) =>
        loggers.GetOrAdd(categoryName, category => new DistillLogger(this, category));

    /// Sets the scope provider used to capture structured logging scopes.
    public void SetScopeProvider(IExternalScopeProvider scopeProvider) =>
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));

    /// Stops the provider and flushes records already accepted into its queue.
    public void Dispose()
    {
        queue.Writer.TryComplete();
        try
        {
            worker.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
            // Shutdown cancellation is expected after the final flush attempt.
        }
        finally
        {
            shutdown.Cancel();
            shutdown.Dispose();
        }
    }

    internal bool IsEnabled(string category, LogLevel logLevel) =>
        logLevel != LogLevel.None && logLevel >= options.MinimumLevel;

    internal void Enqueue(
        string category,
        LogLevel logLevel,
        EventId eventId,
        string message,
        Exception? exception,
        IReadOnlyDictionary<string, object?> attributes,
        IReadOnlyDictionary<string, string> metadata)
    {
        string channel = options.ChannelResolver?.Invoke(category) ?? options.DefaultChannel;
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        var record = new QueuedLog(channel, category, logLevel, eventId, message, exception, attributes, metadata);

        if (options.OverflowPolicy == DistillOverflowPolicy.Block)
        {
            queue.Writer.WriteAsync(record, shutdown.Token).AsTask().GetAwaiter().GetResult();
            return;
        }

        queue.Writer.TryWrite(record);
    }

    private async Task ProcessQueueAsync()
    {
        try
        {
            while (await queue.Reader.WaitToReadAsync(shutdown.Token))
            {
                var batch = new List<QueuedLog>(options.BatchSize);
                while (batch.Count < options.BatchSize && queue.Reader.TryRead(out QueuedLog? record))
                {
                    batch.Add(record);
                }

                if (batch.Count < options.BatchSize)
                {
                    await Task.Delay(options.FlushInterval, shutdown.Token);
                    while (batch.Count < options.BatchSize && queue.Reader.TryRead(out QueuedLog? record))
                    {
                        batch.Add(record);
                    }
                }

                await FlushAsync(batch, shutdown.Token);
            }
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            // Dispose requests cancellation, then the finally block drains accepted records.
        }
        finally
        {
            var remaining = new List<QueuedLog>();
            while (queue.Reader.TryRead(out QueuedLog? record))
            {
                remaining.Add(record);
            }

            if (remaining.Count > 0)
            {
                try
                {
                    await FlushAsync(remaining, CancellationToken.None);
                }
                catch (Exception)
                {
                    // ILogger has no error channel. The application’s explicit DistillClient
                    // API should be used when delivery failure must be actionable.
                }
            }
        }
    }

    private async Task FlushAsync(IReadOnlyList<QueuedLog> records, CancellationToken cancellationToken)
    {
        foreach (IGrouping<string, QueuedLog> group in records.GroupBy(record => record.Channel))
        {
            var events = group.Select(ToDistillEvent).ToArray();
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await client.IngestAsync(events, cancellationToken);
                    break;
                }
                catch (Exception) when (
                    attempt < options.MaxRetries && !cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(options.RetryDelay, cancellationToken);
                }
                catch (Exception exception)
                {
                    options.DeliveryFailure?.Invoke(exception);
                    break;
                }
            }
        }
    }

    private static Distill.Client.DistillEvent ToDistillEvent(QueuedLog record)
    {
        var payload = new Dictionary<string, object?>(record.Attributes, StringComparer.Ordinal)
        {
            ["message"] = record.Message
        };
        if (record.Exception is not null)
        {
            payload["exception_type"] = record.Exception.GetType().FullName;
            payload["exception_message"] = record.Exception.Message;
            payload["exception_stacktrace"] = record.Exception.ToString();
        }

        var metadata = new Dictionary<string, string>(record.Metadata, StringComparer.Ordinal)
        {
            ["category"] = record.Category,
            ["log_level"] = record.LogLevel.ToString()
        };
        if (record.EventId.Id != 0)
        {
            metadata["event_id"] = record.EventId.Id.ToString();
        }

        if (!string.IsNullOrWhiteSpace(record.EventId.Name))
        {
            metadata["event_name"] = record.EventId.Name;
        }

        return new Distill.Client.DistillEvent
        {
            EventId = Guid.NewGuid(),
            Timestamp = DateTimeOffset.UtcNow,
            ChannelId = record.Channel,
            SeverityLevel = ToSeverity(record.LogLevel),
            Payload = payload,
            Metadata = metadata
        };
    }

    private static string ToSeverity(LogLevel level) => level switch
    {
        LogLevel.Trace => "trace",
        LogLevel.Debug => "debug",
        LogLevel.Information => "info",
        LogLevel.Warning => "warning",
        LogLevel.Error => "error",
        LogLevel.Critical => "critical",
        _ => "unknown"
    };

    private sealed record QueuedLog(
        string Channel,
        string Category,
        LogLevel LogLevel,
        EventId EventId,
        string Message,
        Exception? Exception,
        IReadOnlyDictionary<string, object?> Attributes,
        IReadOnlyDictionary<string, string> Metadata);

    private sealed class DistillLogger : ILogger
    {
        private readonly DistillLoggerProvider provider;
        private readonly string category;

        public DistillLogger(DistillLoggerProvider provider, string category)
        {
            this.provider = provider;
            this.category = category;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull =>
            provider.scopeProvider.Push(state);

        public bool IsEnabled(LogLevel logLevel) => provider.IsEnabled(category, logLevel);

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            ArgumentNullException.ThrowIfNull(formatter);
            var attributes = new Dictionary<string, object?>(StringComparer.Ordinal);
            if (state is IEnumerable<KeyValuePair<string, object?>> structuredState)
            {
                foreach ((string key, object? value) in structuredState)
                {
                    if (!string.Equals(key, "{OriginalFormat}", StringComparison.Ordinal))
                    {
                        attributes[key] = value;
                    }
                }
            }

            provider.scopeProvider.ForEachScope(
                static (scope, destination) => AddScope(scope, destination),
                attributes);
            provider.Enqueue(
                category,
                logLevel,
                eventId,
                formatter(state, exception),
                exception,
                attributes,
                new Dictionary<string, string>(StringComparer.Ordinal));
        }

        private static void AddScope(object? scope, Dictionary<string, object?> destination)
        {
            if (scope is IEnumerable<KeyValuePair<string, object?>> structuredScope)
            {
                foreach ((string key, object? value) in structuredScope)
                {
                    destination.TryAdd(key, value);
                }
            }
            else if (scope is not null)
            {
                destination.TryAdd("scope", scope);
            }
        }
    }
}
