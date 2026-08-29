using System.Collections.Concurrent;
using System.Threading.Channels;
using Distill.Client;
using Microsoft.Extensions.Logging;

namespace Distill.Extensions.Logging;

/// Provides Microsoft.Extensions.Logging integration for Distill.
public sealed class DistillLoggerProvider : ILoggerProvider, ISupportExternalScope, IAsyncDisposable
{
    private readonly Distill.Client.DistillClient client;
    private readonly DistillLoggerOptions options;
    private readonly ConcurrentDictionary<string, Channel<QueuedLog>> channelQueues = new();
    private readonly Channel<byte> wakeSignal;
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
        wakeSignal = Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
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
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// Stops the provider and flushes records already accepted into its queue.
    public async ValueTask DisposeAsync()
    {
        CompleteAllChannelWriters();
        wakeSignal.Writer.TryComplete();
        try
        {
            await worker;
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            shutdown.Cancel();
            shutdown.Dispose();
        }
    }

    internal bool IsEnabled(string category, LogLevel logLevel) =>
        logLevel != LogLevel.None && logLevel >= options.GetCategoryLevel(category);

    internal void Enqueue(
        string category,
        LogLevel logLevel,
        EventId eventId,
        string message,
        Exception? exception,
        IReadOnlyDictionary<string, object?> attributes,
        IReadOnlyDictionary<string, string> metadata)
    {
        string channel = ResolveChannel(category);
        var channelQueue = GetOrCreateChannelQueue(channel);
        var channelOpts = options.GetChannelOptions(channel);
        var overflow = channelOpts.OverflowPolicy ?? options.OverflowPolicy;

        var record = new QueuedLog(channel, category, logLevel, eventId, message, exception, attributes, metadata);

        bool admitted;
        if (overflow == DistillOverflowPolicy.Block)
        {
            channelQueue.Writer.WriteAsync(record, shutdown.Token).AsTask().GetAwaiter().GetResult();
            admitted = true;
        }
        else
        {
            admitted = channelQueue.Writer.TryWrite(record);
        }

        if (admitted)
        {
            wakeSignal.Writer.TryWrite(0);
        }
    }

    private string ResolveChannel(string category)
    {
        try
        {
            string? channel = options.ChannelResolver?.Invoke(category);
            if (!string.IsNullOrWhiteSpace(channel))
            {
                return channel;
            }

            if (!string.IsNullOrWhiteSpace(options.DefaultChannel))
            {
                options.ChannelResolutionFailed?.Invoke(category, "Resolver returned null or whitespace");
                return options.DefaultChannel;
            }
        }
        catch (Exception ex)
        {
            options.ChannelResolutionFailed?.Invoke(category, $"Resolver threw: {ex.Message}");
        }

        return options.DefaultChannel;
    }

    private Channel<QueuedLog> GetOrCreateChannelQueue(string channelId)
    {
        return channelQueues.GetOrAdd(channelId, static (key, self) =>
        {
            var channelOpts = self.options.GetChannelOptions(key);
            int capacity = channelOpts.QueueCapacity ?? self.options.QueueCapacity;
            var fullMode = (channelOpts.OverflowPolicy ?? self.options.OverflowPolicy) switch
            {
                DistillOverflowPolicy.DropNewest => BoundedChannelFullMode.DropWrite,
                DistillOverflowPolicy.DropOldest => BoundedChannelFullMode.DropOldest,
                DistillOverflowPolicy.Block => BoundedChannelFullMode.Wait,
                _ => BoundedChannelFullMode.DropWrite,
            };
            return Channel.CreateBounded<QueuedLog>(new BoundedChannelOptions(capacity)
            {
                FullMode = fullMode,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });
        }, this);
    }

    private void CompleteAllChannelWriters()
    {
        foreach (var kvp in channelQueues)
        {
            kvp.Value.Writer.TryComplete();
        }
    }

    private async Task ProcessQueueAsync()
    {
        try
        {
            var signalReader = wakeSignal.Reader;
            while (await signalReader.WaitToReadAsync(shutdown.Token))
            {
                while (signalReader.TryRead(out _))
                {
                }

                foreach (var (channelId, channel) in channelQueues.ToArray())
                {
                    var channelOpts = options.GetChannelOptions(channelId);
                    int batchSize = channelOpts.BatchSize ?? options.BatchSize;

                    var batch = new List<QueuedLog>(batchSize);
                    while (batch.Count < batchSize && channel.Reader.TryRead(out var record))
                    {
                        batch.Add(record);
                    }

                    if (batch.Count == 0)
                    {
                        continue;
                    }

                    if (batch.Count < batchSize)
                    {
                        var flushInterval = channelOpts.FlushInterval ?? options.FlushInterval;
                        await Task.Delay(flushInterval, shutdown.Token);
                        while (batch.Count < batchSize && channel.Reader.TryRead(out var record))
                        {
                            batch.Add(record);
                        }
                    }

                    await FlushAsync(batch, shutdown.Token);
                }
            }
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
        }
        finally
        {
            var remaining = new List<QueuedLog>();
            foreach (var kvp in channelQueues)
            {
                while (kvp.Value.Reader.TryRead(out var record))
                {
                    remaining.Add(record);
                }
            }

            if (remaining.Count > 0)
            {
                try
                {
                    await FlushAsync(remaining, CancellationToken.None);
                }
                catch
                {
                }
            }
        }
    }

    private async Task FlushAsync(IReadOnlyList<QueuedLog> batch, CancellationToken ct)
    {
        if (batch.Count == 0)
        {
            return;
        }

        string channel = batch[0].Channel;
        var channelOpts = options.GetChannelOptions(channel);
        int maxRetries = channelOpts.MaxRetries ?? options.MaxRetries;
        TimeSpan retryDelay = channelOpts.RetryDelay ?? options.RetryDelay;

        var events = batch.Select(ToDistillEvent).ToArray();

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                IngestResponse? response = await client.IngestAsync(events, ct);
                options.DeliverySuccess?.Invoke(new DeliverySuccessContext
                {
                    Channel = channel,
                    EventCount = batch.Count,
                    Response = response!
                });
                break;
            }
            catch (Exception) when (attempt < maxRetries && !ct.IsCancellationRequested)
            {
                await Task.Delay(retryDelay, ct);
            }
            catch (Exception ex)
            {
                options.DeliveryFailure?.Invoke(new DeliveryFailureContext
                {
                    Channel = channel,
                    EventCount = batch.Count,
                    Attempts = attempt + 1,
                    Exception = ex
                });
                break;
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

    private static DistillSeverity ToSeverity(LogLevel level) => level switch
    {
        LogLevel.Trace => DistillSeverity.Trace,
        LogLevel.Debug => DistillSeverity.Debug,
        LogLevel.Information => DistillSeverity.Info,
        LogLevel.Warning => DistillSeverity.Warning,
        LogLevel.Error => DistillSeverity.Error,
        LogLevel.Critical => DistillSeverity.Critical,
        _ => DistillSeverity.Unknown
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
