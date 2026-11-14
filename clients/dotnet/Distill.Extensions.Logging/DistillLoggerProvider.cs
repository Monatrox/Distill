using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using System.Threading.Channels;
using Distill.Client;
using Microsoft.Extensions.Logging;

namespace Distill.Extensions.Logging;

/// Provides bounded, observable delivery through Microsoft.Extensions.Logging.
public sealed class DistillLoggerProvider : ILoggerProvider, ISupportExternalScope, IAsyncDisposable
{
    private readonly Lazy<DistillClient> client;
    private readonly DistillLoggerOptions options;
    private readonly ConcurrentDictionary<string, Channel<DistillEvent>> channelQueues = new();
    private readonly ConcurrentDictionary<string, DistillLogger> loggers = new();
    private readonly Channel<byte> wakeSignal = Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
    {
        FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true, SingleWriter = false
    });
    private readonly CancellationTokenSource shutdown = new();
    private readonly CancellationTokenSource stopBatching = new();
    private readonly object lifecycleGate = new();
    private readonly Task worker;
    private IExternalScopeProvider scopeProvider = new LoggerExternalScopeProvider();
    private int disposed;
    private long dropped;
    private long failed;
    private int inFlight;

    public DistillLoggerProvider(DistillClient client, DistillLoggerOptions? options = null)
        : this(client is null ? throw new ArgumentNullException(nameof(client)) : () => client, options)
    {
    }

    /// Defers HTTP transport resolution until delivery, after the host logger factory exists.
    public DistillLoggerProvider(Func<DistillClient> clientFactory, DistillLoggerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(clientFactory);
        client = new Lazy<DistillClient>(clientFactory);
        this.options = options ?? new DistillLoggerOptions();
        this.options.Validate();
        worker = Task.Run(ProcessQueueAsync);
    }

    public long DroppedCount => Interlocked.Read(ref dropped);
    public long DeliveryFailureCount => Interlocked.Read(ref failed);
    public ILogger CreateLogger(string categoryName) => loggers.GetOrAdd(categoryName, category => new DistillLogger(this, category));
    public void SetScopeProvider(IExternalScopeProvider provider) => scopeProvider = provider ?? throw new ArgumentNullException(nameof(provider));
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        lock (lifecycleGate)
        {
            if (disposed != 0) return;
            Volatile.Write(ref disposed, 1);
            foreach (var queue in channelQueues.Values) queue.Writer.TryComplete();
            wakeSignal.Writer.TryComplete();
        }
        stopBatching.Cancel();
        try { await worker.WaitAsync(options.ShutdownTimeout).ConfigureAwait(false); }
        catch (TimeoutException)
        {
            shutdown.Cancel();
            int pending = Volatile.Read(ref inFlight);
            if (pending > 0) Drop("shutdown.in_flight", pending);
            // A transport that ignores cancellation cannot hold application shutdown forever.
            _ = worker.ContinueWith(_ => { shutdown.Dispose(); stopBatching.Dispose(); }, TaskScheduler.Default);
            return;
        }
        finally
        {
            if (worker.IsCompleted) { shutdown.Dispose(); stopBatching.Dispose(); }
        }
    }

    internal bool IsEnabled(string category, LogLevel level) =>
        Volatile.Read(ref disposed) == 0 && level != LogLevel.None && level >= options.GetCategoryLevel(category);

    internal void Enqueue(string category, LogLevel level, EventId eventId, string message, Exception? exception,
        IReadOnlyDictionary<string, object?> attributes, IReadOnlyDictionary<string, string> metadata)
    {
        string channel = ResolveChannel(category);
        Channel<DistillEvent> queue;
        lock (lifecycleGate)
        {
            if (disposed != 0) { Drop(channel, 1); return; }
            queue = channelQueues.GetOrAdd(channel, CreateQueue);
        }
        var channelOptions = options.GetChannelOptions(channel);
        var payload = new Dictionary<string, object?>(attributes, StringComparer.Ordinal) { ["message"] = message };
        if (exception is not null)
        {
            payload["exception_type"] = exception.GetType().FullName;
            payload["exception_message"] = exception.Message;
            payload["exception_stacktrace"] = exception.ToString();
        }
        var tags = new Dictionary<string, string>(metadata, StringComparer.Ordinal)
        {
            ["category"] = category, ["log_level"] = level.ToString()
        };
        if (eventId.Id != 0) tags["event_id"] = eventId.Id.ToString();
        if (!string.IsNullOrWhiteSpace(eventId.Name)) tags["event_name"] = eventId.Name;
        var timestamp = DateTimeOffset.UtcNow;
        DistillEvent record;
        try
        {
            record = new DistillEvent
            {
                EventId = Guid.NewGuid(), Timestamp = timestamp, ChannelId = channel,
                Version = channelOptions.Version ?? options.DefaultVersion ?? client.Value.GetChannelVersion(channel),
                SeverityLevel = ToSeverity(level), Payload = JsonSerializer.SerializeToElement(payload), Metadata = tags
            };
        }
        catch (Exception error) when (error is JsonException or NotSupportedException)
        {
            ReportFailure(channel, 1, 0, error);
            return;
        }
        try
        {
            bool admitted;
            if ((channelOptions.OverflowPolicy ?? options.OverflowPolicy) == DistillOverflowPolicy.Block)
            {
                queue.Writer.WriteAsync(record, shutdown.Token).AsTask().GetAwaiter().GetResult();
                admitted = true;
            }
            else admitted = queue.Writer.TryWrite(record);
            if (admitted) wakeSignal.Writer.TryWrite(0);
            else Drop(channel, 1);
        }
        catch (Exception error) when (error is OperationCanceledException or ChannelClosedException or ObjectDisposedException)
        {
            Drop(channel, 1);
        }
    }

    private Channel<DistillEvent> CreateQueue(string channel)
    {
        var config = options.GetChannelOptions(channel);
        var mode = (config.OverflowPolicy ?? options.OverflowPolicy) switch
        {
            DistillOverflowPolicy.DropOldest => BoundedChannelFullMode.DropOldest,
            DistillOverflowPolicy.Block => BoundedChannelFullMode.Wait,
            _ => BoundedChannelFullMode.DropWrite
        };
        return Channel.CreateBounded<DistillEvent>(new BoundedChannelOptions(config.QueueCapacity ?? options.QueueCapacity)
        {
            FullMode = mode, SingleReader = true, SingleWriter = false
        }, _ => Drop(channel, 1));
    }

    private string ResolveChannel(string category)
    {
        try { return options.ChannelResolver?.Invoke(category) is { Length: > 0 } channel ? channel : options.DefaultChannel; }
        catch
        {
            try { options.ChannelResolutionFailed?.Invoke(category, "Channel resolver failed"); } catch { }
            return options.DefaultChannel;
        }
    }

    private async Task ProcessQueueAsync()
    {
        try
        {
            while (await wakeSignal.Reader.WaitToReadAsync(shutdown.Token))
            {
                while (wakeSignal.Reader.TryRead(out _)) { }
                bool processed;
                do
                {
                    processed = false;
                    foreach (var (channel, queue) in channelQueues.ToArray())
                    {
                        var config = options.GetChannelOptions(channel);
                        int size = config.BatchSize ?? options.BatchSize;
                        var batch = new List<DistillEvent>(size);
                        while (batch.Count < size && queue.Reader.TryRead(out var record)) batch.Add(record);
                        if (batch.Count == 0) continue;
                        processed = true;
                        Volatile.Write(ref inFlight, batch.Count);
                        if (batch.Count < size && Volatile.Read(ref disposed) == 0)
                        {
                            try { await Task.Delay(config.FlushInterval ?? options.FlushInterval, stopBatching.Token); }
                            catch (OperationCanceledException) when (stopBatching.IsCancellationRequested) { }
                            while (batch.Count < size && queue.Reader.TryRead(out var record)) batch.Add(record);
                        }
                        Volatile.Write(ref inFlight, batch.Count);
                        await FlushAsync(channel, batch.ToArray(), shutdown.Token);
                        Volatile.Write(ref inFlight, 0);
                    }
                } while (processed);
            }
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
        catch (Exception error) { ReportFailure("worker", Volatile.Read(ref inFlight), 0, error); }
        finally
        {
            foreach (var (channel, queue) in channelQueues)
            {
                int count = 0;
                while (queue.Reader.TryRead(out _)) count++;
                if (count > 0) Drop(channel, count);
            }
        }
    }

    private async Task FlushAsync(string channel, DistillEvent[] pending, CancellationToken cancellationToken)
    {
        var config = options.GetChannelOptions(channel);
        int retries = config.MaxRetries ?? options.MaxRetries;
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var response = await client.Value.IngestAsync(pending, cancellationToken)
                    ?? throw new InvalidOperationException("Distill returned an empty ingest response.");
                var errors = response.Errors;
                if (response.Accepted + response.Rejected != pending.Length || response.Rejected != errors.Count
                    || errors.Any(error => error.Index < 0 || error.Index >= pending.Length)
                    || errors.Select(error => error.Index).Distinct().Count() != errors.Count)
                    throw new InvalidOperationException("Distill returned inconsistent per-item ingestion results.");
                if (response.Accepted > 0)
                {
                    try { options.DeliverySuccess?.Invoke(new DeliverySuccessContext { Channel = channel, EventCount = response.Accepted, Response = response }); } catch { }
                }
                var retry = errors.Where(error => IsRetryable(error.ErrorCode)).Select(error => pending[error.Index]).ToArray();
                int terminal = response.Rejected - retry.Length;
                if (terminal > 0) ReportFailure(channel, terminal, attempt + 1, new InvalidOperationException("Distill permanently rejected records in the batch."));
                if (retry.Length == 0) return;
                pending = retry;
                if (attempt >= retries)
                {
                    ReportFailure(channel, pending.Length, attempt + 1, new InvalidOperationException("Distill admission retries exhausted."));
                    return;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                if (attempt >= retries || !IsRetryable(error))
                {
                    ReportFailure(channel, pending.Length, attempt + 1, error);
                    return;
                }
            }
            Volatile.Write(ref inFlight, pending.Length);
            await Task.Delay(config.RetryDelay ?? options.RetryDelay, cancellationToken);
        }
    }

    private static bool IsRetryable(string code) => code is "BUFFER_FULL" or "SERVICE_UNAVAILABLE" or "REQUEST_THROTTLED";
    private static bool IsRetryable(Exception error) => error switch
    {
        DistillApiException api => (int)api.StatusCode >= 500 || api.StatusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests,
        HttpRequestException or TaskCanceledException => true,
        _ => false
    };

    private void Drop(string channel, long count)
    {
        Interlocked.Add(ref dropped, count);
        try { options.DeliveryDropped?.Invoke(channel, count); } catch { }
    }

    private void ReportFailure(string channel, int count, int attempts, Exception error)
    {
        Interlocked.Add(ref failed, count);
        try { options.DeliveryFailure?.Invoke(new DeliveryFailureContext { Channel = channel, EventCount = count, Attempts = attempts, Exception = error }); } catch { }
    }

    private static DistillSeverity ToSeverity(LogLevel level) => level switch
    {
        LogLevel.Trace => DistillSeverity.Trace, LogLevel.Debug => DistillSeverity.Debug,
        LogLevel.Information => DistillSeverity.Info, LogLevel.Warning => DistillSeverity.Warning,
        LogLevel.Error => DistillSeverity.Error, LogLevel.Critical => DistillSeverity.Critical,
        _ => DistillSeverity.Unknown
    };

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
