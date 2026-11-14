using System.Net;
using System.Text.Json;
using Distill.Client;
using Distill.Extensions.Logging;
using Microsoft.Extensions.Logging;
using System.Net.Http.Json;
using System.Collections.Concurrent;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Distill.Aspire;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;

var handler = new CaptureHandler();
var client = new DistillClient(new HttpClient(handler)
{
    BaseAddress = new Uri("https://distill.test/")
}, "ingest-key");

await client.CreateChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = ChannelPreset.Metrics,
    MessageSchema = new MessageSchemaDefinition { MessageType = "orders.Order", Version = "1", DescriptorSetBase64 = "AA==" }
});

using (var request = JsonDocument.Parse(handler.Body))
{
    Assert(request.RootElement.GetProperty("preset").GetString() == "metrics");
    Assert(!request.RootElement.TryGetProperty("aggregator", out _));
    Assert(request.RootElement.GetProperty("message_schema").GetProperty("message_type").GetString() == "orders.Order");
}
Assert(handler.ApiVersion == "1");

await client.CreateChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = ChannelPreset.Metrics,
    DisableAggregation = true
});

using (var request = JsonDocument.Parse(handler.Body))
{
    Assert(request.RootElement.GetProperty("aggregator").ValueKind == JsonValueKind.Null);
}

await VerifyAspireAsync();
await VerifyHostLoggingDependenciesAsync();
await VerifyVersionBindingAsync();
await VerifyLoggingAsync();
Console.WriteLine("Serialization, API version, burst draining, timestamps, partial retry, overflow, and bounded shutdown verified.");
return;

static async Task VerifyHostLoggingDependenciesAsync()
{
    var builder = Host.CreateApplicationBuilder();
    builder.Logging.ClearProviders();
    builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Distill:Endpoint"] = "https://unused.test/",
        ["Distill:ApiKey"] = "di-contract-key"
    });
    int delivered = 0;
    builder.Services.ConfigureHttpClientDefaults(http => http.ConfigurePrimaryHttpMessageHandler(services =>
    {
        // Real ServiceDefaults/resilience handlers also depend on ILoggerFactory.
        _ = services.GetRequiredService<ILoggerFactory>();
        return new DelegateHandler(async (request, token) =>
        {
            var body = await request.Content!.ReadFromJsonAsync<JsonElement>(token);
            int count = body.ValueKind == JsonValueKind.Array ? body.GetArrayLength() : 1;
            Interlocked.Add(ref delivered, count);
            return Results(count, 0);
        });
    }));
    builder.AddDistillClient(configureOptions: options => options.FlushInterval = TimeSpan.FromHours(1));
    builder.Logging.AddFilter<DistillLoggerProvider>("Microsoft", LogLevel.None);
    using var host = builder.Build();
    await host.StartAsync();
    var client = host.Services.GetRequiredService<DistillClient>();
    var response = await client.WriteAsync("di-contract", new SimpleEvent { Message = "probe" });
    Assert(response?.Accepted == 1);
    var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Shutdown.Contract");
    for (int index = 0; index < 20; index++) logger.LogInformation("Pending {value}", index);
    await host.StopAsync();
    if (delivered != 21) throw new InvalidOperationException($"Expected 21 deliveries before host disposal, got {delivered}.");
    Assert(host.Services.GetRequiredService<DistillLoggerProvider>().DeliveryFailureCount == 0);
}

static async Task VerifyAspireAsync()
{
    var builder = DistributedApplication.CreateBuilder(new DistributedApplicationOptions { Args = [], DisableDashboard = true });
    var distill = builder.AddDistill("distill", "shared-contract-key");
    var consumer = builder.AddExecutable("consumer", "dotnet", ".").WithReference(distill);
    var values = new Dictionary<string, object>();
    var context = new EnvironmentCallbackContext(builder.ExecutionContext, consumer.Resource, values, CancellationToken.None);
    foreach (var annotation in consumer.Resource.Annotations.OfType<EnvironmentCallbackAnnotation>()) await annotation.Callback(context);
    Assert((string)values["Distill__ApiKey"] == "shared-contract-key");
    Assert((string)values["Distill__AdminApiKey"] == "shared-contract-key");
    Assert(values["Distill__AdminEndpoint"] is EndpointReference);
    Assert(values.ContainsKey("ConnectionStrings__distill"));
    Assert(builder.Resources.OfType<RabbitMQServerResource>().Count() == 1);
    var external = builder.AddDistill("existing", new Uri("https://distill.test"), "external-key", new Uri("https://admin.test"));
    Assert((IResource)external.Resource is not ContainerResource);
    var externalConsumer = builder.AddExecutable("external-consumer", "dotnet", ".").WithReference(external);
    var externalValues = new Dictionary<string, object>();
    var externalContext = new EnvironmentCallbackContext(builder.ExecutionContext, externalConsumer.Resource, externalValues, CancellationToken.None);
    foreach (var annotation in externalConsumer.Resource.Annotations.OfType<EnvironmentCallbackAnnotation>()) await annotation.Callback(externalContext);
    Assert((string)externalValues["Distill__ApiKey"] == "external-key");
    Assert((string)externalValues["Distill__AdminEndpoint"] == "https://admin.test/");
    using var app = builder.Build();
}

static async Task VerifyVersionBindingAsync()
{
    var adminCalls = new List<string>();
    var handler = new DelegateHandler(async (request, token) =>
    {
        var path = request.RequestUri!.AbsolutePath;
        if (path.StartsWith("/channels/versioned/"))
        {
            Assert(request.RequestUri.Query == "?version=release-2");
            adminCalls.Add(path);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(new ChannelConfigResponse { ChannelId = "versioned", Version = "release-2" }) };
        }
        var body = await request.Content!.ReadFromJsonAsync<JsonElement>(token);
        if (path == "/channels")
        {
            Assert(body.GetProperty("version").GetString() == "release-2");
            return new HttpResponseMessage(HttpStatusCode.Created) { Content = JsonContent.Create(new ChannelConfigResponse { ChannelId = "versioned", Version = "release-2" }) };
        }
        Assert(body[0].GetProperty("version").GetString() == "release-2");
        return Results(1, 0);
    }, handleEmpty: true);
    using var http = new HttpClient(handler) { BaseAddress = new Uri("https://distill.test/") };
    using var versioned = new DistillClient(http, "key");
    await versioned.ReconcileChannelAsync(new CreateChannelRequest { Name = "versioned", Version = "release-2", SinkId = "receiver@1" });
    await versioned.IngestAsync(new[] { new DistillEvent { ChannelId = "versioned", Payload = new { value = 1 } } });
    await versioned.GetChannelAsync("versioned");
    await versioned.PauseChannelAsync("versioned");
    await versioned.ResumeChannelAsync("versioned");
    await versioned.FlushChannelAsync("versioned");
    await versioned.ArchiveChannelAsync("versioned");
    await versioned.CloneChannelAsync("versioned", "copy");
    Assert(adminCalls.Count == 6);
}

static async Task VerifyLoggingAsync()
{
    using (var validationHttp = new HttpClient { BaseAddress = new Uri("https://unused.test/") })
    using (var validationClient = new DistillClient(validationHttp, "key"))
    {
        var invalid = new DistillLoggerOptions();
        invalid.ChannelOptions["invalid.queue"] = new PerChannelOptions { BatchSize = 0 };
        bool rejected = false;
        try { await using var provider = new DistillLoggerProvider(validationClient, invalid); }
        catch (ArgumentException) { rejected = true; }
        Assert(rejected); // Zero-sized channel batches must fail before a worker silently stops draining.
    }

    var received = new ConcurrentQueue<JsonElement>();
    var transport = new DelegateHandler(async (request, token) =>
    {
        var events = await request.Content!.ReadFromJsonAsync<JsonElement>(token);
        foreach (var item in events.EnumerateArray()) received.Enqueue(item.Clone());
        return Results(events.GetArrayLength(), 0);
    });
    using var http = new HttpClient(transport) { BaseAddress = new Uri("https://distill.test/") };
    using var client = new DistillClient(http, "key");
    var options = new DistillLoggerOptions { BatchSize = 32, FlushInterval = TimeSpan.FromMilliseconds(10), DefaultVersion = "v1" };
    await using (var provider = new DistillLoggerProvider(client, options))
    {
        var logger = provider.CreateLogger("test");
        var before = DateTimeOffset.UtcNow;
        for (int i = 0; i < 100; i++) logger.LogInformation("Event {Number}", i);
        var after = DateTimeOffset.UtcNow;
        await WaitUntil(() => received.Count == 100);
        Assert(provider.DroppedCount == 0 && provider.DeliveryFailureCount == 0);
        Assert(received.Select(item => item.GetProperty("event_id").GetGuid()).Distinct().Count() == 100);
        Assert(received.All(item => item.GetProperty("version").GetString() == "v1"));
        Assert(received.All(item => item.GetProperty("timestamp").GetDateTimeOffset() >= before && item.GetProperty("timestamp").GetDateTimeOffset() <= after));
    }
    // Disposing a caller-owned client wrapper must not dispose its HttpClient.
    client.Dispose();
    await http.GetAsync("health");

    var requests = new ConcurrentQueue<JsonElement>();
    int call = 0;
    int successes = 0;
    var partial = new DelegateHandler(async (request, token) =>
    {
        var events = await request.Content!.ReadFromJsonAsync<JsonElement>(token);
        requests.Enqueue(events.Clone());
        if (Interlocked.Increment(ref call) == 1)
            return Results(1, 1, new[] { new IngestItemError { Index = 1, ErrorCode = "SERVICE_UNAVAILABLE" } });
        return Results(events.GetArrayLength(), 0);
    });
    using var partialHttp = new HttpClient(partial) { BaseAddress = new Uri("https://distill.test/") };
    await using (var provider = new DistillLoggerProvider(new DistillClient(partialHttp, "key"), new DistillLoggerOptions
    {
        BatchSize = 2, FlushInterval = TimeSpan.FromMilliseconds(10), RetryDelay = TimeSpan.FromMilliseconds(1),
        DeliverySuccess = result => Interlocked.Add(ref successes, result.EventCount)
    }))
    {
        var logger = provider.CreateLogger("test");
        logger.LogInformation("one");
        logger.LogInformation("two");
        await WaitUntil(() => Volatile.Read(ref successes) == 2);
        var batches = requests.ToArray();
        Assert(batches.Length == 2 && batches[0].GetArrayLength() == 2 && batches[1].GetArrayLength() == 1);
        Assert(batches[0][1].GetProperty("event_id").GetGuid() == batches[1][0].GetProperty("event_id").GetGuid());
        Assert(provider.DeliveryFailureCount == 0);
    }

    var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var hanging = new DelegateHandler(async (_, token) =>
    {
        started.TrySetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, token);
        throw new InvalidOperationException();
    });
    using var blockedHttp = new HttpClient(hanging) { BaseAddress = new Uri("https://distill.test/") };
    var bounded = new DistillLoggerProvider(new DistillClient(blockedHttp, "key"), new DistillLoggerOptions
    {
        QueueCapacity = 2, BatchSize = 1, ShutdownTimeout = TimeSpan.FromMilliseconds(100)
    });
    var blockedLogger = bounded.CreateLogger("test");
    blockedLogger.LogInformation("in flight");
    await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
    for (int i = 0; i < 10; i++) blockedLogger.LogInformation("queued {Index}", i);
    Assert(bounded.DroppedCount == 8);
    await bounded.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
}

static HttpResponseMessage Results(int accepted, int rejected, IReadOnlyList<IngestItemError>? errors = null) => new(rejected == 0 ? HttpStatusCode.Accepted : HttpStatusCode.MultiStatus)
{
    Content = JsonContent.Create(new IngestResponse { Accepted = accepted, Rejected = rejected, Errors = errors ?? [] })
};

static async Task WaitUntil(Func<bool> predicate)
{
    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    while (!predicate()) await Task.Delay(5, deadline.Token);
}

static void Assert(bool condition)
{
    if (!condition)
    {
        throw new InvalidOperationException("Contract assertion failed.");
    }
}

sealed class CaptureHandler : HttpMessageHandler
{
    public string Body { get; private set; } = string.Empty;
    public string? ApiVersion { get; private set; }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        Body = request.Content is null
            ? string.Empty
            : await request.Content.ReadAsStringAsync(cancellationToken);
        ApiVersion = request.Headers.GetValues("X-API-VERSION").Single();
        return new HttpResponseMessage(HttpStatusCode.Created);
    }
}

sealed class DelegateHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handle, bool handleEmpty = false) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        request.Content is null && !handleEmpty ? Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)) : handle(request, cancellationToken);
}
