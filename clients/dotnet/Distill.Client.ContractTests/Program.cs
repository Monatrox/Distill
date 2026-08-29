using System.Net;
using System.Text.Json;
using Distill.Client;

var handler = new CaptureHandler();
var client = new DistillClient(new HttpClient(handler)
{
    BaseAddress = new Uri("https://distill.test/")
}, "ingest-key");

await client.CreateChannelAsync(new CreateChannelRequest
{
    Name = "orders.production",
    Preset = ChannelPreset.Metrics
});

using (var request = JsonDocument.Parse(handler.Body))
{
    Assert(request.RootElement.GetProperty("preset").GetString() == "metrics");
    Assert(!request.RootElement.TryGetProperty("aggregator", out _));
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

return;

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
