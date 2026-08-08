using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Distill.Client;

/// Provides a lightweight, dependency-free client for Distill’s HTTP API.
public sealed class DistillClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient httpClient;

    /// Initializes a client using an existing HTTP client.
    ///
    /// The supplied client remains owned by the caller. Its base address must point to Distill.
    public DistillClient(HttpClient httpClient, string apiKey)
    {
        ArgumentNullException.ThrowIfNull(httpClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(apiKey);

        this.httpClient = httpClient;
        if (!this.httpClient.DefaultRequestHeaders.Contains("X-API-Key"))
        {
            this.httpClient.DefaultRequestHeaders.Add("X-API-Key", apiKey);
        }
    }

    /// Creates a client with a new HTTP client configured for the given Distill address.
    public static DistillClient Create(Uri baseAddress, string apiKey)
    {
        ArgumentNullException.ThrowIfNull(baseAddress);
        return new DistillClient(new HttpClient { BaseAddress = baseAddress }, apiKey);
    }

    /// Creates or replaces a channel using a raw definition object.
    ///
    /// This is the advanced PUT endpoint for callers who know the full server schema.
    /// Most applications should prefer <see cref="CreateChannelAsync(CreateChannelRequest, CancellationToken)"/>
    /// with a typed <see cref="CreateChannelRequest"/>.
    public async Task CreateOrReplaceChannelAsync(
        string channelId,
        object definition,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channelId);
        ArgumentNullException.ThrowIfNull(definition);
        using HttpResponseMessage response = await httpClient.PutAsJsonAsync(
            $"channels/{Uri.EscapeDataString(channelId)}",
            definition,
            JsonOptions,
            cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
    }

    /// Creates a channel from a preset and optional overrides.
    public Task<ChannelConfigResponse?> CreateChannelAsync(
        CreateChannelRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return SendAsync<ChannelConfigResponse>(HttpMethod.Post, "channels", request, cancellationToken);
    }

    /// Validates and resolves a simplified channel request without registering it.
    public Task<ChannelValidationResponse?> ValidateChannelAsync(
        CreateChannelRequest request,
        CancellationToken cancellationToken = default) =>
        SendAsync<ChannelValidationResponse>(HttpMethod.Post, "channels/validate", request, cancellationToken);

    /// Writes one event through the channel-bound simple event endpoint.
    public Task<IngestResponse?> WriteAsync(
        string channelId,
        SimpleEvent eventToWrite,
        CancellationToken cancellationToken = default) =>
        SendAsync<IngestResponse>(
            HttpMethod.Post,
            $"channels/{Uri.EscapeDataString(channelId)}/events",
            eventToWrite,
            cancellationToken);

    /// Sends a batch of fully specified events through the native ingestion endpoint.
    public Task<IngestResponse?> IngestAsync(
        IEnumerable<DistillEvent> events,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(events);
        return SendAsync<IngestResponse>(HttpMethod.Post, "ingest", events, cancellationToken);
    }

    /// Tests how a simple event would be validated and routed without enqueueing it.
    public Task<EventTestResponse?> TestEventAsync(
        string channelId,
        SimpleEvent eventToTest,
        CancellationToken cancellationToken = default) =>
        SendAsync<EventTestResponse>(
            HttpMethod.Post,
            $"channels/{Uri.EscapeDataString(channelId)}/test",
            eventToTest,
            cancellationToken);

    /// Gets the sanitized effective configuration for a channel.
    public Task<ChannelConfigResponse?> GetChannelAsync(
        string channelId,
        CancellationToken cancellationToken = default) =>
        SendAsync<ChannelConfigResponse>(
            HttpMethod.Get,
            $"channels/{Uri.EscapeDataString(channelId)}/config",
            content: null,
            cancellationToken);

    /// Pauses event acceptance for a channel.
    public Task<ChannelConfigResponse?> PauseChannelAsync(
        string channelId,
        CancellationToken cancellationToken = default) =>
        SendAsync<ChannelConfigResponse>(
            HttpMethod.Post,
            $"channels/{Uri.EscapeDataString(channelId)}/pause",
            content: null,
            cancellationToken);

    /// Resumes event acceptance for a channel.
    public Task<ChannelConfigResponse?> ResumeChannelAsync(
        string channelId,
        CancellationToken cancellationToken = default) =>
        SendAsync<ChannelConfigResponse>(
            HttpMethod.Post,
            $"channels/{Uri.EscapeDataString(channelId)}/resume",
            content: null,
            cancellationToken);

    /// Flushes the active aggregate window for a channel.
    public Task<ChannelConfigResponse?> FlushChannelAsync(
        string channelId,
        CancellationToken cancellationToken = default) =>
        SendAsync<ChannelConfigResponse>(
            HttpMethod.Post,
            $"channels/{Uri.EscapeDataString(channelId)}/flush",
            content: null,
            cancellationToken);

    /// Archives a channel after flushing its active aggregate window.
    public Task<ChannelConfigResponse?> ArchiveChannelAsync(
        string channelId,
        CancellationToken cancellationToken = default) =>
        SendAsync<ChannelConfigResponse>(
            HttpMethod.Post,
            $"channels/{Uri.EscapeDataString(channelId)}/archive",
            content: null,
            cancellationToken);

    /// Clones a channel definition under a new channel identifier.
    public Task<ChannelConfigResponse?> CloneChannelAsync(
        string sourceChannelId,
        string targetChannelId,
        CancellationToken cancellationToken = default) =>
        SendAsync<ChannelConfigResponse>(
            HttpMethod.Post,
            $"channels/{Uri.EscapeDataString(sourceChannelId)}/clone",
            new { channel_id = targetChannelId },
            cancellationToken);

    private async Task<T?> SendAsync<T>(
        HttpMethod method,
        string requestUri,
        object? content,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(method, requestUri);
        if (content is not null)
        {
            request.Content = JsonContent.Create(content, options: JsonOptions);
        }

        using HttpResponseMessage response = await httpClient.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NoContent)
        {
            return default;
        }

        return await response.Content.ReadFromJsonAsync<T>(JsonOptions, cancellationToken);
    }

    private static async Task EnsureSuccessAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        string body = await response.Content.ReadAsStringAsync(cancellationToken);
        string? errorCode = null;
        try
        {
            using JsonDocument document = JsonDocument.Parse(body);
            if (document.RootElement.TryGetProperty("error_code", out JsonElement code))
            {
                errorCode = code.GetString();
            }
        }
        catch (JsonException)
        {
            // Preserve the raw response body when the server did not return an error envelope.
        }

        throw new DistillApiException(response.StatusCode, errorCode, body);
    }
}
