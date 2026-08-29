using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Distill.Client;

/// Provides a lightweight, dependency-free client for Distill's HTTP API.
public sealed class DistillClient
{
    private const int MaxDiagnosticBodyLength = 2_048;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true
    };

    private static readonly bool DiagnosticsEnabled = IsDiagnosticsEnabled();

    private readonly HttpClient httpClient;
    private readonly HttpClient? _adminClient;
    private readonly string? _adminApiKey;
    private readonly string _ingestApiKey;

    /// Initializes a client using an existing HTTP client.
    ///
    /// The supplied client remains owned by the caller. Its base address must point to Distill.
    /// When <paramref name="adminApiKey"/> is provided, administration operations (channel
    /// management) use it while ingestion uses <paramref name="apiKey"/>. When omitted,
    /// both use the same key. The <paramref name="adminEndpoint"/> specifies the base URL
    /// for admin operations (defaults to the same base as the public endpoint).
    public DistillClient(HttpClient httpClient, string apiKey, string? adminApiKey = null, string? adminEndpoint = null)
    {
        ArgumentNullException.ThrowIfNull(httpClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(apiKey);

        this.httpClient = httpClient;
        _ingestApiKey = apiKey;

        if (!string.IsNullOrWhiteSpace(adminApiKey) && !string.Equals(adminApiKey, apiKey, StringComparison.Ordinal))
        {
            _adminApiKey = adminApiKey;
        }

        // Create a separate HttpClient for admin operations if a different endpoint is specified.
        if (!string.IsNullOrWhiteSpace(adminEndpoint))
        {
            if (!Uri.TryCreate(adminEndpoint, UriKind.Absolute, out Uri? parsedAdminEndpoint)
                || !IsHttpUri(parsedAdminEndpoint))
            {
                throw new ArgumentException(
                    "The admin endpoint must be an absolute HTTP or HTTPS URI.",
                    nameof(adminEndpoint));
            }

            _adminClient = new HttpClient { BaseAddress = parsedAdminEndpoint };
        }

        if (_adminApiKey is null && _adminClient is null)
        {
            // Single-key mode: set the key on default headers.
            if (!this.httpClient.DefaultRequestHeaders.Contains("X-API-Key"))
            {
                this.httpClient.DefaultRequestHeaders.Add("X-API-Key", apiKey);
            }
        }

        DebugLog(
            $"configured ingestEndpoint={DescribeUri(httpClient.BaseAddress)} "
            + $"adminEndpoint={DescribeUri(_adminClient?.BaseAddress)} "
            + $"ingestKeyConfigured=true adminKeyConfigured={_adminApiKey is not null} "
            + $"mode={(_adminApiKey is null && _adminClient is null ? "single" : "split")}");
    }

    /// Creates a client with a new HTTP client configured for the given Distill address.
    public static DistillClient Create(Uri baseAddress, string apiKey, string? adminApiKey = null, string? adminEndpoint = null)
    {
        ArgumentNullException.ThrowIfNull(baseAddress);
        return new DistillClient(new HttpClient { BaseAddress = baseAddress }, apiKey, adminApiKey, adminEndpoint);
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
        await SendAsync<object?>(
            HttpMethod.Put,
            $"channels/{Uri.EscapeDataString(channelId)}",
            definition,
            cancellationToken);
    }

    /// Checks whether Distill has an active sink-backed channel.
    public async Task CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        _ = await SendAsync<object?>(HttpMethod.Get, "health", content: null, cancellationToken);
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

        var isAdmin = IsAdminRoute(requestUri);
        var route = isAdmin ? "admin" : "ingest";
        HttpClient client = httpClient;

        // When split authentication or an admin endpoint is active, select the
        // key and client per route. In particular, /channels/{id}/events is an
        // ingestion route even though it shares the /channels prefix.
        if (_adminApiKey is not null || _adminClient is not null)
        {
            var apiKey = isAdmin ? _adminApiKey ?? _ingestApiKey : _ingestApiKey;
            request.Headers.TryAddWithoutValidation("X-API-Key", apiKey);
            client = isAdmin && _adminClient is not null ? _adminClient : httpClient;
        }

        var target = DescribeTarget(client, request.RequestUri);
        DebugLog($"request method={method.Method} target={target} route={route}");

        try
        {
            using HttpResponseMessage response = await client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken);
            DebugLog(
                $"response method={method.Method} target={target} route={route} "
                + $"status={(int)response.StatusCode} contentType={response.Content.Headers.ContentType?.MediaType ?? "<none>"} "
                + $"server={GetHeaderValue(response, "Server")}");

            await EnsureSuccessAsync(response, cancellationToken, method, target, route);
            if (response.StatusCode == HttpStatusCode.NoContent
                || response.Content.Headers.ContentLength == 0)
            {
                return default;
            }

            return await response.Content.ReadFromJsonAsync<T>(JsonOptions, cancellationToken);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or InvalidOperationException)
        {
            if (!(ex is TaskCanceledException && cancellationToken.IsCancellationRequested))
            {
                DebugLog(
                    $"transport failure method={method.Method} target={target} route={route} "
                    + $"exception={ex.GetType().Name}: {ex.Message}");
            }

            throw;
        }
    }

    private static async Task EnsureSuccessAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken,
        HttpMethod method,
        string target,
        string route)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        string body = await response.Content.ReadAsStringAsync(cancellationToken);
        DebugLog(
            $"error body method={method.Method} target={target} route={route} "
            + $"body={TruncateForDiagnostics(body)}");
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

    private static bool IsAdminRoute(string requestUri)
    {
        var path = requestUri;
        var queryStart = path.IndexOf('?');
        if (queryStart >= 0)
        {
            path = path[..queryStart];
        }

        path = path.TrimStart('/');
        return path.Equals("channels", StringComparison.OrdinalIgnoreCase)
            || (path.StartsWith("channels/", StringComparison.OrdinalIgnoreCase)
                && !path.EndsWith("/events", StringComparison.OrdinalIgnoreCase));
    }

    private static bool IsHttpUri(Uri uri) =>
        uri.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
        || uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);

    private static bool IsDiagnosticsEnabled()
    {
        var value = Environment.GetEnvironmentVariable("DISTILL_CLIENT_DEBUG");
        return value is not null
            && (value.Equals("1", StringComparison.OrdinalIgnoreCase)
                || value.Equals("true", StringComparison.OrdinalIgnoreCase)
                || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
                || value.Equals("on", StringComparison.OrdinalIgnoreCase));
    }

    private static void DebugLog(string message)
    {
        if (DiagnosticsEnabled)
        {
            Console.Error.WriteLine($"[DistillClient] {message}");
        }
    }

    private static string DescribeTarget(HttpClient client, Uri? requestUri) =>
        requestUri is null
            ? "<unknown>"
            : requestUri.IsAbsoluteUri
                ? DescribeUri(requestUri)
                : client.BaseAddress is null
                    ? requestUri.ToString()
                    : DescribeUri(new Uri(client.BaseAddress, requestUri));

    private static string DescribeUri(Uri? uri)
    {
        if (uri is null)
        {
            return "<not-configured>";
        }

        var safeUri = new UriBuilder(uri)
        {
            UserName = string.Empty,
            Password = string.Empty,
            Query = string.Empty,
            Fragment = string.Empty
        }.Uri;
        return safeUri.AbsoluteUri.TrimEnd('/');
    }

    private static string GetHeaderValue(HttpResponseMessage response, string headerName) =>
        response.Headers.TryGetValues(headerName, out var values)
            ? string.Join(",", values)
            : "<none>";

    private static string TruncateForDiagnostics(string value)
    {
        var normalized = value.Replace("\r", "\\r").Replace("\n", "\\n");
        return normalized.Length <= MaxDiagnosticBodyLength
            ? normalized
            : normalized[..MaxDiagnosticBodyLength] + "…";
    }
}
