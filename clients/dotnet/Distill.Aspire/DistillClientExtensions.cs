using Distill.Client;
using Distill.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Distill.Aspire;

/// Provides Aspire client integration for Distill.
public static class DistillClientExtensions
{
    /// Adds a Distill client and logging provider to the application.
    ///
    /// Reads the Distill URI from <c>ConnectionStrings__{connectionName}</c>
    /// (or <c>Distill__Endpoint</c> as a fallback), and the
    /// API key from configuration key <c>Distill__ApiKey</c>. Falls back to
    /// <c>http://localhost:8080</c> with an empty API key.
    ///
    /// Registers <see cref="DistillClient"/> as a singleton, wires the
    /// Distill logging provider, and adds a health check for the Aspire
    /// dashboard.
    public static IHostApplicationBuilder AddDistillClient(
        this IHostApplicationBuilder builder,
        string connectionName = "distill",
        Action<DistillLoggerOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);

        (Uri endpoint, string endpointSource) = ResolveEndpoint(builder.Configuration, connectionName);
        string apiKey = builder.Configuration["Distill:ApiKey"]
            ?? builder.Configuration["Distill__ApiKey"]
            ?? string.Empty;
        string? adminApiKey = builder.Configuration["Distill:AdminApiKey"]
            ?? builder.Configuration["Distill__AdminApiKey"];

        string? adminEndpoint = builder.Configuration["Distill:AdminEndpoint"]
            ?? builder.Configuration["Distill__AdminEndpoint"];
        Console.WriteLine($"[Distill.Aspire] endpoint={DescribeUri(endpoint)} source={endpointSource}");
        Console.WriteLine($"[Distill.Aspire] adminEndpoint={DescribeEndpoint(adminEndpoint)}");
        Console.WriteLine(
            $"[Distill.Aspire] ingestKeyConfigured={!string.IsNullOrWhiteSpace(apiKey)} "
            + $"adminKeyConfigured={!string.IsNullOrWhiteSpace(adminApiKey)}");

        var client = DistillClient.Create(endpoint, apiKey, adminApiKey, adminEndpoint);
        builder.Services.AddSingleton(client);
        builder.Logging.AddDistill(client, configureOptions);
        builder.Services.AddHealthChecks().AddCheck<DistillHealthCheck>("distill");

        return builder;
    }

    private static (Uri Endpoint, string Source) ResolveEndpoint(
        IConfiguration configuration,
        string connectionName)
    {
        // Aspire references are emitted as ConnectionStrings__{name}. Prefer that
        // value so a stale appsettings.json endpoint cannot bypass service discovery.
        string? connectionString = configuration.GetConnectionString(connectionName);
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            if (TryCreateHttpUri(connectionString, out Uri? connectionEndpoint))
            {
                return (connectionEndpoint!, $"ConnectionStrings:{connectionName}");
            }

            throw new InvalidOperationException(
                $"ConnectionStrings:{connectionName} must be an absolute HTTP or HTTPS URI.");
        }

        var endpointValue = configuration["Distill:Endpoint"]
            ?? configuration["Distill__Endpoint"];
        if (!string.IsNullOrWhiteSpace(endpointValue))
        {
            if (TryCreateHttpUri(endpointValue, out Uri? endpoint))
            {
                return (endpoint!, "Distill:Endpoint");
            }

            throw new InvalidOperationException(
                "Distill:Endpoint must be an absolute HTTP or HTTPS URI.");
        }

        return (new Uri("http://localhost:8080"), "fallback");
    }

    private static bool TryCreateHttpUri(string value, out Uri? uri)
    {
        if (Uri.TryCreate(value, UriKind.Absolute, out uri)
            && (uri.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
                || uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        uri = null;
        return false;
    }

    private static string DescribeEndpoint(string? value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? "<same-as-ingest>"
            : Uri.TryCreate(value, UriKind.Absolute, out Uri? uri)
                ? DescribeUri(uri)
                : "<invalid>";
    }

    private static string DescribeUri(Uri uri)
    {
        var safeUri = new UriBuilder(uri)
        {
            UserName = string.Empty,
            Password = string.Empty,
            Query = string.Empty,
            Fragment = string.Empty
        }.Uri;
        return safeUri.AbsoluteUri.TrimEnd('/');
    }
}
