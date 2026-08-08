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
    /// Reads the Distill URI from configuration key <c>Distill__Endpoint</c>
    /// (or <c>ConnectionStrings__{connectionName}</c> as a fallback), and the
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

        Uri endpoint = ResolveEndpoint(builder.Configuration, connectionName);
        string apiKey = builder.Configuration.GetValue<string>(DistillResource.ApiKeyConfigKey) ?? string.Empty;

        var client = DistillClient.Create(endpoint, apiKey);
        builder.Services.AddSingleton(client);
        builder.Logging.AddDistill(client, configureOptions);
        builder.Services.AddHealthChecks().AddCheck<DistillHealthCheck>("distill");

        return builder;
    }

    private static Uri ResolveEndpoint(IConfiguration configuration, string connectionName)
    {
        Uri? endpoint = configuration.GetValue<Uri>(DistillResource.EndpointConfigKey);
        if (endpoint is not null)
        {
            return endpoint;
        }

        string? connectionString = configuration.GetConnectionString(connectionName);
        if (!string.IsNullOrWhiteSpace(connectionString) && Uri.TryCreate(connectionString, UriKind.Absolute, out Uri? parsed))
        {
            return parsed;
        }

        return new Uri("http://localhost:8080");
    }
}
