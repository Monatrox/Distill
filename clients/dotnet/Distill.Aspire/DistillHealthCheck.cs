using Distill.Client;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Distill.Aspire;

/// Checks that the Distill endpoint is reachable.
///
/// A missing probe channel is treated as healthy because it proves the request
/// reached Distill. Authentication and service-unavailable responses are
/// unhealthy because they indicate a configuration or dependency problem.
public class DistillHealthCheck : IHealthCheck
{
    private readonly DistillClient client;

    /// Initializes the health check with the registered Distill client.
    public DistillHealthCheck(DistillClient client)
    {
        this.client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// Probes the Distill endpoint and reports reachability.
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await client.GetChannelAsync("application", cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (DistillApiException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return HealthCheckResult.Healthy("Distill is reachable; the probe channel is not registered.");
        }
        catch (DistillApiException ex)
        {
            return HealthCheckResult.Unhealthy(
                $"Distill returned HTTP {(int)ex.StatusCode} ({ex.ErrorCode ?? "UNKNOWN_ERROR"}).",
                ex);
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Cannot reach Distill endpoint.", ex);
        }
    }
}
