using Distill.Client;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Distill.Aspire;

/// Checks that the Distill endpoint is reachable.
///
/// Any HTTP response (including API-level errors like 404 or 400) is treated
/// as healthy — it proves the server is reachable and responding. Only
/// transport-level failures (connection refused, DNS resolution failure,
/// timeout) produce an unhealthy result.
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
        catch (DistillApiException)
        {
            // Any API-level response means the endpoint is reachable.
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Cannot reach Distill endpoint.", ex);
        }
    }
}
