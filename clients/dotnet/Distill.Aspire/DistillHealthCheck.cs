using Distill.Client;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Distill.Aspire;

/// Checks that Distill is ready to accept events.
///
/// Distill is healthy only after an active channel has connected to its sink.
public class DistillHealthCheck : IHealthCheck
{
    private readonly DistillClient client;

    /// Initializes the health check with the registered Distill client.
    public DistillHealthCheck(DistillClient client)
    {
        this.client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// Probes Distill readiness.
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await client.CheckHealthAsync(cancellationToken);
            return HealthCheckResult.Healthy();
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
