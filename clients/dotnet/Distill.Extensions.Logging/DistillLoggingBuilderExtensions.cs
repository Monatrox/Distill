using Distill.Client;
using Microsoft.Extensions.Logging;

namespace Distill.Extensions.Logging;

/// Provides dependency-injection registration helpers for the Distill logging provider.
public static class DistillLoggingBuilderExtensions
{
    /// Adds a bounded, batched Distill provider to the logging builder.
    public static ILoggingBuilder AddDistill(
        this ILoggingBuilder builder,
        DistillClient client,
        Action<DistillLoggerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(client);

        var options = new DistillLoggerOptions();
        configure?.Invoke(options);
        options.Validate();
        builder.AddProvider(new DistillLoggerProvider(client, options));
        return builder;
    }
}
