using System.Diagnostics;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Distill.Aspire;

/// Provides Aspire hosting extensions for Distill.
public static class DistillResourceBuilderExtensions
{
    private const int DefaultHttpPort = 8080;

    /// Adds a Distill resource that launches a container.
    ///
    /// Consuming projects reference this resource via <c>.WithReference(distill)</c>
    /// and wire the client with <c>AddDistillClient()</c>. The endpoint is resolved
    /// automatically through Aspire's service discovery.
    ///
    /// To use a custom container image, call <c>.WithImage(...)</c> after this method.
    /// When no image is configured, the integration defaults to a locally-built
    /// <c>distill:latest</c> image (build it with <c>docker build -t distill:latest .</c>).
    ///
    /// Register sinks via <c>.WithSink(...)</c> before consuming projects reference this resource.
    public static IResourceBuilder<DistillResource> AddDistill(
        this IDistributedApplicationBuilder builder,
        string name,
        string? apiKey = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var resource = new DistillResource(name);
        return builder.AddResource(resource)
            .WithImage(DistillDefaults.ImageName, DistillDefaults.ImageTag)
            .WithHttpEndpoint(DefaultHttpPort, DefaultHttpPort, "http")
            .WithEnvironment(ApplyDefaultEnvironment(resource, apiKey));
    }

    /// Adds a Distill resource configured for an externally-running Distill instance.
    ///
    /// No container is launched. The resource sets <c>Distill__Endpoint</c> and
    /// <c>Distill__ApiKey</c> on consuming projects that use <c>.WithReference(distill)</c>.
    public static IResourceBuilder<DistillResource> AddDistill(
        this IDistributedApplicationBuilder builder,
        string name,
        Uri endpoint,
        string? apiKey = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(endpoint);

        var resource = new DistillResource(name)
        {
            ExplicitEndpoint = endpoint
        };

        return builder.AddResource(resource)
            .WithEnvironment(ApplyDefaultEnvironment(resource, apiKey));
    }

    /// Registers a single sink from a connection-string resource (PostgreSQL, etc.)
    /// and marks it as the default.
    public static IResourceBuilder<DistillResource> WithSink(
        this IResourceBuilder<DistillResource> builder,
        IResourceBuilder<IResourceWithConnectionString> sinkResource,
        string name = "default",
        string driver = "postgres")
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sinkResource);

        builder.Resource.Annotations.Add(new DistillSinkAnnotation(
            name, driver, sinkResource.Resource.ConnectionStringExpression, IsDefault: true));
        return builder;
    }

    /// Registers multiple sinks from connection-string resources with an explicit default.
    public static IResourceBuilder<DistillResource> WithSink(
        this IResourceBuilder<DistillResource> builder,
        IReadOnlyDictionary<string, IResourceBuilder<IResourceWithConnectionString>> sinks,
        string driver,
        string defaultSinkName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sinks);
        ArgumentException.ThrowIfNullOrWhiteSpace(driver);
        ArgumentException.ThrowIfNullOrWhiteSpace(defaultSinkName);

        foreach (var (name, resource) in sinks)
        {
            builder.Resource.Annotations.Add(new DistillSinkAnnotation(
                name, driver, resource.Resource.ConnectionStringExpression, name == defaultSinkName));
        }

        return builder;
    }

    /// Registers a single custom sink specification (HTTP, etc.) as the default.
    public static IResourceBuilder<DistillResource> WithSink(
        this IResourceBuilder<DistillResource> builder,
        DistillSinkSpecification sink)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sink);

        var connString = string.IsNullOrEmpty(sink.ConnectionString)
            ? null
            : ReferenceExpression.Create($"{sink.ConnectionString}");

        builder.Resource.Annotations.Add(new DistillSinkAnnotation(
            sink.Name, sink.Driver, connString, IsDefault: true)
        {
            Url = sink.Url,
            BearerToken = sink.BearerToken
        });
        return builder;
    }

    /// Registers multiple custom sink specifications with an explicit default.
    public static IResourceBuilder<DistillResource> WithSink(
        this IResourceBuilder<DistillResource> builder,
        IReadOnlyDictionary<string, DistillSinkSpecification> sinks,
        string defaultSinkName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sinks);
        ArgumentException.ThrowIfNullOrWhiteSpace(defaultSinkName);

        foreach (var (name, spec) in sinks)
        {
            var connString = string.IsNullOrEmpty(spec.ConnectionString)
                ? null
                : ReferenceExpression.Create($"{spec.ConnectionString}");

            builder.Resource.Annotations.Add(new DistillSinkAnnotation(
                spec.Name, spec.Driver, connString, name == defaultSinkName)
            {
                Url = spec.Url,
                BearerToken = spec.BearerToken
            });
        }

        return builder;
    }

    /// Builds the Distill container image from the local Dockerfile.
    ///
    /// Searches upward from the current directory for a file named <c>Dockerfile</c>.
    /// If the image <c>distill:latest</c> does not exist locally, runs <c>docker build</c>
    /// synchronously at configuration time. Docker build caching makes re-builds fast;
    /// only the first invocation compiles the Rust binary.
    ///
    /// Provide explicit paths to override the search:
    /// <code>
    /// .WithLocalBuild(dockerfile: "./docker/distill.Dockerfile", context: "../")
    /// </code>
    public static IResourceBuilder<DistillResource> WithLocalBuild(
        this IResourceBuilder<DistillResource> builder,
        string? dockerfilePath = null,
        string? contextPath = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        dockerfilePath ??= FindDockerfile()
            ?? throw new FileNotFoundException(
                "Could not locate the Distill Dockerfile. Specify the path explicitly:\n" +
                ".WithLocalBuild(dockerfile: \"path/to/Dockerfile\", context: \"path/to/context\")");

        contextPath ??= Path.GetDirectoryName(Path.GetFullPath(dockerfilePath))!;

        var imageName = $"{DistillDefaults.ImageName}:{DistillDefaults.ImageTag}";

        if (!ImageExistsLocally(imageName))
        {
            Console.WriteLine($"Building Distill container image ({imageName}) from {dockerfilePath}...");

            var startInfo = new ProcessStartInfo("docker", $"build -t {imageName} -f \"{dockerfilePath}\" \"{contextPath}\"")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };

            using var build = Process.Start(startInfo)
                ?? throw new InvalidOperationException("Failed to start docker build. Is Docker installed and running?");

            // Read output in the background to prevent deadlocks
            var output = build.StandardOutput.ReadToEndAsync();
            var errors = build.StandardError.ReadToEndAsync();

            if (!build.WaitForExit(600_000))
            {
                build.Kill(true);
                throw new TimeoutException(
                    "Docker build timed out after 10 minutes. The Rust compilation may have stalled.\n" +
                    "Build manually with: docker build -t distill:latest -f \"{dockerfilePath}\" \"{contextPath}\"");
            }

            if (build.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Docker build failed (exit code {build.ExitCode}).\n" +
                    $"Output:\n{output.Result}\n{errors.Result}");
            }

            Console.WriteLine("Distill container image built successfully.");
        }

        return builder;
    }

    private static bool ImageExistsLocally(string imageName)
    {
        try
        {
            var startInfo = new ProcessStartInfo("docker", $"image inspect {imageName}")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };

            using var inspect = Process.Start(startInfo);
            if (inspect is null)
                return false;

            return inspect.WaitForExit(10_000) && inspect.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    private static string? FindDockerfile()
    {
        var dir = Environment.CurrentDirectory;
        for (var i = 0; i < 10; i++)
        {
            var path = Path.Combine(dir, "Dockerfile");
            if (File.Exists(path))
                return Path.GetFullPath(path);

            var parent = Path.GetDirectoryName(dir);
            if (parent is null || parent == dir)
                return null;

            dir = parent;
        }

        return null;
    }

    private static Action<EnvironmentCallbackContext> ApplyDefaultEnvironment(
        DistillResource resource, string? apiKey)
    {
        return context =>
        {
            if (!string.IsNullOrEmpty(apiKey))
            {
                context.EnvironmentVariables["DISTILL_INGEST_API_KEY"] = apiKey;
                context.EnvironmentVariables["DISTILL_ADMIN_API_KEY"] = apiKey;
            }

            context.EnvironmentVariables["DISTILL_BIND_ADDRESS"] = $"0.0.0.0:{DefaultHttpPort}";
            context.EnvironmentVariables["DISTILL_REQUEST_BODY_LIMIT_BYTES"] = "2097152";
            context.EnvironmentVariables["DISTILL_REQUEST_TIMEOUT_SECONDS"] = "30";

            // Forward sink configurations as environment variables
            foreach (var sink in resource.Annotations.OfType<DistillSinkAnnotation>())
            {
                string prefix = $"DISTILL_SINK__{sink.Name.ToUpperInvariant()}";
                context.EnvironmentVariables[$"{prefix}__DRIVER"] = sink.Driver;

                if (sink.ConnectionString is not null)
                {
                    context.EnvironmentVariables[$"{prefix}__CONNECTION_STRING"] = sink.ConnectionString;
                }

                if (sink.Url is not null)
                {
                    context.EnvironmentVariables[$"{prefix}__URL"] = sink.Url.ToString();
                }

                if (sink.BearerToken is not null)
                {
                    context.EnvironmentVariables[$"{prefix}__BEARER_TOKEN"] = sink.BearerToken;
                }

                if (sink.IsDefault)
                {
                    context.EnvironmentVariables["DISTILL_DEFAULT_SINK"] = sink.Name;
                }
            }
        };
    }
}

/// Default constants for the Distill container image.
public static class DistillDefaults
{
    /// Default container image name for the Distill server.
    public const string ImageName = "distill";

    /// Default container image tag for the Distill server.
    public const string ImageTag = "latest";
}

internal sealed record DistillSinkAnnotation(
    string Name,
    string Driver,
    ReferenceExpression? ConnectionString,
    bool IsDefault) : IResourceAnnotation
{
    public Uri? Url { get; init; }
    public string? BearerToken { get; init; }
}
