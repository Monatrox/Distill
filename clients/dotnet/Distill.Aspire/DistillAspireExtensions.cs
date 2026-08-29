using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Distill.Aspire;

/// Provides Aspire hosting extensions for Distill.
public static class DistillResourceBuilderExtensions
{
    private const int DefaultHttpPort = 8080;
    private const int DefaultAdminPort = 8081;
    private const string RabbitMqImage = "rabbitmq:3.13-management";
    private const int RabbitMqAmqpPort = 5672;

    /// Adds a Distill resource that launches a container with its RabbitMQ dependency.
    ///
    /// The integration automatically provisions a RabbitMQ container as a managed
    /// child resource, wires the <c>RABBITMQ_URI</c> environment variable, and
    /// ensures Distill starts after RabbitMQ is running. Both containers appear
    /// in the Aspire dashboard with full log and health visibility.
    ///
    /// Consuming projects reference this resource via <c>.WithReference(distill)</c>
    /// and wire the client with <c>AddDistillClient()</c>. The endpoint is resolved
    /// automatically through Aspire's service discovery.
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

        // Create the RabbitMQ container as a managed child resource.
        var rabbitMq = builder.AddContainer($"{name}-rabbitmq", RabbitMqImage)
            .WithEndpoint(targetPort: RabbitMqAmqpPort, name: "amqp");

        var amqpEndpoint = rabbitMq.GetEndpoint("amqp");
        var rabbitMqUri = ReferenceExpression.Create(
            $"amqp://guest:guest@{amqpEndpoint.Property(EndpointProperty.Host)}:{amqpEndpoint.Property(EndpointProperty.Port)}/%2f");

        var distill = builder.AddResource(resource)
            .WithImage(DistillDefaults.ImageName, DistillDefaults.ImageTag)
            .WithHttpEndpoint(targetPort: DefaultHttpPort, name: "http")
            .WithEndpoint(targetPort: DefaultAdminPort, scheme: "http", name: "admin", isExternal: false)
            .WithEnvironment(ApplyDefaultEnvironment(resource, apiKey, rabbitMqUri))
            .WaitFor(rabbitMq);

        return distill;
    }

    /// Adds a Distill resource configured for an externally-running Distill instance.
    ///
    /// No containers are launched. The resource sets <c>Distill__Endpoint</c> and
    /// <c>Distill__ApiKey</c> on consuming projects that use <c>.WithReference(distill)</c>.
    /// Configure sinks on externally managed Distill instances through their deployment.
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
            .WithEnvironment(ApplyDefaultEnvironment(resource, apiKey, rabbitMqUri: null));
    }

    /// Registers a startup sink from a connection-string resource and marks it as default.
    public static IResourceBuilder<DistillResource> WithSink(
        this IResourceBuilder<DistillResource> builder,
        IResourceBuilder<IResourceWithConnectionString> sinkResource,
        string name = "default",
        string driver = "postgres")
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sinkResource);

        var connectionExpression = GetSinkConnectionExpression(sinkResource.Resource);
        builder.Resource.Annotations.Add(new DistillSinkAnnotation(
            name, driver, connectionExpression, IsDefault: true));

        return builder.WaitFor(sinkResource);
    }

    /// Registers startup sinks from connection-string resources with an explicit default.
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
        if (!sinks.ContainsKey(defaultSinkName))
        {
            throw new ArgumentException(
                $"The default sink '{defaultSinkName}' is not present in the sink collection.",
                nameof(defaultSinkName));
        }

        foreach (var (name, resource) in sinks)
        {
            var connectionExpression = GetSinkConnectionExpression(resource.Resource);
            builder.Resource.Annotations.Add(new DistillSinkAnnotation(
                name, driver, connectionExpression, name == defaultSinkName));

            builder.WaitFor(resource);
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
    /// Aspire builds and runs the image with its detected Docker or Podman runtime.
    /// Set <c>ASPIRE_CONTAINER_RUNTIME</c> to <c>docker</c> or <c>podman</c> to override detection.
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

        dockerfilePath = Path.GetFullPath(dockerfilePath ?? FindDockerfile()
            ?? throw new FileNotFoundException(
                "Could not locate the Distill Dockerfile. Specify the path explicitly:\n" +
                ".WithLocalBuild(dockerfile: \"path/to/Dockerfile\", context: \"path/to/context\")"));
        contextPath = Path.GetFullPath(contextPath ?? Path.GetDirectoryName(dockerfilePath)!);

        return builder.WithDockerfile(contextPath, dockerfilePath);
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
        DistillResource resource,
        string? apiKey,
        ReferenceExpression? rabbitMqUri)
    {
        return context =>
        {
            var env = context.EnvironmentVariables;

            if (!string.IsNullOrEmpty(apiKey))
            {
                env["DISTILL_INGEST_API_KEY"] = apiKey;
                env["DISTILL_ADMIN_API_KEY"] = $"{apiKey}-admin";
            }

            env["DISTILL_BIND_ADDRESS"] = $"0.0.0.0:{DefaultHttpPort}";
            env["DISTILL_ADMIN_BIND_ADDRESS"] = $"0.0.0.0:{DefaultAdminPort}";
            env["DISTILL_ALLOW_ADMIN_NETWORK"] = "true";
            env["DISTILL_REQUEST_BODY_LIMIT_BYTES"] = "2097152";
            env["DISTILL_REQUEST_TIMEOUT_SECONDS"] = "30";

            if (rabbitMqUri is not null)
            {
                env["RABBITMQ_URI"] = rabbitMqUri;
            }

            foreach (var sink in resource.Annotations.OfType<DistillSinkAnnotation>())
            {
                string prefix = $"DISTILL_SINK__{sink.Name.ToUpperInvariant()}";
                env[$"{prefix}__DRIVER"] = sink.Driver;

                if (sink.ConnectionString is not null)
                {
                    env[$"{prefix}__CONNECTION_STRING"] = sink.ConnectionString;
                }

                if (sink.Url is not null)
                {
                    env[$"{prefix}__URL"] = sink.Url.ToString();
                }

                if (sink.BearerToken is not null)
                {
                    env[$"{prefix}__BEARER_TOKEN"] = sink.BearerToken;
                }

                if (sink.IsDefault)
                {
                    env["DISTILL_DEFAULT_SINK"] = sink.Name;
                }
            }
        };
    }

    private static ReferenceExpression GetSinkConnectionExpression(
        IResourceWithConnectionString resource)
    {
        // Aspire's PostgreSQL resource exposes both an ADO.NET connection string
        // and a URI. Distill's sqlx PostgreSQL driver requires the URI form.
        foreach (var property in resource.GetConnectionProperties())
        {
            if (string.Equals(property.Key, "Uri", StringComparison.OrdinalIgnoreCase))
            {
                return property.Value;
            }
        }

        // Keep custom IResourceWithConnectionString implementations working when
        // they do not expose a URI-specific connection property.
        return resource.ConnectionStringExpression;
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
