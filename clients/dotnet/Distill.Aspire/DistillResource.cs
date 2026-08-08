using Aspire.Hosting.ApplicationModel;

namespace Distill.Aspire;

/// A resource that represents a Distill structured logging endpoint.
///
/// When an explicit endpoint is provided (via <c>AddDistill(name, endpoint, apiKey)</c>),
/// the resource does not start a container — it configures consuming projects to point
/// to an externally-running Distill instance.
///
/// When no endpoint is provided (via <c>AddDistill(name, apiKey)</c>), the resource can
/// launch a Distill container if <c>.WithImage(...)</c> is configured. Sinks registered
/// via <c>.WithSink(...)</c> are passed as environment variables and work in both modes.
public class DistillResource : ContainerResource, IResourceWithConnectionString
{
    internal const string EndpointConfigKey = "Distill__Endpoint";
    internal const string ApiKeyConfigKey = "Distill__ApiKey";

    internal Uri? ExplicitEndpoint { get; set; }

    /// Initializes a new Distill resource with the given name.
    public DistillResource(string name) : base(name)
    {
    }

    /// Gets the connection string expression for the Distill endpoint.
    ///
    /// For container-mode, resolves dynamically from the HTTP endpoint.
    /// For external-mode, uses the explicitly-provided endpoint URI.
    public ReferenceExpression ConnectionStringExpression
    {
        get
        {
            if (ExplicitEndpoint is not null)
                return ReferenceExpression.Create($"{ExplicitEndpoint.ToString()}");

            var endpoint = new EndpointReference(this, "http");
            return ReferenceExpression.Create($"{endpoint.Property(EndpointProperty.Url)}");
        }
    }
}
