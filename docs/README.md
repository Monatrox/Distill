# Documentation

Start with the [project README](../README.md) for the current V1 contracts, architecture, setup and deployment limits.

- [CLI guide](cli.md): sink manifests, explicit migrations, local secret helpers and dead-letter operations.
- [C#/.NET integration guide](csharp_integration.md): client, logging and Aspire integration examples.
- [Security](security.md): authentication boundaries, credential storage and safe deployment.
- [.NET SDK](../clients/dotnet/README.md), [logging provider](../clients/dotnet/Distill.Extensions.Logging/README.md) and [Aspire](../clients/dotnet/Distill.Aspire/README.md): package-specific contracts and usage.

Local planning notes, AI handoffs, qualification evidence and superseded specifications belong in `docs/archive/`. That directory is ignored by Git and excluded from the container build context; it is not part of the public documentation or a fresh clone. Keep new public guides outside it. Previously committed documents remain in Git history.
