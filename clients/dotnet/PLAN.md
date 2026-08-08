# Distill .NET SDK Enhancement Plan

Completed across three phases:

**Phase 1** (SDK simplification): `[JsonIgnore(Condition = Never)]` on `Aggregator` property replaces the `DisableAggregator`/`JsonNode` hack. Users set `Aggregator = null` to disable a preset aggregator. `CreateOrReplaceChannelAsync` doc points users to the typed overload.

**Phase 2** (ILogger extension):

| Item | Implementation |
|------|----------------|
| 2.1 Per-channel config | `PerChannelOptions` record + `Dictionary<string, PerChannelOptions>` on `DistillLoggerOptions`; `GetChannelOptions()` helper with fallback |
| 2.2 Per-channel backpressure | Each channel gets its own `Channel<QueuedLog>`; `wakeSignal` (`Channel<byte>` capacity-1) notifies worker to check all channels; no noisy-neighbor starvation |
| 2.3 Auto-channel-creation | `ValidateAndCreateChannelsAsync` at startup: probes `GetChannelAsync` for each configured channel, calls `CreateChannelAsync` if factory is set |
| 2.4 Channel-aware `DeliveryFailure` | `DeliveryFailureContext` with `Channel`, `EventCount`, `Attempts`, `Exception` |
| 2.5 Category-level log levels | `Dictionary<string, LogLevel>` with dotted-path namespace walk; `GetCategoryLevel()` in `IsEnabled` |
| 2.6 Defensive resolver | `ResolveChannel()` with try/catch, fallback to `DefaultChannel`, `ChannelResolutionFailed` callback |
| 2.7 `IAsyncDisposable` | `DisposeAsync()`: completes all channel writers → completes wakeSignal → awaits worker → cancels token; `Dispose()` delegates |
| 2.8 Delivery success | `DeliverySuccessContext` with `Channel`, `EventCount`, `Response`; `DeliverySuccess` callback |

**Phase 3** (Aspire):

| Capability | Implementation |
|------------|----------------|
| Container launch | `DistillResource : ContainerResource, IResourceWithConnectionString` — default image `distill:latest`, configurable via `.WithImage(...)` |
| External mode | `AddDistill(name, endpoint, apiKey)` — no container, uses explicit URI |
| Container mode | `AddDistill(name, apiKey)` — launches container, endpoint resolved via Aspire service discovery |
| Single PostgreSQL sink | `WithSink(IResourceBuilder<IResourceWithConnectionString>, name?, driver?)` — becomes default |
| Multiple DB sinks | `WithSink(Dictionary<string, IResourceBuilder<IResourceWithConnectionString>>, driver, defaultSinkName)` |
| Single custom sink | `WithSink(DistillSinkSpecification)` — HTTP, etc. — becomes default |
| Multiple custom sinks | `WithSink(Dictionary<string, DistillSinkSpecification>, defaultSinkName)` |
| Sink env vars | `DISTILL_SINK__{NAME}__DRIVER`, `DISTILL_SINK__{NAME}__CONNECTION_STRING`, etc. on the Distill container |
| Default sink | `DISTILL_DEFAULT_SINK` env var on the Distill container |
| Health check | `DistillHealthCheck` — any HTTP response = healthy |
| Client registration | `AddDistillClient()` on `IHostApplicationBuilder` — reads `ConnectionStrings__distill` + `Distill__ApiKey` |
| Local Docker build | `WithLocalBuild(dockerfile?, context?)` — walks up from current dir to find `Dockerfile`, runs `docker build` if `distill:latest` doesn't exist |

**Files:**

| File | Purpose |
|------|---------|
| `Distill.Aspire/Distill.Aspire.csproj` | Targets `net8.0`; references `Distill.Client`, `Distill.Extensions.Logging`, `Aspire.Hosting` 8.2.2 |
| `Distill.Aspire/DistillResource.cs` | `ContainerResource` + `IResourceWithConnectionString`; dynamic connection string for container mode, explicit URI for external mode |
| `Distill.Aspire/DistillSinkSpecification.cs` | `DistillSinkSpecification` record — Name, Driver, ConnectionString, Url, BearerToken, IsDefault |
| `Distill.Aspire/DistillAspireExtensions.cs` | Two `AddDistill` overloads + four `WithSink` overloads + `DistillSinkAnnotation` internal + `DistillDefaults` |
| `Distill.Aspire/DistillClientExtensions.cs` | `AddDistillClient()` on `IHostApplicationBuilder` |
| `Distill.Aspire/DistillHealthCheck.cs` | Probes `GetChannelAsync("application")`; any HTTP response = healthy |
| `Distill.Aspire/README.md` | Usage docs with all modes |

## Breaking changes

- `DisableAggregator` removed — set `Aggregator = null` instead
- `DeliveryFailure` type changed from `Action<Exception>?` to `Action<DeliveryFailureContext>?`
- `DistillResource` changed from `Resource` to `ContainerResource` (implements `IResourceWithConnectionString`)

## Known limitations

- **API key flow**: The API key must be set on consuming projects via `.WithEnvironment("Distill__ApiKey", value)` — it does not flow automatically through `.WithReference(distill)`. A follow-up could use `ParameterResource` for the API key and inject it via Aspire's annotation system.
- **Container image**: The default `distill:latest` must be built locally (`docker build -t distill:latest .`) or overridden via `.WithImage(...)` — there is no published image yet.

## Deferred

- **OpenTelemetry metrics** — can add `Meter` for batch sizes, latency, drops when needed
