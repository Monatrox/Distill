//! Generates the `OpenAPI` contract for the HTTP surface.
//!
//! The contract is derived from the `#[utoipa::path]` annotations on the
//! handlers in this module's siblings, so the handler signatures and the
//! published document cannot drift apart.

// Rust guideline compliant 2026-02-21

use super::dto::{
    ChannelConfigResponse, ChannelPreset, ChannelValidationResponse, CloneChannelRequest,
    CreateChannelRequest, EventTestResponse, IngestItemError, IngestResponse, PublicChannelConfig,
    SimpleEventRequest,
};
use super::{
    ChannelLifecycle,
    channels::{
        __path_archive_channel, __path_clone_channel, __path_flush_channel, __path_get_channel,
        __path_pause_channel, __path_post_channel, __path_post_schema, __path_put_channel,
        __path_register_sink, __path_resume_channel, __path_test_event, __path_validate_channel,
        RegisterSinkRequest, RegisterSinkResponse,
    },
    ingest::{
        __path_health, __path_ingest, __path_metrics, __path_otlp_logs, __path_post_event,
        __path_ready,
    },
};
use crate::{
    core::errors::{ErrorCode, ErrorEnvelope},
    domain::{
        AggregationFunction, AggregatorSpec, ChannelDefinition, Granularity, LogEvent,
        MessageSchemaDefinition, OrderingMode, RabbitMqConfig, SeverityLevel, SinkConfig,
    },
};
use utoipa::OpenApi;

#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Distill",
        description = "Distill ingestion and channel-management API.\n\nRequests without an X-API-VERSION header are served at the operator-configured default version (DISTILL_API_DEFAULT_VERSION). A request may pin any supported version with the header; pinned versions never change behavior without an explicit deprecation cycle.",
        version = "1.0.0"
    ),
    paths(
        health,
        ready,
        metrics,
        ingest,
        otlp_logs,
        post_event,
        post_channel,
        register_sink,
        validate_channel,
        put_channel,
        test_event,
        post_schema,
        get_channel,
        pause_channel,
        resume_channel,
        flush_channel,
        archive_channel,
        clone_channel
    ),
    components(schemas(
        ErrorCode,
        ErrorEnvelope,
        IngestResponse,
        IngestItemError,
        SimpleEventRequest,
        CreateChannelRequest,
        RegisterSinkRequest,
        RegisterSinkResponse,
        ChannelPreset,
        ChannelConfigResponse,
        PublicChannelConfig,
        ChannelValidationResponse,
        EventTestResponse,
        CloneChannelRequest,
        ChannelDefinition,
        RabbitMqConfig,
        MessageSchemaDefinition,
        SinkConfig,
        AggregatorSpec,
        AggregationFunction,
        Granularity,
        OrderingMode,
        SeverityLevel,
        ChannelLifecycle,
        LogEvent
    )),
    tags(
        (name = "ingest", description = "Event ingestion endpoints"),
        (name = "channels", description = "Channel lifecycle and configuration endpoints"),
        (name = "operations", description = "Health and metrics endpoints")
    )
)]
pub(super) struct ApiDocumentation;

/// Builds the Swagger UI router exposing the contract at `/openapi.json`.
pub(super) fn swagger_ui() -> utoipa_swagger_ui::SwaggerUi {
    utoipa_swagger_ui::SwaggerUi::new("/swagger-ui")
        .url("/openapi.json", ApiDocumentation::openapi())
}

#[cfg(test)]
mod tests {
    use super::ApiDocumentation;
    use utoipa::OpenApi;

    #[test]
    fn contract_covers_every_route() {
        let document = ApiDocumentation::openapi();
        for path in [
            "/health",
            "/ready",
            "/sinks",
            "/metrics",
            "/ingest",
            "/v1/logs",
            "/channels/{channel_id}/events",
            "/channels",
            "/channels/validate",
            "/channels/{channel_id}",
            "/channels/{channel_id}/test",
            "/channels/{channel_id}/config",
            "/channels/{channel_id}/schema",
            "/channels/{channel_id}/pause",
            "/channels/{channel_id}/resume",
            "/channels/{channel_id}/flush",
            "/channels/{channel_id}/archive",
            "/channels/{channel_id}/clone",
        ] {
            assert!(
                document.paths.paths.contains_key(path),
                "contract is missing route {path}"
            );
        }
        assert!(document.components.is_some());
    }
}
