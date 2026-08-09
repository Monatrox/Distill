//! Contains HTTP endpoint handlers.

// Rust guideline compliant 2026-02-21
use super::{
    AppState, ChannelConfigResponse, ChannelDefinition, ChannelLifecycle,
    ChannelValidationResponse, CloneChannelRequest, CreateChannelRequest, DistillError,
    ErrorEnvelope, EventTestResponse, Json, MessageSchemaDefinition, Path, PublicChannelConfig,
    RouteDecision, SimpleEventRequest, State, StatusCode, classify,
};
use tracing::error;

/// Creates a channel from a simple request or preset.
#[utoipa::path(
    post,
    path = "/channels",
    tag = "channels",
    request_body = CreateChannelRequest,
    responses(
        (status = 201, body = ChannelConfigResponse, description = "Channel created"),
        (status = 400, body = ErrorEnvelope, description = "The request was invalid"),
        (status = 422, body = ErrorEnvelope, description = "The channel configuration is invalid")
    )
)]
pub(super) async fn post_channel(
    State(state): State<AppState>,
    Json(request): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<ChannelConfigResponse>), DistillError> {
    let definition = state.build_channel_definition(&request)?;
    let channel_id = request.name.clone();
    if let Err(error) = state.registry.register_channel(definition).await {
        log_registration_failure(&channel_id, &error);
        return Err(error);
    }
    let record = state.registry.channel_record(&channel_id).await?;
    let response = ChannelConfigResponse::from_record(&record);
    Ok((StatusCode::CREATED, Json(response)))
}

/// Validates a channel request without creating the channel.
#[utoipa::path(
    post,
    path = "/channels/validate",
    tag = "channels",
    request_body = CreateChannelRequest,
    responses(
        (status = 200, body = ChannelValidationResponse, description = "The channel configuration is valid"),
        (status = 422, body = ErrorEnvelope, description = "The channel configuration is invalid")
    )
)]
pub(super) async fn validate_channel(
    State(state): State<AppState>,
    Json(request): Json<CreateChannelRequest>,
) -> Result<Json<ChannelValidationResponse>, DistillError> {
    let definition = state.build_channel_definition(&request)?;
    definition
        .validate()
        .map_err(DistillError::InvalidConfiguration)?;
    Ok(Json(ChannelValidationResponse {
        valid: true,
        channel_id: definition.channel_id.clone(),
        preset: request.preset.as_str().to_string(),
        warnings: Vec::new(),
        effective: PublicChannelConfig::from_definition(&definition),
    }))
}

/// Replaces an existing channel with a complete explicit definition.
#[utoipa::path(
    put,
    path = "/channels/{channel_id}",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    request_body = ChannelDefinition,
    responses(
        (status = 204, description = "Channel replaced"),
        (status = 422, body = ErrorEnvelope, description = "The channel definition is invalid")
    )
)]
pub(super) async fn put_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(definition): Json<ChannelDefinition>,
) -> Result<StatusCode, DistillError> {
    if definition.channel_id != channel_id {
        return Err(DistillError::InvalidConfiguration(
            "path channel_id must match the request body".to_string(),
        ));
    }
    if let Err(error) = state.registry.register_channel(definition).await {
        log_registration_failure(&channel_id, &error);
        return Err(error);
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Dry-runs an event against a channel to preview its routing decision.
#[utoipa::path(
    post,
    path = "/channels/{channel_id}/test",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    request_body = SimpleEventRequest,
    responses(
        (status = 200, body = EventTestResponse, description = "The event is valid and its route is reported"),
        (status = 400, body = ErrorEnvelope, description = "The event was invalid"),
        (status = 404, body = ErrorEnvelope, description = "The channel does not exist")
    )
)]
pub(super) async fn test_event(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(request): Json<SimpleEventRequest>,
) -> Result<Json<EventTestResponse>, DistillError> {
    let record = state.registry.channel_record(&channel_id).await?;
    let event = request.into_event(channel_id);
    event.validate().map_err(DistillError::InvalidRequest)?;
    state
        .registry
        .validate_payload(&event.channel_id, &event.payload, None)
        .await?;
    let route = match classify(&event, &record.definition) {
        RouteDecision::DirectPersist => "direct_persist",
        RouteDecision::Aggregate => "aggregate",
        RouteDecision::PersistAndAggregate => "persist_and_aggregate",
    };
    Ok(Json(EventTestResponse {
        valid: true,
        channel_id: event.channel_id,
        route,
        severity_threshold: record.definition.severity_threshold,
        schema_configured: record.definition.message_schema.is_some(),
    }))
}

/// Installs or replaces the runtime message schema for a channel.
#[utoipa::path(
    post,
    path = "/channels/{channel_id}/schema",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    request_body = MessageSchemaDefinition,
    responses(
        (status = 204, description = "Schema registered"),
        (status = 400, body = ErrorEnvelope, description = "The schema descriptor is invalid"),
        (status = 404, body = ErrorEnvelope, description = "The channel does not exist")
    )
)]
pub(super) async fn post_schema(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(definition): Json<MessageSchemaDefinition>,
) -> Result<StatusCode, DistillError> {
    state
        .registry
        .register_schema(&channel_id, definition)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Returns the resolved effective configuration of a channel.
#[utoipa::path(
    get,
    path = "/channels/{channel_id}/config",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    responses(
        (status = 200, body = ChannelConfigResponse, description = "The channel configuration"),
        (status = 404, body = ErrorEnvelope, description = "The channel does not exist")
    )
)]
pub(super) async fn get_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    let record = state.registry.channel_record(&channel_id).await?;
    Ok(Json(ChannelConfigResponse::from_record(&record)))
}

/// Pauses event ingestion for a channel.
#[utoipa::path(
    post,
    path = "/channels/{channel_id}/pause",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    responses(
        (status = 200, body = ChannelConfigResponse, description = "Channel paused"),
        (status = 404, body = ErrorEnvelope, description = "The channel does not exist")
    )
)]
pub(super) async fn pause_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    let record = state
        .registry
        .set_lifecycle(&channel_id, ChannelLifecycle::Paused)
        .await?;
    Ok(Json(ChannelConfigResponse::from_record(&record)))
}

/// Resumes event ingestion for a paused channel.
#[utoipa::path(
    post,
    path = "/channels/{channel_id}/resume",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    responses(
        (status = 200, body = ChannelConfigResponse, description = "Channel resumed"),
        (status = 404, body = ErrorEnvelope, description = "The channel does not exist")
    )
)]
pub(super) async fn resume_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    let record = state
        .registry
        .set_lifecycle(&channel_id, ChannelLifecycle::Active)
        .await?;
    Ok(Json(ChannelConfigResponse::from_record(&record)))
}

/// Flushes buffered events of a channel to its sink.
#[utoipa::path(
    post,
    path = "/channels/{channel_id}/flush",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    responses(
        (status = 200, body = ChannelConfigResponse, description = "Channel flushed"),
        (status = 404, body = ErrorEnvelope, description = "The channel does not exist")
    )
)]
pub(super) async fn flush_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    let record = state.registry.channel_record(&channel_id).await?;
    record.handle.flush().await?;
    Ok(Json(ChannelConfigResponse::from_record(&record)))
}

/// Flushes and archives a channel, stopping event ingestion.
#[utoipa::path(
    post,
    path = "/channels/{channel_id}/archive",
    tag = "channels",
    params(("channel_id", Path, description = "Channel identifier")),
    responses(
        (status = 200, body = ChannelConfigResponse, description = "Channel archived"),
        (status = 404, body = ErrorEnvelope, description = "The channel does not exist")
    )
)]
pub(super) async fn archive_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    let record = state.registry.channel_record(&channel_id).await?;
    record.handle.flush().await?;
    let archived = state
        .registry
        .set_lifecycle(&channel_id, ChannelLifecycle::Archived)
        .await?;
    Ok(Json(ChannelConfigResponse::from_record(&archived)))
}

/// Clones a channel's configuration under a new identifier.
#[utoipa::path(
    post,
    path = "/channels/{channel_id}/clone",
    tag = "channels",
    params(("channel_id", Path, description = "Source channel identifier")),
    request_body = CloneChannelRequest,
    responses(
        (status = 201, body = ChannelConfigResponse, description = "Channel cloned"),
        (status = 400, body = ErrorEnvelope, description = "The request was invalid"),
        (status = 404, body = ErrorEnvelope, description = "The source channel does not exist")
    )
)]
pub(super) async fn clone_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(request): Json<CloneChannelRequest>,
) -> Result<(StatusCode, Json<ChannelConfigResponse>), DistillError> {
    let source = state.registry.channel_record(&channel_id).await?;
    let mut definition = source.definition;
    definition.channel_id = request.channel_id.clone();
    definition.rabbitmq_config.exchange_name = format!(
        "{}.{}",
        state.registry.exchange_name_prefix, definition.channel_id
    );
    definition.rabbitmq_config.routing_key_prefix = format!("{}.", definition.channel_id);
    let target_channel_id = request.channel_id.clone();
    if let Err(error) = state.registry.register_channel(definition).await {
        log_registration_failure(&target_channel_id, &error);
        return Err(error);
    }
    let record = state.registry.channel_record(&target_channel_id).await?;
    Ok((
        StatusCode::CREATED,
        Json(ChannelConfigResponse::from_record(&record)),
    ))
}

fn log_registration_failure(channel_id: &str, error: &DistillError) {
    error!(
        event.name = "distill.channel.registration_failed",
        channel.id = %channel_id,
        error.code = %error.error_code(),
        error = %error,
        "channel registration failed"
    );
}
