//! Contains HTTP endpoint handlers.

// Rust guideline compliant 2026-02-21
use super::{
    AppState, ChannelConfigResponse, ChannelDefinition, ChannelLifecycle,
    ChannelValidationResponse, CloneChannelRequest, CreateChannelRequest, DistillError,
    EventTestResponse, HeaderMap, Json, MessageSchemaDefinition, Path, PublicChannelConfig,
    RouteDecision, SimpleEventRequest, State, StatusCode, authorize, classify,
};

pub(super) async fn post_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<ChannelConfigResponse>), DistillError> {
    authorize(&headers, &state)?;
    let definition = state.build_channel_definition(&request)?;
    state.register_channel(definition).await?;
    let channel_id = request.name.clone();
    let response = state.channel_config(&channel_id).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

pub(super) async fn validate_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<Json<ChannelValidationResponse>, DistillError> {
    authorize(&headers, &state)?;
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

pub(super) async fn put_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    Json(definition): Json<ChannelDefinition>,
) -> Result<StatusCode, DistillError> {
    authorize(&headers, &state)?;
    if definition.channel_id != channel_id {
        return Err(DistillError::InvalidConfiguration(
            "path channel_id must match the request body".to_string(),
        ));
    }
    state.register_channel(definition).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn test_event(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<SimpleEventRequest>,
) -> Result<Json<EventTestResponse>, DistillError> {
    authorize(&headers, &state)?;
    let record = state
        .channels
        .read()
        .await
        .get(&channel_id)
        .cloned()
        .ok_or_else(|| DistillError::ChannelNotFound(channel_id.clone()))?;
    let event = request.into_event(channel_id);
    event.validate().map_err(DistillError::InvalidRequest)?;
    state
        .schemas
        .validate_json(&event.channel_id, &event.payload, None)
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

pub(super) async fn post_schema(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    Json(definition): Json<MessageSchemaDefinition>,
) -> Result<StatusCode, DistillError> {
    authorize(&headers, &state)?;
    if !state.channels.read().await.contains_key(&channel_id) {
        return Err(DistillError::ChannelNotFound(channel_id));
    }
    state
        .schemas
        .register(&channel_id, definition.clone())
        .await?;
    if let Some(record) = state.channels.write().await.get_mut(&channel_id) {
        record.definition.message_schema = Some(definition);
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn get_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    authorize(&headers, &state)?;
    Ok(Json(state.channel_config(&channel_id).await?))
}

pub(super) async fn pause_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    authorize(&headers, &state)?;
    Ok(Json(
        state
            .set_lifecycle(&channel_id, ChannelLifecycle::Paused)
            .await?,
    ))
}

pub(super) async fn resume_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    authorize(&headers, &state)?;
    Ok(Json(
        state
            .set_lifecycle(&channel_id, ChannelLifecycle::Active)
            .await?,
    ))
}

pub(super) async fn flush_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    authorize(&headers, &state)?;
    let record = state
        .channels
        .read()
        .await
        .get(&channel_id)
        .cloned()
        .ok_or_else(|| DistillError::ChannelNotFound(channel_id.clone()))?;
    record.handle.flush().await?;
    Ok(Json(state.channel_config(&channel_id).await?))
}

pub(super) async fn archive_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    authorize(&headers, &state)?;
    let record = state
        .channels
        .read()
        .await
        .get(&channel_id)
        .cloned()
        .ok_or_else(|| DistillError::ChannelNotFound(channel_id.clone()))?;
    record.handle.flush().await?;
    Ok(Json(
        state
            .set_lifecycle(&channel_id, ChannelLifecycle::Archived)
            .await?,
    ))
}

pub(super) async fn clone_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CloneChannelRequest>,
) -> Result<(StatusCode, Json<ChannelConfigResponse>), DistillError> {
    authorize(&headers, &state)?;
    let source = state
        .channels
        .read()
        .await
        .get(&channel_id)
        .cloned()
        .ok_or_else(|| DistillError::ChannelNotFound(channel_id.clone()))?;
    let mut definition = source.definition;
    definition.channel_id = request.channel_id.clone();
    definition.rabbitmq_config.exchange_name =
        format!("{}.{}", state.exchange_name_prefix, definition.channel_id);
    definition.rabbitmq_config.routing_key_prefix = format!("{}.", definition.channel_id);
    state.register_channel(definition).await?;
    Ok((
        StatusCode::CREATED,
        Json(state.channel_config(&request.channel_id).await?),
    ))
}
