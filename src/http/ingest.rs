//! Contains HTTP endpoint handlers.

// Rust guideline compliant 2026-02-21
use super::{
    AppState, Bytes, CONTENT_TYPE, DistillError, HeaderMap, IngestItemError, IngestResponse,
    IntoResponse, Json, LogEvent, Message, Path, SimpleEventRequest, State, StatusCode, Uuid,
    Value, client_message, error_code, otlp_events,
};

pub(super) async fn health() -> impl IntoResponse {
    StatusCode::OK
}

pub(super) async fn metrics(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, DistillError> {
    Ok((
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.prometheus(),
    ))
}

pub(super) async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<IngestResponse>), DistillError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");
    let events: Vec<LogEvent> = if content_type.starts_with("application/protobuf")
        || content_type.starts_with("application/x-protobuf")
    {
        crate::proto::IngestRequest::decode(body.as_ref())
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?
            .events
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?
    } else {
        serde_json::from_slice(&body)
            .map_err(|error| DistillError::InvalidRequest(error.to_string()))?
    };
    if events.is_empty() {
        return Err(DistillError::InvalidRequest(
            "at least one event is required".to_string(),
        ));
    }
    let response = submit_events(&state, events).await?;
    let status = if response.rejected == 0 {
        StatusCode::ACCEPTED
    } else {
        StatusCode::MULTI_STATUS
    };
    Ok((status, Json(response)))
}

pub(super) async fn otlp_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<(StatusCode, Json<IngestResponse>), DistillError> {
    let default_channel = headers
        .get("X-Distill-Channel")
        .and_then(|value| value.to_str().ok());
    let events = otlp_events(&request, default_channel)?;
    let response = submit_events(&state, events).await?;
    let status = if response.rejected == 0 {
        StatusCode::ACCEPTED
    } else {
        StatusCode::MULTI_STATUS
    };
    Ok((status, Json(response)))
}

pub(super) async fn post_event(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(request): Json<SimpleEventRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), DistillError> {
    let event = request.into_event(channel_id);
    let response = submit_events(&state, vec![event]).await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

pub(super) async fn submit_events(
    state: &AppState,
    events: Vec<LogEvent>,
) -> Result<IngestResponse, DistillError> {
    let request_id = Uuid::new_v4();
    let mut response = IngestResponse {
        accepted: 0,
        rejected: 0,
        request_id,
        durability: "queued",
        errors: Vec::new(),
    };
    for (index, event) in events.into_iter().enumerate() {
        if let Err(error) = state.submit_event(event).await {
            response.rejected += 1;
            response.errors.push(IngestItemError {
                index,
                error_code: error_code(&error).to_string(),
                message: client_message(&error),
            });
        } else {
            response.accepted += 1;
        }
    }
    if response.accepted == 0 {
        let error = response.errors.first().ok_or_else(|| {
            DistillError::InvalidRequest("at least one event is required".to_string())
        })?;
        return Err(DistillError::InvalidRequest(format!(
            "{}: {}",
            error.error_code, error.message
        )));
    }
    Ok(response)
}
