//! Exposes authenticated ingestion and channel-management HTTP endpoints.

// Rust guideline compliant 2026-02-21

use crate::{
    buffer::MemoryBudget,
    channel::{self, ChannelHandle},
    config::{self, Settings},
    domain::{
        AggregatorSpec, ChannelDefinition, Granularity, LogEvent, MessageSchemaDefinition,
        OrderingMode, RabbitMqConfig, SeverityLevel, SinkConfig,
    },
    errors::DistillError,
    metrics::Metrics,
    routing::{RouteDecision, classify},
    schema::SchemaRegistry,
    sink::{RabbitConsumer, RabbitPublisher, Sink, build_sink},
};
use anyhow::Context;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::{get, post, put},
};
use chrono::Utc;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::HashMap, sync::Arc};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use uuid::Uuid;

const STARTUP_CONNECTION_ATTEMPTS: u32 = 30;
const STARTUP_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct AppState {
    channels: Arc<RwLock<HashMap<String, ChannelRecord>>>,
    rabbit: RabbitPublisher,
    consumer: Arc<RabbitConsumer>,
    schemas: SchemaRegistry,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
    api_key: Arc<String>,
    rabbitmq_uri: String,
    default_sink_connection_string: Option<String>,
    exchange_name_prefix: String,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
    default_sink_id: Option<String>,
}

#[derive(Clone)]
struct ChannelRecord {
    definition: ChannelDefinition,
    handle: ChannelHandle,
    lifecycle: ChannelLifecycle,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ChannelLifecycle {
    Active,
    Paused,
    Archived,
}

impl AppState {
    pub async fn connect(settings: Settings) -> anyhow::Result<Self> {
        let rabbit_uri = settings.rabbitmq_uri.clone();
        let rabbit = retry_startup_connection("RabbitMQ publisher", || {
            let rabbit_uri = rabbit_uri.clone();
            async move { RabbitPublisher::connect(&rabbit_uri).await }
        })
        .await?;
        let metrics = Arc::new(Metrics::default());
        let rabbit_uri = settings.rabbitmq_uri.clone();
        let consumer_metrics = Arc::clone(&metrics);
        let consumer = Arc::new(
            retry_startup_connection("RabbitMQ consumer", || {
                let rabbit_uri = rabbit_uri.clone();
                let consumer_metrics = Arc::clone(&consumer_metrics);
                async move { RabbitConsumer::connect(&rabbit_uri, consumer_metrics).await }
            })
            .await?,
        );
        let sinks = Arc::new(RwLock::new(HashMap::new()));
        let mut default_sink_id = None;
        if let Some(path) = settings.sinks_config_path.clone() {
            let contents = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("reading sink configuration from {}", path.display()))?;
            let sink_file = config::parse_sinks(&contents)?;
            default_sink_id = sink_file.default_sink_id;
            for definition in sink_file.sinks {
                let sink_config = config::resolve_sink(&definition)?;
                let sink = build_sink(&sink_config).await?;
                sinks.write().await.insert(definition.sink_id, sink);
            }
        }
        let state = Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            rabbit,
            consumer,
            schemas: SchemaRegistry::default(),
            metrics,
            budget: Arc::new(MemoryBudget::new(
                crate::domain::DEFAULT_PROCESS_MEMORY_BYTES,
            )),
            api_key: Arc::new(settings.api_key),
            rabbitmq_uri: settings.rabbitmq_uri,
            default_sink_connection_string: settings.default_sink_connection_string,
            exchange_name_prefix: settings.exchange_name_prefix,
            sinks,
            default_sink_id,
        };

        if let Some(path) = settings.config_path {
            let contents = tokio::fs::read_to_string(&path).await.with_context(|| {
                format!("reading channel configuration from {}", path.display())
            })?;
            for definition in
                config::parse_channels_with_default(&contents, state.default_sink_id.as_deref())?
            {
                state.register_channel(definition).await?;
            }
        }
        Ok(state)
    }

    async fn register_channel(&self, definition: ChannelDefinition) -> Result<(), DistillError> {
        definition
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
        let channel_id = definition.channel_id.clone();
        if let Some(schema) = definition.message_schema.clone() {
            self.schemas.register(&channel_id, schema).await?;
        } else {
            self.schemas.remove(&channel_id).await;
        }
        let sink: Arc<dyn Sink> = if let Some(sink_id) = &definition.sink_id {
            self.sinks
                .read()
                .await
                .get(sink_id)
                .cloned()
                .ok_or_else(|| {
                    DistillError::InvalidConfiguration(format!(
                        "channel '{channel_id}' references undefined sink '{sink_id}'"
                    ))
                })?
        } else {
            build_sink(&definition.sink_config).await?
        };
        let old = self.channels.read().await.get(&channel_id).cloned();
        if let Some(previous) = old {
            previous.handle.flush().await?;
        }
        self.consumer
            .register_exchange(&definition.rabbitmq_config.exchange_name)
            .await?;
        self.consumer.register_sink(channel_id.clone(), sink).await;
        let handle = channel::spawn(
            definition.clone(),
            self.rabbit.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.budget),
        );
        self.channels.write().await.insert(
            channel_id.clone(),
            ChannelRecord {
                definition,
                handle,
                lifecycle: ChannelLifecycle::Active,
            },
        );
        info!(event.name = "distill.channel.registered", channel.id = %channel_id, "channel registered");
        Ok(())
    }

    fn build_channel_definition(
        &self,
        request: &CreateChannelRequest,
    ) -> Result<ChannelDefinition, DistillError> {
        request.to_definition(
            self.default_sink_connection_string.as_deref(),
            self.default_sink_id.as_deref(),
            &self.exchange_name_prefix,
            &self.rabbitmq_uri,
        )
    }

    async fn submit_event(&self, event: LogEvent) -> Result<(), DistillError> {
        event.validate().map_err(DistillError::InvalidRequest)?;
        let record = self
            .channels
            .read()
            .await
            .get(&event.channel_id)
            .cloned()
            .ok_or_else(|| DistillError::ChannelNotFound(event.channel_id.clone()))?;
        match record.lifecycle {
            ChannelLifecycle::Active => {}
            ChannelLifecycle::Paused => {
                return Err(DistillError::InvalidRequest(format!(
                    "channel {} is paused",
                    event.channel_id
                )));
            }
            ChannelLifecycle::Archived => {
                return Err(DistillError::InvalidRequest(format!(
                    "channel {} is archived",
                    event.channel_id
                )));
            }
        }
        self.schemas
            .validate_json(
                &event.channel_id,
                &event.payload,
                event.payload_bytes.as_deref(),
            )
            .await?;
        record.handle.send(event).await
    }

    async fn set_lifecycle(
        &self,
        channel_id: &str,
        lifecycle: ChannelLifecycle,
    ) -> Result<ChannelConfigResponse, DistillError> {
        let mut channels = self.channels.write().await;
        let record = channels
            .get_mut(channel_id)
            .ok_or_else(|| DistillError::ChannelNotFound(channel_id.to_string()))?;
        record.lifecycle = lifecycle;
        Ok(ChannelConfigResponse::from_record(record))
    }
}

async fn retry_startup_connection<T, F, Fut>(label: &str, mut connect: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, DistillError>>,
{
    let mut last_error = None;
    for attempt in 1..=STARTUP_CONNECTION_ATTEMPTS {
        match connect().await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                last_error = Some(error);
                if attempt < STARTUP_CONNECTION_ATTEMPTS {
                    warn!(
                        event.name = "distill.startup.connection.retry",
                        dependency = label,
                        attempt,
                        "dependency is not ready; retrying"
                    );
                    sleep(STARTUP_RETRY_DELAY).await;
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "{label} did not become available after {STARTUP_CONNECTION_ATTEMPTS} attempts: {}",
        last_error.expect("startup attempts always record an error")
    ))
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/ingest", post(ingest))
        .route("/v1/logs", post(otlp_logs))
        .route("/channels", post(post_channel))
        .route("/channels/validate", post(validate_channel))
        .route("/channels/{channel_id}", put(put_channel))
        .route("/channels/{channel_id}/events", post(post_event))
        .route("/channels/{channel_id}/test", post(test_event))
        .route("/channels/{channel_id}/config", get(get_channel))
        .route("/channels/{channel_id}/schema", post(post_schema))
        .route("/channels/{channel_id}/pause", post(pause_channel))
        .route("/channels/{channel_id}/resume", post(resume_channel))
        .route("/channels/{channel_id}/flush", post(flush_channel))
        .route("/channels/{channel_id}/archive", post(archive_channel))
        .route("/channels/{channel_id}/clone", post(clone_channel))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.prometheus(),
    )
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<IngestResponse>), DistillError> {
    authorize(&headers, &state)?;
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

async fn otlp_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<(StatusCode, Json<IngestResponse>), DistillError> {
    authorize(&headers, &state)?;
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

fn otlp_events(
    request: &Value,
    default_channel: Option<&str>,
) -> Result<Vec<LogEvent>, DistillError> {
    let resource_logs = request
        .get("resourceLogs")
        .and_then(Value::as_array)
        .ok_or_else(|| DistillError::InvalidRequest("OTLP resourceLogs is required".to_string()))?;
    let mut events = Vec::new();
    for resource_log in resource_logs {
        let resource_attributes = otlp_attributes(
            resource_log
                .get("resource")
                .and_then(|resource| resource.get("attributes")),
        );
        let channel = default_channel
            .map(str::to_string)
            .or_else(|| {
                resource_attributes
                    .get("distill.channel")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                DistillError::InvalidRequest(
                    "OTLP logs require X-Distill-Channel or resource attribute distill.channel"
                        .to_string(),
                )
            })?;
        let scope_logs = resource_log
            .get("scopeLogs")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                DistillError::InvalidRequest("OTLP scopeLogs is required".to_string())
            })?;
        for scope_log in scope_logs {
            let records = scope_log
                .get("logRecords")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    DistillError::InvalidRequest("OTLP logRecords is required".to_string())
                })?;
            for record in records {
                let timestamp = otlp_timestamp(record).ok_or_else(|| {
                    DistillError::InvalidRequest(
                        "OTLP log record requires timeUnixNano or observedTimeUnixNano".to_string(),
                    )
                })?;
                let mut attributes = resource_attributes.clone();
                attributes.extend(otlp_attributes(record.get("attributes")));
                let payload = otlp_payload(record.get("body"), &attributes);
                let mut metadata = HashMap::new();
                for (key, value) in &attributes {
                    if let Some(value) = value.as_str() {
                        metadata.insert(key.clone(), value.to_string());
                    }
                }
                if let Some(trace_id) = record.get("traceId").and_then(Value::as_str) {
                    metadata.insert("trace_id".to_string(), trace_id.to_string());
                }
                if let Some(span_id) = record.get("spanId").and_then(Value::as_str) {
                    metadata.insert("span_id".to_string(), span_id.to_string());
                }
                events.push(LogEvent {
                    event_id: Uuid::new_v4(),
                    timestamp,
                    channel_id: channel.clone(),
                    severity_level: otlp_severity(
                        record.get("severityNumber").and_then(Value::as_i64),
                    ),
                    payload,
                    payload_bytes: None,
                    metadata,
                });
            }
        }
    }
    if events.is_empty() {
        return Err(DistillError::InvalidRequest(
            "OTLP request contains no log records".to_string(),
        ));
    }
    Ok(events)
}

fn otlp_attributes(value: Option<&Value>) -> HashMap<String, Value> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|attribute| {
            Some((
                attribute.get("key")?.as_str()?.to_string(),
                otlp_any_value(attribute.get("value")?),
            ))
        })
        .collect()
}

fn otlp_any_value(value: &Value) -> Value {
    value
        .get("stringValue")
        .or_else(|| value.get("boolValue"))
        .or_else(|| value.get("intValue"))
        .or_else(|| value.get("doubleValue"))
        .or_else(|| value.get("bytesValue"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn otlp_payload(body: Option<&Value>, attributes: &HashMap<String, Value>) -> Value {
    let body = body.map_or(Value::Null, otlp_any_value);
    let mut payload = match body {
        Value::Object(object) => object,
        value => {
            let mut object = Map::new();
            object.insert("body".to_string(), value);
            object
        }
    };
    payload.insert(
        "attributes".to_string(),
        serde_json::to_value(attributes).unwrap_or(Value::Object(Map::new())),
    );
    Value::Object(payload)
}

fn otlp_timestamp(record: &Value) -> Option<chrono::DateTime<Utc>> {
    let nanos = record
        .get("timeUnixNano")
        .or_else(|| record.get("observedTimeUnixNano"))
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))?;
    let seconds = i64::try_from(nanos / 1_000_000_000).ok()?;
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    chrono::DateTime::from_timestamp(seconds, subsec_nanos)
}

fn otlp_severity(number: Option<i64>) -> SeverityLevel {
    match number.unwrap_or(0) {
        1..=4 => SeverityLevel::Trace,
        5..=8 => SeverityLevel::Debug,
        9..=12 => SeverityLevel::Info,
        13..=16 => SeverityLevel::Warning,
        17..=20 => SeverityLevel::Error,
        21..=24 => SeverityLevel::Critical,
        _ => SeverityLevel::Unknown,
    }
}

async fn post_event(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<SimpleEventRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), DistillError> {
    authorize(&headers, &state)?;
    let event = request.into_event(channel_id);
    let response = submit_events(&state, vec![event]).await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn submit_events(
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

async fn post_channel(
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

async fn validate_channel(
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

async fn put_channel(
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

async fn test_event(
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

async fn post_schema(
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

async fn get_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ChannelConfigResponse>, DistillError> {
    authorize(&headers, &state)?;
    Ok(Json(state.channel_config(&channel_id).await?))
}

async fn pause_channel(
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

async fn resume_channel(
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

async fn flush_channel(
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

async fn archive_channel(
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

async fn clone_channel(
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

impl AppState {
    async fn channel_config(
        &self,
        channel_id: &str,
    ) -> Result<ChannelConfigResponse, DistillError> {
        let channels = self.channels.read().await;
        let record = channels
            .get(channel_id)
            .ok_or_else(|| DistillError::ChannelNotFound(channel_id.to_string()))?;
        Ok(ChannelConfigResponse::from_record(record))
    }
}

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), DistillError> {
    let provided = headers
        .get("X-API-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !bool::from(provided.as_bytes().ct_eq(state.api_key.as_bytes())) {
        return Err(DistillError::InvalidRequest("invalid API key".to_string()));
    }
    Ok(())
}

fn error_code(error: &DistillError) -> &'static str {
    match error {
        DistillError::InvalidRequest(_) | DistillError::SchemaValidation(_) => "INVALID_REQUEST",
        DistillError::ChannelNotFound(_) => "CHANNEL_NOT_FOUND",
        DistillError::InvalidConfiguration(_) => "INVALID_CONFIGURATION",
        DistillError::BufferFull(_) | DistillError::Persistence(_) => "SERVICE_UNAVAILABLE",
    }
}

fn client_message(error: &DistillError) -> String {
    if matches!(error, DistillError::Persistence(_)) {
        "external persistence is temporarily unavailable".to_string()
    } else {
        error.to_string()
    }
}

#[derive(Debug, Clone)]
struct CreateChannelRequest {
    name: String,
    preset: ChannelPreset,
    sink_id: Option<String>,
    severity_threshold: Option<SeverityLevel>,
    aggregator: Option<Value>,
    rabbitmq_config: Option<RabbitMqConfig>,
    sink_config: Option<SinkConfig>,
    message_schema: Option<MessageSchemaDefinition>,
    retention_hours: Option<u32>,
    compression_enabled: Option<bool>,
    buffer_bytes: Option<usize>,
    ttl_seconds: Option<u64>,
    mailbox_capacity: Option<usize>,
    ordering: Option<OrderingMode>,
    allowed_lateness_ms: Option<u64>,
    aggregator_present: bool,
}

#[derive(Debug, Deserialize)]
struct CreateChannelRequestWire {
    #[serde(alias = "channel_id")]
    name: String,
    #[serde(default)]
    preset: ChannelPreset,
    #[serde(default)]
    sink_id: Option<String>,
    #[serde(default)]
    severity_threshold: Option<SeverityLevel>,
    #[serde(default)]
    aggregator: Option<Value>,
    #[serde(default)]
    rabbitmq_config: Option<RabbitMqConfig>,
    #[serde(default)]
    sink_config: Option<SinkConfig>,
    #[serde(default)]
    message_schema: Option<MessageSchemaDefinition>,
    #[serde(default)]
    retention_hours: Option<u32>,
    #[serde(default)]
    compression_enabled: Option<bool>,
    #[serde(default)]
    buffer_bytes: Option<usize>,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    mailbox_capacity: Option<usize>,
    #[serde(default)]
    ordering: Option<OrderingMode>,
    #[serde(default)]
    allowed_lateness_ms: Option<u64>,
}

impl<'de> Deserialize<'de> for CreateChannelRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let aggregator_present = value
            .as_object()
            .is_some_and(|object| object.contains_key("aggregator"));
        let wire: CreateChannelRequestWire =
            serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(Self {
            name: wire.name,
            preset: wire.preset,
            sink_id: wire.sink_id,
            severity_threshold: wire.severity_threshold,
            aggregator: wire.aggregator,
            rabbitmq_config: wire.rabbitmq_config,
            sink_config: wire.sink_config,
            message_schema: wire.message_schema,
            retention_hours: wire.retention_hours,
            compression_enabled: wire.compression_enabled,
            buffer_bytes: wire.buffer_bytes,
            ttl_seconds: wire.ttl_seconds,
            mailbox_capacity: wire.mailbox_capacity,
            ordering: wire.ordering,
            allowed_lateness_ms: wire.allowed_lateness_ms,
            aggregator_present,
        })
    }
}

impl CreateChannelRequest {
    fn to_definition(
        &self,
        default_sink_connection_string: Option<&str>,
        default_sink_id: Option<&str>,
        exchange_name_prefix: &str,
        rabbitmq_uri: &str,
    ) -> Result<ChannelDefinition, DistillError> {
        let preset = self.preset.defaults();
        if self.sink_id.is_some() && self.sink_config.is_some() {
            return Err(DistillError::InvalidConfiguration(
                "specify either sink_id or sink_config, not both".to_string(),
            ));
        }
        let sink_connection_string = self
            .sink_config
            .as_ref()
            .map(|sink| sink.connection_string.clone())
            .or_else(|| default_sink_connection_string.map(ToOwned::to_owned));
        let sink_id = self.sink_id.clone().or_else(|| {
            (self.sink_config.is_none() && sink_connection_string.is_none())
                .then(|| default_sink_id.map(ToOwned::to_owned))
                .flatten()
        });
        if sink_connection_string.is_none() && sink_id.is_none() {
            return Err(DistillError::InvalidConfiguration(
                "sink_id or sink_config is required unless a default sink is configured"
                    .to_string(),
            ));
        }
        let sink_config = self.sink_config.clone().unwrap_or(SinkConfig {
            driver: "postgres".to_string(),
            connection_string: sink_connection_string.unwrap_or_default(),
            headers: std::collections::HashMap::new(),
        });
        /*
            The empty placeholder is intentionally ignored when `sink_id` is set. It keeps the
            existing internal channel actor shape stable while named sinks are resolved by AppState.
        */
        let rabbitmq_config = self.rabbitmq_config.clone().unwrap_or(RabbitMqConfig {
            connection_string: rabbitmq_uri.to_string(),
            exchange_name: format!("{exchange_name_prefix}.{}", self.name),
            routing_key_prefix: format!("{}.", self.name),
        });
        Ok(ChannelDefinition {
            channel_id: self.name.clone(),
            severity_threshold: self.severity_threshold.unwrap_or(preset.severity_threshold),
            rabbitmq_config,
            sink_id,
            sink_config,
            aggregator: if self.aggregator_present {
                match &self.aggregator {
                    Some(Value::Null) | None => None,
                    Some(value) => {
                        Some(serde_json::from_value(value.clone()).map_err(|error| {
                            DistillError::InvalidConfiguration(format!(
                                "invalid aggregator configuration: {error}"
                            ))
                        })?)
                    }
                }
            } else {
                preset.aggregator
            },
            message_schema: self.message_schema.clone(),
            retention_hours: self.retention_hours,
            compression_enabled: self
                .compression_enabled
                .unwrap_or(preset.compression_enabled),
            buffer_bytes: self.buffer_bytes.unwrap_or(preset.buffer_bytes),
            ttl_seconds: self.ttl_seconds.unwrap_or(preset.ttl_seconds),
            mailbox_capacity: self.mailbox_capacity.unwrap_or(preset.mailbox_capacity),
            ordering: self.ordering.unwrap_or(preset.ordering),
            allowed_lateness_ms: self
                .allowed_lateness_ms
                .unwrap_or(preset.allowed_lateness_ms),
        })
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ChannelPreset {
    #[default]
    RawEvents,
    ErrorsOnly,
    Metrics,
    Audit,
    Count,
}

impl ChannelPreset {
    fn as_str(self) -> &'static str {
        match self {
            Self::RawEvents => "raw_events",
            Self::ErrorsOnly => "errors_only",
            Self::Metrics => "metrics",
            Self::Audit => "audit",
            Self::Count => "count",
        }
    }

    fn defaults(self) -> PresetDefaults {
        let count = || AggregatorSpec {
            function: crate::domain::AggregationFunction::Count,
            granularity: Granularity::PerTTime,
            parameter: Some(300),
            rolling: false,
        };
        match self {
            Self::RawEvents => PresetDefaults {
                severity_threshold: SeverityLevel::Error,
                aggregator: None,
                ..PresetDefaults::default()
            },
            Self::ErrorsOnly => PresetDefaults {
                severity_threshold: SeverityLevel::Warning,
                aggregator: Some(count()),
                ..PresetDefaults::default()
            },
            Self::Metrics => PresetDefaults {
                severity_threshold: SeverityLevel::Error,
                aggregator: Some(AggregatorSpec {
                    function: crate::domain::AggregationFunction::Average,
                    granularity: Granularity::PerTTime,
                    parameter: Some(60),
                    rolling: false,
                }),
                ..PresetDefaults::default()
            },
            Self::Audit => PresetDefaults {
                severity_threshold: SeverityLevel::Info,
                aggregator: None,
                ..PresetDefaults::default()
            },
            Self::Count => PresetDefaults {
                severity_threshold: SeverityLevel::Info,
                aggregator: Some(count()),
                ..PresetDefaults::default()
            },
        }
    }
}

#[derive(Debug, Clone)]
struct PresetDefaults {
    severity_threshold: SeverityLevel,
    aggregator: Option<AggregatorSpec>,
    compression_enabled: bool,
    buffer_bytes: usize,
    ttl_seconds: u64,
    mailbox_capacity: usize,
    ordering: OrderingMode,
    allowed_lateness_ms: u64,
}

impl Default for PresetDefaults {
    fn default() -> Self {
        Self {
            severity_threshold: SeverityLevel::Error,
            aggregator: None,
            compression_enabled: false,
            buffer_bytes: crate::domain::DEFAULT_BUFFER_BYTES,
            ttl_seconds: crate::domain::DEFAULT_TTL_SECONDS,
            mailbox_capacity: crate::domain::DEFAULT_MAILBOX_CAPACITY,
            ordering: OrderingMode::default(),
            allowed_lateness_ms: crate::domain::DEFAULT_ALLOWED_LATENESS_MS,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SimpleEventRequest {
    #[serde(default)]
    event_id: Option<Uuid>,
    #[serde(default)]
    timestamp: Option<chrono::DateTime<Utc>>,
    #[serde(default = "default_info", alias = "severity")]
    severity_level: SeverityLevel,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    payload: Option<Value>,
    #[serde(default)]
    attributes: HashMap<String, Value>,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

impl SimpleEventRequest {
    fn into_event(self, channel_id: String) -> LogEvent {
        let mut payload = match self.payload.unwrap_or_else(|| Value::Object(Map::new())) {
            Value::Object(object) => object,
            value => {
                let mut object = Map::new();
                object.insert("value".to_string(), value);
                object
            }
        };
        if let Some(message) = self.message {
            payload.insert("message".to_string(), Value::String(message));
        }
        for (key, value) in self.attributes {
            payload.insert(key, value);
        }
        LogEvent {
            event_id: self.event_id.unwrap_or_else(Uuid::new_v4),
            timestamp: self.timestamp.unwrap_or_else(Utc::now),
            channel_id,
            severity_level: self.severity_level,
            payload: Value::Object(payload),
            payload_bytes: None,
            metadata: self.metadata,
        }
    }
}

fn default_info() -> SeverityLevel {
    SeverityLevel::Info
}

#[derive(Debug, Deserialize)]
struct CloneChannelRequest {
    channel_id: String,
}

#[derive(Debug, Serialize)]
struct IngestResponse {
    accepted: usize,
    rejected: usize,
    request_id: Uuid,
    durability: &'static str,
    errors: Vec<IngestItemError>,
}

#[derive(Debug, Serialize)]
struct IngestItemError {
    index: usize,
    error_code: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct ChannelConfigResponse {
    channel_id: String,
    active: bool,
    lifecycle: ChannelLifecycle,
    effective: PublicChannelConfig,
}

#[derive(Debug, Serialize)]
struct PublicChannelConfig {
    channel_id: String,
    sink_id: Option<String>,
    severity_threshold: SeverityLevel,
    rabbitmq_exchange: String,
    routing_key_prefix: String,
    sink_driver: String,
    aggregator: Option<AggregatorSpec>,
    retention_hours: Option<u32>,
    compression_enabled: bool,
    buffer_bytes: usize,
    ttl_seconds: u64,
    mailbox_capacity: usize,
    ordering: OrderingMode,
    allowed_lateness_ms: u64,
    schema_configured: bool,
}

impl ChannelConfigResponse {
    fn from_record(record: &ChannelRecord) -> Self {
        Self {
            channel_id: record.definition.channel_id.clone(),
            active: record.lifecycle == ChannelLifecycle::Active,
            lifecycle: record.lifecycle,
            effective: PublicChannelConfig::from_definition(&record.definition),
        }
    }
}

impl PublicChannelConfig {
    fn from_definition(definition: &ChannelDefinition) -> Self {
        Self {
            channel_id: definition.channel_id.clone(),
            sink_id: definition.sink_id.clone(),
            severity_threshold: definition.severity_threshold,
            rabbitmq_exchange: definition.rabbitmq_config.exchange_name.clone(),
            routing_key_prefix: definition.rabbitmq_config.routing_key_prefix.clone(),
            sink_driver: definition.sink_config.driver.clone(),
            aggregator: definition.aggregator.clone(),
            retention_hours: definition.retention_hours,
            compression_enabled: definition.compression_enabled,
            buffer_bytes: definition.buffer_bytes,
            ttl_seconds: definition.ttl_seconds,
            mailbox_capacity: definition.mailbox_capacity,
            ordering: definition.ordering,
            allowed_lateness_ms: definition.allowed_lateness_ms,
            schema_configured: definition.message_schema.is_some(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ChannelValidationResponse {
    valid: bool,
    channel_id: String,
    preset: String,
    warnings: Vec<String>,
    effective: PublicChannelConfig,
}

#[derive(Debug, Serialize)]
struct EventTestResponse {
    valid: bool,
    channel_id: String,
    route: &'static str,
    severity_threshold: SeverityLevel,
    schema_configured: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(value: Value) -> CreateChannelRequest {
        serde_json::from_value(value).expect("test request should deserialize")
    }

    #[test]
    fn raw_events_defaults_to_direct_persistence() {
        let request = request(json!({"name": "orders.production", "preset": "raw_events"}));
        let definition = request
            .to_definition(
                Some("postgres://localhost/logs"),
                None,
                "distill",
                "amqp://rabbit",
            )
            .expect("default sink should make the request complete");

        assert!(definition.aggregator.is_none());
        assert_eq!(definition.severity_threshold, SeverityLevel::Error);
        assert_eq!(
            definition.rabbitmq_config.exchange_name,
            "distill.orders.production"
        );
    }

    #[test]
    fn preset_defaults_can_be_overridden_without_losing_custom_configuration() {
        let request = request(json!({
            "name": "orders.production",
            "preset": "errors_only",
            "severity_threshold": "critical",
            "aggregator": null,
            "buffer_bytes": 4096
        }));
        let definition = request
            .to_definition(
                Some("postgres://localhost/logs"),
                None,
                "distill",
                "amqp://rabbit",
            )
            .expect("default sink should make the request complete");

        assert_eq!(definition.severity_threshold, SeverityLevel::Critical);
        assert!(definition.aggregator.is_none());
        assert_eq!(definition.buffer_bytes, 4096);
    }

    #[test]
    fn simple_event_preserves_message_and_arbitrary_attributes() {
        let request: SimpleEventRequest = serde_json::from_value(json!({
            "severity": "error",
            "message": "payment failed",
            "payload": {"order_id": "ord-123"},
            "attributes": {
                "retryable": true,
                "attempt": 2
            }
        }))
        .expect("simple event should deserialize");
        let event = request.into_event("orders.production".to_string());

        assert_eq!(event.severity_level, SeverityLevel::Error);
        assert_eq!(event.payload["message"], "payment failed");
        assert_eq!(event.payload["order_id"], "ord-123");
        assert_eq!(event.payload["retryable"], true);
        assert_eq!(event.payload["attempt"], 2);
    }

    #[test]
    fn otlp_logs_use_resource_channel_and_preserve_attributes() {
        let request = json!({
            "resourceLogs": [{
                "resource": {
                    "attributes": [{
                        "key": "distill.channel",
                        "value": {"stringValue": "orders.production"}
                    }]
                },
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "1722611045000000000",
                        "severityNumber": 17,
                        "body": {"stringValue": "payment failed"},
                        "attributes": [{
                            "key": "order_id",
                            "value": {"stringValue": "ord-123"}
                        }]
                    }]
                }]
            }]
        });
        let events = otlp_events(&request, None).expect("OTLP request should convert");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].channel_id, "orders.production");
        assert_eq!(events[0].severity_level, SeverityLevel::Error);
        assert_eq!(events[0].payload["body"], "payment failed");
        assert_eq!(events[0].payload["attributes"]["order_id"], "ord-123");
    }
}
