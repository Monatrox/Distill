//! Exposes authenticated ingestion and channel-management HTTP endpoints.

use crate::{
    buffer::MemoryBudget,
    channel::{self, ChannelHandle},
    config::{self, Settings},
    domain::{ChannelDefinition, LogEvent},
    errors::DistillError,
    metrics::Metrics,
    schema::SchemaRegistry,
    sink::{PostgresSink, RabbitConsumer, RabbitPublisher, Sink},
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
use prost::Message;
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

const STARTUP_CONNECTION_ATTEMPTS: u32 = 30;
const STARTUP_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct AppState {
    channels: Arc<RwLock<HashMap<String, ChannelHandle>>>,
    rabbit: RabbitPublisher,
    consumer: Arc<RabbitConsumer>,
    schemas: SchemaRegistry,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
    api_key: Arc<String>,
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
        };

        if let Some(path) = settings.config_path {
            let contents = tokio::fs::read_to_string(&path).await.with_context(|| {
                format!("reading channel configuration from {}", path.display())
            })?;
            for definition in config::parse_channels(&contents)? {
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
        }
        let sink: Arc<dyn Sink> =
            Arc::new(PostgresSink::connect(&definition.sink_config.connection_string).await?);
        let old = self.channels.read().await.get(&channel_id).cloned();
        if let Some(previous) = old {
            previous.flush().await?;
        }
        self.consumer
            .register_exchange(&definition.rabbitmq_config.exchange_name)
            .await?;
        self.consumer.register_sink(channel_id.clone(), sink).await;
        let handle = channel::spawn(
            definition,
            self.rabbit.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.budget),
        );
        self.channels
            .write()
            .await
            .insert(channel_id.clone(), handle);
        info!(event.name = "distill.channel.registered", channel.id = %channel_id, "channel registered");
        Ok(())
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
        .route("/channels/{channel_id}", put(put_channel))
        .route("/channels/{channel_id}/config", get(get_channel))
        .route("/channels/{channel_id}/schema", post(post_schema))
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
) -> Result<Json<IngestResponse>, DistillError> {
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
    let accepted = events.len();
    for event in events {
        let handle = state
            .channels
            .read()
            .await
            .get(&event.channel_id)
            .cloned()
            .ok_or_else(|| DistillError::ChannelNotFound(event.channel_id.clone()))?;
        state
            .schemas
            .validate_json(
                &event.channel_id,
                &event.payload,
                event.payload_bytes.as_deref(),
            )
            .await?;
        handle.send(event).await?;
    }
    Ok(Json(IngestResponse { accepted }))
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

async fn post_schema(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    headers: HeaderMap,
    Json(definition): Json<crate::domain::MessageSchemaDefinition>,
) -> Result<StatusCode, DistillError> {
    authorize(&headers, &state)?;
    if !state.channels.read().await.contains_key(&channel_id) {
        return Err(DistillError::ChannelNotFound(channel_id));
    }
    state.schemas.register(&channel_id, definition).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> Result<Json<ChannelStatus>, DistillError> {
    let exists = state.channels.read().await.contains_key(&channel_id);
    if !exists {
        return Err(DistillError::ChannelNotFound(channel_id));
    }
    Ok(Json(ChannelStatus {
        channel_id,
        active: true,
    }))
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

#[derive(Debug, Serialize)]
struct IngestResponse {
    accepted: usize,
}

#[derive(Debug, Serialize)]
struct ChannelStatus {
    channel_id: String,
    active: bool,
}
