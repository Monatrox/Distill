//! Exposes authenticated ingestion and channel-management HTTP endpoints.

// Rust guideline compliant 2026-02-21

use crate::{
    broker::{RabbitConsumer, RabbitPublisher},
    buffer::MemoryBudget,
    channel::{self, ChannelHandle},
    config::{self, Settings},
    domain::{
        AggregatorSpec, ChannelDefinition, Granularity, LogEvent, MessageSchemaDefinition,
        OrderingMode, RabbitMqConfig, SeverityLevel, SinkConfig,
    },
    errors::DistillError,
    metrics::Metrics,
    persistence::{Sink, build_sink},
    routing::{RouteDecision, classify},
    schema::SchemaRegistry,
};
use anyhow::Context;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::Utc;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};
use subtle::ConstantTimeEq;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::time::{Duration, sleep};
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer, trace::TraceLayer};
use tracing::{info, warn};

mod auth;
mod channels;
mod dto;
mod error;
mod ingest;
mod otlp;

use auth::authenticate;
use channels::{
    archive_channel, clone_channel, flush_channel, get_channel, pause_channel, post_channel,
    post_schema, put_channel, resume_channel, test_event, validate_channel,
};
use dto::{
    ChannelConfigResponse, ChannelValidationResponse, CloneChannelRequest, CreateChannelRequest,
    EventTestResponse, IngestItemError, IngestResponse, PublicChannelConfig, SimpleEventRequest,
};
use error::{client_message, error_code};
use ingest::{health, ingest, metrics, otlp_logs, post_event};
use otlp::otlp_events;
use uuid::Uuid;

const STARTUP_CONNECTION_ATTEMPTS: u32 = 30;
const STARTUP_RETRY_DELAY: Duration = Duration::from_secs(1);
/// Defines the rolling period for the process-wide request allowance.
const REQUEST_RATE_WINDOW: Duration = Duration::from_mins(1);

#[derive(Clone)]
pub struct AppState {
    channels: Arc<RwLock<HashMap<String, ChannelRecord>>>,
    rabbit: RabbitPublisher,
    consumer: Arc<RabbitConsumer>,
    schemas: SchemaRegistry,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
    ingest_api_key: Arc<String>,
    admin_api_key: Arc<String>,
    rabbitmq_uri: String,
    default_sink_connection_string: Option<String>,
    exchange_name_prefix: String,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
    default_sink_id: Option<String>,
    http_limits: config::HttpLimits,
    request_controls: RequestControls,
}

#[derive(Clone, Debug)]
struct RequestControls {
    concurrent: Option<Arc<Semaphore>>,
    rate: Option<Arc<Mutex<RequestRateWindow>>>,
}

#[derive(Debug)]
struct RequestRateWindow {
    maximum: usize,
    admitted: VecDeque<Instant>,
}

struct RequestPermit {
    _concurrency: Option<OwnedSemaphorePermit>,
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
        let http_limits = settings.http.limits.clone();
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
            ingest_api_key: Arc::new(settings.ingest_api_key),
            admin_api_key: Arc::new(settings.admin_api_key),
            rabbitmq_uri: settings.rabbitmq_uri,
            default_sink_connection_string: settings.default_sink_connection_string,
            exchange_name_prefix: settings.exchange_name_prefix,
            sinks,
            default_sink_id,
            request_controls: RequestControls::new(&http_limits),
            http_limits,
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

pub fn public_router(state: AppState) -> Router {
    let authenticated = authenticated_routes(
        Router::new()
            .route("/metrics", get(metrics))
            .route("/ingest", post(ingest))
            .route("/v1/logs", post(otlp_logs))
            .route("/channels/{channel_id}/events", post(post_event)),
        Arc::clone(&state.ingest_api_key),
        &state.http_limits,
    );
    build_router(
        Router::new()
            .route("/health", get(health))
            .merge(authenticated),
        state,
    )
}

/// Builds the loopback-only administrative router.
pub fn admin_router(state: AppState) -> Router {
    let authenticated = authenticated_routes(
        Router::new()
            .route("/channels", post(post_channel))
            .route("/channels/validate", post(validate_channel))
            .route("/channels/{channel_id}", put(put_channel))
            .route("/channels/{channel_id}/test", post(test_event))
            .route("/channels/{channel_id}/config", get(get_channel))
            .route("/channels/{channel_id}/schema", post(post_schema))
            .route("/channels/{channel_id}/pause", post(pause_channel))
            .route("/channels/{channel_id}/resume", post(resume_channel))
            .route("/channels/{channel_id}/flush", post(flush_channel))
            .route("/channels/{channel_id}/archive", post(archive_channel))
            .route("/channels/{channel_id}/clone", post(clone_channel)),
        Arc::clone(&state.admin_api_key),
        &state.http_limits,
    );
    build_router(authenticated, state)
}

fn build_router(router: Router<AppState>, state: AppState) -> Router {
    router
        .layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_request_controls,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn authenticated_routes(
    router: Router<AppState>,
    expected_key: Arc<String>,
    limits: &config::HttpLimits,
) -> Router<AppState> {
    let mut router = router;
    if let Some(limit) = limits.request_body_limit_bytes {
        router = router.layer(RequestBodyLimitLayer::new(limit));
    }
    if let Some(timeout) = limits.request_timeout {
        router = router.layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ));
    }
    router.layer(middleware::from_fn_with_state(expected_key, authenticate))
}

impl RequestControls {
    fn new(limits: &config::HttpLimits) -> Self {
        Self {
            concurrent: limits
                .max_concurrent_requests
                .map(|maximum| Arc::new(Semaphore::new(maximum))),
            rate: limits.max_requests_per_minute.map(|maximum| {
                Arc::new(Mutex::new(RequestRateWindow {
                    maximum,
                    admitted: VecDeque::new(),
                }))
            }),
        }
    }

    fn acquire(&self) -> Result<RequestPermit, DistillError> {
        let permit = self
            .concurrent
            .as_ref()
            .map(|semaphore| semaphore.clone().try_acquire_owned())
            .transpose()
            .map_err(|_| DistillError::RequestThrottled)?;
        if let Some(rate) = &self.rate {
            let mut rate = rate
                .lock()
                .expect("request-rate mutex must not be poisoned");
            let now = Instant::now();
            while rate
                .admitted
                .front()
                .is_some_and(|admitted| now.duration_since(*admitted) >= REQUEST_RATE_WINDOW)
            {
                rate.admitted.pop_front();
            }
            if rate.admitted.len() >= rate.maximum {
                return Err(DistillError::RequestThrottled);
            }
            rate.admitted.push_back(now);
        }
        Ok(RequestPermit {
            _concurrency: permit,
        })
    }
}

async fn enforce_request_controls(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path() == "/health" {
        return next.run(request).await;
    }
    match state.request_controls.acquire() {
        Ok(permit) => {
            let response = next.run(request).await;
            drop(permit);
            response
        }
        Err(error) => error.into_response(),
    }
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
    fn request_controls_reject_excess_concurrency() {
        let controls = RequestControls {
            concurrent: Some(Arc::new(Semaphore::new(1))),
            rate: None,
        };
        let permit = controls
            .acquire()
            .expect("first request should be admitted");
        assert!(matches!(
            controls.acquire(),
            Err(DistillError::RequestThrottled)
        ));
        drop(permit);
        assert!(controls.acquire().is_ok());
    }

    #[test]
    fn request_controls_reject_excess_rate() {
        let controls = RequestControls {
            concurrent: None,
            rate: Some(Arc::new(Mutex::new(RequestRateWindow {
                maximum: 1,
                admitted: VecDeque::new(),
            }))),
        };
        assert!(controls.acquire().is_ok());
        assert!(matches!(
            controls.acquire(),
            Err(DistillError::RequestThrottled)
        ));
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
