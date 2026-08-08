//! Exposes authenticated ingestion and channel-management HTTP endpoints.

// Rust guideline compliant 2026-02-21

use crate::{
    core::{
        errors::{DistillError, ErrorCode, ErrorEnvelope},
        metrics::Metrics,
        registry::{ChannelLifecycle, ChannelRecord, ChannelRegistry},
        routing::{RouteDecision, classify},
    },
    domain::{
        AggregatorSpec, ChannelDefinition, Granularity, LogEvent, MessageSchemaDefinition,
        OrderingMode, RabbitMqConfig, SeverityLevel, SinkConfig,
    },
    infra::config::{self, Settings},
};
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
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};
use subtle::ConstantTimeEq;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Duration;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer, trace::TraceLayer};

mod auth;
mod channels;
mod dto;
mod ingest;
mod openapi;
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
use ingest::{health, ingest, metrics, otlp_logs, post_event};
use openapi::swagger_ui;
use otlp::otlp_events;
use uuid::Uuid;

/// Defines the rolling period for the process-wide request allowance.
const REQUEST_RATE_WINDOW: Duration = Duration::from_mins(1);
/// Length of the window over which failed authentications accumulate.
const AUTH_THROTTLE_WINDOW: Duration = Duration::from_mins(1);
/// Failed authentications within the window that trigger throttled responses.
///
/// The threshold is coarse and process-wide per listener; it bounds brute-force
/// attempts on the static API keys without tracking client identities.
const AUTH_THROTTLE_MAX_FAILURES: usize = 10;

#[derive(Clone)]
pub struct AppState {
    registry: ChannelRegistry,
    metrics: Arc<Metrics>,
    ingest_api_key: Arc<String>,
    admin_api_key: Arc<String>,
    http_limits: config::HttpLimits,
    request_controls: RequestControls,
    ingest_auth_window: Arc<FailedAuthWindow>,
    admin_auth_window: Arc<FailedAuthWindow>,
}

/// Key and throttling state shared with the authentication middleware.
pub(super) struct AuthGuard {
    pub(super) key: String,
    pub(super) failures: Arc<FailedAuthWindow>,
}

/// Tracks consecutive failed authentication attempts within a rolling window.
#[derive(Debug, Default)]
pub(super) struct FailedAuthWindow {
    failures: Mutex<VecDeque<Instant>>,
}

impl FailedAuthWindow {
    /// Rejects the attempt when too many failures occurred in the window.
    fn check(&self) -> Result<(), DistillError> {
        let mut failures = self
            .failures
            .lock()
            .expect("authentication throttle mutex must not be poisoned");
        let now = Instant::now();
        while failures
            .front()
            .is_some_and(|failed| now.duration_since(*failed) >= AUTH_THROTTLE_WINDOW)
        {
            failures.pop_front();
        }
        if failures.len() >= AUTH_THROTTLE_MAX_FAILURES {
            return Err(DistillError::RequestThrottled);
        }
        Ok(())
    }

    /// Records a failed attempt.
    fn record_failure(&self) {
        let mut failures = self
            .failures
            .lock()
            .expect("authentication throttle mutex must not be poisoned");
        let now = Instant::now();
        while failures
            .front()
            .is_some_and(|failed| now.duration_since(*failed) >= AUTH_THROTTLE_WINDOW)
        {
            failures.pop_front();
        }
        failures.push_back(now);
    }

    /// Clears the failure history after a successful authentication.
    fn reset(&self) {
        self.failures
            .lock()
            .expect("authentication throttle mutex must not be poisoned")
            .clear();
    }
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

impl AppState {
    pub async fn connect(settings: Settings) -> anyhow::Result<Self> {
        let http_limits = settings.http.limits.clone();
        let registry = ChannelRegistry::connect(&settings).await?;
        Ok(Self {
            metrics: registry.metrics(),
            registry,
            ingest_api_key: Arc::new(settings.ingest_api_key),
            admin_api_key: Arc::new(settings.admin_api_key),
            request_controls: RequestControls::new(&http_limits),
            http_limits,
            ingest_auth_window: Arc::new(FailedAuthWindow::default()),
            admin_auth_window: Arc::new(FailedAuthWindow::default()),
        })
    }

    fn build_channel_definition(
        &self,
        request: &CreateChannelRequest,
    ) -> Result<ChannelDefinition, DistillError> {
        request.to_definition(
            self.registry.default_sink_connection_string.as_deref(),
            self.registry.default_sink_id.as_deref(),
            &self.registry.exchange_name_prefix,
            &self.registry.rabbitmq_uri,
        )
    }
}

pub fn public_router(state: AppState) -> Router {
    let authenticated = authenticated_routes(
        Router::new()
            .route("/metrics", get(metrics))
            .route("/ingest", post(ingest))
            .route("/v1/logs", post(otlp_logs))
            .route("/channels/{channel_id}/events", post(post_event)),
        Arc::new(AuthGuard {
            key: (*state.ingest_api_key).clone(),
            failures: Arc::clone(&state.ingest_auth_window),
        }),
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
        Arc::new(AuthGuard {
            key: (*state.admin_api_key).clone(),
            failures: Arc::clone(&state.admin_auth_window),
        }),
        &state.http_limits,
    );
    build_router(authenticated.merge(swagger_ui()), state)
}

fn build_router(router: Router<AppState>, state: AppState) -> Router {
    router
        .layer(middleware::from_fn(enforce_api_version))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_request_controls,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn authenticated_routes(
    router: Router<AppState>,
    guard: Arc<AuthGuard>,
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
    router.layer(middleware::from_fn_with_state(guard, authenticate))
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

/// Resolves the API version requested by a client.
///
/// Requests without the `X-API-VERSION` header use the operator-configured
/// default version (`DISTILL_API_DEFAULT_VERSION`); the header pins a specific
/// supported version for clients that must avoid silent drift.
fn resolve_api_version(headers: &HeaderMap) -> Result<(), DistillError> {
    match headers
        .get("X-API-VERSION")
        .and_then(|value| value.to_str().ok())
    {
        Some(version) if config::SUPPORTED_API_VERSIONS.contains(&version) => Ok(()),
        Some(version) => Err(DistillError::UnsupportedApiVersion(version.to_string())),
        None => Ok(()),
    }
}

async fn enforce_api_version(request: Request, next: Next) -> Response {
    match resolve_api_version(request.headers()) {
        Ok(()) => next.run(request).await,
        Err(error) => error.into_response(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
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
    fn api_version_header_accepts_supported_versions_and_default() {
        assert!(resolve_api_version(&HeaderMap::new()).is_ok());
        let mut supported = HeaderMap::new();
        supported.insert("X-API-VERSION", HeaderValue::from_static("1"));
        assert!(resolve_api_version(&supported).is_ok());
        let mut unsupported = HeaderMap::new();
        unsupported.insert("X-API-VERSION", HeaderValue::from_static("2"));
        assert!(matches!(
            resolve_api_version(&unsupported),
            Err(DistillError::UnsupportedApiVersion(_))
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
