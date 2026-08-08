//! Owns registered channels, sinks, schemas, and channel actor lifecycles.
//!
//! The registry is the application-side coordinator for channel management:
//! it resolves sink definitions, registers broker topology, spawns channel
//! actors, validates and forwards events, and tracks lifecycle state. HTTP
//! handlers interact with channels exclusively through this registry so the
//! delivery machinery stays out of the web layer.

// Rust guideline compliant 2026-02-21

use crate::{
    broker::{RabbitConsumer, RabbitPublisher},
    core::{
        buffer::MemoryBudget,
        channel::{self, ChannelHandle},
        errors::DistillError,
        metrics::Metrics,
        schema::SchemaRegistry,
    },
    domain::{ChannelDefinition, LogEvent, MessageSchemaDefinition},
    infra::config::{self, Settings},
    persistence::{Sink, SinkAllowlist, build_sink},
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use utoipa::ToSchema;

/// Startup connection attempts before the process aborts.
///
/// `RabbitMQ` typically lags behind the Distill container during startup; a
/// bounded retry window tolerates this without failing the process.
const STARTUP_CONNECTION_ATTEMPTS: u32 = 30;
/// Delay between startup connection attempts.
const STARTUP_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelLifecycle {
    Active,
    Paused,
    Archived,
}

#[derive(Clone)]
pub(crate) struct ChannelRecord {
    pub(crate) definition: ChannelDefinition,
    pub(crate) handle: ChannelHandle,
    pub(crate) lifecycle: ChannelLifecycle,
}

#[derive(Clone)]
pub(crate) struct ChannelRegistry {
    channels: Arc<RwLock<HashMap<String, ChannelRecord>>>,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
    schemas: SchemaRegistry,
    consumer: Arc<RabbitConsumer>,
    rabbit: RabbitPublisher,
    metrics: Arc<Metrics>,
    budget: Arc<MemoryBudget>,
    pub(crate) rabbitmq_uri: String,
    pub(crate) default_sink_connection_string: Option<String>,
    pub(crate) exchange_name_prefix: String,
    pub(crate) default_sink_id: Option<String>,
    max_channels: usize,
    sink_allowlist: SinkAllowlist,
}

impl ChannelRegistry {
    /// Connects dependencies and registers channels from the startup configuration.
    ///
    /// # Errors
    /// Returns an error when `RabbitMQ` or a configured sink cannot be reached, or
    /// when the channel configuration file is invalid.
    pub(crate) async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        let rabbit = retry_startup_connection("RabbitMQ publisher", || {
            let rabbit_uri = settings.rabbitmq_uri.clone();
            async move { RabbitPublisher::connect(&rabbit_uri).await }
        })
        .await?;
        let metrics = Arc::new(Metrics::default());
        let consumer = Arc::new(
            retry_startup_connection("RabbitMQ consumer", || {
                let rabbit_uri = settings.rabbitmq_uri.clone();
                let consumer_metrics = Arc::clone(&metrics);
                async move { RabbitConsumer::connect(&rabbit_uri, consumer_metrics).await }
            })
            .await?,
        );
        let sinks = Arc::new(RwLock::new(HashMap::new()));
        let mut default_sink_id = None;
        if let Some(path) = &settings.sinks_config_path {
            let contents = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("reading sink configuration from {}", path.display()))?;
            let sink_file = config::parse_sinks(&contents)?;
            default_sink_id = sink_file.default_sink_id;
            for definition in sink_file.sinks {
                let sink_config = config::resolve_sink(&definition)?;
                let sink = build_sink(&sink_config, &settings.sink_allowlist).await?;
                sinks.write().await.insert(definition.sink_id, sink);
            }
        }
        let registry = Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            sinks,
            schemas: SchemaRegistry::default(),
            consumer,
            rabbit,
            metrics,
            budget: Arc::new(MemoryBudget::new(
                crate::domain::DEFAULT_PROCESS_MEMORY_BYTES,
            )),
            rabbitmq_uri: settings.rabbitmq_uri.clone(),
            default_sink_connection_string: settings.default_sink_connection_string.clone(),
            exchange_name_prefix: settings.exchange_name_prefix.clone(),
            default_sink_id,
            max_channels: settings.max_channels,
            sink_allowlist: settings.sink_allowlist.clone(),
        };
        if let Some(path) = &settings.config_path {
            let contents = tokio::fs::read_to_string(path).await.with_context(|| {
                format!("reading channel configuration from {}", path.display())
            })?;
            for definition in
                config::parse_channels_with_default(&contents, registry.default_sink_id.as_deref())?
            {
                registry.register_channel(definition).await?;
            }
        }
        Ok(registry)
    }

    /// Returns a shared handle to the process metrics counters.
    pub(crate) fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Registers or replaces a channel, wiring its sink, schema, and actor.
    ///
    /// # Errors
    /// Returns an error when the definition is invalid, its sink is undefined,
    /// or broker or sink setup fails.
    pub(crate) async fn register_channel(
        &self,
        definition: ChannelDefinition,
    ) -> Result<(), DistillError> {
        definition
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
        let channel_id = definition.channel_id.clone();
        {
            let channels = self.channels.read().await;
            if self.max_channels > 0
                && !channels.contains_key(&channel_id)
                && channels.len() >= self.max_channels
            {
                return Err(DistillError::InvalidConfiguration(format!(
                    "maximum channel count ({}) reached; raise DISTILL_MAX_CHANNELS to allow more",
                    self.max_channels
                )));
            }
        }
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
            build_sink(&definition.sink_config, &self.sink_allowlist).await?
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

    /// Validates and enqueues an event for its channel's actor.
    ///
    /// # Errors
    /// Returns an error for invalid events, unknown or inactive channels,
    /// schema mismatches, and full channel mailboxes.
    pub(crate) async fn submit_event(&self, event: LogEvent) -> Result<(), DistillError> {
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

    /// Changes a channel's lifecycle and returns the updated record.
    ///
    /// # Errors
    /// Returns an error when the channel does not exist.
    pub(crate) async fn set_lifecycle(
        &self,
        channel_id: &str,
        lifecycle: ChannelLifecycle,
    ) -> Result<ChannelRecord, DistillError> {
        let mut channels = self.channels.write().await;
        let record = channels
            .get_mut(channel_id)
            .ok_or_else(|| DistillError::ChannelNotFound(channel_id.to_string()))?;
        record.lifecycle = lifecycle;
        Ok(record.clone())
    }

    /// Returns a copy of a channel's record.
    ///
    /// # Errors
    /// Returns an error when the channel does not exist.
    pub(crate) async fn channel_record(
        &self,
        channel_id: &str,
    ) -> Result<ChannelRecord, DistillError> {
        self.channels
            .read()
            .await
            .get(channel_id)
            .cloned()
            .ok_or_else(|| DistillError::ChannelNotFound(channel_id.to_string()))
    }

    /// Registers a runtime schema for an existing channel.
    ///
    /// # Errors
    /// Returns an error when the channel does not exist or the descriptor is invalid.
    pub(crate) async fn register_schema(
        &self,
        channel_id: &str,
        definition: MessageSchemaDefinition,
    ) -> Result<(), DistillError> {
        if !self.channels.read().await.contains_key(channel_id) {
            return Err(DistillError::ChannelNotFound(channel_id.to_string()));
        }
        self.schemas
            .register(channel_id, definition.clone())
            .await?;
        if let Some(record) = self.channels.write().await.get_mut(channel_id) {
            record.definition.message_schema = Some(definition);
        }
        Ok(())
    }

    /// Validates a payload against a channel's registered schema, if any.
    ///
    /// # Errors
    /// Returns an error when the payload does not match the channel schema.
    pub(crate) async fn validate_payload(
        &self,
        channel_id: &str,
        payload: &serde_json::Value,
        payload_bytes: Option<&[u8]>,
    ) -> Result<(), DistillError> {
        self.schemas
            .validate_json(channel_id, payload, payload_bytes)
            .await
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
