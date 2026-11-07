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
        state::{StateStore, StoredChannel},
    },
    domain::{ChannelDefinition, LogEvent, MessageSchemaDefinition, SinkConfig},
    infra::config::{self, Settings},
    persistence::{Sink, SinkAllowlist, build_sink},
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, sleep, timeout};
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

type ChannelKey = (String, Option<String>);
type SinkCache = Arc<Mutex<HashMap<String, Arc<dyn Sink>>>>;

#[derive(Clone)]
pub(crate) struct ChannelRegistry {
    channels: Arc<RwLock<HashMap<ChannelKey, ChannelRecord>>>,
    sinks: Arc<RwLock<HashMap<String, Arc<dyn Sink>>>>,
    sink_configs: Arc<RwLock<HashMap<String, SinkConfig>>>,
    state: StateStore,
    auto_migrate: bool,
    sink_cache: SinkCache,
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
    max_sinks: usize,
    sink_allowlist: SinkAllowlist,
    accepting_events: Arc<AtomicBool>,
    channel_mutation: Arc<Mutex<()>>,
    shutdown_timeout: Duration,
}

impl ChannelRegistry {
    /// Connects dependencies and registers channels from the startup configuration.
    ///
    /// # Errors
    /// Returns an error when `RabbitMQ` or a configured sink cannot be reached, or
    /// when the channel configuration file is invalid.
    #[expect(
        clippy::too_many_lines,
        reason = "startup restores dependencies and all durable mappings before starting consumption"
    )]
    pub(crate) async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        let state =
            StateStore::open_with_limit(&settings.state_path, settings.state_max_bytes).await?;
        let restored = state.channels().await?;
        if settings.max_channels > 0
            && restored.iter().filter(|record| record.active).count() > settings.max_channels
        {
            anyhow::bail!("restored channel versions exceed DISTILL_MAX_CHANNELS");
        }
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
        let sink_cache = Arc::new(Mutex::new(HashMap::new()));
        let mut sink_configs = HashMap::new();
        for (sink_id, config) in state.sinks().await? {
            let sink = cached_sink(
                &sink_cache,
                &config,
                &settings.sink_allowlist,
                settings.auto_migrate,
                true,
            )
            .await?;
            sinks.write().await.insert(sink_id.clone(), sink);
            sink_configs.insert(sink_id, config);
        }
        let mut default_sink_id = None;
        if let Some(path) = &settings.sinks_config_path {
            let contents = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("reading sink configuration from {}", path.display()))?;
            let sink_file = config::parse_sinks(&contents)?;
            default_sink_id = sink_file.default_sink_id;
            for definition in sink_file.sinks {
                let sink_config = config::resolve_sink(&definition)?;
                let sink_id = definition.sink_id;
                let sink = cached_sink(
                    &sink_cache,
                    &sink_config,
                    &settings.sink_allowlist,
                    settings.auto_migrate,
                    false,
                )
                .await?;
                sink_configs.insert(sink_id.clone(), sink_config.clone());
                sinks.write().await.insert(sink_id, sink);
            }
        }
        for (sink_id, sink_config) in &settings.environment_sinks {
            if sinks.read().await.contains_key(sink_id) {
                anyhow::bail!("sink '{sink_id}' is configured more than once");
            }
            let sink = cached_sink(
                &sink_cache,
                sink_config,
                &settings.sink_allowlist,
                settings.auto_migrate,
                false,
            )
            .await?;
            sink_configs.insert(sink_id.clone(), sink_config.clone());
            sinks.write().await.insert(sink_id.clone(), sink);
        }
        if settings.environment_default_sink_id.is_some() {
            default_sink_id = settings.environment_default_sink_id.clone();
        }
        if let Some(sink_id) = &default_sink_id
            && !sinks.read().await.contains_key(sink_id)
        {
            anyhow::bail!("default sink '{sink_id}' is not configured");
        }
        if settings.max_sinks > 0 && sink_configs.len() > settings.max_sinks {
            anyhow::bail!("restored sinks exceed DISTILL_MAX_SINKS");
        }
        let registry = Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            sinks,
            sink_configs: Arc::new(RwLock::new(sink_configs)),
            state,
            auto_migrate: settings.auto_migrate,
            sink_cache,
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
            max_sinks: settings.max_sinks,
            sink_allowlist: settings.sink_allowlist.clone(),
            accepting_events: Arc::new(AtomicBool::new(true)),
            channel_mutation: Arc::new(Mutex::new(())),
            shutdown_timeout: settings.shutdown_timeout,
        };
        for record in restored {
            registry.restore_channel(record).await?;
        }
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
        registry.consumer.start().await;
        Ok(registry)
    }

    async fn restore_channel(&self, record: StoredChannel) -> Result<(), DistillError> {
        let definition = record.definition;
        definition
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
        let sink = cached_sink(
            &self.sink_cache,
            &definition.sink_config,
            &self.sink_allowlist,
            self.auto_migrate,
            true,
        )
        .await?;
        let schema = definition
            .message_schema
            .as_ref()
            .map(SchemaRegistry::prepare)
            .transpose()?;
        self.schemas
            .check_capacity(
                &schema_key(&definition.channel_id, definition.version.as_deref()),
                schema.as_ref(),
            )
            .await?;
        self.consumer
            .register_exchange(&definition.rabbitmq_config.exchange_name)
            .await?;
        let proposed = channel::spawn(
            definition.clone(),
            Some(record.generation),
            self.rabbit.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.budget),
            self.state.clone(),
        )
        .await?;
        let prepared = self
            .consumer
            .prepare_channel(
                definition.channel_id.clone(),
                record.generation,
                sink,
                proposed,
                definition.aggregator.is_some(),
            )
            .await?;
        let handle = self.consumer.activate_channel(prepared).await;
        if record.active {
            self.schemas
                .activate(
                    &schema_key(&definition.channel_id, definition.version.as_deref()),
                    schema,
                )
                .await;
            self.channels.write().await.insert(
                (definition.channel_id.clone(), definition.version.clone()),
                ChannelRecord {
                    definition,
                    handle,
                    lifecycle: record.lifecycle,
                },
            );
        }
        Ok(())
    }

    /// Returns whether an active, sink-backed channel can accept events.
    pub(crate) async fn is_ready(&self) -> bool {
        self.accepting_events.load(Ordering::Acquire)
            && self
                .channels
                .read()
                .await
                .values()
                .any(|record| record.lifecycle == ChannelLifecycle::Active)
            && self.consumer.consumers_healthy().await
            && self.rabbit.healthy().await
    }

    pub(crate) async fn consumers_healthy(&self) -> bool {
        self.consumer.consumers_healthy().await
    }

    pub(crate) async fn aggregate_in_flight(&self) -> usize {
        self.consumer.aggregate_in_flight().await
    }

    /// Flushes all active channel buffers before process shutdown.
    ///
    /// # Errors
    /// Returns an error when a channel cannot hand buffered aggregates to `RabbitMQ`.
    pub(crate) async fn shutdown(&self) -> Result<(), DistillError> {
        self.stop_admission();
        timeout(self.shutdown_timeout, async {
            self.consumer.flush_aggregators().await?;
            self.consumer.shutdown().await
        })
        .await
        .map_err(|_| {
            DistillError::Persistence(format!(
                "channel shutdown exceeded {} seconds",
                self.shutdown_timeout.as_secs()
            ))
        })?
    }

    pub(crate) fn stop_admission(&self) {
        self.accepting_events.store(false, Ordering::Release);
    }

    pub(crate) fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns a shared handle to the process metrics counters.
    pub(crate) fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    pub(crate) async fn register_sink(
        &self,
        sink_id: &str,
        config: SinkConfig,
    ) -> Result<(), DistillError> {
        let _mutation = self.channel_mutation.lock().await;
        config
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
        if let Some(existing) = self.sink_configs.read().await.get(sink_id) {
            if serde_json::to_value(existing).ok() == serde_json::to_value(&config).ok() {
                return Ok(());
            }
            return Err(DistillError::ConfigurationConflict);
        }
        if self.max_sinks > 0 && self.sink_configs.read().await.len() >= self.max_sinks {
            return Err(DistillError::InvalidConfiguration(
                "DISTILL_MAX_SINKS capacity reached".to_string(),
            ));
        }
        let sink = cached_sink(
            &self.sink_cache,
            &config,
            &self.sink_allowlist,
            self.auto_migrate,
            false,
        )
        .await?;
        self.state.register_sink(sink_id, &config).await?;
        self.sink_configs
            .write()
            .await
            .insert(sink_id.to_string(), config);
        self.sinks.write().await.insert(sink_id.to_string(), sink);
        Ok(())
    }

    /// Reconciles an immutable channel version, wiring its sink, schema, and actor.
    ///
    /// # Errors
    /// Returns an error when the definition is invalid, its sink is undefined,
    /// or broker or sink setup fails.
    #[expect(
        clippy::too_many_lines,
        reason = "registration stages resources, commits recovery state and activates in that order"
    )]
    pub(crate) async fn register_channel(
        &self,
        mut definition: ChannelDefinition,
    ) -> Result<(), DistillError> {
        let _mutation = self.channel_mutation.lock().await;
        definition
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
        if let Some(sink_id) = &definition.sink_id {
            definition.sink_config = self
                .sink_configs
                .read()
                .await
                .get(sink_id)
                .cloned()
                .ok_or_else(|| {
                    DistillError::InvalidConfiguration("named sink is not registered".to_string())
                })?;
        }
        let channel_id = definition.channel_id.clone();
        let key = (channel_id.clone(), definition.version.clone());
        if let Some(existing) = self.channels.read().await.get(&key) {
            if serde_json::to_value(&existing.definition)
                .map_err(|_| DistillError::ConfigurationConflict)?
                == serde_json::to_value(&definition)
                    .map_err(|_| DistillError::ConfigurationConflict)?
            {
                return Ok(());
            }
            return Err(DistillError::ConfigurationConflict);
        }

        definition
            .sink_config
            .validate()
            .map_err(DistillError::InvalidConfiguration)?;
        {
            let channels = self.channels.read().await;
            if self.max_channels > 0
                && !channels.contains_key(&key)
                && channels.len() >= self.max_channels
            {
                return Err(DistillError::InvalidConfiguration(format!(
                    "maximum channel count ({}) reached; raise DISTILL_MAX_CHANNELS to allow more",
                    self.max_channels
                )));
            }
        }
        let schema = definition
            .message_schema
            .as_ref()
            .map(SchemaRegistry::prepare)
            .transpose()?;
        self.schemas
            .check_capacity(
                &schema_key(&definition.channel_id, definition.version.as_deref()),
                schema.as_ref(),
            )
            .await?;
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
            cached_sink(
                &self.sink_cache,
                &definition.sink_config,
                &self.sink_allowlist,
                self.auto_migrate,
                false,
            )
            .await?
        };
        self.consumer
            .register_exchange(&definition.rabbitmq_config.exchange_name)
            .await?;
        let generation = None;
        let proposed_handle = channel::spawn(
            definition.clone(),
            generation,
            self.rabbit.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.budget),
            self.state.clone(),
        )
        .await?;
        let generation = proposed_handle.generation();
        let aggregate_enabled = definition.aggregator.is_some();
        let prepared = self
            .consumer
            .prepare_channel(
                channel_id.clone(),
                generation,
                sink,
                proposed_handle,
                aggregate_enabled,
            )
            .await?;
        self.state
            .activate(&StoredChannel {
                generation,
                definition: definition.clone(),
                lifecycle: ChannelLifecycle::Active,
                active: true,
            })
            .await?;
        self.schemas
            .activate(
                &schema_key(&channel_id, definition.version.as_deref()),
                schema,
            )
            .await;
        let handle = self.consumer.activate_channel(prepared).await;
        self.channels.write().await.insert(
            key,
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
        if !self.accepting_events.load(Ordering::Acquire) {
            return Err(DistillError::Persistence(
                "service is shutting down and no longer accepts events".to_string(),
            ));
        }
        event.validate().map_err(DistillError::InvalidRequest)?;
        let record = self
            .channels
            .read()
            .await
            .get(&(event.channel_id.clone(), event.version.clone()))
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
        self.state
            .duplicate(record.handle.generation(), &event)
            .await?;
        self.schemas
            .validate_json(
                &schema_key(&event.channel_id, event.version.as_deref()),
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
        version: Option<&str>,
        lifecycle: ChannelLifecycle,
    ) -> Result<ChannelRecord, DistillError> {
        let _mutation = self.channel_mutation.lock().await;
        let mut channels = self.channels.write().await;
        let record = channels
            .get_mut(&(channel_id.to_string(), version.map(ToOwned::to_owned)))
            .ok_or_else(|| DistillError::ChannelNotFound(channel_id.to_string()))?;
        self.state
            .set_lifecycle(record.handle.generation(), lifecycle)
            .await?;
        record.lifecycle = lifecycle;
        Ok(record.clone())
    }

    pub(crate) async fn channel_record_versioned(
        &self,
        channel_id: &str,
        version: Option<&str>,
    ) -> Result<ChannelRecord, DistillError> {
        self.channels
            .read()
            .await
            .get(&(channel_id.to_string(), version.map(ToOwned::to_owned)))
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
        let _mutation = self.channel_mutation.lock().await;
        if !self
            .channels
            .read()
            .await
            .contains_key(&(channel_id.to_string(), None))
        {
            return Err(DistillError::ChannelNotFound(channel_id.to_string()));
        }
        let schema = SchemaRegistry::prepare(&definition)?;
        self.schemas
            .check_capacity(&schema_key(channel_id, None), Some(&schema))
            .await?;
        let mut channels = self.channels.write().await;
        if let Some(record) = channels.get_mut(&(channel_id.to_string(), None)) {
            let mut updated = record.definition.clone();
            updated.message_schema = Some(definition);
            self.state
                .activate(&StoredChannel {
                    generation: record.handle.generation(),
                    definition: updated.clone(),
                    lifecycle: record.lifecycle,
                    active: true,
                })
                .await?;
            record.definition = updated;
        }
        self.schemas
            .activate(&schema_key(channel_id, None), Some(schema))
            .await;
        Ok(())
    }

    /// Validates a payload against a channel's registered schema, if any.
    ///
    /// # Errors
    /// Returns an error when the payload does not match the channel schema.
    pub(crate) async fn validate_payload(
        &self,
        channel_id: &str,
        version: Option<&str>,
        payload: &serde_json::Value,
        payload_bytes: Option<&[u8]>,
    ) -> Result<(), DistillError> {
        self.schemas
            .validate_json(&schema_key(channel_id, version), payload, payload_bytes)
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
        last_error
            .expect("startup attempts always record an error")
            .error_code()
    ))
}

fn schema_key(channel_id: &str, version: Option<&str>) -> String {
    format!("{channel_id}@{}", version.unwrap_or_default())
}

async fn cached_sink(
    cache: &SinkCache,
    config: &SinkConfig,
    allowlist: &SinkAllowlist,
    auto_migrate: bool,
    restoring: bool,
) -> Result<Arc<dyn Sink>, DistillError> {
    use sha2::{Digest, Sha256};
    let canonical =
        serde_json::to_value(config).map_err(|_| DistillError::ConfigurationConflict)?;
    let identity = format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()));
    let mut cache = cache.lock().await;
    if let Some(sink) = cache.get(&identity) {
        return Ok(Arc::clone(sink));
    }
    let sink = if restoring {
        let config = config.clone();
        let allowlist = allowlist.clone();
        tokio::task::spawn_blocking(move || crate::persistence::restore_sink(&config, &allowlist))
            .await
            .map_err(|_| {
                DistillError::Persistence("sink recovery preparation failed".to_string())
            })??
    } else {
        build_sink(config, allowlist, auto_migrate).await?
    };
    cache.insert(identity, Arc::clone(&sink));
    Ok(sink)
}
