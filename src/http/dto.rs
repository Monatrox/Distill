//! Contains HTTP endpoint handlers.

// Rust guideline compliant 2026-02-21
use super::{
    AggregatorSpec, ChannelDefinition, ChannelLifecycle, ChannelRecord, DistillError, ErrorCode,
    Granularity, HashMap, LogEvent, Map, MessageSchemaDefinition, OrderingMode, RabbitMqConfig,
    SeverityLevel, SinkConfig, Utc, Uuid, Value,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, ToSchema)]
pub(super) struct CreateChannelRequest {
    pub(super) name: String,
    pub(super) preset: ChannelPreset,
    pub(super) sink_id: Option<String>,
    pub(super) severity_threshold: Option<SeverityLevel>,
    pub(super) aggregator: Option<Value>,
    pub(super) rabbitmq_config: Option<RabbitMqConfig>,
    pub(super) sink_config: Option<SinkConfig>,
    pub(super) message_schema: Option<MessageSchemaDefinition>,
    pub(super) retention_hours: Option<u32>,
    pub(super) compression_enabled: Option<bool>,
    pub(super) buffer_bytes: Option<usize>,
    pub(super) ttl_seconds: Option<u64>,
    pub(super) mailbox_capacity: Option<usize>,
    pub(super) ordering: Option<OrderingMode>,
    pub(super) allowed_lateness_ms: Option<u64>,
    #[schema(ignore = true)]
    pub(super) aggregator_present: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateChannelRequestWire {
    #[serde(alias = "channel_id")]
    pub(super) name: String,
    #[serde(default)]
    pub(super) preset: ChannelPreset,
    #[serde(default)]
    pub(super) sink_id: Option<String>,
    #[serde(default)]
    pub(super) severity_threshold: Option<SeverityLevel>,
    #[serde(default)]
    pub(super) aggregator: Option<Value>,
    #[serde(default)]
    pub(super) rabbitmq_config: Option<RabbitMqConfig>,
    #[serde(default)]
    pub(super) sink_config: Option<SinkConfig>,
    #[serde(default)]
    pub(super) message_schema: Option<MessageSchemaDefinition>,
    #[serde(default)]
    pub(super) retention_hours: Option<u32>,
    #[serde(default)]
    pub(super) compression_enabled: Option<bool>,
    #[serde(default)]
    pub(super) buffer_bytes: Option<usize>,
    #[serde(default)]
    pub(super) ttl_seconds: Option<u64>,
    #[serde(default)]
    pub(super) mailbox_capacity: Option<usize>,
    #[serde(default)]
    pub(super) ordering: Option<OrderingMode>,
    #[serde(default)]
    pub(super) allowed_lateness_ms: Option<u64>,
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
    pub(super) fn to_definition(
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

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum ChannelPreset {
    #[default]
    RawEvents,
    ErrorsOnly,
    Metrics,
    Audit,
    Count,
}

impl ChannelPreset {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::RawEvents => "raw_events",
            Self::ErrorsOnly => "errors_only",
            Self::Metrics => "metrics",
            Self::Audit => "audit",
            Self::Count => "count",
        }
    }

    pub(super) fn defaults(self) -> PresetDefaults {
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
pub(super) struct PresetDefaults {
    pub(super) severity_threshold: SeverityLevel,
    pub(super) aggregator: Option<AggregatorSpec>,
    pub(super) compression_enabled: bool,
    pub(super) buffer_bytes: usize,
    pub(super) ttl_seconds: u64,
    pub(super) mailbox_capacity: usize,
    pub(super) ordering: OrderingMode,
    pub(super) allowed_lateness_ms: u64,
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

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct SimpleEventRequest {
    #[serde(default)]
    pub(super) event_id: Option<Uuid>,
    #[serde(default)]
    pub(super) timestamp: Option<chrono::DateTime<Utc>>,
    #[serde(default = "default_info", alias = "severity")]
    pub(super) severity_level: SeverityLevel,
    #[serde(default)]
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) payload: Option<Value>,
    #[serde(default)]
    pub(super) attributes: HashMap<String, Value>,
    #[serde(default)]
    pub(super) metadata: HashMap<String, String>,
}

impl SimpleEventRequest {
    pub(super) fn into_event(self, channel_id: String) -> LogEvent {
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

#[derive(Debug, Deserialize, ToSchema)]
pub(super) struct CloneChannelRequest {
    pub(super) channel_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct IngestResponse {
    pub(super) accepted: usize,
    pub(super) rejected: usize,
    #[schema(value_type = String, format = "uuid")]
    pub(super) request_id: Uuid,
    pub(super) durability: &'static str,
    pub(super) errors: Vec<IngestItemError>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct IngestItemError {
    pub(super) index: usize,
    pub(super) error_code: ErrorCode,
    pub(super) message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ChannelConfigResponse {
    pub(super) channel_id: String,
    pub(super) active: bool,
    pub(super) lifecycle: ChannelLifecycle,
    pub(super) effective: PublicChannelConfig,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct PublicChannelConfig {
    pub(super) channel_id: String,
    pub(super) sink_id: Option<String>,
    pub(super) severity_threshold: SeverityLevel,
    pub(super) rabbitmq_exchange: String,
    pub(super) routing_key_prefix: String,
    pub(super) sink_driver: String,
    pub(super) aggregator: Option<AggregatorSpec>,
    pub(super) retention_hours: Option<u32>,
    pub(super) compression_enabled: bool,
    pub(super) buffer_bytes: usize,
    pub(super) ttl_seconds: u64,
    pub(super) mailbox_capacity: usize,
    pub(super) ordering: OrderingMode,
    pub(super) allowed_lateness_ms: u64,
    pub(super) schema_configured: bool,
}

impl ChannelConfigResponse {
    pub(super) fn from_record(record: &ChannelRecord) -> Self {
        Self {
            channel_id: record.definition.channel_id.clone(),
            active: record.lifecycle == ChannelLifecycle::Active,
            lifecycle: record.lifecycle,
            effective: PublicChannelConfig::from_definition(&record.definition),
        }
    }
}

impl PublicChannelConfig {
    pub(super) fn from_definition(definition: &ChannelDefinition) -> Self {
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

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ChannelValidationResponse {
    pub(super) valid: bool,
    pub(super) channel_id: String,
    pub(super) preset: String,
    pub(super) warnings: Vec<String>,
    pub(super) effective: PublicChannelConfig,
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct EventTestResponse {
    pub(super) valid: bool,
    pub(super) channel_id: String,
    pub(super) route: &'static str,
    pub(super) severity_threshold: SeverityLevel,
    pub(super) schema_configured: bool,
}
