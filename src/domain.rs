//! Contains validated events, channel definitions, and aggregation values.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, path::PathBuf};
use uuid::Uuid;

pub type EventId = Uuid;
pub type AggregateId = Uuid;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "lowercase")]
pub enum SeverityLevel {
    Unknown = 0,
    Trace = 1,
    Debug = 2,
    Info = 3,
    Warning = 4,
    Error = 5,
    Critical = 6,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogEvent {
    #[serde(default = "Uuid::new_v4")]
    pub event_id: EventId,
    pub timestamp: DateTime<Utc>,
    pub channel_id: String,
    pub severity_level: SeverityLevel,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing)]
    pub payload_bytes: Option<Vec<u8>>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl LogEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.channel_id.is_empty() {
            return Err("channel_id must not be empty".to_string());
        }
        if !self.channel_id.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '-' | '_')
        }) {
            return Err("channel_id contains an invalid character".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChannelDefinition {
    pub channel_id: String,
    pub severity_threshold: SeverityLevel,
    pub rabbitmq_config: RabbitMqConfig,
    #[serde(default)]
    pub sink_id: Option<String>,
    #[serde(default)]
    pub sink_config: SinkConfig,
    pub aggregator: Option<AggregatorSpec>,
    #[serde(default)]
    pub message_schema: Option<MessageSchemaDefinition>,
    pub retention_hours: Option<u32>,
    #[serde(default)]
    pub compression_enabled: bool,
    #[serde(default = "default_buffer_bytes")]
    pub buffer_bytes: usize,
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,
    #[serde(default)]
    pub ordering: OrderingMode,
    #[serde(default = "default_allowed_lateness_ms")]
    pub allowed_lateness_ms: u64,
}

impl ChannelDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.channel_id.is_empty()
            || !self.channel_id.chars().all(|c| {
                c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '-' | '_')
            })
        {
            return Err(
                "channel_id must use lowercase letters, digits, dots, hyphens, or underscores"
                    .to_string(),
            );
        }
        if self.buffer_bytes == 0 || self.mailbox_capacity == 0 {
            return Err("buffer_bytes and mailbox_capacity must be positive".to_string());
        }
        if self.sink_id.is_none() {
            self.sink_config.validate()
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RabbitMqConfig {
    pub connection_string: String,
    pub exchange_name: String,
    #[serde(default)]
    pub routing_key_prefix: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageSchemaDefinition {
    pub message_type: String,
    pub version: String,
    pub descriptor_set_base64: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct SinkConfig {
    pub driver: String,
    pub connection_string: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

impl fmt::Debug for SinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SinkConfig")
            .field("driver", &self.driver)
            .field("connection_string", &"[REDACTED]")
            .field("headers", &"[REDACTED]")
            .finish()
    }
}

impl SinkConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !matches!(
            self.driver.as_str(),
            "postgres" | "mysql" | "sqlserver" | "mssql" | "http"
        ) {
            return Err(format!(
                "unsupported sink driver '{}'; supported drivers are postgres, mysql, sqlserver, mssql, and http",
                self.driver
            ));
        }
        if self.connection_string.is_empty() {
            return Err("sink connection_string must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SinkDefinition {
    pub sink_id: String,
    pub driver: String,
    #[serde(default)]
    pub connection_string: Option<String>,
    #[serde(default)]
    pub connection_string_env: Option<String>,
    #[serde(default)]
    pub connection_string_file: Option<PathBuf>,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregatorSpec {
    pub function: AggregationFunction,
    pub granularity: Granularity,
    #[serde(default)]
    pub parameter: Option<u64>,
    #[serde(default)]
    pub rolling: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AggregationFunction {
    Average,
    #[serde(rename = "average_duration_between_events")]
    AverageDurationBetweenEvents,
    Max,
    Min,
    Sum,
    Count,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    PerNEvents,
    PerTTime,
    Overall,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingMode {
    FifoRequired,
    #[default]
    FirstComeFirstServed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregatedLog {
    pub aggregate_id: AggregateId,
    pub channel_id: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub aggregator_type: AggregationFunction,
    pub aggregated_value: f64,
    pub count: u64,
    pub late_event_count: u64,
    pub rolling: bool,
    pub custom_metadata: HashMap<String, serde_json::Value>,
}

pub const DEFAULT_BUFFER_BYTES: usize = 50 * 1024 * 1024;
pub const DEFAULT_PROCESS_MEMORY_BYTES: usize = 512 * 1024 * 1024;
pub const DEFAULT_TTL_SECONDS: u64 = 3_600;
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1_024;
pub const DEFAULT_ALLOWED_LATENESS_MS: u64 = 5_000;

const fn default_buffer_bytes() -> usize {
    DEFAULT_BUFFER_BYTES
}
const fn default_ttl_seconds() -> u64 {
    DEFAULT_TTL_SECONDS
}
const fn default_mailbox_capacity() -> usize {
    DEFAULT_MAILBOX_CAPACITY
}
const fn default_allowed_lateness_ms() -> u64 {
    DEFAULT_ALLOWED_LATENESS_MS
}
