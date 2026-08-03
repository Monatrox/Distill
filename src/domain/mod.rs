//! Defines validated domain events, channel configuration, sinks, and aggregates.

// Rust guideline compliant 2026-02-21
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

mod aggregation;
mod channel;
mod sink;

pub(crate) use aggregation::{
    AggregatedLog, AggregationFunction, AggregatorSpec, Granularity, OrderingMode,
};
pub(crate) use channel::{ChannelDefinition, MessageSchemaDefinition, RabbitMqConfig};
pub(crate) use sink::{SinkConfig, SinkDefinition};

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

pub const DEFAULT_BUFFER_BYTES: usize = 50 * 1024 * 1024;
pub const DEFAULT_PROCESS_MEMORY_BYTES: usize = 512 * 1024 * 1024;
pub const DEFAULT_TTL_SECONDS: u64 = 3_600;
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1_024;
pub const DEFAULT_ALLOWED_LATENESS_MS: u64 = 5_000;

pub(crate) const fn default_buffer_bytes() -> usize {
    DEFAULT_BUFFER_BYTES
}
pub(crate) const fn default_ttl_seconds() -> u64 {
    DEFAULT_TTL_SECONDS
}
pub(crate) const fn default_mailbox_capacity() -> usize {
    DEFAULT_MAILBOX_CAPACITY
}
pub(crate) const fn default_allowed_lateness_ms() -> u64 {
    DEFAULT_ALLOWED_LATENESS_MS
}
