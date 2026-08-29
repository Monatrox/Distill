//! Defines validated domain events, channel configuration, sinks, and aggregates.

// Rust guideline compliant 2026-02-21
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd, ToSchema)]
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

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct LogEvent {
    #[serde(default = "Uuid::new_v4")]
    #[schema(value_type = String, format = "uuid")]
    pub event_id: EventId,
    pub timestamp: DateTime<Utc>,
    pub channel_id: String,
    pub severity_level: SeverityLevel,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing)]
    #[schema(ignore = true)]
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

/// Maximum per-channel buffer size in bytes.
///
/// Half the default process memory budget so a single channel cannot crowd out
/// the other channels' buffers.
pub const MAX_BUFFER_BYTES: usize = 256 * 1024 * 1024;
/// Maximum per-channel actor mailbox capacity.
///
/// The actor mailbox is an mpsc channel whose capacity is reserved on spawn;
/// this cap bounds that allocation while staying far above the default.
pub const MAX_MAILBOX_CAPACITY: usize = 65_536;
/// Maximum buffered-event age before a forced flush, in seconds.
///
/// Bound the TTL so a misconfiguration cannot suppress flushes indefinitely.
pub const MAX_TTL_SECONDS: u64 = 86_400;
/// Maximum channel identifier length.
///
/// Channel IDs are embedded in `RabbitMQ` exchange and routing-key names; the
/// bound prevents oversized broker topology names.
pub const MAX_CHANNEL_ID_LENGTH: usize = 128;

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
