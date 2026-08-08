//! Defines channel routing and broker configuration.

// Rust guideline compliant 2026-02-21
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    AggregatorSpec, OrderingMode, SeverityLevel, SinkConfig, default_allowed_lateness_ms,
    default_buffer_bytes, default_mailbox_capacity, default_ttl_seconds,
};

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
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

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct RabbitMqConfig {
    pub connection_string: String,
    pub exchange_name: String,
    #[serde(default)]
    pub routing_key_prefix: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct MessageSchemaDefinition {
    pub message_type: String,
    pub version: String,
    pub descriptor_set_base64: String,
}
