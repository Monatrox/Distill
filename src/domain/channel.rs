//! Defines channel routing and broker configuration.

// Rust guideline compliant 2026-02-21
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    AggregatorSpec, MAX_BUFFER_BYTES, MAX_CHANNEL_ID_LENGTH, MAX_MAILBOX_CAPACITY,
    MAX_RETENTION_HOURS, MAX_TTL_SECONDS, OrderingMode, SeverityLevel, SinkConfig,
    default_allowed_lateness_ms, default_buffer_bytes, default_mailbox_capacity,
    default_ttl_seconds,
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
        if self.channel_id.len() > MAX_CHANNEL_ID_LENGTH {
            return Err(format!(
                "channel_id must not exceed {MAX_CHANNEL_ID_LENGTH} characters"
            ));
        }
        if self.buffer_bytes == 0 || self.mailbox_capacity == 0 {
            return Err("buffer_bytes and mailbox_capacity must be positive".to_string());
        }
        if self.buffer_bytes > MAX_BUFFER_BYTES {
            return Err(format!("buffer_bytes must not exceed {MAX_BUFFER_BYTES}"));
        }
        if self.mailbox_capacity > MAX_MAILBOX_CAPACITY {
            return Err(format!(
                "mailbox_capacity must not exceed {MAX_MAILBOX_CAPACITY}"
            ));
        }
        if self.ttl_seconds > MAX_TTL_SECONDS {
            return Err(format!("ttl_seconds must not exceed {MAX_TTL_SECONDS}"));
        }
        if self
            .retention_hours
            .is_some_and(|hours| hours > MAX_RETENTION_HOURS)
        {
            return Err(format!(
                "retention_hours must not exceed {MAX_RETENTION_HOURS}"
            ));
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

#[cfg(test)]
mod tests {
    use super::{ChannelDefinition, OrderingMode, RabbitMqConfig, SeverityLevel, SinkConfig};

    fn definition(
        buffer_bytes: usize,
        mailbox_capacity: usize,
        ttl_seconds: u64,
    ) -> ChannelDefinition {
        ChannelDefinition {
            channel_id: "orders.production".to_string(),
            severity_threshold: SeverityLevel::Error,
            rabbitmq_config: RabbitMqConfig {
                connection_string: "amqp://rabbit".to_string(),
                exchange_name: "distill.orders.production".to_string(),
                routing_key_prefix: "orders.production.".to_string(),
            },
            sink_id: None,
            sink_config: SinkConfig {
                driver: "postgres".to_string(),
                connection_string: "postgres://logs".to_string(),
                headers: std::collections::HashMap::new(),
            },
            aggregator: None,
            message_schema: None,
            retention_hours: None,
            compression_enabled: false,
            buffer_bytes,
            ttl_seconds,
            mailbox_capacity,
            ordering: OrderingMode::default(),
            allowed_lateness_ms: 5_000,
        }
    }

    #[test]
    fn bounds_reject_excessive_resource_settings() {
        assert!(definition(0, 1_024, 3_600).validate().is_err());
        assert!(definition(1_024, 0, 3_600).validate().is_err());
        assert!(definition(1_024, 1_024, 0).validate().is_ok());
        assert!(definition(1_024, 1_024, 3_600).validate().is_ok());
        assert!(
            definition(super::MAX_BUFFER_BYTES + 1, 1_024, 3_600)
                .validate()
                .is_err()
        );
        assert!(
            definition(1_024, super::MAX_MAILBOX_CAPACITY + 1, 3_600)
                .validate()
                .is_err()
        );
        assert!(
            definition(1_024, 1_024, super::MAX_TTL_SECONDS + 1)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn bounds_reject_excessive_channel_identifier_and_retention() {
        let mut long = definition(1_024, 1_024, 3_600);
        long.channel_id = "a".repeat(super::MAX_CHANNEL_ID_LENGTH + 1);
        assert!(long.validate().is_err());

        let mut retention = definition(1_024, 1_024, 3_600);
        retention.retention_hours = Some(super::MAX_RETENTION_HOURS + 1);
        assert!(retention.validate().is_err());
        retention.retention_hours = Some(super::MAX_RETENTION_HOURS);
        assert!(retention.validate().is_ok());
    }
}
