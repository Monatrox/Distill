//! Classifies events into direct persistence or aggregation paths.

// Rust guideline compliant 2026-02-21
use crate::domain::{ChannelDefinition, LogEvent};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RouteDecision {
    DirectPersist,
    Aggregate,
    PersistAndAggregate,
}

pub fn classify(event: &LogEvent, definition: &ChannelDefinition) -> RouteDecision {
    if definition.aggregator.is_none() {
        RouteDecision::DirectPersist
    } else if event.severity_level >= definition.severity_threshold {
        RouteDecision::PersistAndAggregate
    } else {
        RouteDecision::Aggregate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{AggregatorSpec, OrderingMode, RabbitMqConfig, SeverityLevel, SinkConfig};
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn low_severity_without_aggregator_is_persisted() {
        let definition = definition(None);
        let event = event(SeverityLevel::Info);
        assert_eq!(classify(&event, &definition), RouteDecision::DirectPersist);
    }

    #[test]
    fn every_severity_aggregates_when_aggregator_is_configured() {
        let definition = definition(Some(aggregator()));
        assert_eq!(
            classify(&event(SeverityLevel::Info), &definition),
            RouteDecision::Aggregate
        );
        assert_eq!(
            classify(&event(SeverityLevel::Error), &definition),
            RouteDecision::PersistAndAggregate
        );
    }

    fn aggregator() -> AggregatorSpec {
        AggregatorSpec {
            function: crate::domain::AggregationFunction::Count,
            granularity: crate::domain::Granularity::Overall,
            parameter: None,
            rolling: false,
        }
    }

    fn definition(aggregator: Option<AggregatorSpec>) -> ChannelDefinition {
        ChannelDefinition {
            channel_id: "app.test".to_string(),
            severity_threshold: SeverityLevel::Error,
            rabbitmq_config: RabbitMqConfig {
                connection_string: "amqp://localhost".to_string(),
                exchange_name: "test".to_string(),
                routing_key_prefix: String::new(),
            },
            sink_id: None,
            sink_config: SinkConfig {
                driver: "postgres".to_string(),
                connection_string: "postgres://localhost".to_string(),
                headers: std::collections::HashMap::new(),
            },
            aggregator,
            message_schema: None,
            retention_hours: None,
            compression_enabled: false,
            buffer_bytes: 1_024,
            ttl_seconds: 3_600,
            mailbox_capacity: 8,
            ordering: OrderingMode::FirstComeFirstServed,
            allowed_lateness_ms: 5_000,
        }
    }

    fn event(severity_level: SeverityLevel) -> LogEvent {
        LogEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            channel_id: "app.test".to_string(),
            severity_level,
            payload: json!({"value": 1}),
            payload_bytes: None,
            metadata: std::collections::HashMap::new(),
        }
    }
}
